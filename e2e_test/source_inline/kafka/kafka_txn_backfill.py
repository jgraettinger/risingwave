#!/usr/bin/env python3
"""
Reproduction script for Kafka transactional control message backfill bug.

When a Kafka topic's last record in a partition is a transaction control record
(commit/abort marker), RisingWave's source backfill hangs indefinitely because:

  - target_offset = high_watermark - 1, which points to the control record
  - read_committed consumers never deliver control records
  - The backfill condition (offset >= target_offset) is never satisfied

Usage:
  python3 e2e_test/source_inline/kafka/kafka_txn_backfill.py \
    --topic txn-backfill-test \
    --db-name dev

Prerequisites:
  - Kafka broker running (address from RISEDEV_KAFKA_BOOTSTRAP_SERVERS env var)
  - RisingWave running (connects via RISEDEV_RW_FRONTEND_LISTEN_ADDRESS/PORT)
  - pip install confluent-kafka psycopg2-binary
"""

import argparse
import json
import os
import sys
import threading
import time

from confluent_kafka import Producer, Consumer, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic
import psycopg2


def get_rw_conn(db_name):
    host = os.environ.get("RISEDEV_RW_FRONTEND_LISTEN_ADDRESS", "localhost")
    port = os.environ.get("RISEDEV_RW_FRONTEND_PORT", "4566")
    return psycopg2.connect(
        host=host, port=int(port), dbname=db_name, user="root", options="-c idle_in_transaction_session_timeout=0"
    )


def create_topic(broker, topic, num_partitions=1):
    admin = AdminClient({"bootstrap.servers": broker})
    # Delete if exists
    try:
        admin.delete_topics([topic])
        time.sleep(2)
    except Exception:
        pass
    fs = admin.create_topics([NewTopic(topic, num_partitions=num_partitions, replication_factor=1)])
    for t, f in fs.items():
        try:
            f.result()
            print(f"Created topic {t}")
        except Exception as e:
            print(f"Topic {t} creation: {e}")
    time.sleep(1)


def produce_transactional(broker, topic):
    """Produce a single message inside a transaction.

    After this, the partition log is:
      offset 0: data record
      offset 1: transaction commit marker (control record)
      high_watermark = 2
    """
    producer = Producer({
        "bootstrap.servers": broker,
        "transactional.id": f"txn-producer-{topic}",
    })
    producer.init_transactions()
    producer.begin_transaction()
    producer.produce(topic, key=b"k1", value=json.dumps({"id": 1, "val": "hello"}).encode(), partition=0)
    producer.flush()
    producer.commit_transaction()
    print("Transactional message produced and committed.")


def verify_offsets(broker, topic):
    """Verify the offset layout confirms the bug scenario."""
    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": f"verify-{topic}-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "enable.auto.commit": False,
    })

    tp = TopicPartition(topic, 0)
    low, high = consumer.get_watermark_offsets(tp, timeout=10.0)
    print(f"Watermarks: low={low}, high={high}")
    print(f"  target_offset (high - 1) = {high - 1}")

    consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
    delivered = []
    deadline = time.time() + 10
    while time.time() < deadline:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            if delivered:
                break
            continue
        if msg.error():
            continue
        delivered.append(msg.offset())

    consumer.close()

    max_offset = max(delivered) if delivered else None
    print(f"Delivered offsets: {delivered}, max={max_offset}")

    if max_offset is not None and max_offset < high - 1:
        print("CONFIRMED: control record gap exists — backfill will hang.")
        return True
    else:
        print("WARNING: gap not detected — test may not reproduce the bug.")
        return False


def main():
    parser = argparse.ArgumentParser(description="Reproduce Kafka txn control message backfill bug")
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument("--db-name", required=True, help="RisingWave database name")
    parser.add_argument("--timeout", type=int, default=30, help="Seconds to wait for MV creation before declaring hang")
    args = parser.parse_args()

    broker = os.environ.get("RISEDEV_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"Using Kafka broker: {broker}")

    # Step 1: Create topic and produce transactional data
    create_topic(broker, args.topic)
    produce_transactional(broker, args.topic)

    # Step 2: Verify offset layout
    if not verify_offsets(broker, args.topic):
        sys.exit(1)

    # Step 3: Create source in RisingWave
    conn = get_rw_conn(args.db_name)
    conn.autocommit = True
    cur = conn.cursor()

    source_name = f"txn_src_{args.topic.replace('-', '_')}"
    mv_name = f"txn_mv_{args.topic.replace('-', '_')}"

    # Clean up from previous runs
    cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name} CASCADE;")
    cur.execute(f"DROP SOURCE IF EXISTS {source_name} CASCADE;")

    cur.execute(f"""
        CREATE SOURCE {source_name} (payload bytea)
        WITH (
            connector = 'kafka',
            topic = '{args.topic}',
            properties.bootstrap.server = '{broker}',
            scan.startup.mode = 'earliest'
        ) FORMAT PLAIN ENCODE BYTES;
    """)
    print(f"Created source: {source_name}")

    # Step 4: Try to create MV — this should hang
    print(f"Creating materialized view (timeout={args.timeout}s)...")
    print("If the bug exists, this will hang until timeout.")

    start = time.time()
    timed_out = False

    # Use a background thread to cancel the connection after the timeout,
    # since RisingWave's statement_timeout does not apply to DDL.
    def cancel_after_timeout(conn_to_cancel, timeout_sec):
        time.sleep(timeout_sec)
        try:
            conn_to_cancel.cancel()
        except Exception:
            pass

    timer = threading.Thread(target=cancel_after_timeout, args=(conn, args.timeout), daemon=True)
    timer.start()

    try:
        cur.execute(f"CREATE MATERIALIZED VIEW {mv_name} AS SELECT * FROM {source_name};")
        elapsed = time.time() - start
        print(f"MV created in {elapsed:.1f}s — bug NOT reproduced.")
    except psycopg2.errors.QueryCanceled:
        elapsed = time.time() - start
        timed_out = True
        print(f"MV creation timed out after {elapsed:.1f}s — BUG REPRODUCED!")
        print("The DDL hung because backfill target_offset points to a control record")
        print("that read_committed consumers never deliver.")
    except Exception as e:
        elapsed = time.time() - start
        print(f"MV creation failed after {elapsed:.1f}s with: {e}")
        timed_out = "cancel" in str(e).lower() or elapsed >= args.timeout - 1

    # Cleanup
    conn2 = get_rw_conn(args.db_name)
    conn2.autocommit = True
    cur2 = conn2.cursor()
    cur2.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name} CASCADE;")
    cur2.execute(f"DROP SOURCE IF EXISTS {source_name} CASCADE;")
    cur2.close()
    conn2.close()

    cur.close()
    conn.close()

    if timed_out:
        print("\n=== BUG CONFIRMED ===")
        print("Source backfill hangs when last record is a transaction control message.")
        sys.exit(0)
    else:
        print("\n=== BUG NOT REPRODUCED ===")
        sys.exit(1)


if __name__ == "__main__":
    main()
