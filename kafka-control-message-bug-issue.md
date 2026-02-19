## Describe the bug

Source backfill can never complete when consuming from a transactional Kafka topic whose last record in a partition is a transaction control record (commit/abort marker).

RisingWave's Kafka consumer is configured with `isolation.level=read_committed` ([`src/connector/src/source/kafka/mod.rs:224`](https://github.com/risingwavelabs/risingwave/blob/main/src/connector/src/source/kafka/mod.rs#L224)). Under this isolation level, the consumer never delivers [Kafka control records](https://kafka.apache.org/documentation/#controlrecord) (transaction commit/abort markers) to the application — they are silently skipped by librdkafka. However, these control records still occupy offsets in the partition log and are included in the broker-reported `high_watermark`.

The source backfill executor uses `high_watermark - 1` as the `target_offset` — the offset it must reach before considering backfill complete ([`src/connector/src/source/kafka/source/reader.rs:135-141`](https://github.com/risingwavelabs/risingwave/blob/main/src/connector/src/source/kafka/source/reader.rs#L135-L141)):

```rust
backfill_info.insert(
    split.id(),
    BackfillInfo::HasDataToBackfill {
        latest_offset: (high - 1).to_string(),
    },
);
```

When the last record in a partition is a control record at offset `N`:
- `high_watermark` = `N + 1`
- `target_offset` = `N`
- The last *consumable* message has an offset `< N`
- Backfill compares the last consumed offset against `target_offset` and will never find it `>=` ([`src/stream/src/executor/source/source_backfill_executor.rs:234-235`](https://github.com/risingwavelabs/risingwave/blob/main/src/stream/src/executor/source/source_backfill_executor.rs#L234-L235))
- Backfill hangs indefinitely

This is a common scenario in practice. Any Kafka topic written to by a transactional producer (e.g. Kafka Streams, Kafka Connect with exactly-once, Flink, Debezium) will have commit markers after every transaction. If the most recent record on any partition is such a marker, the `CREATE SOURCE` (or `CREATE MATERIALIZED VIEW` on a source) DDL will block forever.

## Error message/log

There is no error message. The DDL blocks indefinitely and the backfill state remains in `Backfilling` for the affected partition(s). The only observable symptom is that the source never transitions to a ready state.

In logs, you would see the watermarks fetched normally:
```
fetch kafka watermarks: low: 0, high: <N+1>, split: ...
```
But the backfill offset for the affected split never reaches `N` because offset `N` is a control record that is never delivered.

## To Reproduce

1. Create a Kafka topic and produce messages using a transactional producer:

```python
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'transactional.id': 'test-txn-producer',
})
producer.init_transactions()
producer.begin_transaction()
producer.produce('txn-topic', key=b'key', value=b'value', partition=0)
producer.commit_transaction()
# At this point, the partition log has: [data@0, control@1]
# high_watermark = 2, but offset 1 (the commit marker) is never delivered to read_committed consumers
```

2. Create a source in RisingWave that consumes from this topic:

```sql
CREATE SOURCE txn_source (
    payload bytea
) WITH (
    connector = 'kafka',
    topic = 'txn-topic',
    properties.bootstrap.server = 'localhost:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE BYTES;
```

Note: `ENCODE BYTES` only accepts a single `BYTEA` column.

3. Create a materialized view (or any streaming job that triggers source backfill):

```sql
CREATE MATERIALIZED VIEW txn_mv AS SELECT * FROM txn_source;
```

4. The DDL will hang indefinitely. The backfill `target_offset` is `1` (the control record), but the consumer only delivers offset `0` (the data record). The backfill condition `offset >= target_offset` is never satisfied.

## Expected behavior

Source backfill should complete successfully even when the last record(s) in a Kafka partition are transaction control records. The `target_offset` calculation should account for the fact that `read_committed` consumers will never see control records, so using `high_watermark - 1` as an inclusive target is incorrect for transactional topics.

## How did you deploy RisingWave?

This affects all deployment methods. The bug is in the core source backfill logic.

## The version of RisingWave

All versions since the introduction of the `BackfillInfo`-based target offset mechanism (post [#18299](https://github.com/risingwavelabs/risingwave/issues/18299)).

## Additional context

### Root cause

The root cause is in two places:

1. **Target offset calculation** in [`src/connector/src/source/kafka/source/reader.rs:139`](https://github.com/risingwavelabs/risingwave/blob/main/src/connector/src/source/kafka/source/reader.rs#L139): `latest_offset: (high - 1).to_string()` uses the raw `high_watermark` from `fetch_watermarks()`, which includes control record offsets.

2. **Backfill completion check** in [`src/stream/src/executor/source/source_backfill_executor.rs:234-235`](https://github.com/risingwavelabs/risingwave/blob/main/src/stream/src/executor/source/source_backfill_executor.rs#L234-L235): compares the last *delivered* message offset against `target_offset`. Since control records are never delivered, this comparison can never succeed when the target points to a control record.

The consumer's `isolation.level` is hardcoded to `read_committed` at [`src/connector/src/source/kafka/mod.rs:224`](https://github.com/risingwavelabs/risingwave/blob/main/src/connector/src/source/kafka/mod.rs#L224).

### How Materialize avoids this

For comparison, [Materialize](https://github.com/MaterializeInc/materialize) avoids this class of bug by using the **consumer position** (`consumer.position()`) rather than the last delivered message offset to track progress ([`src/storage/src/source/kafka.rs` ~L1076-1086](https://github.com/MaterializeInc/materialize/blob/main/src/storage/src/source/kafka.rs#L1076-L1086)):

```rust
let positions = reader.consumer.position().unwrap();
let topic_positions = positions.elements_for_topic(&reader.topic_name);
for position in topic_positions {
    if let Offset::Offset(offset) = position.offset() {
        snapshot_staged += offset.try_into().unwrap_or(0u64);
        // ...
    }
}
```

The key distinction: `consumer.position()` returns the offset of the *next* message to be fetched — which advances past control records even though they are not delivered to the application. This means Materialize's progress tracking correctly accounts for transaction markers without any special-case handling.

### Possible fixes

1. **Use `consumer.position()` instead of last-delivered offset**: After consuming all available messages, query the consumer's position for each partition. This position reflects offsets skipped by `read_committed` filtering (including control records). Use this as the basis for backfill completion.

2. **Use a >= comparison with tolerance**: When the consumed offset is within a small delta of `target_offset` and no more messages are available, treat backfill as complete. This is fragile and not recommended.

3. **Query the LSO (Last Stable Offset) instead of high watermark**: The LSO is the offset up to which `read_committed` consumers can read. However, the LSO still includes control record offsets, so this alone does not fully solve the problem — it only helps with open (uncommitted) transactions.

### Related issues

- [#18299](https://github.com/risingwavelabs/risingwave/issues/18299) — "Let source backfill finish when there's no data from upstream" — introduced the `target_offset = high_watermark - 1` mechanism that contains this bug.
