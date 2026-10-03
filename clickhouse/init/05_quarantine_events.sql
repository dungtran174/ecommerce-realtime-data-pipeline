-- The RawBLOB consumer retains every CDC payload independently of the typed
-- consumers. Bad JSON or an invalid Debezium envelope is copied here for
-- inspection while the typed consumers skip parser errors (up to 100/block).
CREATE TABLE IF NOT EXISTS bronze.quarantine_events (
    topic LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset UInt64,
    payload String,
    reason LowCardinality(String),
    detected_at DateTime64(3)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(detected_at)
ORDER BY (topic, kafka_partition, kafka_offset)
TTL toDateTime(detected_at) + INTERVAL 30 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_quarantine_events
TO bronze.quarantine_events AS
SELECT topic, kafka_partition, kafka_offset, payload,
       multiIf(NOT isValidJSON(payload), 'invalid_json',
               NOT JSONHas(payload, '__op'), 'missing_op',
               JSONExtractString(payload, '__op') NOT IN ('c', 'u', 'd', 'r'), 'invalid_op',
               NOT JSONHas(payload, '__table'), 'missing_table',
               NOT JSONHas(payload, '__source_ts_ms'), 'missing_source_ts',
               '') AS reason,
       now64(3) AS detected_at
FROM bronze.raw_events
WHERE reason != '';
