-- Routine Load consumes Debezium flattened JSON from Kafka topic mysql-user.test.user
-- Debezium op field mapping:
--   c/u/r -> __op = 0 (upsert)
--   d     -> __op = 1 (delete)
CREATE ROUTINE LOAD cdc_demo.user_cdc_load
ON user
COLUMNS
(
    id,
    username,
    sex,
    age,
    create_time_ms,
    update_time_ms,
    __table,
    __op_raw,
    __source_ts_ms_raw,

    -- StarRocks delete semantic column
    __op = if(__op_raw = 'd', 1, 0),
    -- fallback to 0 only for malformed records; normal records should have __source_ts_ms
    __source_ts_ms = ifnull(__source_ts_ms_raw, 0),
    -- convert Debezium millisecond timestamps to DATETIME
    create_time = if(create_time_ms is null, null, from_unixtime(create_time_ms / 1000)),
    update_time = if(update_time_ms is null, null, from_unixtime(update_time_ms / 1000))
)
PROPERTIES
(
    "format" = "json",
    -- Note: jsonpaths order must match raw input columns order above
    "jsonpaths" = "[\"$.id\",\"$.username\",\"$.sex\",\"$.age\",\"$.create_time\",\"$.update_time\",\"$.__table\",\"$.__op\",\"$.__source_ts_ms\"]",
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "10",
    "max_error_number" = "0",
    "max_filter_ratio" = "1.0",
    "strict_mode" = "true",
    "log_rejected_record_num" = "100"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "mysql-user.test.user",
    -- dedicated consumer group for this routine load
    "property.group.id" = "starrocks_user_cdc",
    -- first run consumes from beginning; change to OFFSET_END in incremental-only scenarios
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);