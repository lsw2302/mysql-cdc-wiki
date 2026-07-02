-- Routine Load 从 Kafka 主题 mysql-user.test.user 持续消费 Debezium 扁平 JSON
-- Debezium 操作类型映射：
--   c/u/r -> __op = 0（插入或更新）
--   d     -> __op = 1（删除）
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

    -- StarRocks 删除语义列
    __op = if(__op_raw = 'd', 1, 0),
    -- 仅在异常记录时回退为 0；正常记录应携带 __source_ts_ms
    __source_ts_ms = ifnull(__source_ts_ms_raw, 0),
    -- 将 Debezium 毫秒时间戳转换为 DATETIME
    create_time = if(create_time_ms is null, null, from_unixtime(create_time_ms / 1000)),
    update_time = if(update_time_ms is null, null, from_unixtime(update_time_ms / 1000))
)
PROPERTIES
(
    "format" = "json",
    -- 注意：jsonpaths 顺序必须与上方原始输入列顺序一致
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
    -- 该 Routine Load 使用的专用消费组
    "property.group.id" = "starrocks_user_cdc",
    -- 首次运行从最早位点消费；仅需增量时可改为 OFFSET_END
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);