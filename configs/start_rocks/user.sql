-- StarRocks target table for merged sharded tables (user00-user99 -> user)
-- Primary key: (__table, id)
-- Sequence column: __source_ts_ms, used to prevent late old snapshots from overwriting newer CDC events
CREATE TABLE user (
    __table varchar(128) NOT NULL COMMENT "physical source table name, e.g. user06",
    id bigint NOT NULL COMMENT "business primary key",
    username varchar(1024) NULL COMMENT "username",
    sex tinyint NULL COMMENT "gender",
    age int NULL COMMENT "age",
    create_time datetime NULL COMMENT "row create time from source",
    update_time datetime NULL COMMENT "row update time from source",
    __source_ts_ms bigint NOT NULL COMMENT "debezium source timestamp (ms), used as sequence"
)
ENGINE=OLAP
PRIMARY KEY(__table, id)
DISTRIBUTED BY HASH(__table, id)
ORDER BY(__table, id, __source_ts_ms)
PROPERTIES (
    -- compression and index settings for local demo / small production setup
    "compression" = "LZ4",
    "enable_persistent_index" = "true",
    "fast_schema_evolution" = "true",
    "replicated_storage" = "true",
    "replication_num" = "1",
    -- critical: compare and keep latest version by __source_ts_ms on key conflicts
    "function_column.sequence_col" = "__source_ts_ms"
);