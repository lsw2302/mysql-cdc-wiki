-- StarRocks 目标表：将分表数据（user00-user99）合并到逻辑表 user
-- 主键：(__table, id)
-- 序列列：__source_ts_ms，用于避免晚到旧快照覆盖较新的 CDC 数据
CREATE TABLE user (
    __table varchar(128) NOT NULL COMMENT "源物理分表名，例如 user06",
    id bigint NOT NULL COMMENT "业务主键",
    username varchar(1024) NULL COMMENT "用户名",
    sex tinyint NULL COMMENT "性别",
    age int NULL COMMENT "年龄",
    create_time datetime NULL COMMENT "源表创建时间",
    update_time datetime NULL COMMENT "源表更新时间",
    __source_ts_ms bigint NOT NULL COMMENT "Debezium 源事件时间戳（毫秒），用于序列比较"
)
ENGINE=OLAP
PRIMARY KEY(__table, id)
DISTRIBUTED BY HASH(__table, id)
ORDER BY(__table, id, __source_ts_ms)
PROPERTIES (
    -- 压缩与索引配置，适用于本地演示或小规模部署
    "compression" = "LZ4",
    "enable_persistent_index" = "true",
    "fast_schema_evolution" = "true",
    "replicated_storage" = "true",
    "replication_num" = "1",
    -- 关键配置：主键冲突时按 __source_ts_ms 选取较新版本
    "function_column.sequence_col" = "__source_ts_ms"
);