# start_rocks 配置说明

本目录包含两个可执行 SQL 文件：

- user.sql：目标表建表语句
- cdc_user_routine_load.sql：Kafka 到 StarRocks 的持续导入任务

## user.sql 说明

- 表模型：PRIMARY KEY
- 主键：`__table` + `id`
- 作用：把分表（例如 user00-user99）合并到一张逻辑表

关键点：

- `__table`
  - 源物理分表名，来自 Debezium 元字段 `table`
- `id`
  - 业务主键
- `__source_ts_ms`
  - 版本时间戳，来自 Debezium 元字段 `source.ts_ms`
- `function_column.sequence_col = __source_ts_ms`
  - 同主键冲突时按版本时间戳胜出，避免旧数据晚到覆盖新数据

## cdc_user_routine_load.sql 说明

- 数据源 topic：mysql-user.test.user
- 数据格式：JSON
- 路径规则：使用扁平 JSON 路径（如 `$.id`）

字段映射逻辑：

- 原始输入列
  - `create_time_ms` / `update_time_ms`：毫秒时间戳
  - `__op_raw`：Debezium 事件类型（c/u/d/r）
  - `__source_ts_ms_raw`：版本时间戳

- 派生目标列
  - `__op = if(__op_raw = 'd', 1, 0)`
    - 1 表示删除
    - 0 表示 upsert
  - `__source_ts_ms = ifnull(__source_ts_ms_raw, 0)`
  - `create_time` / `update_time`：由毫秒时间戳转换成 DATETIME

消费参数说明：

- `property.group.id = starrocks_user_cdc`
  - Routine Load 对应 Kafka 消费组
- `property.kafka_default_offsets = OFFSET_BEGINNING`
  - 首次任务从最早位点开始读
  - 如果只想接入新数据，可改成 `OFFSET_END`

## 建议执行顺序

1. 先执行 user.sql 建表
2. 再执行 cdc_user_routine_load.sql 创建导入任务
3. 使用 SHOW ROUTINE LOAD 和 SHOW ROUTINE LOAD TASK 检查状态与错误
