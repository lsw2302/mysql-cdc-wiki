# kafka-connect 配置说明

文件: mysql-connect.json

说明: JSON 标准不支持注释。为了保证该文件可直接通过 Kafka Connect REST API 提交，不在 JSON 内写注释，解释统一放在这里。

## 核心字段解释

- connector.class
  - 使用 Debezium MySQL Connector。

- database.hostname / database.port / database.user / database.password
  - MySQL 连接信息。当前示例使用 host.docker.internal，适用于 MySQL 在宿主机、Connect 在容器内。

- database.server.id
  - Debezium 作为 MySQL 复制客户端的 server id，需与 MySQL 集群中的其他 server id 不冲突。

- topic.prefix
  - Debezium 输出 topic 前缀。最终 topic 由前缀 + 库表名组合。

- database.include.list
  - 只采集指定库，当前为 test。

- table.include.list
  - 只采集匹配正则的表，当前为 test.user00-user99。

- snapshot.mode = initial
  - 首次启动先做全量快照，再持续消费 binlog 增量。

- snapshot.locking.mode = none
  - 降低快照加锁影响，适合在线业务场景。

- include.schema.changes = false
  - 不将 DDL 变更事件写入业务 topic。

- schema.history.internal.kafka.\*
  - Debezium 内部 schema history 存储到 Kafka 的配置。

- heartbeat.interval.ms
  - 心跳间隔，便于监控延迟和链路健康状态。

## Converter 相关

- key.converter / value.converter = JsonConverter
  - 使用 JSON 编码。

- key.converter.schemas.enable = false
- value.converter.schemas.enable = false
  - 输出扁平 JSON，不带 schema/payload 外壳，便于 StarRocks Routine Load 直接使用 $.id 这种路径。

## SMT 相关

- transforms = Reroute,unwrap
  - 先路由，再展开事件。

- transforms.Reroute.\*
  - 把多个分表 topic 归并到逻辑 topic。
  - key.enforce.uniqueness = true: 为不同物理表键增加唯一性保护，避免分表主键碰撞。

- transforms.unwrap.type = ExtractNewRecordState
  - 去掉 Debezium envelope，保留行级数据。

- transforms.unwrap.drop.tombstones = true
  - 丢弃 tombstone 记录，减少下游处理复杂度。

- transforms.unwrap.delete.handling.mode = rewrite
  - 删除事件改写为普通 JSON 记录，并保留 op 字段供下游映射删除。

- transforms.unwrap.add.fields
  - 追加元字段:
    - table: 源物理表名
    - op: 事件类型(c/u/d/r)
    - source.ts_ms: 源事件时间戳(ms)
    - source.file/source.pos/source.row: binlog 位点信息

## 与 StarRocks 配合建议

- StarRocks 主键表使用字段 \_\_table 和 id 组成主键。
- 使用字段 \_\_source_ts_ms 作为 sequence 列，避免旧数据晚到覆盖新数据。
- Routine Load 需把事件类型字段 \_\_op 中的 d 映射成 StarRocks 删除标记 1。
