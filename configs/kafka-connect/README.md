# kafka-connect 配置说明

文件：mysql-connect.json

说明：JSON 标准不支持注释。为了保证该文件可直接通过 Kafka Connect REST API 提交，JSON 文件内不写注释，统一在本说明中解释。

## 核心字段解释

- connector.class
  - 指定使用 Debezium 的 MySQL 连接器。

- database.hostname / database.port / database.user / database.password
  - MySQL 连接信息。当前示例使用 host.docker.internal，适用于 MySQL 在宿主机、Connect 在容器内的场景。

- database.server.id
  - Debezium 作为 MySQL 复制客户端的 server id，必须与 MySQL 集群中的其他 server id 不冲突。

- topic.prefix
  - Debezium 输出 topic 前缀。最终 topic 由前缀与库表名组合而成。

- database.include.list
  - 只采集指定数据库，当前为 test。

- table.include.list
  - 只采集匹配正则的表，当前为 test.user00-user99。

- snapshot.mode = initial
  - 首次启动先做全量快照，再持续消费 binlog 增量。

- snapshot.locking.mode = none
  - 降低快照加锁影响，适合在线业务。

- include.schema.changes = false
  - 不把 DDL 变更事件写入业务 topic。

- schema.history.internal.kafka.\*
  - Debezium 内部 schema 历史信息写入 Kafka 的配置。

- heartbeat.interval.ms
  - 心跳间隔，用于监控延迟和链路健康状态。

## Converter 相关

- key.converter / value.converter = JsonConverter
  - 使用 JSON 编码格式。

- key.converter.schemas.enable = false
- value.converter.schemas.enable = false
  - 输出扁平 JSON，不带 schema/payload 外层结构，便于 StarRocks Routine Load 直接使用 $.id 这类路径。

## SMT 相关

- transforms = Reroute,unwrap
  - 先做路由，再展开事件。

- transforms.Reroute.\*
  - 把多个分表 topic 归并到一个逻辑 topic。
  - key.enforce.uniqueness = true：为不同物理表键增加唯一性保护，避免分表主键冲突。

- transforms.unwrap.type = ExtractNewRecordState
  - 去掉 Debezium envelope 外层，保留行级数据。

- transforms.unwrap.drop.tombstones = true
  - 丢弃 tombstone 记录，减少下游处理复杂度和无效数据量。

- transforms.unwrap.delete.handling.mode = rewrite
  - 将删除事件改写为普通 JSON 记录，并保留 op 字段供下游映射删除。

- transforms.unwrap.add.fields
  - 追加元字段：
  - table：源物理表名
  - op：事件类型(c/u/d/r)
  - source.ts_ms：源事件时间戳(ms)
  - source.file/source.pos/source.row：binlog 位点信息

## 与 StarRocks 配合建议

- StarRocks 主键表使用字段 \_\_table 和 id 组成主键。
- 使用字段 \_\_source_ts_ms 作为 sequence 列，避免旧数据晚到覆盖新数据。
- Routine Load 需把事件类型字段 \_\_op 中的 d 映射成 StarRocks 删除标记 1。

## 场景模板

下面补充两个常见场景，后续可以直接参考。

### 场景一：历史分表订单集中到同一个 topic

适用场景：订单按月或按历史分表（例如 order_202301、order_202302），希望统一写到一个逻辑 topic，便于下游统一消费。

关键配置示例：

```json
{
  "topic.prefix": "mysql-order",
  "database.include.list": "trade",
  "table.include.list": "trade\\.order_[0-9]{6}",

  "transforms": "Reroute,unwrap",
  "transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
  "transforms.Reroute.topic.regex": "(.*)\\.order_[0-9]{6}",
  "transforms.Reroute.topic.replacement": "$1.order",
  "transforms.Reroute.key.enforce.uniqueness": "true",

  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "true",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.unwrap.add.fields": "table,op,source.ts_ms,source.file,source.pos,source.row"
}
```

说明：

- 输入物理 topic 类似 mysql-order.trade.order_202301。
- 经过 Reroute 后，下游统一消费 mysql-order.trade.order。
- 建议保留 table 元字段，方便下游识别来源分表。
- 分表场景建议保持 key.enforce.uniqueness=true，避免不同分表相同主键冲突。

### 场景二：单表采集（不做分表归并）

适用场景：只有一张业务表（例如 trade.order），不需要 Reroute。

关键配置示例：

```json
{
  "topic.prefix": "mysql-order",
  "database.include.list": "trade",
  "table.include.list": "trade\\.order",

  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "true",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.unwrap.add.fields": "table,op,source.ts_ms,source.file,source.pos,source.row"
}
```

说明：

- 单表默认输出 topic 为 mysql-order.trade.order。
- 因为没有跨分表主键冲突问题，通常不需要 ByLogicalTableRouter。
- 如果未来从单表演进到分表，可以按场景一无缝加上 Reroute。

### 场景三：当前 user 水平分表（本项目默认）

适用场景：user 按编号水平分表（例如 user00-user99），并统一写入一个逻辑 topic 供下游消费。

关键配置示例（与当前 mysql-connect.json 对齐）：

```json
{
  "topic.prefix": "mysql-user",
  "database.include.list": "test",
  "table.include.list": "test\\.user[0-9]{2}",

  "snapshot.mode": "initial",
  "snapshot.locking.mode": "none",
  "include.schema.changes": "false",

  "transforms": "Reroute,unwrap",
  "transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
  "transforms.Reroute.topic.regex": "(.*)\\.user[0-9]{2}",
  "transforms.Reroute.topic.replacement": "$1.user",
  "transforms.Reroute.key.enforce.uniqueness": "true",

  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "true",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.unwrap.add.fields": "table,op,source.ts_ms,source.file,source.pos,source.row"
}
```

说明：

- 输入物理 topic 类似 mysql-user.test.user06。
- 经过 Reroute 后，下游统一消费 mysql-user.test.user。
- 保留 table 元字段后，下游可以区分来源分表（例如 user06）。
- 该场景通常与 StarRocks 主键 (\_\_table, id) 配合，避免跨分表 id 冲突。
