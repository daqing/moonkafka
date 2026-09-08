# moonkafka 示例

本目录包含一个可运行的 MoonBit 原生示例，展示三种常见的客户端用法：

- 使用 `Producer` 发布消息；
- 不加入消费组，直接消费主题；
- 作为 KIP-848 消费组成员消费，并自动提交偏移量。

示例默认连接到 `127.0.0.1:9092` 上的 Kafka 4.x KRaft broker，并假设目标主题
已经存在。也可以通过可选参数传入其他主机和端口。

## 前置条件

你需要准备：

- MoonBit 工具链；
- 原生编译环境（库使用原生异步 socket）；
- demo 进程可以访问的 Kafka 4.x KRaft broker。

仓库内置了本地 Kafka 4.3 KRaft 配置。启动 broker，并创建示例主题：

```sh
make docker-up

docker exec moonkafka-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic events \
  --partitions 3 --replication-factor 1
```

确认 broker 可访问：

```sh
docker exec moonkafka-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

使用完毕后运行 `make docker-down` 停止本地 broker。

## 运行

在仓库根目录执行：

```sh
# 查看帮助
moon run --target native docs/demo -- --help

# 发布一条消息（acks = all）
moon run --target native docs/demo -- produce events "来自 MoonBit 的问候"

# 不加入消费组，从最早偏移量开始消费
moon run --target native docs/demo -- consume events

# 加入消费组；在两个终端中运行可以观察 Kafka 分配不同分区
moon run --target native docs/demo -- consume-group events payments
```

连接远程 broker：

```sh
moon run --target native docs/demo -- produce events hello kafka.example.com 9092
moon run --target native docs/demo -- consume-group events payments kafka.example.com 9092
```

使用 `Ctrl-C` 停止消费者。示例中的 `defer consumer.close()` 会优雅地离开消费组，
启用自动提交时还会在关闭前执行最后一次提交。

## 命令行参数

| 命令 | 必填参数 | 可选参数 |
| --- | --- | --- |
| `produce` | `<topic> <value>` | `[host] [port]` |
| `consume` | `<topic>` | `[host] [port]` |
| `consume-group` | `<topic> <group-id>` | `[host] [port]` |

默认值为 `host=127.0.0.1`、`port=9092`。示例不会自动创建主题；请先使用 Kafka
的 `kafka-topics.sh` 创建主题。

## 示例如何工作

三个命令都运行在 `@async.with_task_group` 中。任务组很重要，因为
`Producer` 和配置了消费组的 `Consumer` 会启动后台任务：生产者的 sender 负责
排空批次，消费组消费者负责发送心跳和处理分配。离开任务组作用域时，这些任务
会被等待完成，而不是被遗弃。

### 生产者流程

`produce` 的执行过程如下：

1. 从命令行解析主题、消息、主机和端口；
2. 通过 `Producer::connect` 连接，协商 broker API 版本并解析主题元数据；
3. 使用 `producer.send` 追加一条 UTF-8 消息；
4. 等待 broker 返回偏移量。示例使用 `acks=-1`，因此 Kafka 会等待所有同步副本
   接收消息后再确认；
5. 通过 `defer` 调用 `producer.close()`，排空待处理工作并关闭连接。

示例消息没有 key，因此使用默认分区器选择分区。生产环境中如需配置批处理、
重试、TLS、SASL、分区策略、幂等或事务，可以改用
`Producer::connect_with_config` 和 `ProducerConfig`。

### 不加入消费组的消费者

`consume` 使用便捷的 `Consumer::connect` API，直接读取主题分区，并从
`StartFrom::Earliest` 开始。每次 `poll()` 返回当前可用的记录，循环打印偏移量
和值，然后继续轮询。位置只保存在当前进程中，不会提交给 Kafka，因此重启命令
后会根据配置的起始位置重新读取。

这种模式适合简单的检查工具，或由应用自己管理偏移量的场景。当多个进程需要
共享处理工作时，通常应该使用消费组模式。

### 消费组流程

`consume-group` 使用显式配置路径：

1. `ConsumerConfig::new` 设置 `group_id`、起始偏移策略和
   `enable_auto_commit`；
2. `Consumer::connect_with_config` 连接 broker，并启动配置的自动提交任务；
3. `consumer.subscribe([topic], listener=None)` 加入消费组。默认的
   `ConsumerProtocol` 使用 KIP-848 服务端驱动的分配；
4. Kafka 将分区分配给该成员。多个终端使用同一个 group id 时，每个活动成员
   只会获得其中一部分分区；
5. 循环持续调用 `poll()`。客户端在后台发送心跳、应用分配变更，并定期提交位置；
6. `consumer.close()` 优雅离开消费组，并完成最后一次自动提交。

要显式使用兼容的经典协议，将配置设置为
`group_protocol=@moonkafka.ClassicProtocol`。也可以使用 `PreferConsumer` 或
`PreferClassic`，让客户端优先选择一种协议并在必要时回退。

## 生产者核心用法

```mbt nocheck
@async.with_task_group(fn(group) {
  let producer = @moonkafka.Producer::connect(
    group~,
    host="127.0.0.1",
    port=9092,
    topic="events",
    acks=-1,
  )
  defer producer.close()
  let offset = producer.send(value=@utf8.encode("hello"))
  println("published at offset \{offset}")
})
```

## 消费者核心用法

```mbt nocheck
let config = @moonkafka.ConsumerConfig::new(
  ["127.0.0.1:9092"],
  "events",
  start_from=@moonkafka.StartFrom::Earliest,
  group_id=Some("payments"),
  enable_auto_commit=true,
  group_protocol=@moonkafka.ConsumerProtocol,
)
let consumer = @moonkafka.Consumer::connect_with_config(group~, config)
consumer.subscribe(["events"], listener=None)
```

消费组消费者应持续调用 `poll()`。Kafka 会在同一消费组的成员之间分配分区，
`enable_auto_commit=true` 会定期提交当前位置，并在关闭时再提交一次。

## 安全配置

相同的配置构造函数支持 `security_protocol`、`sasl` 和 `tls` 选项。可参考公开的
`ProducerConfig` 和 `ConsumerConfig` 定义，了解可用字段和校验规则。可运行示例
故意使用本地明文连接，以便复制后立即尝试。

## 故障排查

- **连接被拒绝：** 使用 `make docker-up` 启动 Kafka，确认 9092 端口，或传入正确的主机和端口。
- **找不到主题：** 在启动示例前创建主题；示例是客户端示例，不是 admin 工具。
- **没有打印记录：** `consume-group` 可能正在等待成员分配，或者同一消费组的其他成员持有分区。发布一条新消息，或运行 `consume` 独立检查主题。
- **消费组协议不可用：** 默认的 `ConsumerProtocol` 要求 broker 通告 KIP-848。对于使用经典 API 的 broker/消费组，选择 `ClassicProtocol`，或者使用 `PreferConsumer` 允许回退。
- **停止进程：** 使用 `Ctrl-C`；示例的延迟关闭路径会负责离开消费组并关闭 broker 连接。

使用完毕后：

```sh
make docker-down
```
