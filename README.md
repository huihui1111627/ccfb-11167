# mqueue — 自研分区日志型消息队列

一个用 Rust 实现的单机分区日志型消息队列，对标 Kafka 的核心子集，目标是**够用、好运维、省资源**。
包含消息存储、生产/消费协议、运维控制台，以及一套带混沌测试的压测与正确性校验工具。

## 架构

```
crates/
├── mqueue-storage   段文件 + 稀疏索引 + 崩溃恢复（同步 IO，无 async 依赖）
├── mqueue-proto     长度前缀 TCP 帧协议（bincode）、FNV 分区哈希、消息类型
├── mqueue-broker    分区管理、消费组再平衡、幂等去重、HTTP 运维控制台（二进制）
├── mqueue-client    幂等生产者、消费组客户端（自动再平衡 / 位点续传）
└── mqueue-bench     压测 + 混沌测试（随机杀进程 / 增减消费者）+ 校验 + SVG 报告（二进制）
```

### 存储模型（mqueue-storage）

- 每个分区是一个只追加日志，落在 `topics/<topic>/<partition>/` 下。
- 日志切成多个**段（segment）**：`<base_offset>.log`（记录帧）+ `<base_offset>.idx`（稀疏索引）。
- 记录帧自带 52 字节定长头：magic、版本、长度、**CRC32**、offset、producer id、序列号、时间戳、key/value 长度。
- 活动段满 `max_segment_bytes` 后滚动为只读段；只读段通过 **mmap 零拷贝**提供读取。
- 稀疏索引每 64 条记录一个条目（offset → 字节位置 + 时间戳），支撑按 offset 和按时间戳的二分定位。
- 保留策略：按时间（`max_age_ms`）或按总量（`max_total_bytes`）删除旧的**密封段**，活动段永不删。

### 关键正确性保证

1. **按键分区、同键有序**：`partition = fnv1a64(key) % partition_count`，分区内 offset 严格递增。
2. **幂等生产、重试不重复**：生产者为每个分区维护单调递增序列号，发送前把序列号
   fsync 到本地状态文件；broker 用 `(producer_id, partition, sequence)` 去重窗口，
   命中则返回首次分配的 offset。broker 重启后从日志重建去重窗口。
3. **崩溃恢复**：启动时逐段扫描 `.log`，靠 magic + CRC 识别不完整的尾部帧并截断，
   然后**从日志完全重建所有段的 `.idx`**（索引只是日志的派生缓存，绝不信任旧索引）。
4. **恢复后追加不覆盖**：活动段恢复重开文件时必须 `seek` 到已校验的末尾，
   否则后续 append 会从字节 0 践踏历史数据（有 `append_after_recovery` 回归测试守护）。
5. **消费位点续传**：消费组位点持久化到 `offsets/<group>.offsets.json`，
   重启后从上次提交位置继续；先落消费 journal 再提交位点，提供至少一次语义。
6. **消费组再平衡**：心跳会话超时 + generation 围栏；成员增减时按范围重分配分区，
   旧 generation 的提交被拒绝并触发重新入组。
7. **客户端多连接隔离**：消费者使用三条独立 TCP 连接——fetch（数据面，长轮询）、
   control（join/commit/位点查询）、heartbeat——任一连接的重连退避都不会阻塞其它角色。

## 构建

```bash
cargo build --release
# 产物：target/release/mqueue-broker、target/release/mqueue-bench
cargo test              # 存储层单元/集成测试（含崩溃恢复回归）
```

## 启动 broker

```bash
mqueue-broker \
  --data-dir ./mqueue-data \
  --bind 127.0.0.1:9100 \        # 生产/消费 TCP 协议
  --console 127.0.0.1:9101 \     # HTTP 运维控制台
  --segment-bytes 67108864 \
  --retention-age-ms 604800000 \
  --retention-bytes 1073741824
```

打开 `http://127.0.0.1:9101/` 即可看到仪表盘：主题与分区、生产/消费速率、
消费组与积压、存储段、去重命中数，每 2 秒自动刷新。也可直接调 JSON API：

| 路由 | 说明 |
| --- | --- |
| `GET /api/overview` | 累计计数、速率、运行时长 |
| `GET /api/topics` | 分区位点、数据量、每个存储段 |
| `GET /api/groups` | 消费组成员、分配、已提交位点、总积压 |
| `GET /api/metrics?seconds=300` | 每秒生产/消费序列、延迟 |
| `POST /api/topics` | 创建主题（JSON body） |

## 压测与混沌测试

`mqueue-bench run` 会自动拉起 broker、生产者和消费者进程，在运行期间随机注入故障，
结束后独立从 broker 读全量日志作为「真值」，核对所有消费 journal，最后产出 HTML 报告。

```bash
mqueue-bench run \
  --addr 127.0.0.1:19100 --console 127.0.0.1:19101 \
  --topic bench --partitions 4 --messages 20000 --key-space 64 \
  --batch-size 200 --producers 2 --consumers 3 \
  --duration-secs 30 --chaos-min-secs 3 --chaos-max-secs 7 \
  --kill-broker true --run-dir ./bench-run
```

注入的混沌事件（随机间隔）：

- **SIGKILL 生产者/消费者**后立即用相同 id 和状态目录重启（模拟硬崩溃）；
- **动态新增消费者**触发消费组再平衡；
- **SIGKILL broker** 后重启，走完整崩溃恢复路径。

### 校验内容

校验器以 broker 日志为准，检查所有消费 journal：

1. **至少一次覆盖**：日志里每条记录都被某个消费者处理过；
2. **无重复处理**：同一消费者不会处理同一条记录两次（再平衡导致的跨消费者重复属正常）；
3. **分区内有序**：每个消费者视角下分区 offset 严格递增，同 key 的消息按序消费；
4. **无幽灵记录**：journal 不包含日志中不存在的记录。

产物：`report.html`（吞吐曲线 + 端到端延迟分布图 + 混沌时序）、
`verify.json`、`perf.json`、`chaos.json`，以及 `logs/` 下每个进程的独立日志。

## 设计取舍

- **单机**：不做分布式共识，分区元数据用单个 JSON 文件原子改写；够用且大幅降低运维复杂度。
- **同步存储 + spawn_blocking**：存储层保持纯同步、易测试；broker 用线程池桥接 async，
  不同分区可并发服务。
- **ack 前 fsync**：生产请求在返回前对分区做一次 fsync，批量请求摊薄成本，
  保证已确认数据在 SIGKILL 后不丢。
- **无限重连 + 有界退避**：客户端对传输类故障无限重试（指数退避封顶 2 秒），
  应用类错误（generation 围栏、未知消费者）立即上抛触发再平衡。
