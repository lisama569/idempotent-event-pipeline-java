# 本地幂等事件投递流水线（Idempotent Event Pipeline）

一条完全本地化的“事件接收 → 持久化 → 至少一次投递 → 重试/死信 → 重放”流水线。
仅依赖嵌入式 H2 文件库与 Spring Boot，无需任何外部服务、消息中间件或真实凭据。

## 1. 投递语义总览

| 主题 | 语义与边界 |
| --- | --- |
| 投递保证 | **至少一次（at-least-once）**：失败会按退避重试、崩溃后恢复续投；下游副作用以 `eventId` 为幂等键，重复成功不产生重复业务效果 |
| 幂等边界 | 接收幂等由 `events.event_id` 唯一约束保证（含并发竞态）；投递幂等由下游副作用键保证。**边界**：本工程的“下游”是内存计数器（进程内有效），真实部署需把该键落到下游存储；事件库本身记录 `SUCCEEDED`，不会再次投递同一事件 |
| 保序边界 | 同一 `partition` 内严格按接收序号（`seq_in_partition`）处理：队头未到终态（成功/死信）前不拾取后续事件，因此失败、重试、死信都会“顶住”分区。**不同分区之间不保证顺序、可并行** |
| 并发模型 | 单实例：JVM 内每分区一把互斥锁 + DB 行级 CAS 占用令牌（`locked_by`）。DB 锁用于崩溃后僵死锁回收；多实例部署不在本工程范围内 |
| 失败分类 | `RETRYABLE_FAILURE`（退避重试）/ `PERMANENT_FAILURE`（立即死信）；接收阶段为 `MALFORMED_JSON` / `UNSUPPORTED_VERSION` / `MISSING_FIELD` / `TYPE_MISMATCH`；重复接收为 `DUPLICATE_EVENT`。类别互不混淆、不静默丢弃 |

## 2. 事件状态机

```
RECEIVED ──占用──▶ IN_FLIGHT ──成功──────────────▶ SUCCEEDED（终态）
                        │
                        ├──可重试失败─▶ RETRYING ──退避到期──▶ IN_FLIGHT（循环）
                        │
                        └──永久失败 / 重试耗尽────▶ DEAD_LETTER（可重放）
                                                         │
                              管理员按原序重放 ──────────┘（复位为 RECEIVED，预算重置）
```

- **重试次数**：`pipeline.max-attempts`（默认 5，测试为 3），`attempt_count` 每次尝试 +1。
- **退避（确定性）**：`next_visible_at = now + base * multiplier^(attempt-1)`；
  默认 `base=100ms, multiplier=2`，即 100ms、200ms、400ms……到期前队头不可见。
- **重试耗尽**：第 `maxAttempts` 次仍失败 → `DEAD_LETTER`，原因为 `RETRY_EXHAUSTED`。
- **不可处理事件**：下游返回永久失败 → 不重试，立即 `DEAD_LETTER`，原因为
  `PERMANENT_DELIVERY_FAILURE`，与重试耗尽可明确区分。

## 3. 死信隔离与重放

- 查询：`GET /dead-letters?partition=xxx`（省略分区则查全部），按 `(partition, seq)` 原序返回。
- 重放：`POST /dead-letters/replay?partition=xxx`。
  - **按原顺序**：只按分区重放，按 seq 逐个复位，并在分区锁内串行推进。
  - **不绕过幂等**：`event_id` 不变，副作用幂等键不变；已 `SUCCEEDED` 的事件不在死信中，永远不会再产生副作用。
  - **不绕过保序**：重放事件回到原 seq 的队头位置，仍只有队头可被处理；队头恢复后自动排空此前被顶住的后续事件。
  - **重试预算重置**：重放是显式管理员动作，`attempt_count` 归零、退避清零，但重试上限与退避规则照常生效，再次失败仍会带着分类原因回到死信。

## 4. 事件版本兼容规则

事件信封：`version`（必填整数）、`eventId`/`partition`/`type`（必填非空白字符串）、
`payload`（可选，任意 JSON，缺省 null）、`priority`（**v2 新增可选整数，缺省确定为 0**）。

| 情形 | 判定阶段 | 结论 |
| --- | --- | --- |
| 不是合法 JSON / 顶层不是对象 | 接收解析 | `MALFORMED_JSON`（400） |
| `version` 缺失/非整数 | 接收解析 | `MISSING_FIELD` / `TYPE_MISMATCH` |
| `version` 不在支持范围 `[1,2]`（含未知更高必需版本） | 接收解析 | `UNSUPPORTED_VERSION`（400） |
| 必填字段缺失或空白 | 接收解析 | `MISSING_FIELD`（400） |
| 字段类型不匹配（如 eventId=9、priority="high"） | 接收解析 | `TYPE_MISMATCH`（400） |
| `priority` 省略（v1/v2 同样） | 接收解析 | 稳定缺省 `0` |
| 携带未识别的额外字段 | 接收解析 | 忽略，不参与判定（前向兼容） |

校验是纯函数：同一输入多次提交判定结果一致。校验拒绝发生在**接收阶段**，事件不落库、不进入投递。

## 5. HTTP 接口

| 方法/路径 | 说明 |
| --- | --- |
| `POST /events` | 接收原始 JSON。200 返回判定（`duplicate`、`seqInPartition`）；非法输入 400 + `rejectReason` |
| `POST /dispatch?partition=..` | 手动推动一轮（便于本地验证/测试），返回 `processed` |
| `GET /events/{eventId}` | 状态、投递次数、拒绝类别/详情、观测到的副作用次数 |
| `GET /dead-letters?partition=..` | 死信列表（原序） |
| `POST /dead-letters/replay?partition=..` | 按原序重放某分区死信 |

## 6. 本地验证方法

```bash
# 运行全部测试（16 个，含重复投递/并发/重试耗尽/重启恢复/版本兼容/HTTP）
mvn test

# 启动应用（使用 ./data/pipelinedb 文件库，删除该目录即可从零开始）
rm -rf data && mvn spring-boot:run
```

另开终端：

```bash
# 1) 接收事件
curl -s -XPOST localhost:8080/events -H 'Content-Type: application/json' \
  -d '{"version":2,"eventId":"e1","partition":"p1","type":"OrderCreated","priority":3}'

# 2) 重复投递 -> duplicate=true / DUPLICATE_EVENT，不新增、无副作用
curl -s -XPOST localhost:8080/events -H 'Content-Type: application/json' \
  -d '{"version":2,"eventId":"e1","partition":"p1","type":"OrderCreated","priority":3}'

# 3) 非法输入 -> 400 且原因可分类
curl -i -XPOST localhost:8080/events -H 'Content-Type: application/json' \
  -d '{"version":42,"eventId":"e2","partition":"p1","type":"T"}'

# 4) 查询状态（attemptCount、observedSideEffects）
curl -s localhost:8080/events/e1

# 5) 死信与重放（默认 maxAttempts=5、退避 100ms 起）
curl -s 'localhost:8080/dead-letters?partition=p1'
curl -s -XPOST 'localhost:8080/dead-letters/replay?partition=p1'

# 6) 验证重启恢复：Ctrl-C 停止后重新 mvn spring-boot:run，
#    已 SUCCEEDED 事件不重复投递，IN_FLIGHT 僵死锁超过 stale 阈值后自动回收续投。
```

关键参数（`src/main/resources/application.properties`，可覆盖）：

```
pipeline.max-attempts=5
pipeline.backoff.base-millis=100
pipeline.backoff.multiplier=2
pipeline.dispatcher.enabled=true        # 后台轮询开关
pipeline.dispatcher.interval-millis=200
pipeline.recovery-lock-stale-millis=10000
```

## 7. 持久化与重启恢复

- 主配置使用文件库 `jdbc:h2:file:./data/pipelinedb`；状态（状态、次数、退避时间、
  分区序号、行锁）全部落盘，进程重启结论可预期。
- 重启时：`SUCCEEDED` 不再投递；`RETRYING` 到期后续投，未到期继续等待；
  `RECEIVED` 正常投递；`IN_FLIGHT` 若持锁早于 `recovery-lock-stale-millis`
  判定为崩溃遗留，可被新执行器 CAS 抢占回收。
- 测试使用独立内存库（`src/test/resources/application.properties`），互不干扰。

## 8. 可观测日志

所有关键判定均打印 `eventId`、`partition`、投递次数（`attempt`）与判定依据，例如：

```
接收判定 eventId=e1 partition=p1 seq=1 result=ACCEPTED 判定依据=校验通过+首次落库
接收判定 eventId=e1 partition=p1 attempt=N/A result=DUPLICATE_EVENT 判定依据=event_id 唯一约束
投递判定 eventId=e1 partition=p1 attempt=1 result=RETRYABLE_FAILURE 判定依据=handler/ALWAYS_FAIL
状态流转 eventId=e1 partition=p1 seq=1 attempt=3 to=DEAD_LETTER 判定依据=重试耗尽(maxAttempts=3)
死信重放 eventId=e-a partition=p1 seq=1 attempt=3 判定依据=管理员重放，按原序回到队头
```

## 9. 工程结构

```
domain/      枚举（状态/结果/拒绝原因）与校验异常
model/       事件信封、持久化记录、接收与投递判定 DTO
repo/        EventRepository + JdbcEventRepository（H2，唯一约束/序号/CAS 行锁）
service/     EventValidator / EventReceiver / EventDispatcher
             / DeadLetterService / LocalDeliveryHandler / DispatchLoop
web/         REST 控制器与分类异常处理
tests/       冒烟、重复投递、分区并发、重试死信、版本兼容、重启恢复、HTTP 集成
```
