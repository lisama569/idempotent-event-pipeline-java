# 本地幂等事件流水线

一条完全本地的事件接收与投递流水线：本地文件持久化事件与投递进度，
保证重复投递、并发处理、失败重试与进程重启后的结论可预期。
不依赖任何外部服务或真实凭据。

接收与投递已解耦：提交事件只拿到“已受理”的中间结果，投递由后台调度器推进；
重启后未投完的事件自动接着投；任意时刻可按事件标识查询投递进度。

## 投递语义

- **接收与投递解耦**：`POST /events` 只做校验、幂等落库与入队，立即返回
  接收结论（HTTP 202 + `ACCEPTED`），不等投递跑完；真正的投递由
  `DeliveryDispatcher` 在后台推进。即使事件要重试，调用方也不会被卡住。
- **至少一次投递 + 幂等**：`eventId` 是幂等键。同一事件重复提交返回既有结论
  （已投递 → `DELIVERED`，其余终态 → `REJECTED`，在途 → `ACCEPTED`），
  业务副作用只生效一次。
- **分区内保序**：同一 `partitionKey` 的事件按 `sequence`（从 1 起单调递增）
  严格保序投递，分区锁串行化；不同分区在调度线程池上并行推进、互不影响。
  某个分区卡在重试等待里时，其队列只是暂时为空，不会连带堵住其他分区。
- **乱序处理**：序号不等于“下一期望序号”的事件以
  `OUT_OF_ORDER_SEQUENCE` 明确拒绝（终态），不会静默缓冲或丢弃。
- **失败重试**：处理器返回 `RETRYABLE_FAILURE` 时按指数退避重试
  （`base * 2^(attempt-1)`）。退避通过定时任务在到期后重新入队，
  不让线程睡眠、不靠死循环空转；达到 `pipeline.retry.max-attempts`
  后进入死信（`DEAD`）。
- **死信阻塞分区**：死信事件不推进分区序号，后续事件因保序被拒绝，
  直到死信被重放——严格保序的必然结果。

## 投递时序（解耦之后）

```
调用方                DeliveryEngine            DeliveryDispatcher          EventHandler
  |  POST /events          |                          |                        |
  |----------------------->| 校验/落库/保序检查          |                        |
  |  202 ACCEPTED          | 入队 ------------------->|                        |
  |<-----------------------|                          | 分区队列串行排空          |
  |                        |<--------- deliver --------|                        |
  |                        | 尝试 1..n（分区锁内）       |----------------------->|
  |                        | 成功→DELIVERED / 拒绝→REJECTED /                    |
  |                        | 失败→PENDING+nextAttemptAt |                        |
  |                        |-- enqueueDelayed(退避) --->| 到期后再入队（定时任务）  |
  |  GET /events/{id}/progress（任意时刻只读查询进度）   |                        |
```

## 重启自愈

- 状态快照在 `<pipeline.store.dir>/store.json`，采用“临时文件 + 原子改名”写入，
  崩溃不会出现半写文件。
- 重启时 `DeliveryDispatcher.recover()` 扫描所有 `PENDING` 事件并重新排期：
  - **还没开始投的**（`attempts=0`）：立即入队投递；
  - **正处在两次重试之间的**（`nextAttemptAtMillis` 在未来）：按持久化的
    剩余退避时间排期，重启前积累的尝试次数保留，退避节奏延续、不清零。
- 重启前后同一事件的最终结论一致：已 `DELIVERED` 的事件重复提交返回原结论，
  不会再次产生副作用；`DEAD` 事件保持可查询、可重放。
- 边界：恢复只覆盖已落库的事件；接收侧在“落库后、入队前”崩溃的极端窗口
  由恢复扫描兜底（该事件仍是 `PENDING`，会被重新排期）。

## 进度查询

`GET /events/{eventId}/progress` 返回只读进度视图（未知事件返回 404）：

```json
{
  "eventId": "w1",
  "partitionKey": "p1",
  "stage": "RETRY_WAIT",
  "attempts": 2,
  "nextAttemptAtMillis": 1791298214634,
  "rejectionReason": null
}
```

- `stage`：`PENDING`（还没投/等待投递）、`RETRY_WAIT`（重试等待中）、
  `DELIVERED`（已成功）、`REJECTED`（被拒绝）、`DEAD`（进死信）。
- `attempts`：已投递尝试次数；`nextAttemptAtMillis`：下一次尝试时间
  （0 表示无排期）；`rejectionReason`：拒绝原因类别（仅 `REJECTED` 时有值）。
- 在途与重试等待中的事件同样可查；查询只读，不改变任何状态、
  不触发业务副作用。

## 幂等与保序边界

- 幂等边界：以 `eventId` 为单位。终态（DELIVERED / REJECTED / DEAD）持久化，
  重复提交（包括并发重复提交）返回原结论，不产生新副作用。
- 保序边界：以 `partitionKey` 为单位。分区内严格有序，分区间无序、并行。
  同一分区需等上一事件出结论后再提交下一序号，否则按乱序拒绝。

## 死信查询与重放

- `GET /dead-letters`：按 `(partitionKey, sequence)` 原顺序返回死信。
- `POST /dead-letters/{eventId}/replay`：把死信重置为 PENDING（尝试次数清零）
  后重新走正常接收路径（返回 `ACCEPTED`，投递由后台推进）——
  不绕过幂等与保序约束；已成功处理的事件因幂等分支不会再次产生副作用。

## 版本兼容规则

- 支持的必需版本：`1`、`2`；其余版本 → `UNSUPPORTED_VERSION`。
- 必需字段：`payload.type`(String)、`payload.amount`(Number)；
  缺失 → `MISSING_REQUIRED_FIELD`，类型不符 → `FIELD_TYPE_MISMATCH`。
- v2 新增可选字段 `payload.currency`(String)，缺省时赋予稳定缺省值 `"USD"`；
  v1 事件不注入该字段。缺省语义确定，同一输入多次处理判定一致。
- 校验发生在投递前的明确阶段（`EventValidator`），是纯函数。

## 拒绝原因类别

`UNSUPPORTED_VERSION`、`MISSING_REQUIRED_FIELD`、`FIELD_TYPE_MISMATCH`、
`OUT_OF_ORDER_SEQUENCE`、`BUSINESS_REJECTED`（处理器返回 `REJECTED`）。
拒绝原因持久化在事件记录中，可通过进度查询区分、不混淆。

## 配置

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `pipeline.store.dir` | `./data/event-store` | 本地状态目录 |
| `pipeline.retry.max-attempts` | `3` | 最大投递尝试次数 |
| `pipeline.retry.base-backoff-ms` | `50` | 退避基数（毫秒） |
| `pipeline.dispatcher.threads` | `4` | 后台投递调度线程数 |

## 本地验证

```bash
mvn test          # 运行全部单元测试（无需网络与外部服务）
mvn spring-boot:run

# 提交事件：立即返回 202 ACCEPTED，不等待投递完成
curl -i -X POST localhost:8080/events -H 'Content-Type: application/json' -d '{
  "eventId":"e1","partitionKey":"p1","sequence":1,"version":2,
  "payload":{"type":"ORDER","amount":10},"attributes":{}}'

# 随时查询投递进度（在途、重试等待中也可查）
curl localhost:8080/events/e1/progress

# 模拟可重试失败：type=FAIL 会退避重试，可观察到 RETRY_WAIT 与 attempts 增长
curl -X POST localhost:8080/events -H 'Content-Type: application/json' -d '{
  "eventId":"e2","partitionKey":"p2","sequence":1,"version":2,
  "payload":{"type":"FAIL","amount":1},"attributes":{}}'
curl localhost:8080/events/e2/progress

# 重启进程后：未投完的事件自动接着投，无需重新提交
curl localhost:8080/dead-letters
curl -X POST localhost:8080/dead-letters/e2/replay
```

测试覆盖：重复投递幂等、同一分区并发重复提交、分区内保序与分区间并行、
重试耗尽进死信与按序重放、重启恢复（含重试等待续投与未投递事件恢复）、
接收即返回（投递卡住不阻塞调用方与其他分区）、进度查询（阶段/次数/
下次时间/拒绝类别、只读性）、版本兼容与拒绝原因区分、可选字段缺省语义。
单测日志打印事件标识、分区、投递次数与判定依据，例如：

```
event=w1 partition=p1 attempts=1 decision=RETRY backoffMs=300 nextAttemptAt=1791298214634
event=w1 partition=p1 attempts=1 decision=RECOVER delayMs=250
event=w1 partition=p1 attempts=2 decision=DELIVERED
```
