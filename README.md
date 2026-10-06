# 本地幂等事件流水线

一条完全本地的事件接收与投递流水线：本地文件持久化事件与投递进度，
保证重复投递、并发处理、失败重试与进程重启后的结论可预期。
不依赖任何外部服务或真实凭据。

## 接收与投递解耦

接收（`DeliveryEngine`）与投递（`DeliveryScheduler`）是分开的两个阶段：

- **接收**：`POST /events` 只做“校验 + 持久化 + 通知后台调度”，立即返回回执
  （HTTP 202 + `decision=ACCEPTED`），**调用方不会被投递与重试阻塞**。
  校验失败同步返回 `decision=REJECTED` 及原因类别；已有终态的重复提交返回
  `decision=DUPLICATE` 及既有结论。
- **投递**：由后台调度器推进。每个分区同一时刻至多一个推进任务，
  分区内严格按 `sequence` 投递；不同分区并行推进、互不影响——
  某分区卡在重试等待里不会连带阻塞其他分区。
- **不空转**：重试等待通过定时唤醒（`ScheduledExecutorService.schedule`）
  实现，等待期间不占用线程、不循环空转。

### 投递时序

```
调用方                接收侧                    后台调度器                 处理器
  |  POST /events       |                          |                      |
  |-------------------->| 校验 + 持久化(PENDING)     |                      |
  |  202 ACCEPTED       | kick(partition)          |                      |
  |<--------------------|------------------------->|                      |
  |                     |                          | 取分区队首(序号最小)     |
  |                     |                          | 序校验 -> 投递 -------->|
  |                     |                          |  成功: DELIVERED        |
  |                     |                          |  拒绝: REJECTED(原因)   |
  |                     |                          |  可重试失败: PENDING +   |
  |                     |                          |   nextAttemptAt(退避),   |
  |                     |                          |   到期定时唤醒再投        |
  |                     |                          |  耗尽: DEAD(死信)        |
  |  GET /events/{id}   |                          |                      |
  |-------------------->| 只读查询当前进度            |                      |
  |<--------------------|                          |                      |
```

## 投递语义

- **至少一次投递 + 幂等**：`eventId` 是幂等键。同一事件重复提交返回既有结论
  （回执 `decision=DUPLICATE` 并携带既有状态），业务副作用只生效一次。
- **分区内保序**：同一 `partitionKey` 的事件按 `sequence`（从 1 起单调递增）
  严格保序投递；不同分区并行推进、互不影响。
- **乱序处理**：序号不等于“下一期望序号”的事件以
  `OUT_OF_ORDER_SEQUENCE` 明确拒绝（终态），不会静默缓冲或丢弃。
- **失败重试**：处理器返回 `RETRYABLE_FAILURE`（或抛异常）时按指数退避重试
  （`base * 2^(attempt-1)`），次数与退避由配置决定、可判定；
  达到 `pipeline.retry.max-attempts` 后进入死信（`DEAD`）。
- **死信阻塞分区**：死信事件不推进分区序号，后续事件因保序被拒绝，
  直到死信被重放——严格保序的必然结果。

## 重启自愈

状态快照在 `<pipeline.store.dir>/store.json`（临时文件 + 原子改名，崩溃不会
出现半写文件）。进程重启后，调度器启动时扫描所有未完结（`PENDING`）事件并
自动接续推进，**不需要人工重新提交**：

- **还没开始投的**（`PENDING` 且 `attempts=0`）：恢复后直接参与正常投递。
- **排在两次重试之间的**（`PENDING` 且 `attempts>0`）：按持久化的
  `nextAttemptAtMillis` 到期后继续重试；已积累的尝试次数不清零，
  后续退避按累计次数继续指数增长，节奏不因重启乱套。
- **已有终态的**（`DELIVERED` / `REJECTED` / `DEAD`）：不参与恢复，
  结论与重启前一致；重复提交返回既有结论，副作用不重复。
- 同一事件在重启前后的最终结论一致，业务副作用总数一致（恰好一次）。

边界：恢复以 `eventId` 幂等键与分区序号为依据；死信仍需显式重放才会重新
进入投递路径。

## 进度与结论查询

`GET /events/{eventId}` 随时可查某事件当前投到哪一步（只读：不改变任何
状态、不触发业务副作用；未知事件返回 404）：

```json
{
  "eventId": "e1",
  "partitionKey": "p1",
  "sequence": 1,
  "stage": "RETRY_WAIT",
  "attempts": 2,
  "nextAttemptAtMillis": 1759700000000,
  "rejectionReason": null
}
```

- `stage`：`PENDING_DELIVERY`（还没投）/ `RETRY_WAIT`（重试等待中）/
  `DELIVERED`（已成功）/ `REJECTED`（被拒绝）/ `DEAD`（进死信）。
- `attempts`：已投递尝试次数；`nextAttemptAtMillis`：下一次尝试时间
  （仅重试等待中非空）；`rejectionReason`：拒绝原因类别（仅被拒绝时非空）。
- 还在等待投递或等待重试的事件同样可查，不是只能看到已结束的状态。

## 幂等与保序边界

- 幂等边界：以 `eventId` 为单位。终态（DELIVERED / REJECTED / DEAD）持久化，
  重复提交（包括并发重复提交）返回原结论，不产生新副作用。
- 保序边界：以 `partitionKey` 为单位。分区内严格有序，分区间无序、并行。

## 死信查询与重放

- `GET /dead-letters`：按 `(partitionKey, sequence)` 原顺序返回死信。
- `POST /dead-letters/{eventId}/replay`：把死信重置为 PENDING（尝试次数清零）
  并通知后台调度重新走正常投递路径——不绕过幂等与保序约束；已成功处理的
  事件因幂等分支不会再次产生副作用。

## 版本兼容规则

- 支持的必需版本：`1`、`2`；其余版本 → `UNSUPPORTED_VERSION`。
- 必需字段：`payload.type`(String)、`payload.amount`(Number)；
  缺失 → `MISSING_REQUIRED_FIELD`，类型不符 → `FIELD_TYPE_MISMATCH`。
- v2 新增可选字段 `payload.currency`(String)，缺省时赋予稳定缺省值 `"USD"`；
  v1 事件不注入该字段。缺省语义确定，同一输入多次处理判定一致。
- 校验发生在接收阶段（`EventValidator`），是纯函数。

## 拒绝原因类别

`UNSUPPORTED_VERSION`、`MISSING_REQUIRED_FIELD`、`FIELD_TYPE_MISMATCH`、
`OUT_OF_ORDER_SEQUENCE`、`BUSINESS_REJECTED`（处理器返回 `REJECTED`）。
拒绝原因持久化在事件记录中，可区分、不混淆，并可通过进度查询读取。

## 配置

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `pipeline.store.dir` | `./data/event-store` | 本地状态目录 |
| `pipeline.retry.max-attempts` | `3` | 最大投递尝试次数 |
| `pipeline.retry.base-backoff-ms` | `50` | 退避基数（毫秒） |
| `pipeline.scheduler.threads` | `4` | 后台投递调度线程数 |

## 本地验证

```bash
mvn test          # 运行全部单元测试（无需网络与外部服务）
mvn spring-boot:run

# 提交事件：立即返回 202 ACCEPTED，投递在后台推进
curl -i -X POST localhost:8080/events -H 'Content-Type: application/json' -d '{
  "eventId":"e1","partitionKey":"p1","sequence":1,"version":2,
  "payload":{"type":"ORDER","amount":10},"attributes":{}}'

# 随时查询投递进度（只读）
curl localhost:8080/events/e1

# 模拟可重试失败：payload.type=FAIL 会按退避重试，最终进死信
curl -X POST localhost:8080/events -H 'Content-Type: application/json' -d '{
  "eventId":"e2","partitionKey":"p1","sequence":2,"version":2,
  "payload":{"type":"FAIL","amount":1},"attributes":{}}'
curl localhost:8080/events/e2        # 可看到 RETRY_WAIT -> DEAD 的过程
curl localhost:8080/dead-letters

# 重启进程后：未完结事件自动接续投递，无需重新提交；
# 死信保持可查询、可重放
curl -X POST localhost:8080/dead-letters/e2/replay
```

测试覆盖：重复投递幂等、同一分区并发重复提交、分区内保序与分区间并行、
重试耗尽进死信与按序重放、重启恢复（终态结论一致 / 未投事件自动接续 /
重试等待续投且次数累计）、提交即时返回与后台投递成功/进死信、
慢分区不阻塞其他分区、并发提交下同分区保序、进度查询各阶段与只读性、
版本兼容与拒绝原因区分、可选字段缺省语义。
单测日志打印事件标识、分区、投递次数与判定依据，例如：

```
event=p2 partition=pa attempts=1 decision=RETRY backoffMs=2000
recovery: resumed 1 pending event(s) across partitions
event=p2 partition=pa attempts=2 decision=DELIVERED
event=p3 partition=pa attempts=3 decision=DEAD reason=RETRY_EXHAUSTED maxAttempts=3
```
