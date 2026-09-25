# 本地幂等事件流水线

一条完全本地的事件接收与投递流水线：本地文件持久化事件与投递进度，
保证重复投递、并发处理、失败重试与进程重启后的结论可预期。
不依赖任何外部服务或真实凭据。

## 投递语义

- **至少一次投递 + 幂等**：`eventId` 是幂等键。同一事件重复提交返回既有结论
  （已投递 → `SUCCESS`，其余终态 → `REJECTED`），业务副作用只生效一次。
- **分区内保序**：同一 `partitionKey` 的事件按 `sequence`（从 1 起单调递增）
  严格保序投递，分区锁串行化；不同分区并行推进、互不影响。
- **乱序处理**：序号不等于“下一期望序号”的事件以
  `OUT_OF_ORDER_SEQUENCE` 明确拒绝（终态），不会静默缓冲或丢弃。
- **失败重试**：处理器返回 `RETRYABLE_FAILURE` 时按指数退避重试
  （`base * 2^(attempt-1)`），次数与退避由配置决定、可判定；
  达到 `pipeline.retry.max-attempts` 后进入死信（`DEAD`）。
- **死信阻塞分区**：死信事件不推进分区序号，后续事件因保序被拒绝，
  直到死信被重放——严格保序的必然结果。

## 幂等与保序边界

- 幂等边界：以 `eventId` 为单位。终态（DELIVERED / REJECTED / DEAD）持久化，
  重复提交（包括并发重复提交）返回原结论，不产生新副作用。
- 保序边界：以 `partitionKey` 为单位。分区内严格有序，分区间无序、并行。
- 重启恢复：状态快照在 `<pipeline.store.dir>/store.json`，
  采用“临时文件 + 原子改名”写入，崩溃不会出现半写文件；重启后
  已投递事件不会再次产生副作用，死信保持可查询、可重放。

## 死信查询与重放

- `GET /dead-letters`：按 `(partitionKey, sequence)` 原顺序返回死信。
- `POST /dead-letters/{eventId}/replay`：把死信重置为 PENDING（尝试次数清零）
  后重新走正常投递路径——不绕过幂等与保序约束；已成功处理的事件
  因幂等分支不会再次产生副作用。

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
拒绝原因持久化在事件记录中，可区分、不混淆。

## 配置

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `pipeline.store.dir` | `./data/event-store` | 本地状态目录 |
| `pipeline.retry.max-attempts` | `3` | 最大投递尝试次数 |
| `pipeline.retry.base-backoff-ms` | `50` | 退避基数（毫秒） |

## 本地验证

```bash
mvn test          # 运行全部单元测试（无需网络与外部服务）
mvn spring-boot:run
curl -X POST localhost:8080/events -H 'Content-Type: application/json' -d '{
  "eventId":"e1","partitionKey":"p1","sequence":1,"version":2,
  "payload":{"type":"ORDER","amount":10},"attributes":{}}'
curl localhost:8080/dead-letters
```

测试覆盖：重复投递幂等、同一分区并发重复提交、分区内保序与分区间并行、
重试耗尽进死信与按序重放、重启恢复、版本兼容与拒绝原因区分、
可选字段缺省语义。单测日志打印事件标识、分区、投递次数与判定依据，例如：

```
event=r2 partition=p1 attempts=3 decision=DEAD reason=RETRY_EXHAUSTED maxAttempts=3
```
