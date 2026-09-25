package com.github.lisama569.idempotenteventpipelinejava.pipeline.store;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventEnvelope;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.RejectionReason;

import java.util.List;
import java.util.Optional;

/** 本地持久化事件与投递状态存储。 */
public interface EventStore {

    enum EventStatus { PENDING, DELIVERED, REJECTED, DEAD }

    record StoredEvent(EventEnvelope envelope, EventStatus status, int attempts,
                       RejectionReason rejectionReason, long nextAttemptAtMillis) {
    }

    /** 幂等写入：已存在同 eventId 记录时返回已有记录，不产生新记录。 */
    StoredEvent saveIfAbsent(EventEnvelope envelope);

    Optional<StoredEvent> find(String eventId);

    /** 更新投递状态（尝试次数、状态、拒绝原因、下次可投时间）。 */
    void updateStatus(String eventId, EventStatus status, int attempts,
                      RejectionReason reason, long nextAttemptAtMillis);

    /** 分区内已成功投递的最大序号（用于保序与重启恢复）。 */
    long lastDeliveredSequence(String partitionKey);

    /** 查询死信，按 (partitionKey, sequence) 原顺序返回。 */
    List<StoredEvent> listDeadLetters();
}
