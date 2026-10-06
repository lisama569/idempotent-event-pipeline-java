package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;

/**
 * 事件投递进度（只读视图）。
 *
 * @param eventId              事件标识
 * @param partitionKey         逻辑分区键
 * @param stage                当前阶段：PENDING / RETRY_WAIT / DELIVERED / REJECTED / DEAD
 * @param attempts             已投递尝试次数
 * @param nextAttemptAtMillis  下一次尝试时间（epoch 毫秒，0 表示无排期）
 * @param rejectionReason      拒绝原因类别（仅 REJECTED 时有值）
 */
public record EventProgress(String eventId,
                            String partitionKey,
                            String stage,
                            int attempts,
                            long nextAttemptAtMillis,
                            RejectionReason rejectionReason) {

    /** 从持久化记录推导阶段视图；PENDING 且下次尝试时间在未来视为重试等待中。 */
    public static EventProgress from(EventStore.StoredEvent stored, long nowMillis) {
        String stage = switch (stored.status()) {
            case PENDING -> stored.nextAttemptAtMillis() > nowMillis ? "RETRY_WAIT" : "PENDING";
            default -> stored.status().name();
        };
        return new EventProgress(stored.envelope().eventId(),
                stored.envelope().partitionKey(), stage, stored.attempts(),
                stored.nextAttemptAtMillis(), stored.rejectionReason());
    }
}
