package com.github.lisama569.idempotenteventpipelinejava.pipeline;

/**
 * 事件投递进度（只读视图）：按事件标识查询当前推进到哪一步。
 *
 * @param eventId              事件标识
 * @param partitionKey         逻辑分区
 * @param sequence             分区内序号
 * @param stage                当前阶段
 * @param attempts             已投递尝试次数
 * @param nextAttemptAtMillis  下一次尝试时间（epoch 毫秒；无待定时为 null）
 * @param rejectionReason      拒绝原因类别（仅 REJECTED 时非空）
 */
public record DeliveryProgress(String eventId,
                               String partitionKey,
                               long sequence,
                               Stage stage,
                               int attempts,
                               Long nextAttemptAtMillis,
                               RejectionReason rejectionReason) {

    public enum Stage {
        /** 已接收，还没开始投 */
        PENDING_DELIVERY,
        /** 可重试失败，等待下一次重试 */
        RETRY_WAIT,
        /** 已成功投递 */
        DELIVERED,
        /** 被拒绝（终态，原因见 rejectionReason） */
        REJECTED,
        /** 重试耗尽，进死信（终态） */
        DEAD
    }
}
