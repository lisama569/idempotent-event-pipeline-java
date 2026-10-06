package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;

/**
 * 接收侧对提交方的即时回执：投递在后台异步推进，调用方无需等待。
 *
 * @param eventId         事件标识
 * @param decision        接收判定
 * @param status          回执时的事件状态（DUPLICATE 时为既有终态）
 * @param rejectionReason 拒绝原因（仅 REJECTED 时非空）
 */
public record SubmissionAck(String eventId,
                            Decision decision,
                            EventStore.EventStatus status,
                            RejectionReason rejectionReason) {

    public enum Decision {
        /** 已接收并持久化，后台会继续投递 */
        ACCEPTED,
        /** 已有结论的重复提交，直接返回既有结论，不产生新副作用 */
        DUPLICATE,
        /** 接收前校验失败，明确拒绝（终态） */
        REJECTED
    }
}
