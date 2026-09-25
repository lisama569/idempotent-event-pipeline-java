package com.github.lisama569.idempotenteventpipelinejava.pipeline;

/** 单次投递的处理结论。 */
public enum ProcessingOutcome {
    SUCCESS,
    /** 可重试失败 */
    RETRYABLE_FAILURE,
    /** 不可重试，直接拒绝 */
    REJECTED
}
