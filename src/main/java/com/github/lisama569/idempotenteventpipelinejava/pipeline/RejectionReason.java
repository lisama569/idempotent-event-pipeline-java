package com.github.lisama569.idempotenteventpipelinejava.pipeline;

/** 可区分的拒绝原因类别。 */
public enum RejectionReason {
    /** 未知或不支持的必需版本 */
    UNSUPPORTED_VERSION,
    /** 必需字段缺失 */
    MISSING_REQUIRED_FIELD,
    /** 字段类型不匹配 */
    FIELD_TYPE_MISMATCH,
    /** 分区内序号乱序（非下一期望序号且非已处理的重复） */
    OUT_OF_ORDER_SEQUENCE,
    /** 业务处理器明确拒绝（不可重试） */
    BUSINESS_REJECTED
}
