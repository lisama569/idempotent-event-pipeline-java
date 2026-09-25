package com.github.lisama569.idempotenteventpipelinejava.domain;

/** 可区分的拒绝/判定原因类别。 */
public enum RejectReason {
    NONE,
    MALFORMED_JSON,
    UNSUPPORTED_VERSION,
    MISSING_FIELD,
    TYPE_MISMATCH,
    DUPLICATE_EVENT,
    PERMANENT_DELIVERY_FAILURE,
    RETRY_EXHAUSTED
}
