package com.github.lisama569.idempotenteventpipelinejava.domain;

/** 接收阶段的校验拒绝（可分类）。 */
public class ValidationException extends RuntimeException {
    private final RejectReason reason;

    public ValidationException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
