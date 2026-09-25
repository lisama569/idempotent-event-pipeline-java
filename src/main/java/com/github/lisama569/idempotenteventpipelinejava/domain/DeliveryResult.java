package com.github.lisama569.idempotenteventpipelinejava.domain;

/** 一次投递判定结果。 */
public enum DeliveryResult {
    SUCCESS,
    RETRYABLE_FAILURE,
    PERMANENT_FAILURE
}
