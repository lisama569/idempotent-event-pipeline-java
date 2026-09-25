package com.github.lisama569.idempotenteventpipelinejava.domain;

/** 事件生命周期状态。 */
public enum EventStatus {
    RECEIVED,
    IN_FLIGHT,
    SUCCEEDED,
    RETRYING,
    DEAD_LETTER,
    PERMANENT_REJECT
}
