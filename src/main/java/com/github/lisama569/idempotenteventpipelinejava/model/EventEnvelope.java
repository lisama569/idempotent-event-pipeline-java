package com.github.lisama569.idempotenteventpipelinejava.model;

/** 已解析、已校验的事件信封（v1 字段）。 */
public class EventEnvelope {
    public int version;
    public String eventId;
    public String partition;
    public String type;
    public String payload;
    public int priority;
}
