package com.github.lisama569.idempotenteventpipelinejava.model;

import java.time.Instant;

import com.github.lisama569.idempotenteventpipelinejava.domain.EventStatus;
import com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason;

/** 事件持久化行。 */
public class EventRecord {
    public long id;
    public String eventId;
    public String partition;
    public long seqInPartition;
    public int version;
    public String type;
    public String payloadJson;
    public int priority;
    public EventStatus status;
    public int attemptCount;
    public Instant nextVisibleAt;
    public String lockedBy;
    public Instant lockedAt;
    public RejectReason rejectReason;
    public String rejectDetail;
    public Instant createdAt;
    public Instant updatedAt;

    public EventRecord() {
    }

    /** 接收落库时使用的构造器。 */
    public EventRecord(String eventId, String partition, long seqInPartition,
                       int version, String type, String payloadJson, int priority) {
        this.eventId = eventId;
        this.partition = partition;
        this.seqInPartition = seqInPartition;
        this.version = version;
        this.type = type;
        this.payloadJson = payloadJson;
        this.priority = priority;
        this.status = EventStatus.RECEIVED;
    }
}
