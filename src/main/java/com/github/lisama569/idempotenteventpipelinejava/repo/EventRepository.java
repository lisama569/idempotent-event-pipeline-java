package com.github.lisama569.idempotenteventpipelinejava.repo;

import java.util.List;

import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;

/** 事件表持久化操作。 */
public interface EventRepository {

    boolean existsByEventId(String eventId);

    EventRecord insertIfAbsent(EventRecord record);

    EventRecord findByEventId(String eventId);

    long nextSequence(String partition);

    List<String> findActivePartitions();

    /** 取出某分区当前可推进的队头（未成功、未被其它执行器占用）。 */
    EventRecord findHeadForDispatch(String partition);

    boolean tryAcquire(EventRecord record, String workerToken, java.time.Instant lockAcquiredBefore);

    void applySuccess(EventRecord record, String workerToken);

    void applyRetry(EventRecord record, String workerToken, java.time.Instant nextVisibleAt);

    void applyDeadLetter(EventRecord record, String workerToken,
                         com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason reason,
                         String detail);

    List<EventRecord> findDeadLetters(String partition);

    int replayDeadLetter(long id, String workerToken);
}
