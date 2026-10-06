package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 死信查询与重放。
 *
 * <p>重放把死信事件重置为 PENDING（尝试次数清零）后通知后台调度重新走
 * 正常投递路径，因此不绕过幂等与保序约束；已成功处理的事件状态为
 * DELIVERED，接收侧的幂等分支保证其不会再次产生副作用。
 */
@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

    private final EventStore store;
    private final DeliveryScheduler scheduler;

    public DeadLetterService(EventStore store, DeliveryScheduler scheduler) {
        this.store = store;
        this.scheduler = scheduler;
    }

    /** 查询死信，按 (partitionKey, sequence) 原顺序返回。 */
    public List<EventStore.StoredEvent> list() {
        return store.listDeadLetters();
    }

    /** 重放指定死信事件：重置为待投递并通知后台调度，返回接收回执。 */
    public SubmissionAck replay(String eventId) {
        EventStore.StoredEvent dead = store.find(eventId)
                .orElseThrow(() -> new IllegalArgumentException("unknown eventId: " + eventId));
        if (dead.status() != EventStore.EventStatus.DEAD) {
            throw new IllegalStateException("event " + eventId + " is not DEAD: " + dead.status());
        }
        store.updateStatus(eventId, EventStore.EventStatus.PENDING, 0, null, 0L);
        scheduler.kick(dead.envelope().partitionKey());
        log.info("event={} partition={} attempts=0 decision=REPLAY from=DEAD",
                dead.envelope().eventId(), dead.envelope().partitionKey());
        return new SubmissionAck(eventId, SubmissionAck.Decision.ACCEPTED,
                EventStore.EventStatus.PENDING, null);
    }
}
