package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 接收侧入口（已与传统投递解耦）。
 *
 * <p>语义：
 * <ul>
 *   <li>提交只做“校验 + 持久化 + 通知后台调度”，立即返回 {@link SubmissionAck}，
 *       调用方不再被投递与重试阻塞；</li>
 *   <li>幂等：同一 eventId 重复提交返回既有结论，不产生新副作用；</li>
 *   <li>真正的投递（保序、重试、死信）由 {@link DeliveryScheduler} 在后台推进。</li>
 * </ul>
 */
@Component
public class DeliveryEngine {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEngine.class);

    private final EventStore store;
    private final EventValidator validator;
    private final DeliveryScheduler scheduler;

    public DeliveryEngine(EventStore store,
                          EventValidator validator,
                          DeliveryScheduler scheduler) {
        this.store = store;
        this.validator = validator;
        this.scheduler = scheduler;
    }

    /** 接收一个事件：尽快返回回执，投递由后台继续推进。 */
    public SubmissionAck submit(EventEnvelope raw) {
        RejectionReason invalid = validator.validate(raw);
        if (invalid != null) {
            store.saveIfAbsent(raw);
            store.updateStatus(raw.eventId(), EventStore.EventStatus.REJECTED,
                    0, invalid, 0L);
            log.info("event={} partition={} attempts=0 decision=REJECTED reason={}",
                    raw.eventId(), raw.partitionKey(), invalid);
            return new SubmissionAck(raw.eventId(), SubmissionAck.Decision.REJECTED,
                    EventStore.EventStatus.REJECTED, invalid);
        }
        EventEnvelope event = validator.applyDefaults(raw);

        EventStore.StoredEvent stored = store.saveIfAbsent(event);
        if (stored.status() != EventStore.EventStatus.PENDING) {
            // 幂等：已有终态结论，重复提交直接返回原结论，不产生新副作用
            log.info("event={} partition={} attempts={} decision=DUPLICATE status={}",
                    event.eventId(), event.partitionKey(), stored.attempts(), stored.status());
            return new SubmissionAck(event.eventId(), SubmissionAck.Decision.DUPLICATE,
                    stored.status(), stored.rejectionReason());
        }

        scheduler.kick(event.partitionKey());
        log.info("event={} partition={} decision=ACCEPTED (delivery continues in background)",
                event.eventId(), event.partitionKey());
        return new SubmissionAck(event.eventId(), SubmissionAck.Decision.ACCEPTED,
                EventStore.EventStatus.PENDING, null);
    }
}
