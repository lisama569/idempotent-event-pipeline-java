package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 投递引擎。
 *
 * <p>语义：
 * <ul>
 *   <li>至少一次投递 + 幂等：同一 eventId 重复提交不会产生第二次业务副作用；</li>
 *   <li>同一分区内按 sequence 严格保序（分区锁串行化），不同分区并行推进；</li>
 *   <li>可重试失败按指数退避重试，超过上限进入死信（DEAD）；</li>
 *   <li>不可重试拒绝（校验失败 / 业务拒绝 / 乱序）给出可区分原因，不静默丢弃。</li>
 * </ul>
 */
@Component
public class DeliveryEngine {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEngine.class);

    private final EventStore store;
    private final EventValidator validator;
    private final EventHandler handler;
    private final int maxAttempts;
    private final long baseBackoffMs;
    private final Map<String, Object> partitionLocks = new ConcurrentHashMap<>();

    public DeliveryEngine(EventStore store,
                          EventValidator validator,
                          EventHandler handler,
                          @Value("${pipeline.retry.max-attempts:3}") int maxAttempts,
                          @Value("${pipeline.retry.base-backoff-ms:50}") long baseBackoffMs) {
        this.store = store;
        this.validator = validator;
        this.handler = handler;
        this.maxAttempts = maxAttempts;
        this.baseBackoffMs = baseBackoffMs;
    }

    /** 接收并投递一个事件，返回最终判定。 */
    public ProcessingOutcome submit(EventEnvelope raw) {
        RejectionReason invalid = validator.validate(raw);
        if (invalid != null) {
            store.saveIfAbsent(raw);
            store.updateStatus(raw.eventId(), EventStore.EventStatus.REJECTED, 0, invalid, 0L);
            log.info("event={} partition={} attempts=0 decision=REJECTED reason={}",
                    raw.eventId(), raw.partitionKey(), invalid);
            return ProcessingOutcome.REJECTED;
        }
        EventEnvelope event = validator.applyDefaults(raw);

        EventStore.StoredEvent stored = store.saveIfAbsent(event);
        if (stored.status() != EventStore.EventStatus.PENDING) {
            // 幂等：已有终态结论，重复投递直接返回原结论，不产生新副作用
            ProcessingOutcome outcome = stored.status() == EventStore.EventStatus.DELIVERED
                    ? ProcessingOutcome.SUCCESS : ProcessingOutcome.REJECTED;
            log.info("event={} partition={} attempts={} decision=DUPLICATE status={} -> {}",
                    event.eventId(), event.partitionKey(), stored.attempts(),
                    stored.status(), outcome);
            return outcome;
        }

        Object lock = partitionLocks.computeIfAbsent(event.partitionKey(), k -> new Object());
        synchronized (lock) {
            // 并发重复提交：进入分区锁后复查，已被其他线程处理出终态则直接返回原结论
            EventStore.StoredEvent current = store.find(event.eventId()).orElseThrow();
            if (current.status() != EventStore.EventStatus.PENDING) {
                ProcessingOutcome outcome = current.status() == EventStore.EventStatus.DELIVERED
                        ? ProcessingOutcome.SUCCESS : ProcessingOutcome.REJECTED;
                log.info("event={} partition={} attempts={} decision=DUPLICATE status={} -> {}",
                        event.eventId(), event.partitionKey(), current.attempts(),
                        current.status(), outcome);
                return outcome;
            }
            long expected = store.lastDeliveredSequence(event.partitionKey()) + 1;
            if (event.sequence() != expected) {
                store.updateStatus(event.eventId(), EventStore.EventStatus.REJECTED,
                        0, RejectionReason.OUT_OF_ORDER_SEQUENCE, 0L);
                log.info("event={} partition={} attempts=0 decision=REJECTED "
                                + "reason=OUT_OF_ORDER_SEQUENCE expected={} actual={}",
                        event.eventId(), event.partitionKey(), expected, event.sequence());
                return ProcessingOutcome.REJECTED;
            }
            return deliverWithRetry(event, stored.attempts());
        }
    }

    private ProcessingOutcome deliverWithRetry(EventEnvelope event, int priorAttempts) {
        int attempts = priorAttempts;
        while (true) {
            attempts++;
            ProcessingOutcome outcome = handler.handle(event);
            switch (outcome) {
                case SUCCESS -> {
                    store.updateStatus(event.eventId(), EventStore.EventStatus.DELIVERED,
                            attempts, null, 0L);
                    log.info("event={} partition={} attempts={} decision=DELIVERED",
                            event.eventId(), event.partitionKey(), attempts);
                    return ProcessingOutcome.SUCCESS;
                }
                case REJECTED -> {
                    store.updateStatus(event.eventId(), EventStore.EventStatus.REJECTED,
                            attempts, RejectionReason.BUSINESS_REJECTED, 0L);
                    log.info("event={} partition={} attempts={} decision=REJECTED "
                                    + "reason=BUSINESS_REJECTED",
                            event.eventId(), event.partitionKey(), attempts);
                    return ProcessingOutcome.REJECTED;
                }
                case RETRYABLE_FAILURE -> {
                    if (attempts >= maxAttempts) {
                        store.updateStatus(event.eventId(), EventStore.EventStatus.DEAD,
                                attempts, null, 0L);
                        log.info("event={} partition={} attempts={} decision=DEAD "
                                        + "reason=RETRY_EXHAUSTED maxAttempts={}",
                                event.eventId(), event.partitionKey(), attempts, maxAttempts);
                        return ProcessingOutcome.RETRYABLE_FAILURE;
                    }
                    long backoff = baseBackoffMs * (1L << (attempts - 1));
                    long nextAt = System.currentTimeMillis() + backoff;
                    store.updateStatus(event.eventId(), EventStore.EventStatus.PENDING,
                            attempts, null, nextAt);
                    log.info("event={} partition={} attempts={} decision=RETRY "
                                    + "backoffMs={}",
                            event.eventId(), event.partitionKey(), attempts, backoff);
                    sleep(backoff);
                }
            }
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
