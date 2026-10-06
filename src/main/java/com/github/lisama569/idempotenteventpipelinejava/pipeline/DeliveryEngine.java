package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import jakarta.annotation.PostConstruct;
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
 *   <li>接收与投递解耦：{@link #submit} 只做校验、幂等落库与入队，
 *       立即返回 {@link ReceiveOutcome#ACCEPTED}，真正的投递由
 *       {@link DeliveryDispatcher} 在后台调用 {@link #deliver} 推进；</li>
 *   <li>至少一次投递 + 幂等：同一 eventId 重复提交不会产生第二次业务副作用；</li>
 *   <li>同一分区内按 sequence 严格保序（分区锁串行化），不同分区并行推进；</li>
 *   <li>可重试失败按指数退避重试（定时再入队，不占用线程睡眠），
 *       超过上限进入死信（DEAD）；重试次数持久化，重启后延续；</li>
 *   <li>不可重试拒绝（校验失败 / 业务拒绝 / 乱序）给出可区分原因，不静默丢弃。</li>
 * </ul>
 */
@Component
public class DeliveryEngine {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEngine.class);

    private final EventStore store;
    private final EventValidator validator;
    private final EventHandler handler;
    private final DeliveryDispatcher dispatcher;
    private final int maxAttempts;
    private final long baseBackoffMs;
    private final Map<String, Object> partitionLocks = new ConcurrentHashMap<>();

    public DeliveryEngine(EventStore store,
                          EventValidator validator,
                          EventHandler handler,
                          DeliveryDispatcher dispatcher,
                          @Value("${pipeline.retry.max-attempts:3}") int maxAttempts,
                          @Value("${pipeline.retry.base-backoff-ms:50}") long baseBackoffMs) {
        this.store = store;
        this.validator = validator;
        this.handler = handler;
        this.dispatcher = dispatcher;
        this.maxAttempts = maxAttempts;
        this.baseBackoffMs = baseBackoffMs;
    }

    /** 装配投递动作并触发重启恢复（调度器回调与恢复都依赖本引擎就绪）。 */
    @PostConstruct
    public void init() {
        dispatcher.setDeliveryAction(this::deliver);
        dispatcher.recover();
    }

    /**
     * 接收一个事件：校验、幂等落库、保序检查后立即返回，
     * 投递由后台调度器推进。
     */
    public ReceiveOutcome submit(EventEnvelope raw) {
        RejectionReason invalid = validator.validate(raw);
        if (invalid != null) {
            store.saveIfAbsent(raw);
            store.updateStatus(raw.eventId(), EventStore.EventStatus.REJECTED, 0, invalid, 0L);
            log.info("event={} partition={} attempts=0 decision=REJECTED reason={}",
                    raw.eventId(), raw.partitionKey(), invalid);
            return ReceiveOutcome.REJECTED;
        }
        EventEnvelope event = validator.applyDefaults(raw);

        Object lock = partitionLocks.computeIfAbsent(event.partitionKey(), k -> new Object());
        synchronized (lock) {
            EventStore.StoredEvent stored = store.saveIfAbsent(event);
            if (stored.status() != EventStore.EventStatus.PENDING) {
                // 幂等：已有终态结论，重复投递直接返回原结论，不产生新副作用
                ReceiveOutcome outcome = stored.status() == EventStore.EventStatus.DELIVERED
                        ? ReceiveOutcome.DELIVERED : ReceiveOutcome.REJECTED;
                log.info("event={} partition={} attempts={} decision=DUPLICATE status={} -> {}",
                        event.eventId(), event.partitionKey(), stored.attempts(),
                        stored.status(), outcome);
                return outcome;
            }
            long expected = store.lastDeliveredSequence(event.partitionKey()) + 1;
            if (event.sequence() != expected) {
                store.updateStatus(event.eventId(), EventStore.EventStatus.REJECTED,
                        0, RejectionReason.OUT_OF_ORDER_SEQUENCE, 0L);
                log.info("event={} partition={} attempts=0 decision=REJECTED "
                                + "reason=OUT_OF_ORDER_SEQUENCE expected={} actual={}",
                        event.eventId(), event.partitionKey(), expected, event.sequence());
                return ReceiveOutcome.REJECTED;
            }
            // 仍在 PENDING：新受理（或重放重置 / 重复提交在途事件）。
            // 重复入队无害：deliver 在分区锁内复查状态，已出终态的会直接跳过。
            dispatcher.enqueue(event.eventId(), event.partitionKey());
            log.info("event={} partition={} attempts={} decision=ACCEPTED",
                    event.eventId(), event.partitionKey(), stored.attempts());
            return ReceiveOutcome.ACCEPTED;
        }
    }

    /**
     * 单次投递尝试（由调度器在事件到期时调用）。
     *
     * <p>在分区锁内复查状态并推进：成功 / 拒绝写终态；可重试失败更新
     * 尝试次数与下次尝试时间后，交给调度器定时再入队（不睡眠、不空转）。
     */
    public void deliver(String eventId) {
        EventStore.StoredEvent stored = store.find(eventId).orElse(null);
        if (stored == null || stored.status() != EventStore.EventStatus.PENDING) {
            return;
        }
        EventEnvelope event = stored.envelope();
        Object lock = partitionLocks.computeIfAbsent(event.partitionKey(), k -> new Object());
        synchronized (lock) {
            EventStore.StoredEvent current = store.find(eventId).orElseThrow();
            if (current.status() != EventStore.EventStatus.PENDING) {
                // 重复入队或已被其他路径推进出终态：跳过，不产生副作用
                return;
            }
            long now = System.currentTimeMillis();
            if (current.nextAttemptAtMillis() > now) {
                // 退避未到期（如重试等待期间被重复提交触发）：按剩余时间重新排期
                dispatcher.enqueueDelayed(eventId, event.partitionKey(),
                        current.nextAttemptAtMillis() - now);
                return;
            }
            long expected = store.lastDeliveredSequence(event.partitionKey()) + 1;
            if (event.sequence() != expected) {
                store.updateStatus(eventId, EventStore.EventStatus.REJECTED,
                        current.attempts(), RejectionReason.OUT_OF_ORDER_SEQUENCE, 0L);
                log.info("event={} partition={} attempts={} decision=REJECTED "
                                + "reason=OUT_OF_ORDER_SEQUENCE expected={} actual={}",
                        eventId, event.partitionKey(), current.attempts(),
                        expected, event.sequence());
                return;
            }
            int attempts = current.attempts() + 1;
            ProcessingOutcome outcome = handler.handle(event);
            switch (outcome) {
                case SUCCESS -> {
                    store.updateStatus(eventId, EventStore.EventStatus.DELIVERED,
                            attempts, null, 0L);
                    log.info("event={} partition={} attempts={} decision=DELIVERED",
                            eventId, event.partitionKey(), attempts);
                }
                case REJECTED -> {
                    store.updateStatus(eventId, EventStore.EventStatus.REJECTED,
                            attempts, RejectionReason.BUSINESS_REJECTED, 0L);
                    log.info("event={} partition={} attempts={} decision=REJECTED "
                                    + "reason=BUSINESS_REJECTED",
                            eventId, event.partitionKey(), attempts);
                }
                case RETRYABLE_FAILURE -> {
                    if (attempts >= maxAttempts) {
                        store.updateStatus(eventId, EventStore.EventStatus.DEAD,
                                attempts, null, 0L);
                        log.info("event={} partition={} attempts={} decision=DEAD "
                                        + "reason=RETRY_EXHAUSTED maxAttempts={}",
                                eventId, event.partitionKey(), attempts, maxAttempts);
                        return;
                    }
                    long backoff = baseBackoffMs * (1L << (attempts - 1));
                    long nextAt = System.currentTimeMillis() + backoff;
                    store.updateStatus(eventId, EventStore.EventStatus.PENDING,
                            attempts, null, nextAt);
                    log.info("event={} partition={} attempts={} decision=RETRY "
                                    + "backoffMs={} nextAttemptAt={}",
                            eventId, event.partitionKey(), attempts, backoff, nextAt);
                    dispatcher.enqueueDelayed(eventId, event.partitionKey(), backoff);
                }
            }
        }
    }
}
