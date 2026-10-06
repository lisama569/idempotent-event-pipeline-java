package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 后台投递调度器：接收与投递解耦后，由它在后台推进投递。
 *
 * <p>语义：
 * <ul>
 *   <li>每个分区同一时刻至多一个推进任务，分区内严格按序号投递；</li>
 *   <li>不同分区并行推进，某分区处于重试等待不阻塞其他分区；</li>
 *   <li>重试等待通过定时唤醒实现，不空转、不占用线程睡眠；</li>
 *   <li>启动时扫描未完结事件自动恢复推进，重试次数与退避节奏延续。</li>
 * </ul>
 */
@Component
public class DeliveryScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DeliveryScheduler.class);

    private final EventStore store;
    private final EventHandler handler;
    private final int maxAttempts;
    private final long baseBackoffMs;
    private final ScheduledExecutorService executor;
    private final Set<String> running = ConcurrentHashMap.newKeySet();

    public DeliveryScheduler(EventStore store,
                             EventHandler handler,
                             @Value("${pipeline.retry.max-attempts:3}") int maxAttempts,
                             @Value("${pipeline.retry.base-backoff-ms:50}") long baseBackoffMs,
                             @Value("${pipeline.scheduler.threads:4}") int threads) {
        this.store = store;
        this.handler = handler;
        this.maxAttempts = maxAttempts;
        this.baseBackoffMs = baseBackoffMs;
        this.executor = Executors.newScheduledThreadPool(Math.max(1, threads));
    }

    /** 启动恢复：扫描所有未完结（PENDING）事件，按分区触发后台推进。 */
    @PostConstruct
    public void start() {
        var pending = store.listPending();
        pending.stream().map(se -> se.envelope().partitionKey()).distinct()
                .forEach(this::kick);
        if (!pending.isEmpty()) {
            log.info("recovery: resumed {} pending event(s) across partitions", pending.size());
        }
    }

    /** 通知某分区可能有可推进的事件（幂等，可重复调用）。 */
    public void kick(String partitionKey) {
        if (running.add(partitionKey)) {
            executor.execute(() -> drain(partitionKey));
        }
    }

    /**
     * 推进一个分区：循环取出分区内序号最小且已到期的未完结事件逐次投递，
     * 直到没有到期事件；若队首事件处于重试等待，则按 nextAttemptAt 定时唤醒。
     * 分区内串行由 {@link #running} 保证，无需额外线程睡眠。
     */
    private void drain(String partitionKey) {
        try {
            while (true) {
                var next = store.nextPendingInPartition(partitionKey);
                if (next.isEmpty()) {
                    return;
                }
                EventStore.StoredEvent head = next.get();
                long now = System.currentTimeMillis();
                if (head.nextAttemptAtMillis() > now) {
                    // 重试等待中：定时唤醒，不空转、不占线程
                    executor.schedule(() -> kick(partitionKey),
                            head.nextAttemptAtMillis() - now,
                            java.util.concurrent.TimeUnit.MILLISECONDS);
                    return;
                }
                attempt(head);
            }
        } finally {
            running.remove(partitionKey);
            // 防丢失唤醒：移除运行标记后若已有到期事件（移除前被跳过的 kick），重新触发
            store.nextPendingInPartition(partitionKey)
                    .filter(se -> se.nextAttemptAtMillis() <= System.currentTimeMillis())
                    .ifPresent(se -> kick(partitionKey));
        }
    }

    /** 对单个事件做一次投递尝试，并按结果推进状态机。 */
    private void attempt(EventStore.StoredEvent stored) {
        EventEnvelope event = stored.envelope();
        String eventId = event.eventId();
        String partition = event.partitionKey();

        long expected = store.lastDeliveredSequence(partition) + 1;
        if (event.sequence() != expected) {
            store.updateStatus(eventId, EventStore.EventStatus.REJECTED,
                    stored.attempts(), RejectionReason.OUT_OF_ORDER_SEQUENCE, 0L);
            log.info("event={} partition={} attempts={} decision=REJECTED "
                            + "reason=OUT_OF_ORDER_SEQUENCE expected={} actual={}",
                    eventId, partition, stored.attempts(), expected, event.sequence());
            return;
        }

        int attempts = stored.attempts() + 1;
        ProcessingOutcome outcome;
        try {
            outcome = handler.handle(event);
        } catch (RuntimeException e) {
            log.warn("event={} partition={} attempts={} handler threw, treated as retryable",
                    eventId, partition, attempts, e);
            outcome = ProcessingOutcome.RETRYABLE_FAILURE;
        }
        switch (outcome) {
            case SUCCESS -> {
                store.updateStatus(eventId, EventStore.EventStatus.DELIVERED,
                        attempts, null, 0L);
                log.info("event={} partition={} attempts={} decision=DELIVERED",
                        eventId, partition, attempts);
            }
            case REJECTED -> {
                store.updateStatus(eventId, EventStore.EventStatus.REJECTED,
                        attempts, RejectionReason.BUSINESS_REJECTED, 0L);
                log.info("event={} partition={} attempts={} decision=REJECTED "
                        + "reason=BUSINESS_REJECTED", eventId, partition, attempts);
            }
            case RETRYABLE_FAILURE -> {
                if (attempts >= maxAttempts) {
                    store.updateStatus(eventId, EventStore.EventStatus.DEAD,
                            attempts, null, 0L);
                    log.info("event={} partition={} attempts={} decision=DEAD "
                                    + "reason=RETRY_EXHAUSTED maxAttempts={}",
                            eventId, partition, attempts, maxAttempts);
                } else {
                    long backoff = baseBackoffMs * (1L << (attempts - 1));
                    long nextAt = System.currentTimeMillis() + backoff;
                    store.updateStatus(eventId, EventStore.EventStatus.PENDING,
                            attempts, null, nextAt);
                    log.info("event={} partition={} attempts={} decision=RETRY backoffMs={}",
                            eventId, partition, attempts, backoff);
                }
            }
        }
    }

    @Override
    @PreDestroy
    public void close() {
        executor.shutdownNow();
    }
}
