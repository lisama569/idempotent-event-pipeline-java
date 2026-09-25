package com.github.lisama569.idempotenteventpipelinejava.service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.github.lisama569.idempotenteventpipelinejava.domain.DeliveryResult;
import com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason;
import com.github.lisama569.idempotenteventpipelinejava.model.DispatchDecision;
import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;
import com.github.lisama569.idempotenteventpipelinejava.repo.EventRepository;

/**
 * 投递调度器。
 *
 * <p>并发与保序模型（单实例本地）：</p>
 * <ul>
 *   <li>每个分区在 JVM 内有一把 {@code partitionLocks} 锁：同一分区严格串行处理，
 *       保证 seq 顺序；不同分区拿不同锁，可并行推进。</li>
 *   <li>DB 行上还有 CAS 占用令牌（locked_by）：即便同分区被多个线程误触，
 *       也只有一个执行器能进入处理；stale 锁在崩溃重启后可被回收。</li>
 *   <li>队头事件未到终态前，不拾取同分区后续 seq —— 失败重试/死信都会“顶住”队，
 *       保证分区内顺序结论确定。</li>
 *   <li>退避：nextVisibleAt = now + base * multiplier^(attempt-1)，仅对可重试失败；
 *       达到 maxAttempts 进入 DEAD_LETTER，原因 RETRY_EXHAUSTED；
 *       PERMANENT_FAILURE 立即进入 DEAD_LETTER，原因 PERMANENT_DELIVERY_FAILURE。</li>
 * </ul>
 */
@Service
public class EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(EventDispatcher.class);

    private final EventRepository repository;
    private final DeliveryHandler handler;
    private final PipelineProperties props;

    private final ConcurrentHashMap<String, Object> partitionLocks = new ConcurrentHashMap<>();
    private final String workerToken = UUID.randomUUID().toString();

    public EventDispatcher(EventRepository repository, DeliveryHandler handler, PipelineProperties props) {
        this.repository = repository;
        this.handler = handler;
        this.props = props;
    }

    /** 扫描所有活跃分区，每分区尝试推进一个队头事件；返回实际处理的事件数。 */
    public int processOnce() {
        List<String> partitions = repository.findActivePartitions();
        int processed = 0;
        for (String p : partitions) {
            processed += processPartitionOnce(p);
        }
        return processed;
    }

    /**
     * 推进单个分区一次：只处理队头事件。
     * 返回 1 表示产生了一次投递尝试，0 表示当前无可推进事件（空队列/退避未到期/被占用）。
     */
    public int processPartitionOnce(String partition) {
        Object lock = partitionLocks.computeIfAbsent(partition, k -> new Object());
        synchronized (lock) {
            EventRecord head = repository.findHeadForDispatch(partition);
            if (head == null) {
                return 0;
            }
            Instant staleBefore = Instant.now().minusMillis(props.getRecoveryLockStaleMillis());
            if (!repository.tryAcquire(head, workerToken, staleBefore)) {
                log.info("跳过 eventId={} partition={} seq={} attempt={} 判定依据=行锁被其它执行器占用",
                        head.eventId, partition, head.seqInPartition, head.attemptCount + 1);
                return 0;
            }
            // 占用成功后重新读取，确保 attempt/status 是最新值
            EventRecord acquired = repository.findByEventId(head.eventId);
            deliver(acquired);
            return 1;
        }
    }

    private void deliver(EventRecord event) {
        int attempt = event.attemptCount + 1;
        DispatchDecision decision;
        try {
            decision = handler.handle(event);
        } catch (RuntimeException e) {
            // handler 抛出的未预期异常按可重试失败处理，但不吞分类信息
            decision = new DispatchDecision();
            decision.result = DeliveryResult.RETRYABLE_FAILURE;
            decision.detail = "handler 抛出异常: " + e;
            log.warn("投递异常 eventId={} partition={} attempt={} 判定依据=异常按瞬时失败处理",
                    event.eventId, event.partition, attempt, e);
        }

        switch (decision.result) {
            case SUCCESS -> {
                repository.applySuccess(event, workerToken);
                log.info("状态流转 eventId={} partition={} seq={} attempt={} to=SUCCEEDED 判定依据=投递成功",
                        event.eventId, event.partition, event.seqInPartition, attempt);
            }
            case PERMANENT_FAILURE -> {
                repository.applyDeadLetter(event, workerToken,
                        RejectReason.PERMANENT_DELIVERY_FAILURE, decision.detail);
                log.info("状态流转 eventId={} partition={} seq={} attempt={} to=DEAD_LETTER 判定依据=永久失败，不重试",
                        event.eventId, event.partition, event.seqInPartition, attempt);
            }
            case RETRYABLE_FAILURE -> {
                if (attempt >= props.getMaxAttempts()) {
                    repository.applyDeadLetter(event, workerToken,
                            RejectReason.RETRY_EXHAUSTED,
                            "重试 " + attempt + " 次仍失败: " + decision.detail);
                    log.info("状态流转 eventId={} partition={} seq={} attempt={} to=DEAD_LETTER 判定依据=重试耗尽(maxAttempts={})",
                            event.eventId, event.partition, event.seqInPartition, attempt,
                            props.getMaxAttempts());
                } else {
                    long backoff = backoffMillis(attempt);
                    repository.applyRetry(event, workerToken, Instant.now().plusMillis(backoff));
                    log.info("状态流转 eventId={} partition={} seq={} attempt={} to=RETRYING nextVisibleIn={}ms 判定依据=可重试失败退避",
                            event.eventId, event.partition, event.seqInPartition, attempt, backoff);
                }
            }
        }
    }

    /** 确定性退避：base * multiplier^(attempt-1)。 */
    long backoffMillis(int attempt) {
        double v = props.getBackoff().getBaseMillis()
                * Math.pow(props.getBackoff().getMultiplier(), attempt - 1);
        return Math.max(0L, (long) v);
    }
}
