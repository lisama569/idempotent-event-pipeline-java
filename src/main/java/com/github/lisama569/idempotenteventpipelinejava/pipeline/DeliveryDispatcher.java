package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 后台投递调度器：接收与投递解耦的执行侧。
 *
 * <p>职责：
 * <ul>
 *   <li>每个分区一条内存队列，同一分区串行排空（保序），不同分区在线程池上并行；</li>
 *   <li>重试通过定时任务在退避到期后重新入队，不空转、不让线程睡眠；</li>
 *   <li>启动恢复时扫描存储中所有 PENDING 事件（含重启前处于重试等待的），
 *       按其持久化的下次尝试时间排期，重试次数与退避节奏延续。</li>
 * </ul>
 *
 * <p>某个分区卡在重试等待时，其队列只是暂时为空，不会阻塞其他分区的排空任务。
 */
@Component
public class DeliveryDispatcher {

    private static final Logger log = LoggerFactory.getLogger(DeliveryDispatcher.class);

    private final EventStore store;
    private final ScheduledExecutorService executor;
    private final Map<String, PartitionQueue> partitions = new ConcurrentHashMap<>();
    private volatile Consumer<String> deliveryAction;

    @Autowired
    public DeliveryDispatcher(EventStore store,
                              @Value("${pipeline.dispatcher.threads:4}") int threads) {
        this(store, Executors.newScheduledThreadPool(threads));
    }

    /** 测试用：注入自有线程池。 */
    public DeliveryDispatcher(EventStore store, ScheduledExecutorService executor) {
        this.store = store;
        this.executor = executor;
    }

    /** 由投递引擎在装配期注入单次投递动作（避免构造期循环依赖）。 */
    public void setDeliveryAction(Consumer<String> deliveryAction) {
        this.deliveryAction = deliveryAction;
    }

    /** 立即把一个到期事件放入其分区队列（由接收路径或重试定时任务调用）。 */
    public void enqueue(String eventId, String partitionKey) {
        PartitionQueue pq = partitions.computeIfAbsent(partitionKey, k -> new PartitionQueue());
        pq.queue.add(eventId);
        if (pq.running.compareAndSet(false, true)) {
            executor.execute(() -> drain(partitionKey, pq));
        }
    }

    /** 退避到期后再入队（定时任务，期间不占用任何线程）。 */
    public void enqueueDelayed(String eventId, String partitionKey, long delayMs) {
        executor.schedule(() -> enqueue(eventId, partitionKey),
                Math.max(0, delayMs), TimeUnit.MILLISECONDS);
    }

    /**
     * 启动恢复：把所有未走到终态的事件按持久化的下次尝试时间重新排期。
     * 重启前积累的尝试次数保留在存储中，退避节奏由此延续。
     */
    public void recover() {
        List<EventStore.StoredEvent> pending = store.listPending();
        long now = System.currentTimeMillis();
        for (EventStore.StoredEvent e : pending) {
            long delay = Math.max(0, e.nextAttemptAtMillis() - now);
            log.info("event={} partition={} attempts={} decision=RECOVER delayMs={}",
                    e.envelope().eventId(), e.envelope().partitionKey(), e.attempts(), delay);
            enqueueDelayed(e.envelope().eventId(), e.envelope().partitionKey(), delay);
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }

    /** 串行排空一个分区的到期事件；队列空即让出线程，再次入队时重新触发。 */
    private void drain(String partitionKey, PartitionQueue pq) {
        try {
            String eventId;
            while ((eventId = pq.queue.poll()) != null) {
                try {
                    deliveryAction.accept(eventId);
                } catch (RuntimeException e) {
                    // 单次投递异常不拖垮调度器；事件仍为 PENDING，
                    // 由重启恢复或后续重放兜底推进
                    log.error("event={} partition={} decision=DELIVER_ERROR {}",
                            eventId, partitionKey, e.toString());
                }
            }
        } finally {
            pq.running.set(false);
            // 排空与入队竞态：让出前再检查一次，避免事件滞留
            if (!pq.queue.isEmpty() && pq.running.compareAndSet(false, true)) {
                executor.execute(() -> drain(partitionKey, pq));
            }
        }
    }

    /** 分区队列：串行排空由 running 标志保证，同一分区同一时刻至多一个排空任务。 */
    private static final class PartitionQueue {
        final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
        final AtomicBoolean running = new AtomicBoolean();
    }
}
