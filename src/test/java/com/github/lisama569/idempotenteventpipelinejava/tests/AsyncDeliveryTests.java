package com.github.lisama569.idempotenteventpipelinejava.tests;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeadLetterService;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeliveryEngine;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeliveryProgress;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeliveryProgressService;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeliveryScheduler;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventEnvelope;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventHandler;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventValidator;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.ProcessingOutcome;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.RejectionReason;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.SubmissionAck;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.FileEventStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 新能力单测：接收与投递解耦、重启自愈、进度查询。
 * 日志可见事件标识、分区、投递次数与判定依据。
 */
class AsyncDeliveryTests {

    @TempDir
    Path dir;

    private static EventEnvelope event(String id, String partition, long seq) {
        return new EventEnvelope(id, partition, seq, 2,
                Map.of("type", "ORDER", "amount", 10), Map.of());
    }

    static class TestHandler implements EventHandler {
        final AtomicInteger effects = new AtomicInteger();
        final ConcurrentLinkedQueue<String> deliveryOrder = new ConcurrentLinkedQueue<>();
        volatile Function<EventEnvelope, ProcessingOutcome> behavior =
                e -> ProcessingOutcome.SUCCESS;

        @Override
        public ProcessingOutcome handle(EventEnvelope event) {
            ProcessingOutcome outcome = behavior.apply(event);
            if (outcome == ProcessingOutcome.SUCCESS) {
                effects.incrementAndGet();
                deliveryOrder.add(event.eventId());
            }
            return outcome;
        }
    }

    private record Rig(FileEventStore store, TestHandler handler,
                       DeliveryScheduler scheduler, DeliveryEngine engine,
                       DeadLetterService deadLetters,
                       DeliveryProgressService progress) implements AutoCloseable {
        @Override
        public void close() {
            scheduler.close();
        }
    }

    private Rig newRig(Path storeDir, int maxAttempts, long baseBackoffMs) {
        FileEventStore store = new FileEventStore(storeDir.toString());
        TestHandler handler = new TestHandler();
        DeliveryScheduler scheduler =
                new DeliveryScheduler(store, handler, maxAttempts, baseBackoffMs, 4);
        scheduler.start();
        DeliveryEngine engine = new DeliveryEngine(store, new EventValidator(), scheduler);
        return new Rig(store, handler, scheduler, engine,
                new DeadLetterService(store, scheduler),
                new DeliveryProgressService(store));
    }

    private Rig newRig(Path storeDir) {
        return newRig(storeDir, 3, 10);
    }

    static EventStore.StoredEvent awaitStatus(EventStore store, String eventId,
                                              EventStore.EventStatus expected) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            var cur = store.find(eventId);
            if (cur.isPresent() && cur.get().status() == expected) {
                return cur.get();
            }
            sleep(5);
        }
        return fail("timed out waiting for " + eventId + " to reach " + expected
                + ", current=" + store.find(eventId));
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---------- 接收与投递解耦 ----------

    @Test
    void submitReturnsImmediatelyAndBackgroundDeliverySucceeds() {
        // 退避 3 秒：若同步等待重试，调用方至少被卡 3+6 秒；解耦后应立即返回
        try (Rig rig = newRig(dir, 3, 3_000)) {
            rig.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
            long t0 = System.currentTimeMillis();
            SubmissionAck ack = rig.engine().submit(event("a1", "p1", 1));
            long elapsed = System.currentTimeMillis() - t0;
            assertEquals(SubmissionAck.Decision.ACCEPTED, ack.decision());
            assertTrue(elapsed < 3_000,
                    "提交不应等待投递与重试，耗时=" + elapsed + "ms");
            // 后台完成首次尝试后进入重试等待（而不是阻塞调用方）
            awaitAttempts(rig.store(), "a1", 1);
            // 修复故障：退避到期后后台自动继续推进并最终成功
            rig.handler().behavior = e -> ProcessingOutcome.SUCCESS;
            awaitStatus(rig.store(), "a1", EventStore.EventStatus.DELIVERED);
            assertEquals(1, rig.handler().effects.get(), "副作用必须恰好一次");
        }
    }

    @Test
    void backgroundDeliveryExhaustsRetriesAndGoesDead() {
        try (Rig rig = newRig(dir, 3, 5)) {
            rig.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
            rig.engine().submit(event("a2", "p1", 1));
            EventStore.StoredEvent dead =
                    awaitStatus(rig.store(), "a2", EventStore.EventStatus.DEAD);
            assertEquals(3, dead.attempts());
            assertEquals(0, rig.handler().effects.get());
            assertEquals(1, rig.deadLetters().list().size());
        }
    }

    private static void awaitAttempts(EventStore store, String eventId, int attempts) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            var cur = store.find(eventId);
            if (cur.isPresent() && cur.get().attempts() >= attempts) {
                return;
            }
            sleep(5);
        }
        fail("timed out waiting attempts>=" + attempts + " for " + eventId
                + ", current=" + store.find(eventId));
    }

    // ---------- 重启自愈 ----------

    @Test
    void restartResumesNeverAttemptedPendingEvent() {
        // 事件已接收持久化，但进程在后台投递前“宕机”
        FileEventStore storeOnly = new FileEventStore(dir.toString());
        storeOnly.saveIfAbsent(event("p1", "pa", 1));
        assertEquals(EventStore.EventStatus.PENDING,
                storeOnly.find("p1").orElseThrow().status());

        // 重启：恢复扫描应自动接续投递，无需人工重新提交
        try (Rig rig = newRig(dir)) {
            EventStore.StoredEvent delivered =
                    awaitStatus(rig.store(), "p1", EventStore.EventStatus.DELIVERED);
            assertEquals(1, delivered.attempts());
            assertEquals(1, rig.handler().effects.get());
        }
    }

    @Test
    void restartResumesRetryWaitWithAttemptsPreserved() {
        // 第一次运行：首次尝试失败，进入重试等待（退避 2 秒，保证重启时仍在等待中）
        try (Rig first = newRig(dir, 3, 2_000)) {
            first.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
            first.engine().submit(event("p2", "pa", 1));
            awaitAttempts(first.store(), "p2", 1);
            EventStore.StoredEvent waiting = first.store().find("p2").orElseThrow();
            assertEquals(EventStore.EventStatus.PENDING, waiting.status());
            assertTrue(waiting.nextAttemptAtMillis() > System.currentTimeMillis(),
                    "重启前应处于重试等待中");
            assertEquals(0, first.handler().effects.get());
        }

        // 重启：重试次数接着算（不清零），退避到期后由后台完成投递
        try (Rig second = newRig(dir, 3, 1)) {
            second.handler().behavior = e -> ProcessingOutcome.SUCCESS;
            EventStore.StoredEvent delivered =
                    awaitStatus(second.store(), "p2", EventStore.EventStatus.DELIVERED);
            assertEquals(2, delivered.attempts(),
                    "重启前的 1 次尝试必须累计，不能清零重算");
            assertEquals(1, second.handler().effects.get(),
                    "重启前后副作用总数必须恰好一次");
        }
    }

    @Test
    void restartResumesRetryWaitUntilDeadWithConsistentConclusion() {
        try (Rig first = newRig(dir, 3, 2_000)) {
            first.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
            first.engine().submit(event("p3", "pa", 1));
            awaitAttempts(first.store(), "p3", 1);
        }
        // 重启后故障依旧：从第 2 次继续重试，耗尽后进死信，总次数 = 3
        try (Rig second = newRig(dir, 3, 1)) {
            second.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
            EventStore.StoredEvent dead =
                    awaitStatus(second.store(), "p3", EventStore.EventStatus.DEAD);
            assertEquals(3, dead.attempts(), "重启前后累计尝试次数应等于上限");
            assertEquals(0, second.handler().effects.get());
            assertEquals(1, second.deadLetters().list().size());
        }
    }

    // ---------- 并发：同分区保序、分区间并行 ----------

    @Test
    void slowPartitionDoesNotBlockOtherPartitions() throws InterruptedException {
        try (Rig rig = newRig(dir)) {
            CountDownLatch slowEntered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            rig.handler().behavior = e -> {
                if (e.partitionKey().equals("slow")) {
                    slowEntered.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
                return ProcessingOutcome.SUCCESS;
            };
            rig.engine().submit(event("s1", "slow", 1));
            assertTrue(slowEntered.await(10, TimeUnit.SECONDS),
                    "slow 分区应已进入投递");
            // slow 分区卡在投递里时，其他分区必须能独立推进
            rig.engine().submit(event("f1", "fast", 1));
            awaitStatus(rig.store(), "f1", EventStore.EventStatus.DELIVERED);
            assertEquals(EventStore.EventStatus.PENDING,
                    rig.store().find("s1").orElseThrow().status(),
                    "slow 分区仍在投递中，不应被快进");
            release.countDown();
            awaitStatus(rig.store(), "s1", EventStore.EventStatus.DELIVERED);
            assertEquals(2, rig.handler().effects.get());
        }
    }

    @Test
    void concurrentSubmissionKeepsPerPartitionOrder() throws InterruptedException {
        try (Rig rig = newRig(dir)) {
            int partitions = 3, perPartition = 10;
            ExecutorService pool = Executors.newFixedThreadPool(partitions);
            CountDownLatch start = new CountDownLatch(1);
            for (int p = 0; p < partitions; p++) {
                String partition = "pp" + p;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 1; i <= perPartition; i++) {
                            rig.engine().submit(
                                    event(partition + "-" + i, partition, i));
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            for (int p = 0; p < partitions; p++) {
                awaitStatus(rig.store(), "pp" + p + "-" + perPartition,
                        EventStore.EventStatus.DELIVERED);
            }
            // 每个分区内部的投递顺序必须严格等于序号顺序
            List<String> order = List.copyOf(rig.handler().deliveryOrder);
            assertEquals(partitions * perPartition, order.size());
            for (int p = 0; p < partitions; p++) {
                String partition = "pp" + p;
                List<String> perPart = order.stream()
                        .filter(id -> id.startsWith(partition + "-")).toList();
                List<String> expected = new ArrayList<>();
                for (int i = 1; i <= perPartition; i++) {
                    expected.add(partition + "-" + i);
                }
                assertEquals(expected, perPart,
                        "分区 " + partition + " 内投递顺序必须保序");
            }
        }
    }

    // ---------- 进度与结论查询 ----------

    @Test
    void progressQueryCoversAllStagesAndIsReadOnly() {
        try (Rig rig = newRig(dir, 3, 3_600_000)) {
            // 未知事件
            assertTrue(rig.progress().progressOf("nobody").isEmpty());

            // 还没投：直接落库为 PENDING（模拟已接收未调度），可查
            rig.store().saveIfAbsent(event("q-pending", "pq", 1));
            DeliveryProgress pending =
                    rig.progress().progressOf("q-pending").orElseThrow();
            assertEquals(DeliveryProgress.Stage.PENDING_DELIVERY, pending.stage());
            assertEquals(0, pending.attempts());
            assertNull(pending.nextAttemptAtMillis());
            assertNull(pending.rejectionReason());

            // 重试等待中：可查，能看到已投次数与下次尝试时间
            rig.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
            rig.engine().submit(event("q-retry", "pr", 1));
            awaitAttempts(rig.store(), "q-retry", 1);
            DeliveryProgress retryWait =
                    rig.progress().progressOf("q-retry").orElseThrow();
            assertEquals(DeliveryProgress.Stage.RETRY_WAIT, retryWait.stage());
            assertEquals(1, retryWait.attempts());
            assertNotNull(retryWait.nextAttemptAtMillis());
            assertTrue(retryWait.nextAttemptAtMillis() > System.currentTimeMillis());

            // 被拒绝（业务拒绝）：可查，原因类别可区分
            rig.handler().behavior = e -> ProcessingOutcome.REJECTED;
            rig.engine().submit(event("q-reject", "pj", 1));
            awaitStatus(rig.store(), "q-reject", EventStore.EventStatus.REJECTED);
            DeliveryProgress rejected =
                    rig.progress().progressOf("q-reject").orElseThrow();
            assertEquals(DeliveryProgress.Stage.REJECTED, rejected.stage());
            assertEquals(RejectionReason.BUSINESS_REJECTED, rejected.rejectionReason());

            // 被拒绝（接收前校验）：原因类别与业务拒绝不同
            EventEnvelope badVersion = new EventEnvelope("q-badver", "pv", 1, 99,
                    Map.of("type", "T", "amount", 1), Map.of());
            rig.engine().submit(badVersion);
            DeliveryProgress badVer =
                    rig.progress().progressOf("q-badver").orElseThrow();
            assertEquals(DeliveryProgress.Stage.REJECTED, badVer.stage());
            assertEquals(RejectionReason.UNSUPPORTED_VERSION, badVer.rejectionReason());

            // 已成功
            rig.handler().behavior = e -> ProcessingOutcome.SUCCESS;
            rig.engine().submit(event("q-ok", "pk", 1));
            awaitStatus(rig.store(), "q-ok", EventStore.EventStatus.DELIVERED);
            DeliveryProgress delivered =
                    rig.progress().progressOf("q-ok").orElseThrow();
            assertEquals(DeliveryProgress.Stage.DELIVERED, delivered.stage());
            assertEquals(1, delivered.attempts());

            // 查询是只读的：反复查询不改变任何状态、不触发副作用
            int effectsBefore = rig.handler().effects.get();
            EventStore.StoredEvent before = rig.store().find("q-retry").orElseThrow();
            for (int i = 0; i < 5; i++) {
                rig.progress().progressOf("q-retry");
                rig.progress().progressOf("q-ok");
            }
            EventStore.StoredEvent after = rig.store().find("q-retry").orElseThrow();
            assertEquals(before, after, "查询不得改变事件状态");
            assertEquals(effectsBefore, rig.handler().effects.get(),
                    "查询不得触发业务副作用");
        }
    }

    @Test
    void progressQueryShowsDeadLetterStage() {
        try (Rig rig = newRig(dir, 3, 5)) {
            rig.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
            rig.engine().submit(event("q-dead", "pd", 1));
            awaitStatus(rig.store(), "q-dead", EventStore.EventStatus.DEAD);
            DeliveryProgress dead = rig.progress().progressOf("q-dead").orElseThrow();
            assertEquals(DeliveryProgress.Stage.DEAD, dead.stage());
            assertEquals(3, dead.attempts());
            assertNull(dead.rejectionReason());
        }
    }
}
