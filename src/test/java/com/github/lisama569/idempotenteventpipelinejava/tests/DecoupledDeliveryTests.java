package com.github.lisama569.idempotenteventpipelinejava.tests;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeliveryDispatcher;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeliveryEngine;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventEnvelope;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventProgress;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventProgressService;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventValidator;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.ProcessingOutcome;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.ReceiveOutcome;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.RejectionReason;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.FileEventStore;
import com.github.lisama569.idempotenteventpipelinejava.tests.PipelineTests.TestHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 本轮新能力：接收与投递解耦、重启自愈、进度可查。
 * 日志可见事件标识、分区、投递次数与判定依据（decision=...）。
 */
class DecoupledDeliveryTests {

    @TempDir
    Path dir;

    private static EventEnvelope event(String id, String partition, long seq) {
        return new EventEnvelope(id, partition, seq, 2,
                Map.of("type", "ORDER", "amount", 10), Map.of());
    }

    private record Rig(FileEventStore store, TestHandler handler, DeliveryEngine engine,
                       EventProgressService progress, ScheduledExecutorService executor) {
        void close() {
            executor.shutdownNow();
        }
    }

    private static Rig newRig(Path storeDir, int maxAttempts, long baseBackoffMs) {
        FileEventStore store = new FileEventStore(storeDir.toString());
        TestHandler handler = new TestHandler();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
        DeliveryDispatcher dispatcher = new DeliveryDispatcher(store, executor);
        DeliveryEngine engine = new DeliveryEngine(store, new EventValidator(), handler,
                dispatcher, maxAttempts, baseBackoffMs);
        engine.init();
        return new Rig(store, handler, engine, new EventProgressService(store), executor);
    }

    private static void awaitTrue(Check cond, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        fail("超时: " + what);
    }

    @FunctionalInterface
    private interface Check {
        boolean getAsBoolean();
    }

    private static void awaitStatus(FileEventStore store, String eventId,
                                    EventStore.EventStatus expected) throws InterruptedException {
        awaitTrue(() -> store.find(eventId).map(e -> e.status() == expected).orElse(false),
                "等待事件 " + eventId + " 到达状态 " + expected);
    }

    @Test
    void submitReturnsImmediatelyWhileDeliveryInFlight() throws InterruptedException {
        Rig rig = newRig(dir, 3, 50);
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        rig.handler().behavior = e -> {
            if (e.eventId().equals("blk")) {
                handlerEntered.countDown();
                try {
                    release.await();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            return ProcessingOutcome.SUCCESS;
        };
        // 处理器被卡住时，接收侧仍应立即给出“已受理”结论
        long start = System.currentTimeMillis();
        ReceiveOutcome outcome = rig.engine().submit(event("blk", "p1", 1));
        long elapsed = System.currentTimeMillis() - start;
        assertEquals(ReceiveOutcome.ACCEPTED, outcome);
        assertTrue(elapsed < 2_000, "接收侧不应等投递跑完, elapsed=" + elapsed + "ms");
        assertTrue(handlerEntered.await(5, TimeUnit.SECONDS), "后台应已开始投递");
        // 卡住期间进度可查：还在投，没有终态
        EventProgress mid = rig.progress().progress("blk").orElseThrow();
        assertEquals("PENDING", mid.stage());
        // 另一个分区不受阻塞影响
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(event("free", "p2", 1)));
        awaitStatus(rig.store(), "free", EventStore.EventStatus.DELIVERED);
        release.countDown();
        awaitStatus(rig.store(), "blk", EventStore.EventStatus.DELIVERED);
        assertEquals(2, rig.handler().effects.get());
        rig.close();
    }

    @Test
    void backgroundDeliveryEventuallySucceedsAndEventuallyDies() throws InterruptedException {
        Rig rig = newRig(dir, 3, 1);
        // 提交后由后台完成投递并最终成功
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(event("ok", "p1", 1)));
        awaitStatus(rig.store(), "ok", EventStore.EventStatus.DELIVERED);
        assertEquals(1, rig.handler().effects.get());
        // 持续失败最终进死信，尝试次数可判定
        rig.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(event("bad", "p2", 1)));
        awaitStatus(rig.store(), "bad", EventStore.EventStatus.DEAD);
        EventStore.StoredEvent dead = rig.store().find("bad").orElseThrow();
        assertEquals(3, dead.attempts());
        assertEquals(1, rig.handler().effects.get(), "失败事件不得产生副作用");
        rig.close();
    }

    @Test
    void retryWaitDoesNotBlockOtherPartitions() throws InterruptedException {
        // p1 失败进入长时间重试等待；p2 必须照常推进
        Rig rig = newRig(dir, 5, 60_000);
        rig.handler().behavior = e -> e.eventId().equals("stuck")
                ? ProcessingOutcome.RETRYABLE_FAILURE : ProcessingOutcome.SUCCESS;
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(event("stuck", "p1", 1)));
        awaitTrue(() -> rig.store().find("stuck")
                        .map(e -> e.status() == EventStore.EventStatus.PENDING && e.attempts() >= 1)
                        .orElse(false),
                "等待 stuck 进入重试等待");
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(event("go", "p2", 1)));
        awaitStatus(rig.store(), "go", EventStore.EventStatus.DELIVERED);
        assertEquals(1, rig.handler().effects.get());
        // p1 仍在重试等待中，未被放弃也未乱跳
        EventProgress stuck = rig.progress().progress("stuck").orElseThrow();
        assertEquals("RETRY_WAIT", stuck.stage());
        assertTrue(stuck.nextAttemptAtMillis() > System.currentTimeMillis());
        rig.close();
    }

    @Test
    void restartResumesRetryWaitAndNeverAttemptedEvents() throws InterruptedException {
        // 重试等待中的事件：重启后续投，尝试次数与退避节奏延续
        Rig first = newRig(dir, 5, 300);
        first.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
        assertEquals(ReceiveOutcome.ACCEPTED, first.engine().submit(event("w1", "p1", 1)));
        awaitTrue(() -> first.store().find("w1")
                        .map(e -> e.status() == EventStore.EventStatus.PENDING && e.attempts() >= 1)
                        .orElse(false),
                "等待 w1 积累至少一次尝试");
        int attemptsAtCrash = first.store().find("w1").orElseThrow().attempts();
        // 还没开始投的事件：模拟崩溃前已受理落库但尚未投递
        first.store().saveIfAbsent(event("w0", "p2", 1));
        first.close(); // 模拟进程退出：定时任务全部丢失

        Rig second = newRig(dir, 5, 300);
        // 无需人工重新提交，两类未完成事件都自动接着投
        awaitStatus(second.store(), "w1", EventStore.EventStatus.DELIVERED);
        awaitStatus(second.store(), "w0", EventStore.EventStatus.DELIVERED);
        EventStore.StoredEvent w1 = second.store().find("w1").orElseThrow();
        assertTrue(w1.attempts() > attemptsAtCrash,
                "重试次数应延续而非清零: crash=" + attemptsAtCrash + " final=" + w1.attempts());
        assertEquals(1, second.handler().deliveryOrder.stream()
                .filter(id -> id.equals("w1")).count(), "w1 副作用只生效一次");
        assertEquals(1, second.handler().deliveryOrder.stream()
                .filter(id -> id.equals("w0")).count(), "w0 副作用只生效一次");
        second.close();
    }

    @Test
    void progressQueryShowsStageAttemptsAndReason() throws InterruptedException {
        Rig rig = newRig(dir, 3, 10_000);
        // 重试等待中
        rig.handler().behavior = e -> e.eventId().equals("f1")
                ? ProcessingOutcome.RETRYABLE_FAILURE : ProcessingOutcome.SUCCESS;
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(event("f1", "p1", 1)));
        awaitTrue(() -> rig.store().find("f1").map(e -> e.attempts() >= 1).orElse(false),
                "等待 f1 完成首次尝试");
        EventProgress retryWait = rig.progress().progress("f1").orElseThrow();
        assertEquals("RETRY_WAIT", retryWait.stage());
        assertEquals(1, retryWait.attempts());
        assertTrue(retryWait.nextAttemptAtMillis() > System.currentTimeMillis());
        // 已成功
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(event("s1", "p2", 1)));
        awaitStatus(rig.store(), "s1", EventStore.EventStatus.DELIVERED);
        EventProgress done = rig.progress().progress("s1").orElseThrow();
        assertEquals("DELIVERED", done.stage());
        assertEquals(1, done.attempts());
        // 业务拒绝：原因类别可区分
        rig.handler().behavior = e -> e.eventId().equals("rj")
                ? ProcessingOutcome.REJECTED : ProcessingOutcome.SUCCESS;
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(event("rj", "p3", 1)));
        awaitStatus(rig.store(), "rj", EventStore.EventStatus.REJECTED);
        EventProgress rejected = rig.progress().progress("rj").orElseThrow();
        assertEquals("REJECTED", rejected.stage());
        assertEquals(RejectionReason.BUSINESS_REJECTED, rejected.rejectionReason());
        // 校验拒绝：同步即终态，原因类别不同
        EventEnvelope badVersion = new EventEnvelope("bv", "p4", 1, 99,
                Map.of("type", "T", "amount", 1), Map.of());
        assertEquals(ReceiveOutcome.REJECTED, rig.engine().submit(badVersion));
        assertEquals(RejectionReason.UNSUPPORTED_VERSION,
                rig.progress().progress("bv").orElseThrow().rejectionReason());
        // 未知事件
        assertEquals(Optional.empty(), rig.progress().progress("nope"));
        // 查询是只读的：反复查询不改变状态、不触发副作用
        int effectsBefore = rig.handler().effects.get();
        for (int i = 0; i < 5; i++) {
            rig.progress().progress("f1");
            rig.progress().progress("s1");
        }
        assertEquals(1, rig.store().find("f1").orElseThrow().attempts());
        assertEquals(effectsBefore, rig.handler().effects.get());
        rig.close();
    }

    @Test
    void concurrentSubmissionKeepsPerPartitionOrderAndParallelism() throws InterruptedException {
        Rig rig = newRig(dir, 3, 1);
        int perPartition = 5;
        List<Thread> threads = new ArrayList<>();
        for (String partition : List.of("pa", "pb", "pc")) {
            threads.add(new Thread(() -> {
                for (int i = 1; i <= perPartition; i++) {
                    String id = partition + "-" + i;
                    try {
                        assertEquals(ReceiveOutcome.ACCEPTED,
                                rig.engine().submit(event(id, partition, i)));
                        awaitStatus(rig.store(), id, EventStore.EventStatus.DELIVERED);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }));
        }
        threads.forEach(Thread::start);
        for (Thread t : threads) {
            t.join(15_000);
            assertTrue(!t.isAlive(), "分区提交线程应在限时内完成");
        }
        // 每个分区内部投递顺序与序号一致
        for (String partition : List.of("pa", "pb", "pc")) {
            List<String> expected = new ArrayList<>();
            for (int i = 1; i <= perPartition; i++) {
                expected.add(partition + "-" + i);
            }
            List<String> actual = rig.handler().deliveryOrder.stream()
                    .filter(id -> id.startsWith(partition + "-")).toList();
            assertEquals(expected, actual, "分区 " + partition + " 内必须保序");
        }
        assertEquals(3 * perPartition, rig.handler().effects.get());
        rig.close();
    }
}
