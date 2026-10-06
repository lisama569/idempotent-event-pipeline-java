package com.github.lisama569.idempotenteventpipelinejava.tests;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeadLetterService;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeliveryDispatcher;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeliveryEngine;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventEnvelope;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventHandler;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventValidator;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.ProcessingOutcome;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.ReceiveOutcome;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.RejectionReason;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 既有行为回归：接收与投递解耦后，幂等、保序、重试耗尽进死信、
 * 版本兼容与拒绝原因区分这些行为和边界不回归。
 */
class PipelineTests {

    @TempDir
    Path dir;

    static EventEnvelope event(String id, String partition, long seq) {
        return new EventEnvelope(id, partition, seq, 2,
                Map.of("type", "ORDER", "amount", 10), Map.of());
    }

    /** 可编程行为的测试处理器，记录副作用次数与投递顺序。 */
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

    record Rig(FileEventStore store, TestHandler handler,
               DeliveryEngine engine, DeadLetterService deadLetters,
               EventValidator validator, ScheduledExecutorService executor) {
        void close() {
            executor.shutdownNow();
        }
    }

    static Rig newRig(Path storeDir) {
        FileEventStore store = new FileEventStore(storeDir.toString());
        TestHandler handler = new TestHandler();
        EventValidator validator = new EventValidator();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
        DeliveryDispatcher dispatcher = new DeliveryDispatcher(store, executor);
        DeliveryEngine engine = new DeliveryEngine(store, validator, handler, dispatcher, 3, 1);
        engine.init();
        return new Rig(store, handler, engine, new DeadLetterService(store, engine),
                validator, executor);
    }

    static void awaitTrue(BooleanSupplier cond, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        fail("超时: " + what);
    }

    static void awaitStatus(FileEventStore store, String eventId,
                            EventStore.EventStatus expected) throws InterruptedException {
        awaitTrue(() -> store.find(eventId).map(e -> e.status() == expected).orElse(false),
                "等待事件 " + eventId + " 到达状态 " + expected);
    }

    @Test
    void duplicateDeliveryIsIdempotent() throws InterruptedException {
        Rig rig = newRig(dir);
        EventEnvelope e = event("e1", "p1", 1);
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(e));
        awaitStatus(rig.store(), "e1", EventStore.EventStatus.DELIVERED);
        // 已有终态结论后重复提交：返回原结论，不产生新副作用
        assertEquals(ReceiveOutcome.DELIVERED, rig.engine().submit(e));
        assertEquals(ReceiveOutcome.DELIVERED, rig.engine().submit(e));
        assertEquals(1, rig.handler().effects.get(), "重复投递不得产生重复副作用");
        rig.close();
    }

    @Test
    void samePartitionConcurrentDuplicateSubmission() throws InterruptedException {
        Rig rig = newRig(dir);
        EventEnvelope e = event("e1", "p1", 1);
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<ReceiveOutcome> outcomes = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    synchronized (outcomes) {
                        outcomes.add(rig.engine().submit(e));
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        awaitStatus(rig.store(), "e1", EventStore.EventStatus.DELIVERED);
        assertEquals(threads, outcomes.size());
        assertTrue(outcomes.stream().allMatch(o -> o == ReceiveOutcome.ACCEPTED
                        || o == ReceiveOutcome.DELIVERED),
                "并发重复提交应全部受理或返回成功结论: " + outcomes);
        assertEquals(1, rig.handler().effects.get(), "并发重复提交副作用必须恰好一次");
        rig.close();
    }

    @Test
    void samePartitionOrderedDeliveryAcrossPartitionsParallel() throws InterruptedException {
        Rig rig = newRig(dir);
        // 同一分区按序提交 20 个事件（每个等上一事件出结论后再提交，符合保序契约）
        for (int i = 1; i <= 20; i++) {
            assertEquals(ReceiveOutcome.ACCEPTED,
                    rig.engine().submit(event("a-" + i, "pa", i)));
            awaitStatus(rig.store(), "a-" + i, EventStore.EventStatus.DELIVERED);
        }
        // 校验投递顺序与提交顺序一致
        List<String> expected = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            expected.add("a-" + i);
        }
        assertEquals(expected, List.copyOf(rig.handler().deliveryOrder));
        // 乱序提交被明确拒绝
        assertEquals(ReceiveOutcome.REJECTED,
                rig.engine().submit(event("a-25", "pa", 25)));
        assertEquals(RejectionReason.OUT_OF_ORDER_SEQUENCE,
                rig.store().find("a-25").orElseThrow().rejectionReason());
        // 不同分区独立推进
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(event("b-1", "pb", 1)));
        awaitStatus(rig.store(), "b-1", EventStore.EventStatus.DELIVERED);
        rig.close();
    }

    @Test
    void retryExhaustionGoesToDeadLetterAndReplayKeepsOrder() throws InterruptedException {
        Rig rig = newRig(dir);
        rig.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(event("d1", "p1", 1)));
        awaitStatus(rig.store(), "d1", EventStore.EventStatus.DEAD);
        EventStore.StoredEvent dead = rig.store().find("d1").orElseThrow();
        assertEquals(3, dead.attempts(), "重试次数需可判定");
        assertEquals(0, rig.handler().effects.get());
        assertEquals(1, rig.deadLetters().list().size());
        // 死信阻塞分区：后续事件因保序被拒绝，不会静默越过
        assertEquals(ReceiveOutcome.REJECTED, rig.engine().submit(event("d2", "p1", 2)));
        // 修复故障后按原顺序重放
        rig.handler().behavior = e -> ProcessingOutcome.SUCCESS;
        assertEquals(ReceiveOutcome.ACCEPTED, rig.deadLetters().replay("d1"));
        awaitStatus(rig.store(), "d1", EventStore.EventStatus.DELIVERED);
        assertEquals(1, rig.handler().effects.get());
        assertEquals(0, rig.deadLetters().list().size());
        // 重放成功后分区恢复推进
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(event("d3", "p1", 2)));
        awaitStatus(rig.store(), "d3", EventStore.EventStatus.DELIVERED);
        assertEquals(2, rig.handler().effects.get());
        rig.close();
    }

    @Test
    void restartRecoveryKeepsConclusions() throws InterruptedException {
        Rig first = newRig(dir);
        assertEquals(ReceiveOutcome.ACCEPTED, first.engine().submit(event("r1", "p1", 1)));
        awaitStatus(first.store(), "r1", EventStore.EventStatus.DELIVERED);
        first.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
        assertEquals(ReceiveOutcome.ACCEPTED, first.engine().submit(event("r2", "p1", 2)));
        awaitStatus(first.store(), "r2", EventStore.EventStatus.DEAD);
        assertEquals(1, first.handler().effects.get());
        first.close();

        // 模拟进程重启：同一目录新建存储与引擎
        Rig second = newRig(dir);
        assertEquals(EventStore.EventStatus.DELIVERED,
                second.store().find("r1").orElseThrow().status());
        assertEquals(EventStore.EventStatus.DEAD,
                second.store().find("r2").orElseThrow().status());
        assertEquals(1, second.store().lastDeliveredSequence("p1"));
        // 重启后重复投递已成功事件：结论一致且不产生新副作用
        assertEquals(ReceiveOutcome.DELIVERED, second.engine().submit(event("r1", "p1", 1)));
        assertEquals(0, second.handler().effects.get());
        // 死信在重启后仍可查询、可重放
        assertEquals(1, second.deadLetters().list().size());
        assertEquals(ReceiveOutcome.ACCEPTED, second.deadLetters().replay("r2"));
        awaitStatus(second.store(), "r2", EventStore.EventStatus.DELIVERED);
        assertEquals(1, second.handler().effects.get());
        second.close();
    }

    @Test
    void validationRejectionsAreDistinguishableAndDeterministic() {
        Rig rig = newRig(dir);
        EventEnvelope badVersion = new EventEnvelope("v1", "p", 1, 99,
                Map.of("type", "T", "amount", 1), Map.of());
        EventEnvelope missingField = new EventEnvelope("v2", "p", 1, 2,
                Map.of("amount", 1), Map.of());
        EventEnvelope wrongType = new EventEnvelope("v3", "p", 1, 2,
                Map.of("type", "T", "amount", "not-a-number"), Map.of());

        assertEquals(RejectionReason.UNSUPPORTED_VERSION, rig.validator().validate(badVersion));
        assertEquals(RejectionReason.MISSING_REQUIRED_FIELD, rig.validator().validate(missingField));
        assertEquals(RejectionReason.FIELD_TYPE_MISMATCH, rig.validator().validate(wrongType));
        // 同一输入多次校验结论一致
        assertEquals(RejectionReason.UNSUPPORTED_VERSION, rig.validator().validate(badVersion));

        assertEquals(ReceiveOutcome.REJECTED, rig.engine().submit(badVersion));
        assertEquals(ReceiveOutcome.REJECTED, rig.engine().submit(missingField));
        assertEquals(ReceiveOutcome.REJECTED, rig.engine().submit(wrongType));
        assertEquals(RejectionReason.UNSUPPORTED_VERSION,
                rig.store().find("v1").orElseThrow().rejectionReason());
        assertEquals(RejectionReason.MISSING_REQUIRED_FIELD,
                rig.store().find("v2").orElseThrow().rejectionReason());
        assertEquals(RejectionReason.FIELD_TYPE_MISMATCH,
                rig.store().find("v3").orElseThrow().rejectionReason());
        assertEquals(0, rig.handler().effects.get());
        rig.close();
    }

    @Test
    void optionalFieldDefaultsAreStable() throws InterruptedException {
        Rig rig = newRig(dir);
        EventEnvelope v2NoCurrency = event("c1", "p1", 1);
        EventEnvelope normalized = rig.validator().applyDefaults(v2NoCurrency);
        assertEquals("USD", normalized.payload().get("currency"));
        // 缺省语义确定：再次归一化结果相同
        assertEquals(normalized, rig.validator().applyDefaults(v2NoCurrency));
        // v1 事件不注入 v2 可选字段
        EventEnvelope v1 = new EventEnvelope("c2", "p1", 2, 1,
                Map.of("type", "T", "amount", 1), Map.of());
        assertNull(rig.validator().applyDefaults(v1).payload().get("currency"));
        assertEquals(ReceiveOutcome.ACCEPTED, rig.engine().submit(v2NoCurrency));
        awaitStatus(rig.store(), "c1", EventStore.EventStatus.DELIVERED);
        assertEquals("USD", rig.store().find("c1").orElseThrow().envelope().payload().get("currency"));
        rig.close();
    }
}
