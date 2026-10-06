package com.github.lisama569.idempotenteventpipelinejava.tests;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeadLetterService;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeliveryEngine;
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

    private static EventEnvelope event(String id, String partition, long seq) {
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

    private record Rig(FileEventStore store, TestHandler handler,
                       DeliveryScheduler scheduler, DeliveryEngine engine,
                       DeadLetterService deadLetters, EventValidator validator,
                       DeliveryProgressService progress) implements AutoCloseable {
        @Override
        public void close() {
            scheduler.close();
        }
    }

    private Rig newRig(Path storeDir) {
        FileEventStore store = new FileEventStore(storeDir.toString());
        TestHandler handler = new TestHandler();
        EventValidator validator = new EventValidator();
        DeliveryScheduler scheduler = new DeliveryScheduler(store, handler, 3, 1, 2);
        scheduler.start();
        DeliveryEngine engine = new DeliveryEngine(store, validator, scheduler);
        return new Rig(store, handler, scheduler, engine,
                new DeadLetterService(store, scheduler), validator,
                new DeliveryProgressService(store));
    }

    /** 轮询等待事件到达期望状态（后台异步投递，测试显式等待终态）。 */
    static EventStore.StoredEvent awaitStatus(EventStore store, String eventId,
                                              EventStore.EventStatus expected) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            var cur = store.find(eventId);
            if (cur.isPresent() && cur.get().status() == expected) {
                return cur.get();
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return fail("timed out waiting for " + eventId + " to reach " + expected
                + ", current=" + store.find(eventId));
    }

    @Test
    void duplicateDeliveryIsIdempotent() {
        try (Rig rig = newRig(dir)) {
            EventEnvelope e = event("e1", "p1", 1);
            assertEquals(SubmissionAck.Decision.ACCEPTED, rig.engine().submit(e).decision());
            // 投递在后台推进：重复提交可能落在投递前（ACCEPTED）或投递后（DUPLICATE），
            // 两种都是合法回执，关键是副作用只发生一次
            assertTrue(rig.engine().submit(e).decision() != SubmissionAck.Decision.REJECTED);
            awaitStatus(rig.store(), "e1", EventStore.EventStatus.DELIVERED);
            // 已有终态后重复提交：返回既有结论，不产生新副作用
            SubmissionAck dup = rig.engine().submit(e);
            assertEquals(SubmissionAck.Decision.DUPLICATE, dup.decision());
            assertEquals(EventStore.EventStatus.DELIVERED, dup.status());
            assertEquals(1, rig.handler().effects.get(), "重复投递不得产生重复副作用");
        }
    }

    @Test
    void samePartitionConcurrentDuplicateSubmission() throws InterruptedException {
        try (Rig rig = newRig(dir)) {
            EventEnvelope e = event("e1", "p1", 1);
            int threads = 8;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<SubmissionAck> acks = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        synchronized (acks) {
                            acks.add(rig.engine().submit(e));
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertEquals(threads, acks.size());
            assertTrue(acks.stream().allMatch(a -> a.decision() != SubmissionAck.Decision.REJECTED),
                    "并发重复提交不应被拒绝: " + acks);
            awaitStatus(rig.store(), "e1", EventStore.EventStatus.DELIVERED);
            assertEquals(1, rig.handler().effects.get(), "并发重复提交副作用必须恰好一次");
        }
    }

    @Test
    void samePartitionOrderedDeliveryAcrossPartitionsParallel() {
        try (Rig rig = newRig(dir)) {
            // 同一分区按序提交 20 个事件
            for (int i = 1; i <= 20; i++) {
                assertEquals(SubmissionAck.Decision.ACCEPTED,
                        rig.engine().submit(event("a-" + i, "pa", i)).decision());
            }
            awaitStatus(rig.store(), "a-20", EventStore.EventStatus.DELIVERED);
            // 校验投递顺序与提交顺序一致
            List<String> expected = new ArrayList<>();
            for (int i = 1; i <= 20; i++) {
                expected.add("a-" + i);
            }
            assertEquals(expected, List.copyOf(rig.handler().deliveryOrder));
            // 乱序提交被明确拒绝
            rig.engine().submit(event("a-25", "pa", 25));
            awaitStatus(rig.store(), "a-25", EventStore.EventStatus.REJECTED);
            assertEquals(RejectionReason.OUT_OF_ORDER_SEQUENCE,
                    rig.store().find("a-25").orElseThrow().rejectionReason());
            // 不同分区独立推进
            rig.engine().submit(event("b-1", "pb", 1));
            awaitStatus(rig.store(), "b-1", EventStore.EventStatus.DELIVERED);
        }
    }

    @Test
    void retryExhaustionGoesToDeadLetterAndReplayKeepsOrder() {
        try (Rig rig = newRig(dir)) {
            rig.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
            rig.engine().submit(event("d1", "p1", 1));
            EventStore.StoredEvent dead =
                    awaitStatus(rig.store(), "d1", EventStore.EventStatus.DEAD);
            assertEquals(3, dead.attempts(), "重试次数需可判定");
            assertEquals(0, rig.handler().effects.get());
            assertEquals(1, rig.deadLetters().list().size());
            // 死信阻塞分区：后续事件因保序被拒绝，不会静默越过
            rig.engine().submit(event("d2", "p1", 2));
            awaitStatus(rig.store(), "d2", EventStore.EventStatus.REJECTED);
            assertEquals(RejectionReason.OUT_OF_ORDER_SEQUENCE,
                    rig.store().find("d2").orElseThrow().rejectionReason());
            // 修复故障后按原顺序重放
            rig.handler().behavior = e -> ProcessingOutcome.SUCCESS;
            assertEquals(SubmissionAck.Decision.ACCEPTED, rig.deadLetters().replay("d1").decision());
            awaitStatus(rig.store(), "d1", EventStore.EventStatus.DELIVERED);
            assertEquals(1, rig.handler().effects.get());
            assertEquals(0, rig.deadLetters().list().size());
            // 重放成功后分区恢复推进
            rig.engine().submit(event("d3", "p1", 2));
            awaitStatus(rig.store(), "d3", EventStore.EventStatus.DELIVERED);
            assertEquals(2, rig.handler().effects.get());
        }
    }

    @Test
    void restartRecoveryKeepsConclusions() {
        try (Rig first = newRig(dir)) {
            first.engine().submit(event("r1", "p1", 1));
            awaitStatus(first.store(), "r1", EventStore.EventStatus.DELIVERED);
            first.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
            first.engine().submit(event("r2", "p1", 2));
            awaitStatus(first.store(), "r2", EventStore.EventStatus.DEAD);
            assertEquals(1, first.handler().effects.get());
        }

        // 模拟进程重启：同一目录新建存储与引擎
        try (Rig second = newRig(dir)) {
            assertEquals(EventStore.EventStatus.DELIVERED,
                    second.store().find("r1").orElseThrow().status());
            assertEquals(EventStore.EventStatus.DEAD,
                    second.store().find("r2").orElseThrow().status());
            assertEquals(1, second.store().lastDeliveredSequence("p1"));
            // 重启后重复提交已成功事件：结论一致且不产生新副作用
            SubmissionAck dup = second.engine().submit(event("r1", "p1", 1));
            assertEquals(SubmissionAck.Decision.DUPLICATE, dup.decision());
            assertEquals(EventStore.EventStatus.DELIVERED, dup.status());
            assertEquals(0, second.handler().effects.get());
            // 死信在重启后仍可查询、可重放
            assertEquals(1, second.deadLetters().list().size());
            second.handler().behavior = e -> ProcessingOutcome.SUCCESS;
            second.deadLetters().replay("r2");
            awaitStatus(second.store(), "r2", EventStore.EventStatus.DELIVERED);
            assertEquals(1, second.handler().effects.get());
        }
    }

    @Test
    void validationRejectionsAreDistinguishableAndDeterministic() {
        try (Rig rig = newRig(dir)) {
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

            assertEquals(SubmissionAck.Decision.REJECTED, rig.engine().submit(badVersion).decision());
            assertEquals(SubmissionAck.Decision.REJECTED, rig.engine().submit(missingField).decision());
            assertEquals(SubmissionAck.Decision.REJECTED, rig.engine().submit(wrongType).decision());
            assertEquals(RejectionReason.UNSUPPORTED_VERSION,
                    rig.store().find("v1").orElseThrow().rejectionReason());
            assertEquals(RejectionReason.MISSING_REQUIRED_FIELD,
                    rig.store().find("v2").orElseThrow().rejectionReason());
            assertEquals(RejectionReason.FIELD_TYPE_MISMATCH,
                    rig.store().find("v3").orElseThrow().rejectionReason());
            assertEquals(0, rig.handler().effects.get());
        }
    }

    @Test
    void optionalFieldDefaultsAreStable() {
        try (Rig rig = newRig(dir)) {
            EventEnvelope v2NoCurrency = event("c1", "p1", 1);
            EventEnvelope normalized = rig.validator().applyDefaults(v2NoCurrency);
            assertEquals("USD", normalized.payload().get("currency"));
            // 缺省语义确定：再次归一化结果相同
            assertEquals(normalized, rig.validator().applyDefaults(v2NoCurrency));
            // v1 事件不注入 v2 可选字段
            EventEnvelope v1 = new EventEnvelope("c2", "p1", 2, 1,
                    Map.of("type", "T", "amount", 1), Map.of());
            assertNull(rig.validator().applyDefaults(v1).payload().get("currency"));
            rig.engine().submit(v2NoCurrency);
            awaitStatus(rig.store(), "c1", EventStore.EventStatus.DELIVERED);
            assertEquals("USD",
                    rig.store().find("c1").orElseThrow().envelope().payload().get("currency"));
        }
    }
}
