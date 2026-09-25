package com.github.lisama569.idempotenteventpipelinejava.tests;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeadLetterService;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.DeliveryEngine;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventEnvelope;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventHandler;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.EventValidator;
import com.github.lisama569.idempotenteventpipelinejava.pipeline.ProcessingOutcome;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                       DeliveryEngine engine, DeadLetterService deadLetters,
                       EventValidator validator) {
    }

    private Rig newRig(Path storeDir) {
        FileEventStore store = new FileEventStore(storeDir.toString());
        TestHandler handler = new TestHandler();
        EventValidator validator = new EventValidator();
        DeliveryEngine engine = new DeliveryEngine(store, validator, handler, 3, 1);
        return new Rig(store, handler, engine, new DeadLetterService(store, engine), validator);
    }

    @Test
    void duplicateDeliveryIsIdempotent() {
        Rig rig = newRig(dir);
        EventEnvelope e = event("e1", "p1", 1);
        assertEquals(ProcessingOutcome.SUCCESS, rig.engine().submit(e));
        assertEquals(ProcessingOutcome.SUCCESS, rig.engine().submit(e));
        assertEquals(ProcessingOutcome.SUCCESS, rig.engine().submit(e));
        assertEquals(1, rig.handler().effects.get(), "重复投递不得产生重复副作用");
        assertEquals(EventStore.EventStatus.DELIVERED,
                rig.store().find("e1").orElseThrow().status());
    }

    @Test
    void samePartitionConcurrentDuplicateSubmission() throws InterruptedException {
        Rig rig = newRig(dir);
        EventEnvelope e = event("e1", "p1", 1);
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<ProcessingOutcome> outcomes = new ArrayList<>();
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
        assertEquals(threads, outcomes.size());
        assertTrue(outcomes.stream().allMatch(o -> o == ProcessingOutcome.SUCCESS),
                "并发重复提交应全部得到成功结论: " + outcomes);
        assertEquals(1, rig.handler().effects.get(), "并发重复提交副作用必须恰好一次");
    }

    @Test
    void samePartitionOrderedDeliveryAcrossPartitionsParallel() {
        Rig rig = newRig(dir);
        // 同一分区按序提交 20 个事件
        for (int i = 1; i <= 20; i++) {
            assertEquals(ProcessingOutcome.SUCCESS,
                    rig.engine().submit(event("a-" + i, "pa", i)));
        }
        // 校验投递顺序与提交顺序一致
        List<String> expected = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            expected.add("a-" + i);
        }
        assertEquals(expected, List.copyOf(rig.handler().deliveryOrder));
        // 乱序提交被明确拒绝
        assertEquals(ProcessingOutcome.REJECTED,
                rig.engine().submit(event("a-25", "pa", 25)));
        assertEquals(RejectionReason.OUT_OF_ORDER_SEQUENCE,
                rig.store().find("a-25").orElseThrow().rejectionReason());
        // 不同分区独立推进
        assertEquals(ProcessingOutcome.SUCCESS, rig.engine().submit(event("b-1", "pb", 1)));
    }

    @Test
    void retryExhaustionGoesToDeadLetterAndReplayKeepsOrder() {
        Rig rig = newRig(dir);
        rig.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
        assertEquals(ProcessingOutcome.RETRYABLE_FAILURE,
                rig.engine().submit(event("d1", "p1", 1)));
        EventStore.StoredEvent dead = rig.store().find("d1").orElseThrow();
        assertEquals(EventStore.EventStatus.DEAD, dead.status());
        assertEquals(3, dead.attempts(), "重试次数需可判定");
        assertEquals(0, rig.handler().effects.get());
        assertEquals(1, rig.deadLetters().list().size());
        // 死信阻塞分区：后续事件因保序被拒绝，不会静默越过
        assertEquals(ProcessingOutcome.REJECTED, rig.engine().submit(event("d2", "p1", 2)));
        // 修复故障后按原顺序重放
        rig.handler().behavior = e -> ProcessingOutcome.SUCCESS;
        assertEquals(ProcessingOutcome.SUCCESS, rig.deadLetters().replay("d1"));
        assertEquals(1, rig.handler().effects.get());
        assertEquals(0, rig.deadLetters().list().size());
        // 重放成功后分区恢复推进
        assertEquals(ProcessingOutcome.SUCCESS, rig.engine().submit(event("d3", "p1", 2)));
        assertEquals(2, rig.handler().effects.get());
    }

    @Test
    void restartRecoveryKeepsConclusions() {
        Rig first = newRig(dir);
        assertEquals(ProcessingOutcome.SUCCESS, first.engine().submit(event("r1", "p1", 1)));
        first.handler().behavior = e -> ProcessingOutcome.RETRYABLE_FAILURE;
        assertEquals(ProcessingOutcome.RETRYABLE_FAILURE,
                first.engine().submit(event("r2", "p1", 2)));
        assertEquals(1, first.handler().effects.get());

        // 模拟进程重启：同一目录新建存储与引擎
        Rig second = newRig(dir);
        assertEquals(EventStore.EventStatus.DELIVERED,
                second.store().find("r1").orElseThrow().status());
        assertEquals(EventStore.EventStatus.DEAD,
                second.store().find("r2").orElseThrow().status());
        assertEquals(1, second.store().lastDeliveredSequence("p1"));
        // 重启后重复投递已成功事件：结论一致且不产生新副作用
        assertEquals(ProcessingOutcome.SUCCESS, second.engine().submit(event("r1", "p1", 1)));
        assertEquals(0, second.handler().effects.get());
        // 死信在重启后仍可查询、可重放
        assertEquals(1, second.deadLetters().list().size());
        assertEquals(ProcessingOutcome.SUCCESS, second.deadLetters().replay("r2"));
        assertEquals(1, second.handler().effects.get());
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

        assertEquals(ProcessingOutcome.REJECTED, rig.engine().submit(badVersion));
        assertEquals(ProcessingOutcome.REJECTED, rig.engine().submit(missingField));
        assertEquals(ProcessingOutcome.REJECTED, rig.engine().submit(wrongType));
        assertEquals(RejectionReason.UNSUPPORTED_VERSION,
                rig.store().find("v1").orElseThrow().rejectionReason());
        assertEquals(RejectionReason.MISSING_REQUIRED_FIELD,
                rig.store().find("v2").orElseThrow().rejectionReason());
        assertEquals(RejectionReason.FIELD_TYPE_MISMATCH,
                rig.store().find("v3").orElseThrow().rejectionReason());
        assertEquals(0, rig.handler().effects.get());
    }

    @Test
    void optionalFieldDefaultsAreStable() {
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
        assertEquals(ProcessingOutcome.SUCCESS, rig.engine().submit(v2NoCurrency));
        assertEquals("USD", rig.store().find("c1").orElseThrow().envelope().payload().get("currency"));
    }
}
