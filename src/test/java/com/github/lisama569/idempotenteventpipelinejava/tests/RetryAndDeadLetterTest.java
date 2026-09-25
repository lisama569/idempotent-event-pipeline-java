package com.github.lisama569.idempotenteventpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason;
import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;
import com.github.lisama569.idempotenteventpipelinejava.repo.EventRepository;
import com.github.lisama569.idempotenteventpipelinejava.service.DeadLetterService;
import com.github.lisama569.idempotenteventpipelinejava.service.EventDispatcher;
import com.github.lisama569.idempotenteventpipelinejava.service.EventReceiver;

/**
 * 失败重试、退避、死信隔离与按原顺序重放。
 * 测试参数：maxAttempts=3, backoff base=10ms, multiplier=2 -> 退避 10ms、20ms。
 */
class RetryAndDeadLetterTest extends AbstractPipelineTest {

    @Autowired
    private EventReceiver receiver;
    @Autowired
    private EventDispatcher dispatcher;
    @Autowired
    private DeadLetterService deadLetters;
    @Autowired
    private EventRepository repository;

    @Test
    void retryThenSuccessRespectsBackoff() {
        receiver.receive(eventJson("flaky", "p1", "T"));
        handler.failFirstN("flaky", 2);

        // 第 1 次：失败 -> RETRYING, 退避 10ms
        assertEquals(1, dispatcher.processOnce());
        EventRecord r1 = repository.findByEventId("flaky");
        assertEquals("RETRYING", r1.status.name());
        assertEquals(1, r1.attemptCount);
        Instant visible = r1.nextVisibleAt;
        long backoff = Duration.between(Instant.now(), visible).toMillis();
        assertTrue(backoff >= 5 && backoff <= 15, "首次退避应为 ~10ms，实际=" + backoff);
        assertEquals(0, handler.sideEffectCount("flaky"));

        // 退避未到期，队头不可见
        assertEquals(0, dispatcher.processOnce());

        // 到期后第 2 次：仍失败 -> 退避 20ms
        try {
            Thread.sleep(12);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertEquals(1, dispatcher.processOnce());
        EventRecord r2 = repository.findByEventId("flaky");
        assertEquals("RETRYING", r2.status.name());
        assertEquals(2, r2.attemptCount);
        long backoff2 = Duration.between(Instant.now(), r2.nextVisibleAt).toMillis();
        assertTrue(backoff2 >= 15 && backoff2 <= 25, "二次退避应为 ~20ms，实际=" + backoff2);

        // 第 3 次成功
        try {
            Thread.sleep(22);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertEquals(1, dispatcher.processOnce());
        EventRecord r3 = repository.findByEventId("flaky");
        assertEquals("SUCCEEDED", r3.status.name());
        assertEquals(3, r3.attemptCount);
        assertEquals(1, handler.sideEffectCount("flaky"), "失败尝试不产生副作用，最终成功恰好一次");
    }

    @Test
    void retriesExhaustedEntersDeadLetter() {
        receiver.receive(eventJson("doomed", "p1", "T"));
        handler.alwaysFail("doomed");

        // 驱动 3 次尝试（每次之间等待退避到期）
        for (int round = 0; round < 3; round++) {
            dispatcher.processOnce();
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        EventRecord r = repository.findByEventId("doomed");
        assertEquals("DEAD_LETTER", r.status.name());
        assertEquals(3, r.attemptCount);
        assertEquals(RejectReason.RETRY_EXHAUSTED, r.rejectReason);
        assertTrue(r.rejectDetail.contains("重试 3 次"));
        assertEquals(0, handler.sideEffectCount("doomed"));
        assertEquals(1, deadLetters.list("p1").size());

        // 死信期间普通调度不再处理它
        assertEquals(0, dispatcher.processOnce());
        assertEquals("DEAD_LETTER", repository.findByEventId("doomed").status.name());
    }

    @Test
    void permanentFailureIsRejectedImmediatelyWithDistinctReason() {
        receiver.receive(eventJson("poison", "p1", "T"));
        handler.permanentFail("poison");

        dispatcher.processOnce();
        EventRecord r = repository.findByEventId("poison");
        assertEquals("DEAD_LETTER", r.status.name());
        assertEquals(1, r.attemptCount, "永久失败不重试");
        assertEquals(RejectReason.PERMANENT_DELIVERY_FAILURE, r.rejectReason);
    }

    @Test
    void replayDeadLetterInOriginalOrderSucceedsWithoutDuplicateSideEffects() {
        // 两个事件：e-a 先失败耗尽进入死信并顶住分区，e-b 排在其后
        receiver.receive(eventJson("e-a", "p1", "T"));
        receiver.receive(eventJson("e-b", "p1", "T"));
        handler.alwaysFail("e-a");

        for (int round = 0; round < 3; round++) {
            dispatcher.processOnce();
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // e-a 死信顶住队头：e-b 始终无法被处理（保序）
        assertEquals("DEAD_LETTER", repository.findByEventId("e-a").status.name());
        assertEquals("RECEIVED", repository.findByEventId("e-b").status.name());
        assertEquals(0, handler.sideEffectCount("e-b"));

        // 重放前修复下游：e-a 变为成功
        handler.clearFailureMode("e-a");
        int replayed = deadLetters.replay("p1");
        assertEquals(1, replayed);

        EventRecord a = repository.findByEventId("e-a");
        EventRecord b = repository.findByEventId("e-b");
        assertEquals("SUCCEEDED", a.status.name(), "重放走同一条投递链路，幂等不绕过");
        assertEquals("SUCCEEDED", b.status.name(), "队头释放后，后续事件按原序补处理");
        assertEquals(1, a.seqInPartition);
        assertEquals(2, b.seqInPartition);
        assertEquals(1, handler.sideEffectCount("e-a"));
        assertEquals(1, handler.sideEffectCount("e-b"));
        assertTrue(deadLetters.list("p1").isEmpty());

        // 再次重放（已无死信）不得让成功事件再产生副作用
        assertEquals(0, deadLetters.replay("p1"));
        dispatcher.processOnce();
        assertEquals(1, handler.sideEffectCount("e-a"));
        assertEquals(1, handler.sideEffectCount("e-b"));
    }
}
