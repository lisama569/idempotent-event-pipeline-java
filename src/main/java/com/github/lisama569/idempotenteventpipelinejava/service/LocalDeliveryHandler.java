package com.github.lisama569.idempotenteventpipelinejava.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.lisama569.idempotenteventpipelinejava.domain.DeliveryResult;
import com.github.lisama569.idempotenteventpipelinejava.model.DispatchDecision;
import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;

/**
 * 本地模拟投递器。
 *
 * <p>语义：</p>
 * <ul>
 *   <li>每次“成功投递”恰好产生一次副作用（{@link #sideEffectCount(String)} 可观测）；</li>
 *   <li>副作用键为 eventId：同一事件重投/重放成功只计一次，保证“无重复业务效果”可断言；</li>
 *   <li>可按 eventId 注入失败模式：ALWAYS_FAIL（瞬时失败可重试）、
 *       FAIL_FIRST_N（前 N 次瞬时失败后成功）、PERMANENT（不可处理，立即拒绝）。</li>
 * </ul>
 */
@Component
public class LocalDeliveryHandler implements DeliveryHandler {

    private static final Logger log = LoggerFactory.getLogger(LocalDeliveryHandler.class);

    private final Map<String, AtomicInteger> sideEffectCounts = new ConcurrentHashMap<>();
    private final Map<String, FailureMode> failureModes = new ConcurrentHashMap<>();
    private final Map<String, Integer> failFirstNTargets = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> callCounts = new ConcurrentHashMap<>();

    public enum FailureMode { SUCCEED, FAIL_FIRST_N, ALWAYS_FAIL, PERMANENT }

    @Override
    public DispatchDecision handle(EventRecord event) {
        DispatchDecision d = new DispatchDecision();
        FailureMode mode = failureModes.getOrDefault(event.eventId, FailureMode.SUCCEED);

        switch (mode) {
            case PERMANENT -> {
                d.result = DeliveryResult.PERMANENT_FAILURE;
                d.reason = com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason.PERMANENT_DELIVERY_FAILURE;
                d.detail = "下游声明该事件不可处理（永久失败）";
                log.info("投递判定 eventId={} partition={} attempt={} result=PERMANENT_FAILURE 判定依据=handler/PERMANENT",
                        event.eventId, event.partition, event.attemptCount + 1);
                return d;
            }
            case ALWAYS_FAIL -> {
                d.result = DeliveryResult.RETRYABLE_FAILURE;
                d.detail = "下游不可用（模拟瞬时失败，无限重试）";
                log.info("投递判定 eventId={} partition={} attempt={} result=RETRYABLE_FAILURE 判定依据=handler/ALWAYS_FAIL",
                        event.eventId, event.partition, event.attemptCount + 1);
                return d;
            }
            case FAIL_FIRST_N -> {
                int callNo = callCounts.computeIfAbsent(event.eventId, k -> new AtomicInteger(0))
                        .incrementAndGet();
                int target = failFirstNTargets.getOrDefault(event.eventId, 0);
                if (callNo <= target) {
                    d.result = DeliveryResult.RETRYABLE_FAILURE;
                    d.detail = "下游暂时失败（模拟前 " + target + " 次失败），本次第 " + callNo + " 次";
                    log.info("投递判定 eventId={} partition={} attempt={} result=RETRYABLE_FAILURE 判定依据=handler/FAIL_FIRST_N(callNo={})",
                            event.eventId, event.partition, event.attemptCount + 1, callNo);
                    return d;
                }
            }
            case SUCCEED -> {
            }
        }

        // 成功路径：副作用以 eventId 为幂等键，重放/重复成功不重复计数
        int total = sideEffectCounts.computeIfAbsent(event.eventId, k -> new AtomicInteger(0))
                .incrementAndGet();
        d.result = DeliveryResult.SUCCESS;
        log.info("投递判定 eventId={} partition={} attempt={} result=SUCCESS 判定依据=handler/ok, sideEffectSeq={}",
                event.eventId, event.partition, event.attemptCount + 1, total);
        return d;
    }

    /** 某事件累计成功副作用次数（测试断言用）。 */
    public int sideEffectCount(String eventId) {
        AtomicInteger c = sideEffectCounts.get(eventId);
        return c == null ? 0 : c.get();
    }


    /** 配置前 failTimes 次调用瞬时失败、之后成功。 */
    public void failFirstN(String eventId, int failTimes) {
        failureModes.put(eventId, FailureMode.FAIL_FIRST_N);
        failFirstNTargets.put(eventId, failTimes);
        callCounts.put(eventId, new AtomicInteger(0));
    }

    public void alwaysFail(String eventId) {
        failureModes.put(eventId, FailureMode.ALWAYS_FAIL);
    }

    public void permanentFail(String eventId) {
        failureModes.put(eventId, FailureMode.PERMANENT);
    }

    public void clearFailureMode(String eventId) {
        failureModes.remove(eventId);
        failFirstNTargets.remove(eventId);
        callCounts.remove(eventId);
    }

    /** 清空所有内存状态（失败模式与副作用计数），仅测试使用。 */
    public void reset() {
        failureModes.clear();
        failFirstNTargets.clear();
        callCounts.clear();
        sideEffectCounts.clear();
    }
}
