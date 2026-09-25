package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 默认业务处理器（演示用）：把已生效的 eventId 记入内存集合作为“业务副作用”。
 * 负载 {@code type=FAIL} 模拟可重试失败，{@code type=REJECT} 模拟业务拒绝。
 */
@Component
public class DefaultEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(DefaultEventHandler.class);

    private final Set<String> appliedEventIds = ConcurrentHashMap.newKeySet();

    @Override
    public ProcessingOutcome handle(EventEnvelope event) {
        Object type = event.payload().get("type");
        if ("FAIL".equals(type)) {
            return ProcessingOutcome.RETRYABLE_FAILURE;
        }
        if ("REJECT".equals(type)) {
            return ProcessingOutcome.REJECTED;
        }
        appliedEventIds.add(event.eventId());
        log.info("event={} partition={} side-effect applied (total={})",
                event.eventId(), event.partitionKey(), appliedEventIds.size());
        return ProcessingOutcome.SUCCESS;
    }

    /** 已生效的副作用次数（测试与演示用）。 */
    public int appliedCount() {
        return appliedEventIds.size();
    }
}
