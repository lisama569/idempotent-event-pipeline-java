package com.github.lisama569.idempotenteventpipelinejava.pipeline;

/** 业务事件处理器：同一事件的副作用只允许生效一次由流水线保证。 */
public interface EventHandler {
    ProcessingOutcome handle(EventEnvelope event);
}
