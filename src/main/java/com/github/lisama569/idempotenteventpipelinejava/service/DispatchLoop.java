package com.github.lisama569.idempotenteventpipelinejava.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 后台轮询：周期性推动所有分区前进。可通过 pipeline.dispatcher.enabled 关闭（测试中手动驱动）。 */
@Component
public class DispatchLoop {

    private static final Logger log = LoggerFactory.getLogger(DispatchLoop.class);

    private final EventDispatcher dispatcher;
    private final PipelineProperties props;

    public DispatchLoop(EventDispatcher dispatcher, PipelineProperties props) {
        this.dispatcher = dispatcher;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${pipeline.dispatcher.interval-millis:200}")
    public void tick() {
        if (!props.getDispatcher().isEnabled()) {
            return;
        }
        try {
            dispatcher.processOnce();
        } catch (RuntimeException e) {
            // 单轮失败不杀死调度线程，下一轮继续（事件状态由 DB 保证不丢）
            log.warn("调度轮次异常，下一轮继续: {}", e.toString());
        }
    }
}
