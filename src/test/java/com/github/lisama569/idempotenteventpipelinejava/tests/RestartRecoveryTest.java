package com.github.lisama569.idempotenteventpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.sql.Timestamp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import com.github.lisama569.idempotenteventpipelinejava.Application;
import com.github.lisama569.idempotenteventpipelinejava.repo.EventRepository;
import com.github.lisama569.idempotenteventpipelinejava.service.EventDispatcher;
import com.github.lisama569.idempotenteventpipelinejava.service.EventReceiver;
import com.github.lisama569.idempotenteventpipelinejava.service.LocalDeliveryHandler;

/**
 * 进程重启恢复：使用文件型 H2，顺序启动两个应用上下文模拟重启。
 * - 重启前已 SUCCEEDED 的事件：重启后不得重复投递（无新副作用）；
 * - 崩溃时遗留在 IN_FLIGHT 的僵死锁：超过 stale 阈值后被新执行器回收并续投；
 * - RETRYING（退避已到期）与 RECEIVED：重启后继续推进；
 * - 最终状态确定，副作用次数一致。
 */
class RestartRecoveryTest {

    private ConfigurableApplicationContext startContext(Path dir) {
        return new SpringApplicationBuilder(Application.class)
                .web(WebApplicationType.NONE)
                .properties(
                        "spring.datasource.url=jdbc:h2:file:" + dir.toAbsolutePath() + "/db",
                        "spring.sql.init.mode=always",
                        "pipeline.max-attempts=3",
                        "pipeline.backoff.base-millis=10",
                        "pipeline.backoff.multiplier=2",
                        "pipeline.dispatcher.enabled=false",
                        "pipeline.recovery-lock-stale-millis=500")
                .run();
    }

    private static String json(String id, String p) {
        return "{\"version\":1,\"eventId\":\"" + id + "\",\"partition\":\"" + p
                + "\",\"type\":\"T\",\"payload\":{}}";
    }

    @Test
    void stateSurvivesRestartAndStaleLockIsReclaimed(@TempDir Path dir) throws Exception {
        ConfigurableApplicationContext first = startContext(dir);
        try {
            EventReceiver receiver = first.getBean(EventReceiver.class);
            EventDispatcher dispatcher = first.getBean(EventDispatcher.class);
            JdbcTemplate jdbc = first.getBean(JdbcTemplate.class);
            LocalDeliveryHandler handler = first.getBean(LocalDeliveryHandler.class);

            receiver.receive(json("a-done", "p1"));
            receiver.receive(json("b-flight", "p1"));
            receiver.receive(json("c-retry", "p1"));
            receiver.receive(json("d-new", "p1"));

            // 全部尝试一轮：a 成功；b/c/d 默认 handler 也会成功，所以手工制造崩溃现场
            dispatcher.processOnce(); // a-done -> SUCCEEDED
            assertEquals("SUCCEEDED", jdbc.queryForObject(
                    "SELECT status FROM events WHERE event_id='a-done'", String.class));

            // 模拟进程在 b 投递途中崩溃：IN_FLIGHT + 过期持有者锁
            jdbc.update("UPDATE events SET status='IN_FLIGHT', locked_by='dead-worker', "
                            + "locked_at=?, attempt_count=1 WHERE event_id='b-flight'",
                    Timestamp.from(Instant.now().minusSeconds(60)));
            // c 处于退避（next_visible_at 已到期）
            jdbc.update("UPDATE events SET status='RETRYING', attempt_count=1, "
                            + "next_visible_at=? WHERE event_id='c-retry'",
                    Timestamp.from(Instant.now().minusSeconds(1)));
            // d 保持 RECEIVED
            assertEquals(1, handler.sideEffectCount("a-done"));
        } finally {
            first.close();
        }

        // 等待超过 stale 锁阈值，确保新执行器判定 b 的锁已僵死
        Thread.sleep(700);

        ConfigurableApplicationContext second = startContext(dir);
        try {
            EventDispatcher dispatcher = second.getBean(EventDispatcher.class);
            EventRepository repository = second.getBean(EventRepository.class);
            LocalDeliveryHandler handler = second.getBean(LocalDeliveryHandler.class);

            // 新进程内反复推动直到没有可处理事件
            int guard = 0;
            while (dispatcher.processOnce() > 0 && guard++ < 20) {
                // b 的僵死锁被回收 -> b；随后 c、d 按 seq 顺序补处理
            }

            for (String id : new String[]{"a-done", "b-flight", "c-retry", "d-new"}) {
                assertEquals("SUCCEEDED", repository.findByEventId(id).status.name(),
                        id + " 重启后最终必须成功");
            }
            // a 在重启前已成功，重启后绝不能再执行一次副作用
            assertEquals(0, handler.sideEffectCount("a-done"),
                    "已成功事件在重启后不得重复产生副作用");
            // b/c/d 在新进程各被投递成功一次
            assertEquals(1, handler.sideEffectCount("b-flight"));
            assertEquals(1, handler.sideEffectCount("c-retry"));
            assertEquals(1, handler.sideEffectCount("d-new"));
            // seq 顺序保持
            assertEquals(1, repository.findByEventId("a-done").seqInPartition);
            assertEquals(2, repository.findByEventId("b-flight").seqInPartition);
            assertEquals(3, repository.findByEventId("c-retry").seqInPartition);
            assertEquals(4, repository.findByEventId("d-new").seqInPartition);
            assertTrue(repository.findDeadLetters("p1").isEmpty());
        } finally {
            second.close();
        }
    }
}
