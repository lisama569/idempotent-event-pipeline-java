package com.github.lisama569.idempotenteventpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason;
import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;
import com.github.lisama569.idempotenteventpipelinejava.repo.EventRepository;
import com.github.lisama569.idempotenteventpipelinejava.service.EventDispatcher;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 重复投递：
 * 1) 同一事件串行提交两次 -> 第二次 DUPLICATE_EVENT；
 * 2) 同一事件多线程并发提交 -> 库里恰有一行；
 * 3) 重复接收 + 多轮调度后，成功副作用恰好一次。
 */
class DuplicateDeliveryTest extends AbstractPipelineTest {

    @Autowired
    private com.github.lisama569.idempotenteventpipelinejava.service.EventReceiver receiver;
    @Autowired
    private EventDispatcher dispatcher;
    @Autowired
    private EventRepository repository;
    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void serialDuplicateIsAbsorbed() {
        var first = receiver.receive(eventJson("dup-1", "p1", "T"));
        var second = receiver.receive(eventJson("dup-1", "p1", "T"));

        assertTrue(!first.duplicate);
        assertTrue(second.duplicate);
        assertEquals(RejectReason.DUPLICATE_EVENT, second.reason);
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM events WHERE event_id='dup-1'", Integer.class));
        assertEquals(1, jdbc.queryForObject(
                "SELECT seq_in_partition FROM events WHERE event_id='dup-1'", Long.class));

        dispatcher.processOnce();
        // 成功后再次重复投递，仍然幂等
        var third = receiver.receive(eventJson("dup-1", "p1", "T"));
        assertTrue(third.duplicate);
        dispatcher.processOnce();

        assertEquals("SUCCEEDED", repository.findByEventId("dup-1").status.name());
        assertEquals(1, handler.sideEffectCount("dup-1"), "重复投递不得产生重复业务效果");
    }

    @Test
    void concurrentDuplicateProducesSingleRow() throws Exception {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> receiver.receive(eventJson("dup-2", "pX", "T")));
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        Integer rows = jdbc.queryForObject(
                "SELECT COUNT(*) FROM events WHERE event_id='dup-2'", Integer.class);
        assertEquals(1, rows, "并发重复接收必须只落一行");

        int duplicates = 0;
        for (int i = 0; i < 10; i++) {
            dispatcher.processOnce();
        }
        EventRecord r = repository.findByEventId("dup-2");
        assertEquals("SUCCEEDED", r.status.name());
        assertEquals(1, handler.sideEffectCount("dup-2"));
    }
}
