package com.github.lisama569.idempotenteventpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;
import com.github.lisama569.idempotenteventpipelinejava.repo.EventRepository;
import com.github.lisama569.idempotenteventpipelinejava.service.EventDispatcher;
import com.github.lisama569.idempotenteventpipelinejava.service.EventReceiver;

/**
 * 同一分区并发处理：
 * 多线程同时推动同一分区，事件必须严格按 seq 成功、每个事件副作用恰好一次；
 * 另验证不同分区可并行推进。
 */
class PartitionConcurrencyTest extends AbstractPipelineTest {

    @Autowired
    private EventReceiver receiver;
    @Autowired
    private EventDispatcher dispatcher;
    @Autowired
    private EventRepository repository;

    @Test
    void samePartitionProcessedInOrderOnceEach() throws Exception {
        int n = 20;
        for (int i = 1; i <= n; i++) {
            receiver.receive(eventJson("ord-" + i, "ordered", "T"));
        }

        int threads = 6;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                // 每线程反复推动该分区，直到没有可处理事件
                while (dispatcher.processPartitionOnce("ordered") == 1) {
                    // spin
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        for (int i = 1; i <= n; i++) {
            EventRecord r = repository.findByEventId("ord-" + i);
            assertEquals("SUCCEEDED", r.status.name(), "event ord-" + i);
            assertEquals(i, r.seqInPartition, "seq 必须按接收顺序分配");
            assertEquals(1, r.attemptCount, "每个事件只能被成功处理一次");
            assertEquals(1, handler.sideEffectCount("ord-" + i), "副作用恰好一次");
        }
    }

    @Test
    void differentPartitionsAdvanceInParallel() throws Exception {
        for (String p : List.of("pa", "pb", "pc")) {
            for (int i = 1; i <= 5; i++) {
                receiver.receive(eventJson(p + "-" + i, p, "T"));
            }
        }

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(3);
        List<String> processedOrder = Collections.synchronizedList(new ArrayList<>());
        ConcurrentHashMap<String, Boolean> threads = new ConcurrentHashMap<>();
        for (String p : List.of("pa", "pb", "pc")) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                threads.put(Thread.currentThread().getName(), Boolean.TRUE);
                while (dispatcher.processPartitionOnce(p) == 1) {
                    processedOrder.add(p);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        for (String p : List.of("pa", "pb", "pc")) {
            for (int i = 1; i <= 5; i++) {
                EventRecord r = repository.findByEventId(p + "-" + i);
                assertEquals("SUCCEEDED", r.status.name());
                assertEquals(i, r.seqInPartition);
                assertEquals(1, handler.sideEffectCount(p + "-" + i));
            }
        }
    }
}
