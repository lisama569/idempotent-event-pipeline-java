package com.github.lisama569.idempotenteventpipelinejava.web;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;
import com.github.lisama569.idempotenteventpipelinejava.model.ReceiveOutcome;
import com.github.lisama569.idempotenteventpipelinejava.repo.EventRepository;
import com.github.lisama569.idempotenteventpipelinejava.service.DeadLetterService;
import com.github.lisama569.idempotenteventpipelinejava.service.EventDispatcher;
import com.github.lisama569.idempotenteventpipelinejava.service.EventReceiver;
import com.github.lisama569.idempotenteventpipelinejava.service.LocalDeliveryHandler;
import com.github.lisama569.idempotenteventpipelinejava.web.ApiDtos.StatusView;

@RestController
public class EventController {

    private final EventReceiver receiver;
    private final EventDispatcher dispatcher;
    private final DeadLetterService deadLetters;
    private final EventRepository repository;
    private final LocalDeliveryHandler handler;

    public EventController(EventReceiver receiver, EventDispatcher dispatcher,
                           DeadLetterService deadLetters, EventRepository repository,
                           LocalDeliveryHandler handler) {
        this.receiver = receiver;
        this.dispatcher = dispatcher;
        this.deadLetters = deadLetters;
        this.repository = repository;
        this.handler = handler;
    }

    /** 接收事件。重复投递返回 200 + duplicate=true；非法输入由全局异常处理器分类拒绝（400）。 */
    @PostMapping("/events")
    public ResponseEntity<ReceiveOutcome> receive(@RequestBody String rawJson) {
        ReceiveOutcome outcome = receiver.receive(rawJson);
        return ResponseEntity.ok(outcome);
    }

    /** 手动推动一轮调度（测试与本地验证用）。 */
    @PostMapping("/dispatch")
    public Map<String, Integer> dispatch(@RequestParam(required = false) String partition) {
        int n = partition == null ? dispatcher.processOnce() : dispatcher.processPartitionOnce(partition);
        return Map.of("processed", n);
    }

    /** 查询单个事件状态（含投递次数、拒绝类别与本地副作用观测次数）。 */
    @GetMapping("/events/{eventId}")
    public ResponseEntity<StatusView> status(@PathVariable String eventId) {
        EventRecord r = repository.findByEventId(eventId);
        if (r == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(toView(r));
    }

    /** 死信列表：可按分区过滤；结果按分区与原 seq 排序。 */
    @GetMapping("/dead-letters")
    public List<StatusView> listDeadLetters(@RequestParam(required = false) String partition) {
        return deadLetters.list(partition).stream().map(this::toView).toList();
    }

    /** 按原顺序重放某分区死信。 */
    @PostMapping("/dead-letters/replay")
    public Map<String, Integer> replay(@RequestParam String partition) {
        return Map.of("replayed", deadLetters.replay(partition));
    }

    private StatusView toView(EventRecord r) {
        StatusView v = new StatusView();
        v.eventId = r.eventId;
        v.partition = r.partition;
        v.seqInPartition = r.seqInPartition;
        v.status = r.status.name();
        v.attemptCount = r.attemptCount;
        v.rejectReason = r.rejectReason == null ? null : r.rejectReason.name();
        v.rejectDetail = r.rejectDetail;
        v.observedSideEffects = handler.sideEffectCount(r.eventId);
        return v;
    }
}
