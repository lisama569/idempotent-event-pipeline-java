package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 事件接收、进度查询与死信管理 REST 入口。 */
@RestController
public class EventController {

    private final DeliveryEngine engine;
    private final DeadLetterService deadLetters;
    private final DeliveryProgressService progress;

    public EventController(DeliveryEngine engine,
                           DeadLetterService deadLetters,
                           DeliveryProgressService progress) {
        this.engine = engine;
        this.deadLetters = deadLetters;
        this.progress = progress;
    }

    /** 接收事件：立即返回回执（202 表示后台继续投递），不等待投递完成。 */
    @PostMapping("/events")
    public ResponseEntity<Map<String, Object>> receive(@RequestBody EventEnvelope event) {
        SubmissionAck ack = engine.submit(event);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("eventId", ack.eventId());
        body.put("decision", ack.decision().name());
        body.put("status", ack.status().name());
        if (ack.rejectionReason() != null) {
            body.put("rejectionReason", ack.rejectionReason().name());
        }
        HttpStatus http = ack.decision() == SubmissionAck.Decision.ACCEPTED
                ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(http).body(body);
    }

    /** 按事件标识查询投递进度（只读，不改变状态、不触发副作用）。 */
    @GetMapping("/events/{eventId}")
    public ResponseEntity<DeliveryProgress> progressOf(@PathVariable String eventId) {
        return progress.progressOf(eventId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/dead-letters")
    public List<EventStore.StoredEvent> listDeadLetters() {
        return deadLetters.list();
    }

    @PostMapping("/dead-letters/{eventId}/replay")
    public ResponseEntity<Map<String, Object>> replay(@PathVariable String eventId) {
        try {
            SubmissionAck ack = deadLetters.replay(eventId);
            return ResponseEntity.accepted().body(Map.of(
                    "eventId", eventId, "decision", ack.decision().name()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(
                    Map.of("eventId", eventId, "error", e.getMessage()));
        }
    }
}
