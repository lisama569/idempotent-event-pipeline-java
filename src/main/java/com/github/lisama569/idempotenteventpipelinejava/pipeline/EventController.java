package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** 事件接收、进度查询与死信管理 REST 入口。 */
@RestController
public class EventController {

    private final DeliveryEngine engine;
    private final DeadLetterService deadLetters;
    private final EventProgressService progress;

    public EventController(DeliveryEngine engine, DeadLetterService deadLetters,
                           EventProgressService progress) {
        this.engine = engine;
        this.deadLetters = deadLetters;
        this.progress = progress;
    }

    /** 接收事件：立即返回接收结论（ACCEPTED 表示后台会继续投递）。 */
    @PostMapping("/events")
    public ResponseEntity<Map<String, Object>> receive(@RequestBody EventEnvelope event) {
        ReceiveOutcome outcome = engine.submit(event);
        HttpStatus status = outcome == ReceiveOutcome.ACCEPTED
                ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status)
                .body(Map.of("eventId", event.eventId(), "outcome", outcome.name()));
    }

    /** 按事件标识查询投递进度（只读，在途与重试等待中的事件也可查）。 */
    @GetMapping("/events/{eventId}/progress")
    public ResponseEntity<EventProgress> progress(@PathVariable String eventId) {
        return progress.progress(eventId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/dead-letters")
    public List<EventStore.StoredEvent> listDeadLetters() {
        return deadLetters.list();
    }

    @PostMapping("/dead-letters/{eventId}/replay")
    public ResponseEntity<Map<String, Object>> replay(@PathVariable String eventId) {
        try {
            ReceiveOutcome outcome = deadLetters.replay(eventId);
            return ResponseEntity.accepted().body(
                    Map.of("eventId", eventId, "outcome", outcome.name()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(
                    Map.of("eventId", eventId, "error", e.getMessage()));
        }
    }
}
