package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import com.github.lisama569.idempotenteventpipelinejava.pipeline.store.EventStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** 事件接收与死信管理 REST 入口。 */
@RestController
public class EventController {

    private final DeliveryEngine engine;
    private final DeadLetterService deadLetters;

    public EventController(DeliveryEngine engine, DeadLetterService deadLetters) {
        this.engine = engine;
        this.deadLetters = deadLetters;
    }

    @PostMapping("/events")
    public Map<String, Object> receive(@RequestBody EventEnvelope event) {
        ProcessingOutcome outcome = engine.submit(event);
        return Map.of("eventId", event.eventId(), "outcome", outcome.name());
    }

    @GetMapping("/dead-letters")
    public List<EventStore.StoredEvent> listDeadLetters() {
        return deadLetters.list();
    }

    @PostMapping("/dead-letters/{eventId}/replay")
    public ResponseEntity<Map<String, Object>> replay(@PathVariable String eventId) {
        try {
            ProcessingOutcome outcome = deadLetters.replay(eventId);
            return ResponseEntity.ok(Map.of("eventId", eventId, "outcome", outcome.name()));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ResponseEntity.badRequest().body(
                    Map.of("eventId", eventId, "error", e.getMessage()));
        }
    }
}
