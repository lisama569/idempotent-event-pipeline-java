package com.github.lisama569.idempotenteventpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.lisama569.idempotenteventpipelinejava.model.EventRecord;
import com.github.lisama569.idempotenteventpipelinejava.repo.EventRepository;
import com.github.lisama569.idempotenteventpipelinejava.service.EventDispatcher;
import com.github.lisama569.idempotenteventpipelinejava.service.EventReceiver;

class SmokePipelineTest extends AbstractPipelineTest {

    @Autowired
    private EventReceiver receiver;
    @Autowired
    private EventDispatcher dispatcher;
    @Autowired
    private EventRepository repository;

    @Test
    void happyPath() {
        var outcome = receiver.receive(eventJson("e1", "p1", "T"));
        assertFalse(outcome.duplicate);
        assertEquals(1, dispatcher.processOnce());

        EventRecord r = repository.findByEventId("e1");
        assertEquals("SUCCEEDED", r.status.name());
        assertEquals(1, r.attemptCount);
        assertEquals(1, handler.sideEffectCount("e1"));
    }
}
