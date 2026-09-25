package com.github.lisama569.idempotenteventpipelinejava.tests;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import com.github.lisama569.idempotenteventpipelinejava.service.LocalDeliveryHandler;

@SpringBootTest
public abstract class AbstractPipelineTest {

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected LocalDeliveryHandler handler;

    @BeforeEach
    void cleanState() {
        jdbc.update("DELETE FROM events");
        jdbc.update("DELETE FROM partition_seq");
        handler.reset();
    }

    protected static String eventJson(String eventId, String partition, String type) {
        return "{\"version\":1,\"eventId\":\"" + eventId + "\",\"partition\":\"" + partition
                + "\",\"type\":\"" + type + "\",\"payload\":{\"x\":1}}";
    }
}
