package com.github.lisama569.idempotenteventpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@org.springframework.boot.test.context.SpringBootTest(webEnvironment =
        org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT)
class HttpApiIntegrationTest extends AbstractPipelineTest {

    @LocalServerPort
    private int port;
    @Autowired
    private TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    void receiveRejectDuplicateDispatchAndQuery() {
        String body = eventJson("h1", "hp", "T");

        ResponseEntity<String> ok = rest.postForEntity(url("/events"), new HttpEntity<>(body), String.class);
        assertEquals(HttpStatus.OK, ok.getStatusCode());
        assertTrue(ok.getBody().contains("\"duplicate\":false"));

        ResponseEntity<String> dup = rest.postForEntity(url("/events"), new HttpEntity<>(body), String.class);
        assertTrue(dup.getBody().contains("DUPLICATE_EVENT"));

        ResponseEntity<String> dispatched = rest.postForEntity(
                url("/dispatch"), new HttpEntity<>(""), String.class);
        assertTrue(dispatched.getBody().contains("\"processed\":1"));

        ResponseEntity<String> status = rest.getForEntity(url("/events/h1"), String.class);
        assertEquals(HttpStatus.OK, status.getStatusCode());
        assertTrue(status.getBody().contains("SUCCEEDED"));
        assertTrue(status.getBody().contains("\"attemptCount\":1"));
        assertTrue(status.getBody().contains("\"observedSideEffects\":1"));

        assertEquals(HttpStatus.NOT_FOUND, rest.getForEntity(url("/events/nope"), String.class).getStatusCode());
    }

    @Test
    void rejectsAreClassifiedOverHttp() {
        ResponseEntity<String> badVersion = rest.postForEntity(url("/events"),
                new HttpEntity<>("{\"version\":42,\"eventId\":\"x\",\"partition\":\"p\",\"type\":\"T\"}"),
                String.class);
        assertEquals(HttpStatus.BAD_REQUEST, badVersion.getStatusCode());
        assertTrue(badVersion.getBody().contains("UNSUPPORTED_VERSION"));

        ResponseEntity<String> malformed = rest.postForEntity(url("/events"),
                new HttpEntity<>("{nope"), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, malformed.getStatusCode());
        assertTrue(malformed.getBody().contains("MALFORMED_JSON"));

        ResponseEntity<String> missing = rest.postForEntity(url("/events"),
                new HttpEntity<>("{\"version\":1,\"partition\":\"p\",\"type\":\"T\"}"), String.class);
        assertEquals(HttpStatus.BAD_REQUEST, missing.getStatusCode());
        assertTrue(missing.getBody().contains("MISSING_FIELD"));

        ResponseEntity<String> typeMismatch = rest.postForEntity(url("/events"),
                new HttpEntity<>("{\"version\":1,\"eventId\":9,\"partition\":\"p\",\"type\":\"T\"}"),
                String.class);
        assertEquals(HttpStatus.BAD_REQUEST, typeMismatch.getStatusCode());
        assertNotNull(typeMismatch.getBody());
        assertTrue(typeMismatch.getBody().contains("TYPE_MISMATCH"));
    }
}
