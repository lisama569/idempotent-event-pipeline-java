package com.github.lisama569.idempotenteventpipelinejava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason;
import com.github.lisama569.idempotenteventpipelinejava.domain.ValidationException;
import com.github.lisama569.idempotenteventpipelinejava.model.EventEnvelope;
import com.github.lisama569.idempotenteventpipelinejava.service.EventValidator;

/**
 * 事件版本兼容：
 * - 新增可选字段 priority 有确定性缺省值 0；
 * - v1/v2 均可处理；未知版本（含更高必需版本）被拒绝；
 * - 字段缺失 / 类型不匹配按类别拒绝；
 * - 同一输入多次解析判定一致。
 */
class VersionCompatibilityTest {

    private final EventValidator validator = new EventValidator();

    private ValidationException expect(RejectReason reason, String json) {
        ValidationException ex = assertThrows(ValidationException.class,
                () -> validator.parseAndValidate(json));
        assertEquals(reason, ex.getReason());
        return ex;
    }

    @Test
    void optionalPriorityHasDeterministicDefault() {
        EventEnvelope v1 = validator.parseAndValidate(
                "{\"version\":1,\"eventId\":\"e\",\"partition\":\"p\",\"type\":\"T\"}");
        assertEquals(0, v1.priority, "v1 缺省 priority 必须为确定的 0");
        assertEquals(null, v1.payload);

        EventEnvelope v2default = validator.parseAndValidate(
                "{\"version\":2,\"eventId\":\"e2\",\"partition\":\"p\",\"type\":\"T\"}");
        assertEquals(0, v2default.priority, "v2 省略 priority 缺省为 0");

        EventEnvelope v2 = validator.parseAndValidate(
                "{\"version\":2,\"eventId\":\"e3\",\"partition\":\"p\",\"type\":\"T\",\"priority\":7}");
        assertEquals(7, v2.priority);

        // 未知额外字段被忽略（不参与判定）
        EventEnvelope extra = validator.parseAndValidate(
                "{\"version\":1,\"eventId\":\"e4\",\"partition\":\"p\",\"type\":\"T\","
                        + "\"futureField\":{\"a\":1}}");
        assertEquals(0, extra.priority);

        // payload 任意 JSON
        EventEnvelope withPayload = validator.parseAndValidate(
                "{\"version\":1,\"eventId\":\"e5\",\"partition\":\"p\",\"type\":\"T\","
                        + "\"payload\":[1,2,{\"k\":\"v\"}]}");
        assertEquals("[1,2,{\"k\":\"v\"}]", withPayload.payload);
    }

    @Test
    void rejectedInputsAreClassifiedAndDeterministic() {
        List<String> badJson = List.of(
                "not-json", "", "{", "null", "[1,2]", "\"str\"");
        for (String s : badJson) {
            expect(RejectReason.MALFORMED_JSON, s);
            expect(RejectReason.MALFORMED_JSON, s); // 结论一致
        }

        expect(RejectReason.UNSUPPORTED_VERSION,
                "{\"version\":3,\"eventId\":\"e\",\"partition\":\"p\",\"type\":\"T\"}");
        expect(RejectReason.UNSUPPORTED_VERSION,
                "{\"version\":0,\"eventId\":\"e\",\"partition\":\"p\",\"type\":\"T\"}");
        expect(RejectReason.UNSUPPORTED_VERSION,
                "{\"version\":99,\"eventId\":\"e\",\"partition\":\"p\",\"type\":\"T\"}");

        expect(RejectReason.MISSING_FIELD,
                "{\"eventId\":\"e\",\"partition\":\"p\",\"type\":\"T\"}"); // 无 version
        expect(RejectReason.MISSING_FIELD,
                "{\"version\":1,\"partition\":\"p\",\"type\":\"T\"}"); // 无 eventId
        expect(RejectReason.MISSING_FIELD,
                "{\"version\":1,\"eventId\":\"e\",\"type\":\"T\"}"); // 无 partition
        expect(RejectReason.MISSING_FIELD,
                "{\"version\":1,\"eventId\":\"e\",\"partition\":\"p\"}"); // 无 type
        expect(RejectReason.MISSING_FIELD,
                "{\"version\":1,\"eventId\":\"  \",\"partition\":\"p\",\"type\":\"T\"}"); // 空白

        expect(RejectReason.TYPE_MISMATCH,
                "{\"version\":\"1\",\"eventId\":\"e\",\"partition\":\"p\",\"type\":\"T\"}");
        expect(RejectReason.TYPE_MISMATCH,
                "{\"version\":1,\"eventId\":123,\"partition\":\"p\",\"type\":\"T\"}");
        expect(RejectReason.TYPE_MISMATCH,
                "{\"version\":1,\"eventId\":\"e\",\"partition\":1,\"type\":\"T\"}");
        expect(RejectReason.TYPE_MISMATCH,
                "{\"version\":1,\"eventId\":\"e\",\"partition\":\"p\",\"type\":true}");
        expect(RejectReason.TYPE_MISMATCH,
                "{\"version\":1,\"eventId\":\"e\",\"partition\":\"p\",\"type\":\"T\","
                        + "\"priority\":\"high\"}");
    }

    @Test
    void sameInputAlwaysSameDecision() {
        String good = "{\"version\":2,\"eventId\":\"g\",\"partition\":\"p\",\"type\":\"T\","
                + "\"priority\":5,\"payload\":{\"ok\":true}}";
        for (int i = 0; i < 5; i++) {
            EventEnvelope e = validator.parseAndValidate(good);
            assertEquals(2, e.version);
            assertEquals(5, e.priority);
        }
        String bad = "{\"version\":7,\"eventId\":\"g\",\"partition\":\"p\",\"type\":\"T\"}";
        for (int i = 0; i < 5; i++) {
            expect(RejectReason.UNSUPPORTED_VERSION, bad);
        }
    }
}
