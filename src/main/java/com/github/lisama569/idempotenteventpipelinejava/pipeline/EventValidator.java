package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 事件版本兼容校验。
 *
 * <p>规则（对同一输入多次校验结论一致，纯函数）：
 * <ul>
 *   <li>支持的必需版本集合为 {1, 2}，其余版本以 {@link RejectionReason#UNSUPPORTED_VERSION} 拒绝；</li>
 *   <li>必需字段：{@code type}(String)、{@code amount}(Number)，缺失或以 null 值出现
 *       以 {@link RejectionReason#MISSING_REQUIRED_FIELD} 拒绝；</li>
 *   <li>字段类型不匹配以 {@link RejectionReason#FIELD_TYPE_MISMATCH} 拒绝；</li>
 *   <li>v2 新增可选字段 {@code currency}(String)，缺省时由 {@link #applyDefaults} 赋予
 *       稳定缺省值 {@code "USD"}；缺省语义确定，不随时间/环境变化。</li>
 * </ul>
 */
@Component
public class EventValidator {

    public static final int MIN_SUPPORTED_VERSION = 1;
    public static final int MAX_SUPPORTED_VERSION = 2;
    public static final String DEFAULT_CURRENCY = "USD";

    /** 校验通过返回 null，否则返回拒绝原因。 */
    public RejectionReason validate(EventEnvelope event) {
        if (event == null) {
            return RejectionReason.MISSING_REQUIRED_FIELD;
        }
        if (event.version() < MIN_SUPPORTED_VERSION || event.version() > MAX_SUPPORTED_VERSION) {
            return RejectionReason.UNSUPPORTED_VERSION;
        }
        Map<String, Object> payload = event.payload();
        if (payload == null) {
            return RejectionReason.MISSING_REQUIRED_FIELD;
        }
        if (!payload.containsKey("type") || payload.get("type") == null) {
            return RejectionReason.MISSING_REQUIRED_FIELD;
        }
        if (!(payload.get("type") instanceof String)) {
            return RejectionReason.FIELD_TYPE_MISMATCH;
        }
        if (!payload.containsKey("amount") || payload.get("amount") == null) {
            return RejectionReason.MISSING_REQUIRED_FIELD;
        }
        if (!(payload.get("amount") instanceof Number)) {
            return RejectionReason.FIELD_TYPE_MISMATCH;
        }
        Object currency = payload.get("currency");
        if (currency != null && !(currency instanceof String)) {
            return RejectionReason.FIELD_TYPE_MISMATCH;
        }
        return null;
    }

    /** 为可选字段填充稳定缺省值，返回归一化后的新信封（不修改入参）。 */
    public EventEnvelope applyDefaults(EventEnvelope event) {
        Map<String, Object> payload = new HashMap<>(event.payload());
        if (event.version() >= 2) {
            payload.putIfAbsent("currency", DEFAULT_CURRENCY);
        }
        Map<String, String> attributes = event.attributes() == null
                ? Map.of() : Map.copyOf(event.attributes());
        return new EventEnvelope(event.eventId(), event.partitionKey(), event.sequence(),
                event.version(), Map.copyOf(payload), attributes);
    }
}
