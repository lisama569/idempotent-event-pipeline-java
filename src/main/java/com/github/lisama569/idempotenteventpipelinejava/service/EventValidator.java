package com.github.lisama569.idempotenteventpipelinejava.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.idempotenteventpipelinejava.domain.RejectReason;
import com.github.lisama569.idempotenteventpipelinejava.domain.ValidationException;
import com.github.lisama569.idempotenteventpipelinejava.model.EventEnvelope;

/**
 * 接收阶段解析与版本兼容校验。
 *
 * <p>版本兼容规则（确定性）：</p>
 * <ul>
 *   <li>version 为必填整数；当前仅支持 1 与 2；其它值（含更高版本）一律拒绝
 *       {@link RejectReason#UNSUPPORTED_VERSION}。</li>
 *   <li>eventId / partition / type 为必填非空白字符串，缺失拒绝 MISSING_FIELD，类型不对拒绝 TYPE_MISMATCH。</li>
 *   <li>payload 为可选，任意 JSON 节点，缺省为 null。</li>
 *   <li>priority 是 v2 新增的可选整数字段，缺省语义稳定为 {@link #DEFAULT_PRIORITY}；
 *       v1 事件携带或缺省同样得到该缺省值；类型不对拒绝 TYPE_MISMATCH。</li>
 *   <li>未识别的额外字段一律忽略（确定性前向兼容：不参与判定）。</li>
 *   <li>同一非法输入多次解析，结论一致（纯函数，无外部状态）。</li>
 * </ul>
 */
@Component
public class EventValidator {

    private static final Logger log = LoggerFactory.getLogger(EventValidator.class);

    public static final int DEFAULT_PRIORITY = 0;
    public static final int MIN_SUPPORTED_VERSION = 1;
    public static final int MAX_SUPPORTED_VERSION = 2;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public EventEnvelope parseAndValidate(String rawJson) {
        JsonNode root;
        try {
            root = objectMapper.readTree(rawJson == null ? "" : rawJson);
        } catch (Exception e) {
            return reject(RejectReason.MALFORMED_JSON, "请求体不是合法 JSON: " + e.getMessage());
        }
        if (root == null || !root.isObject()) {
            return reject(RejectReason.MALFORMED_JSON, "顶层必须是 JSON 对象");
        }

        Integer version = requiredInt(root, "version");
        if (version == null) {
            return null; // 异常已抛出
        }
        if (version < MIN_SUPPORTED_VERSION || version > MAX_SUPPORTED_VERSION) {
            return reject(RejectReason.UNSUPPORTED_VERSION,
                    "不支持的事件版本: " + version + "，支持范围 ["
                            + MIN_SUPPORTED_VERSION + "," + MAX_SUPPORTED_VERSION + "]");
        }

        EventEnvelope env = new EventEnvelope();
        env.version = version;
        env.eventId = requiredNonBlankString(root, "eventId");
        env.partition = requiredNonBlankString(root, "partition");
        env.type = requiredNonBlankString(root, "type");

        JsonNode payloadNode = root.get("payload");
        if (payloadNode != null && !payloadNode.isNull()) {
            try {
                env.payload = objectMapper.writeValueAsString(payloadNode);
            } catch (Exception e) {
                return reject(RejectReason.MALFORMED_JSON, "payload 不可序列化: " + e.getMessage());
            }
        }

        JsonNode priorityNode = root.get("priority");
        if (priorityNode == null || priorityNode.isNull()) {
            env.priority = DEFAULT_PRIORITY;
        } else if (priorityNode.isIntegralNumber()) {
            env.priority = priorityNode.asInt();
        } else {
            return reject(RejectReason.TYPE_MISMATCH,
                    "字段 priority 必须是整数，实际类型: " + priorityNode.getNodeType());
        }

        log.info("校验通过 eventId={} partition={} version={} priority={} 判定依据=schema/v{}/optional-default",
                env.eventId, env.partition, env.version, env.priority, version);
        return env;
    }

    private Integer requiredInt(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            reject(RejectReason.MISSING_FIELD, "缺少必填字段: " + field);
        }
        if (!node.isIntegralNumber()) {
            reject(RejectReason.TYPE_MISMATCH,
                    "字段 " + field + " 必须是整数，实际类型: " + node.getNodeType());
        }
        return node.asInt();
    }

    private String requiredNonBlankString(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            reject(RejectReason.MISSING_FIELD, "缺少必填字段: " + field);
        }
        if (!node.isTextual()) {
            reject(RejectReason.TYPE_MISMATCH,
                    "字段 " + field + " 必须是字符串，实际类型: " + node.getNodeType());
        }
        String value = node.asText();
        if (value.isBlank()) {
            reject(RejectReason.MISSING_FIELD, "字段 " + field + " 不得为空白");
        }
        return value;
    }

    private EventEnvelope reject(RejectReason reason, String detail) {
        log.warn("接收校验拒绝 reason={} 判定依据={} detail={}", reason, reason, detail);
        throw new ValidationException(reason, detail);
    }
}
