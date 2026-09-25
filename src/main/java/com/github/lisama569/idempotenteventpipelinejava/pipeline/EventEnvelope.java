package com.github.lisama569.idempotenteventpipelinejava.pipeline;

import java.util.Map;

/**
 * 事件信封：承载事件标识、逻辑分区、分区内序号、版本与负载。
 *
 * @param eventId       全局唯一事件标识（幂等键）
 * @param partitionKey  逻辑分区键，同一分区内事件保序
 * @param sequence      分区内单调递增序号
 * @param version       事件结构版本
 * @param payload       业务负载
 * @param attributes    可选扩展属性（版本兼容的新增可选字段落在这里）
 */
public record EventEnvelope(String eventId,
                            String partitionKey,
                            long sequence,
                            int version,
                            Map<String, Object> payload,
                            Map<String, String> attributes) {
}
