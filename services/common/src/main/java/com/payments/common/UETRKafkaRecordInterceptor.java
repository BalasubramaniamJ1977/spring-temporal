package com.payments.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * Mirrors UETRTraceFilter for Kafka consumers.
 *
 * On every Kafka record:
 *   1. Parses the value as JSON, extracts the "uetr" field.
 *   2. Sets MDC "payment.uetr"   → appears in every log line produced by the listener.
 *   3. Sets span attribute "payment.uetr" → TraceQL { span.payment.uetr = "..." } works in Tempo.
 *
 * Cleaned up in afterRecord() so the MDC is never leaked to other tasks.
 * Applied to all @KafkaListener containers via KafkaUETRInterceptorConfigurer.
 */
public class UETRKafkaRecordInterceptor implements RecordInterceptor<String, String> {

    private static final ObjectMapper MAPPER  = new ObjectMapper();
    private static final String       MDC_KEY = "payment.uetr";

    @Override
    public ConsumerRecord<String, String> intercept(ConsumerRecord<String, String> record,
                                                     Consumer<String, String> consumer) {
        String uetr = extractUetr(record.value());
        if (uetr != null) {
            MDC.put(MDC_KEY, uetr);
            Span span = Span.current();
            if (span.isRecording()) {
                span.setAttribute(AttributeKey.stringKey("payment.uetr"), uetr);
            }
        }
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<String, String> record, Consumer<String, String> consumer) {
        MDC.remove(MDC_KEY);
    }

    private String extractUetr(String json) {
        if (json == null || json.isBlank()) return null;
        try {
            JsonNode node = MAPPER.readTree(json).get("uetr");
            return (node != null && node.isTextual()) ? node.asText() : null;
        } catch (Exception e) {
            return null;
        }
    }
}
