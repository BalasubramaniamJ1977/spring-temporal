package com.payments.settlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
@RestController
@Slf4j
public class SettlementProcessorApplication {

    @Value("${spring.application.name}") private String svc;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        SpringApplication.run(SettlementProcessorApplication.class, args);
    }

    @GetMapping("/health")
    public Map<String, String> health() { return Map.of("status", "ok", "service", svc); }

    // OTEL agent auto-instruments @JmsListener — extracts trace context from JMS headers
    @SuppressWarnings("unchecked")
    @JmsListener(destination = "payment.settlement")
    public void onMessage(String message) throws Exception {
        long delay = ThreadLocalRandom.current().nextLong(100, 5000);
        Thread.sleep(delay);

        Map<String, Object> payment = objectMapper.readValue(message, Map.class);
        String uetr             = (String) payment.get("uetr");
        String settlementStatus = (String) payment.getOrDefault("settlement_status", "SETTLED");

        String settlementRef = "SET-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        payment.put("settlement_ref", settlementRef);
        payment.put("stage",          "SETTLEMENT_COMPLETED");

        log.info("[{}] Settlement processed uetr={} ref={} status={} delay={}ms",
            svc, uetr, settlementRef, settlementStatus, delay);

        kafkaTemplate.send("settlement.completed", uetr, objectMapper.writeValueAsString(payment));
        log.info("[{}] Published to Kafka settlement.completed uetr={}", svc, uetr);
    }
}
