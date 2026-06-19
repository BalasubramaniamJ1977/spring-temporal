package com.payments.sanctions;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
@RestController
@Slf4j
public class SanctionsScreeningApplication {

    @Value("${spring.application.name}") private String svc;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private ObjectMapper objectMapper;

    public static void main(String[] args) {
        SpringApplication.run(SanctionsScreeningApplication.class, args);
    }

    @GetMapping("/health")
    public Map<String, String> health() { return Map.of("status", "ok", "service", svc); }

    @SuppressWarnings("unchecked")
    @KafkaListener(topics = "payment.accepted", groupId = "sanctions-group")
    public void screen(String message) throws Exception {
        long delay = ThreadLocalRandom.current().nextLong(100, 5000);
        Thread.sleep(delay);

        Map<String, Object> payment = objectMapper.readValue(message, Map.class);
        String uetr = (String) payment.get("uetr");

        // 2% REVIEW, 98% PASS
        double roll = ThreadLocalRandom.current().nextDouble();
        String result = roll < 0.02 ? "REVIEW" : "PASS";

        Map<String, Object> event = Map.of(
            "uetr",              uetr,
            "screeningId",       "SCR-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
            "screeningResult",   result,
            "screeningDatabase", "OFAC/UN/EU",
            "matchScore",        roll < 0.02 ? (int)(roll * 5000) : 0,
            "delay_ms",          delay
        );

        if ("REVIEW".equals(result)) {
            log.warn("[{}] Sanctions REVIEW uetr={} — manual review required", svc, uetr);
        } else {
            log.info("[{}] Sanctions PASS uetr={} delay={}ms", svc, uetr, delay);
        }

        kafkaTemplate.send("sanctions.completed", uetr, objectMapper.writeValueAsString(event));
    }
}
