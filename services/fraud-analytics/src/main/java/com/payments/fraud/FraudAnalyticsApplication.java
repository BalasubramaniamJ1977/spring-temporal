package com.payments.fraud;

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
public class FraudAnalyticsApplication {

    private static final String[] RISK_TYPES = {"VELOCITY", "GEO_ANOMALY", "AMOUNT_SPIKE", "PATTERN_MATCH", "DEVICE_RISK"};

    @Value("${spring.application.name}") private String svc;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private ObjectMapper objectMapper;

    public static void main(String[] args) {
        SpringApplication.run(FraudAnalyticsApplication.class, args);
    }

    @GetMapping("/health")
    public Map<String, String> health() { return Map.of("status", "ok", "service", svc); }

    @SuppressWarnings("unchecked")
    @KafkaListener(topics = "payment.accepted", groupId = "fraud-group")
    public void analyse(String message) throws Exception {
        long delay = ThreadLocalRandom.current().nextLong(100, 5000);
        Thread.sleep(delay);

        Map<String, Object> payment = objectMapper.readValue(message, Map.class);
        String uetr = (String) payment.get("uetr");

        // Gaussian risk score centred on 20, std-dev 15, clamped [0,100]
        double raw   = ThreadLocalRandom.current().nextGaussian() * 15 + 20;
        int riskScore = (int) Math.round(Math.max(0, Math.min(100, raw)));

        // 5% REVIEW (score > 70), 95% LOW
        String riskLevel = riskScore > 70 ? "REVIEW" : "LOW";
        String riskType  = RISK_TYPES[ThreadLocalRandom.current().nextInt(RISK_TYPES.length)];

        Map<String, Object> event = Map.of(
            "uetr",       uetr,
            "eventId",    "FRD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
            "riskType",   riskType,
            "riskScore",  riskScore,
            "riskLevel",  riskLevel,
            "delay_ms",   delay
        );

        if ("REVIEW".equals(riskLevel)) {
            log.warn("[{}] Fraud REVIEW uetr={} riskType={} score={}", svc, uetr, riskType, riskScore);
        } else {
            log.info("[{}] Fraud LOW uetr={} riskType={} score={} delay={}ms", svc, uetr, riskType, riskScore, delay);
        }

        kafkaTemplate.send("fraud.event.generated", uetr, objectMapper.writeValueAsString(event));
    }
}
