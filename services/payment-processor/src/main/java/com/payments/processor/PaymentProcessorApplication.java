package com.payments.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

@SpringBootApplication
@RestController
@Slf4j
public class PaymentProcessorApplication {

    @Value("${spring.application.name}") private String svc;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private ObjectMapper objectMapper;

    public static void main(String[] args) {
        SpringApplication.run(PaymentProcessorApplication.class, args);
    }

    @GetMapping("/health")
    public Map<String, String> health() { return Map.of("status", "ok", "service", svc); }

    @PostMapping("/process")
    public Map<String, Object> process(@RequestBody Map<String, Object> payment) throws Exception {
        String uetr = (String) payment.get("uetr");
        String trackingId = "TRK-" + uetr.substring(0, 8).toUpperCase();

        payment.put("tracking_id", trackingId);
        payment.put("status", "ACCEPTED");
        payment.put("accepted_at", Instant.now().toString());

        String json = objectMapper.writeValueAsString(payment);
        kafkaTemplate.send("payment.accepted", uetr, json);

        log.info("[{}] ACCEPTED uetr={} trackingId={} → published to payment.accepted", svc, uetr, trackingId);

        return Map.of("uetr", uetr, "status", "ACCEPTED", "trackingId", trackingId);
    }
}
