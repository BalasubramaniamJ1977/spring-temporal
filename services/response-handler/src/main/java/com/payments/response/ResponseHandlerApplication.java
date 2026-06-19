package com.payments.response;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@SpringBootApplication
@RestController
@Slf4j
public class ResponseHandlerApplication {

    @Value("${spring.application.name}") private String svc;
    @Autowired private ObjectMapper objectMapper;

    public static void main(String[] args) {
        SpringApplication.run(ResponseHandlerApplication.class, args);
    }

    @GetMapping("/health")
    public Map<String, String> health() { return Map.of("status", "ok", "service", svc); }

    @SuppressWarnings("unchecked")
    @KafkaListener(topics = "settlement.completed", groupId = "response-group")
    public void processSettlement(String message) throws Exception {
        Map<String, Object> payment = objectMapper.readValue(message, Map.class);
        String uetr             = (String) payment.get("uetr");
        String settlementStatus = (String) payment.getOrDefault("settlement_status", "UNKNOWN");
        String settlementRef    = (String) payment.getOrDefault("settlement_ref", "N/A");
        String dispatchRef      = (String) payment.getOrDefault("dispatch_ref", "N/A");
        String postingRef       = (String) payment.getOrDefault("posting_ref", "N/A");

        log.info("[{}] Payment lifecycle COMPLETE uetr={} settlement={} settlementRef={} postingRef={} dispatchRef={}",
            svc, uetr, settlementStatus, settlementRef, postingRef, dispatchRef);
    }
}
