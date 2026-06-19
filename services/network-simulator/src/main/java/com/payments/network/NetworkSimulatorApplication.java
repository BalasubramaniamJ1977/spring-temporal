package com.payments.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.jms.Message;
import jakarta.jms.Session;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
@RestController
@Slf4j
public class NetworkSimulatorApplication {

    @Value("${spring.application.name}") private String svc;
    @Autowired private JmsTemplate jmsTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        SpringApplication.run(NetworkSimulatorApplication.class, args);
    }

    @GetMapping("/health")
    public Map<String, String> health() { return Map.of("status", "ok", "service", svc); }

    // OTEL agent auto-instruments @JmsListener — extracts trace context from JMS headers
    @SuppressWarnings("unchecked")
    @JmsListener(destination = "payment.outbound")
    public void onMessage(String message) throws Exception {
        long delay = ThreadLocalRandom.current().nextLong(100, 10_000);
        Thread.sleep(delay);

        Map<String, Object> payment = objectMapper.readValue(message, Map.class);
        String uetr = (String) payment.get("uetr");

        // 3% settlement rejection
        String settlementStatus = ThreadLocalRandom.current().nextDouble() < 0.03 ? "REJECTED" : "SETTLED";
        payment.put("settlement_status", settlementStatus);
        payment.put("network_ack",       "NET-" + uetr.substring(0, 8).toUpperCase());
        payment.put("stage",             "NETWORK_PROCESSED");

        if ("REJECTED".equals(settlementStatus)) {
            log.warn("[{}] Settlement REJECTED uetr={} delay={}ms", svc, uetr, delay);
        } else {
            log.info("[{}] Network SETTLED uetr={} delay={}ms", svc, uetr, delay);
        }

        String json = objectMapper.writeValueAsString(payment);
        jmsTemplate.send("payment.settlement", (Session s) -> {
            Message msg = s.createTextMessage(json);
            msg.setStringProperty("payment_uetr", uetr);
            return msg;
        });

        log.info("[{}] Published to Artemis payment.settlement uetr={}", svc, uetr);
    }
}
