package com.payments.dispatcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
@RestController
@Slf4j
public class DispatcherApplication {

    private static final Map<String, String> ENDPOINTS = Map.of(
        "SWIFT", "swift.network.internal:9000",
        "SEPA",  "sepa.clearing.internal:9001",
        "CHAPS", "chaps.boe.internal:9002",
        "BACS",  "bacs.clearing.internal:9003",
        "FPS",   "fps.link.internal:9004",
        "RTP",   "rtp.network.internal:9005"
    );

    @Value("${spring.application.name}") private String svc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JmsTemplate jmsTemplate;

    public static void main(String[] args) {
        SpringApplication.run(DispatcherApplication.class, args);
    }

    @GetMapping("/health")
    public Map<String, String> health() { return Map.of("status", "ok", "service", svc); }

    @SuppressWarnings("unchecked")
    @KafkaListener(topics = "posting.completed", groupId = "dispatcher-group")
    public void processPosting(String message) throws Exception {
        long delay = ThreadLocalRandom.current().nextLong(100, 5000);
        Thread.sleep(delay);

        Map<String, Object> payment = objectMapper.readValue(message, Map.class);
        String uetr    = (String) payment.get("uetr");
        String rail    = (String) payment.getOrDefault("routing_rail", "SWIFT");
        String postRef = (String) payment.getOrDefault("posting_ref", uetr.substring(0, 8).toUpperCase());

        payment.put("network_endpoint", ENDPOINTS.getOrDefault(rail, "unknown.network:9999"));
        payment.put("dispatch_ref",     "DSP-" + postRef);
        payment.put("stage",            "DISPATCHED");

        String json = objectMapper.writeValueAsString(payment);

        // OTEL agent auto-instruments JmsTemplate — trace context injected into JMS headers
        jmsTemplate.send("payment.outbound", (Session s) -> {
            Message msg = s.createTextMessage(json);
            msg.setStringProperty("payment_uetr", uetr);
            return msg;
        });

        log.info("[{}] Dispatched uetr={} rail={} delay={}ms → Artemis payment.outbound", svc, uetr, rail, delay);
    }
}
