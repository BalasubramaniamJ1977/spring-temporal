package com.payments.accounting;

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
public class AccountingGeneratorApplication {

    @Value("${spring.application.name}") private String svc;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private ObjectMapper objectMapper;

    public static void main(String[] args) {
        SpringApplication.run(AccountingGeneratorApplication.class, args);
    }

    @GetMapping("/health")
    public Map<String, String> health() { return Map.of("status", "ok", "service", svc); }

    @SuppressWarnings("unchecked")
    @KafkaListener(topics = "payment.accepted", groupId = "accounting-group")
    public void processPayment(String message) throws Exception {
        long delay = ThreadLocalRandom.current().nextLong(100, 5000);
        Thread.sleep(delay);

        Map<String, Object> payment = objectMapper.readValue(message, Map.class);
        String uetr = (String) payment.get("uetr");

        String jid = UUID.randomUUID().toString();
        payment.put("journal_id",   jid);
        payment.put("debit_entry",  "DR-" + jid.substring(0, 8).toUpperCase());
        payment.put("credit_entry", "CR-" + jid.substring(0, 8).toUpperCase());
        payment.put("stage",        "JOURNAL_CREATED");

        log.info("[{}] Journal created uetr={} jid={} delay={}ms", svc, uetr, jid, delay);

        kafkaTemplate.send("journal.created", uetr, objectMapper.writeValueAsString(payment));
    }
}
