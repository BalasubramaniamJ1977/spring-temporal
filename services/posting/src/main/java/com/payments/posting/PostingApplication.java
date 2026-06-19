package com.payments.posting;

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
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
@RestController
@Slf4j
public class PostingApplication {

    @Value("${spring.application.name}") private String svc;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private ObjectMapper objectMapper;

    public static void main(String[] args) {
        SpringApplication.run(PostingApplication.class, args);
    }

    @GetMapping("/health")
    public Map<String, String> health() { return Map.of("status", "ok", "service", svc); }

    @SuppressWarnings("unchecked")
    @KafkaListener(topics = "journal.created", groupId = "posting-group")
    public void processJournal(String message) throws Exception {
        long delay = ThreadLocalRandom.current().nextLong(100, 5000);
        Thread.sleep(delay);

        Map<String, Object> payment = objectMapper.readValue(message, Map.class);
        String uetr = (String) payment.get("uetr");
        String jid  = (String) payment.getOrDefault("journal_id", "UNKNOWN");

        // 1% posting failure simulation
        if (ThreadLocalRandom.current().nextDouble() < 0.01) {
            log.error("[{}] Posting FAILED uetr={} — ledger system unavailable", svc, uetr);
            payment.put("posting_status", "FAILED");
            payment.put("posting_error",  "Ledger system unavailable");
            kafkaTemplate.send("posting.completed", uetr, objectMapper.writeValueAsString(payment));
            return;
        }

        String pref = "POST-" + jid.substring(0, Math.min(8, jid.length())).toUpperCase();
        payment.put("posting_ref",    pref);
        payment.put("posting_status", "COMPLETED");
        payment.put("stage",          "POSTING_COMPLETED");

        log.info("[{}] Posted uetr={} ref={} delay={}ms", svc, uetr, pref, delay);

        kafkaTemplate.send("posting.completed", uetr, objectMapper.writeValueAsString(payment));
    }
}
