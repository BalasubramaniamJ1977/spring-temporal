package com.payments.temporal.activity;

import com.payments.temporal.api.activity.ComplianceActivities;
import com.payments.temporal.api.model.FraudResult;
import com.payments.temporal.api.model.SanctionsResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
@Slf4j
public class ComplianceActivitiesImpl implements ComplianceActivities {

    private static final String[] RISK_TYPES =
        {"VELOCITY", "GEO_ANOMALY", "AMOUNT_SPIKE", "PATTERN_MATCH", "DEVICE_RISK"};

    @Override
    public SanctionsResult screenSanctions(String uetr) {
        simulateWork(100, 5000);

        double roll   = ThreadLocalRandom.current().nextDouble();
        String result = roll < 0.02 ? "REVIEW" : "PASS"; // 2% flagged for review
        String id     = "SCR-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        if ("REVIEW".equals(result)) {
            log.warn("[sanctions] REVIEW uetr={} — manual review required", uetr);
        } else {
            log.info("[sanctions] PASS uetr={}", uetr);
        }

        return SanctionsResult.builder()
            .screeningId(id)
            .result(result)
            .database("OFAC/UN/EU")
            .matchScore(roll < 0.02 ? (int) (roll * 5000) : 0)
            .build();
    }

    @Override
    public FraudResult detectFraud(String uetr, double amount) {
        simulateWork(100, 5000);

        // Gaussian risk score centred at 20, std-dev 15, clamped [0, 100].
        double raw       = ThreadLocalRandom.current().nextGaussian() * 15 + 20;
        int    riskScore = (int) Math.round(Math.max(0, Math.min(100, raw)));
        String riskLevel = riskScore > 45 ? "REVIEW" : "LOW"; // ~5% chance: P(N(20,15) > 45) ~= 4.8%
        String riskType  = RISK_TYPES[ThreadLocalRandom.current().nextInt(RISK_TYPES.length)];
        String eventId   = "FRD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        if ("REVIEW".equals(riskLevel)) {
            log.warn("[fraud] REVIEW uetr={} riskType={} score={}", uetr, riskType, riskScore);
        } else {
            log.info("[fraud] LOW uetr={} riskType={} score={}", uetr, riskType, riskScore);
        }

        return FraudResult.builder()
            .eventId(eventId)
            .riskType(riskType)
            .riskScore(riskScore)
            .riskLevel(riskLevel)
            .build();
    }

    private static void simulateWork(long minMs, long maxMs) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(minMs, maxMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
