package com.payments.temporal.activity;

import com.payments.temporal.api.activity.PaymentActivities;
import com.payments.temporal.api.model.DispatchResult;
import com.payments.temporal.api.model.JournalResult;
import com.payments.temporal.api.model.NetworkResult;
import com.payments.temporal.api.model.PaymentRequest;
import com.payments.temporal.api.model.PostingResult;
import com.payments.temporal.api.model.RouteResult;
import com.payments.temporal.api.model.SettlementResult;
import io.temporal.activity.Activity;
import io.temporal.failure.ApplicationFailure;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class PaymentActivitiesImpl implements PaymentActivities {

    /** Chaos flag: set true to force every postToLedger call to throw LedgerError. */
    public static final AtomicBoolean FORCE_POSTING_FAILURE = new AtomicBoolean(false);

    private static final String[] RAILS = {"SWIFT", "SEPA", "CHAPS", "BACS", "FPS", "RTP"};

    private static final Map<String, String> FORMAT_MAP = Map.of(
        "SWIFT", "MT103",
        "SEPA",  "pacs.008",
        "CHAPS", "pacs.008",
        "BACS",  "Bacs-STD-18",
        "FPS",   "ISO20022",
        "RTP",   "pacs.008"
    );

    private static final Map<String, String> ENDPOINTS = Map.of(
        "SWIFT", "swift.network.internal:9000",
        "SEPA",  "sepa.clearing.internal:9001",
        "CHAPS", "chaps.boe.internal:9002",
        "BACS",  "bacs.clearing.internal:9003",
        "FPS",   "fps.link.internal:9004",
        "RTP",   "rtp.network.internal:9005"
    );

    @Override
    public RouteResult routeAndTransform(PaymentRequest request) {
        simulateWork(100, 1000);
        String rail   = RAILS[ThreadLocalRandom.current().nextInt(RAILS.length)];
        String format = FORMAT_MAP.getOrDefault(rail, "ISO20022");
        log.info("[routing] uetr={} rail={} format={}", request.getUetr(), rail, format);
        return RouteResult.builder().routingRail(rail).messageFormat(format).build();
    }

    @Override
    public String acceptPayment(String uetr) {
        simulateWork(50, 500);
        String trackingId = "TRK-" + uetr.substring(0, 8).toUpperCase();
        log.info("[payment-processor] uetr={} trackingId={} status=ACCEPTED", uetr, trackingId);
        return trackingId;
    }

    @Override
    public JournalResult generateJournalEntry(String uetr) {
        simulateWork(100, 5000);
        String jid    = UUID.randomUUID().toString();
        String debit  = "DR-" + jid.substring(0, 8).toUpperCase();
        String credit = "CR-" + jid.substring(0, 8).toUpperCase();
        log.info("[accounting] uetr={} journalId={}", uetr, jid);
        return JournalResult.builder()
            .journalId(jid)
            .debitEntry(debit)
            .creditEntry(credit)
            .build();
    }

    @Override
    public PostingResult postToLedger(String uetr, String journalId) {
        simulateWork(100, 5000);

        // Chaos flag overrides the 1% natural failure — set via POST /chaos/posting/break.
        if (FORCE_POSTING_FAILURE.get() || ThreadLocalRandom.current().nextDouble() < 0.01) {
            log.warn("[posting] Ledger unavailable uetr={} attempt={}", uetr,
                Activity.getExecutionContext().getInfo().getAttempt());
            throw ApplicationFailure.newFailure("Ledger system unavailable", "LedgerError");
        }

        String ref = "POST-" + journalId.substring(0, 8).toUpperCase();
        log.info("[posting] uetr={} postingRef={} status=COMPLETED", uetr, ref);
        return PostingResult.builder().postingRef(ref).status("COMPLETED").build();
    }

    @Override
    public void reverseJournalEntry(String uetr, String journalId) {
        simulateWork(100, 1000);
        log.warn("[accounting-compensation] Journal reversed uetr={} journalId={}", uetr, journalId);
    }

    @Override
    public DispatchResult dispatchToNetwork(String uetr, String routingRail, String postingRef) {
        simulateWork(100, 5000);
        String endpoint   = ENDPOINTS.getOrDefault(routingRail, "default.network.internal:9000");
        String dispatchRef = "DSP-" + postingRef.substring(4); // reuse the posting ref suffix
        log.info("[dispatcher] uetr={} dispatchRef={} endpoint={}", uetr, dispatchRef, endpoint);
        return DispatchResult.builder().dispatchRef(dispatchRef).networkEndpoint(endpoint).build();
    }

    @Override
    public NetworkResult simulateNetworkSettlement(String uetr, String dispatchRef, String routingRail) {
        simulateWork(100, 10_000); // external network can be slow

        String status = ThreadLocalRandom.current().nextDouble() < 0.03 ? "REJECTED" : "SETTLED";
        String ack    = "NET-" + uetr.substring(0, 8).toUpperCase();

        if ("REJECTED".equals(status)) {
            log.warn("[network-simulator] Settlement REJECTED uetr={}", uetr);
        } else {
            log.info("[network-simulator] Settlement SETTLED uetr={} ack={}", uetr, ack);
        }
        return NetworkResult.builder().networkAck(ack).settlementStatus(status).build();
    }

    @Override
    public SettlementResult processSettlement(String uetr, String networkAck, String settlementStatus) {
        simulateWork(100, 5000);
        String ref = "SET-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        log.info("[settlement-processor] uetr={} settlementRef={} status={}", uetr, ref, settlementStatus);
        return SettlementResult.builder().settlementRef(ref).settlementStatus(settlementStatus).build();
    }

    private static void simulateWork(long minMs, long maxMs) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(minMs, maxMs));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
