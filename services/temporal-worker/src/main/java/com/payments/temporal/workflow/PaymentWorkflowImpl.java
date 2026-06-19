package com.payments.temporal.workflow;

import com.payments.temporal.api.activity.ComplianceActivities;
import com.payments.temporal.api.activity.PaymentActivities;
import com.payments.temporal.api.model.DispatchResult;
import com.payments.temporal.api.model.FraudResult;
import com.payments.temporal.api.model.JournalResult;
import com.payments.temporal.api.model.NetworkResult;
import com.payments.temporal.api.model.PaymentLifecycle;
import com.payments.temporal.api.model.PaymentRequest;
import com.payments.temporal.api.model.PostingResult;
import com.payments.temporal.api.model.RouteResult;
import com.payments.temporal.api.model.SanctionsResult;
import com.payments.temporal.api.model.SettlementResult;
import com.payments.temporal.api.workflow.PaymentWorkflow;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ActivityFailure;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;

import java.time.Duration;

public class PaymentWorkflowImpl implements PaymentWorkflow {

    // Standard activities: 3 retries with exponential backoff — Temporal handles this durably.
    private final ActivityOptions standardOptions = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofMinutes(2))
        .setRetryOptions(RetryOptions.newBuilder()
            .setInitialInterval(Duration.ofSeconds(1))
            .setMaximumAttempts(3)
            .setBackoffCoefficient(2.0)
            .build())
        .build();

    // Network leg is slower and gets its own timeout; settlement status is a business outcome, not a failure.
    private final ActivityOptions networkOptions = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofMinutes(5))
        .setRetryOptions(RetryOptions.newBuilder()
            .setInitialInterval(Duration.ofSeconds(2))
            .setMaximumAttempts(2)
            .build())
        .build();

    // Compliance checks run fast and in parallel — keep them on the standard policy.
    private final PaymentActivities    paymentActivities    =
        Workflow.newActivityStub(PaymentActivities.class, standardOptions);
    private final ComplianceActivities complianceActivities =
        Workflow.newActivityStub(ComplianceActivities.class, standardOptions);

    private String  status          = "INITIATED";
    private boolean manuallyApproved = false;

    @Override
    public PaymentLifecycle processPayment(PaymentRequest request) {

        // ── 1. Route & transform ─────────────────────────────────────────────
        status = "ROUTING";
        RouteResult route = paymentActivities.routeAndTransform(request);

        // ── 2. Accept ────────────────────────────────────────────────────────
        status = "ACCEPTED";
        String trackingId = paymentActivities.acceptPayment(request.getUetr());

        // ── 3. Parallel compliance checks ────────────────────────────────────
        status = "COMPLIANCE_SCREENING";
        Promise<SanctionsResult> sanctionsPromise =
            Async.function(complianceActivities::screenSanctions, request.getUetr());
        Promise<FraudResult> fraudPromise =
            Async.function(complianceActivities::detectFraud, request.getUetr(), request.getAmount());

        SanctionsResult sanctions = sanctionsPromise.get();
        FraudResult     fraud     = fraudPromise.get();

        // ── 4. Manual review gate (signal-driven) ───────────────────────────
        if ("REVIEW".equals(fraud.getRiskLevel())) {
            status = "PENDING_FRAUD_REVIEW";
            // Block here until approveManualReview() signal arrives or 30-min timeout fires.
            boolean approved = Workflow.await(Duration.ofMinutes(30), () -> manuallyApproved);
            if (!approved) {
                status = "REJECTED";
                return PaymentLifecycle.builder()
                    .uetr(request.getUetr())
                    .trackingId(trackingId)
                    .status("REJECTED")
                    .rejectionReason("Fraud review timeout — payment expired without approval")
                    .fraudRiskLevel(fraud.getRiskLevel())
                    .fraudRiskScore(fraud.getRiskScore())
                    .fraudEventId(fraud.getEventId())
                    .sanctionsResult(sanctions.getResult())
                    .build();
            }
        }

        // ── 5. Accounting ────────────────────────────────────────────────────
        status = "ACCOUNTING";
        JournalResult journal = paymentActivities.generateJournalEntry(request.getUetr());

        // ── 6. Ledger posting (with automatic retry + saga compensation) ─────
        status = "POSTING";
        PostingResult posting;
        try {
            posting = paymentActivities.postToLedger(request.getUetr(), journal.getJournalId());
        } catch (ActivityFailure e) {
            // All retries exhausted — reverse the journal entry (saga compensation).
            status = "POSTING_FAILED_COMPENSATING";
            paymentActivities.reverseJournalEntry(request.getUetr(), journal.getJournalId());
            status = "FAILED_COMPENSATED";
            return PaymentLifecycle.builder()
                .uetr(request.getUetr())
                .trackingId(trackingId)
                .routingRail(route.getRoutingRail())
                .journalId(journal.getJournalId())
                .status("FAILED_COMPENSATED")
                .rejectionReason("Ledger posting failed after all retries; journal entry reversed")
                .build();
        }

        // ── 7. Network dispatch ──────────────────────────────────────────────
        status = "DISPATCHING";
        DispatchResult dispatch = paymentActivities.dispatchToNetwork(
            request.getUetr(), route.getRoutingRail(), posting.getPostingRef());

        // ── 8. External network simulation (wider timeout, fewer retries) ────
        status = "NETWORK_PROCESSING";
        PaymentActivities networkActivities =
            Workflow.newActivityStub(PaymentActivities.class, networkOptions);
        NetworkResult network = networkActivities.simulateNetworkSettlement(
            request.getUetr(), dispatch.getDispatchRef(), route.getRoutingRail());

        // ── 9. Settlement ────────────────────────────────────────────────────
        status = "SETTLING";
        SettlementResult settlement = paymentActivities.processSettlement(
            request.getUetr(), network.getNetworkAck(), network.getSettlementStatus());

        // ── 10. Done ─────────────────────────────────────────────────────────
        status = "COMPLETED";
        return PaymentLifecycle.builder()
            .uetr(request.getUetr())
            .trackingId(trackingId)
            .routingRail(route.getRoutingRail())
            .messageFormat(route.getMessageFormat())
            .sanctionsId(sanctions.getScreeningId())
            .sanctionsResult(sanctions.getResult())
            .fraudEventId(fraud.getEventId())
            .fraudRiskLevel(fraud.getRiskLevel())
            .fraudRiskScore(fraud.getRiskScore())
            .journalId(journal.getJournalId())
            .postingRef(posting.getPostingRef())
            .dispatchRef(dispatch.getDispatchRef())
            .networkEndpoint(dispatch.getNetworkEndpoint())
            .networkAck(network.getNetworkAck())
            .settlementStatus(network.getSettlementStatus())
            .settlementRef(settlement.getSettlementRef())
            .status("COMPLETED")
            .build();
    }

    @Override
    public void approveManualReview(String approvedBy) {
        Workflow.getLogger(PaymentWorkflowImpl.class)
            .info("Manual review approved by {} for workflow {}", approvedBy,
                Workflow.getInfo().getWorkflowId());
        manuallyApproved = true;
    }

    @Override
    public String getStatus() {
        return status;
    }
}
