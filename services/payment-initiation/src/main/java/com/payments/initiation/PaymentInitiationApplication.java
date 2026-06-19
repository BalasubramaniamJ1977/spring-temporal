package com.payments.initiation;

import com.payments.temporal.api.model.PaymentLifecycle;
import com.payments.temporal.api.model.PaymentRequest;
import com.payments.temporal.api.workflow.PaymentWorkflow;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Map;

@SpringBootApplication
@RestController
@Slf4j
public class PaymentInitiationApplication {

    @Value("${spring.application.name}") private String svc;

    @Autowired private WorkflowClient workflowClient;

    public static void main(String[] args) {
        SpringApplication.run(PaymentInitiationApplication.class, args);
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "ok", "service", svc);
    }

    /**
     * Starts a durable PaymentWorkflow for the incoming payment and returns immediately.
     * Settlement completes asynchronously in Temporal. Poll /status/{uetr} to track progress.
     */
    @PostMapping("/initiate")
    public Map<String, Object> initiate(@RequestBody Map<String, Object> body) {
        String uetr   = (String) body.get("uetr");
        double amount = ((Number) body.getOrDefault("amount", 0)).doubleValue();

        PaymentRequest request = PaymentRequest.builder()
            .uetr(uetr)
            .amount(amount)
            .currency((String) body.getOrDefault("currency", "USD"))
            .channel((String) body.getOrDefault("channel", "API"))
            .debtorAccount((String) body.getOrDefault("debtor_account", ""))
            .creditorAccount((String) body.getOrDefault("creditor_account", ""))
            .build();

        String workflowId = "payment-" + uetr;
        WorkflowOptions options = WorkflowOptions.newBuilder()
            .setWorkflowId(workflowId)
            .setTaskQueue("payment-processing")
            .setWorkflowExecutionTimeout(Duration.ofMinutes(60))
            .build();

        // Start the workflow asynchronously — it runs in Temporal and survives any service restart.
        PaymentWorkflow workflow = workflowClient.newWorkflowStub(PaymentWorkflow.class, options);
        WorkflowClient.start(workflow::processPayment, request);

        String trackingId = "TRK-" + uetr.substring(0, 8).toUpperCase();
        log.info("[{}] Workflow started uetr={} workflowId={}", svc, uetr, workflowId);

        return Map.of(
            "uetr",       uetr,
            "status",     "ACCEPTED",
            "trackingId", trackingId,
            "workflowId", workflowId
        );
    }

    /**
     * Returns the current stage of a running or completed payment workflow via Temporal Query API.
     */
    @GetMapping("/status/{uetr}")
    public Map<String, Object> status(@PathVariable String uetr) {
        String workflowId = "payment-" + uetr;
        try {
            PaymentWorkflow workflow = workflowClient.newWorkflowStub(
                PaymentWorkflow.class, workflowId);
            String currentStatus = workflow.getStatus();
            return Map.of(
                "uetr",       uetr,
                "workflowId", workflowId,
                "status",     currentStatus
            );
        } catch (Exception e) {
            return Map.of(
                "uetr",       uetr,
                "workflowId", workflowId,
                "status",     "NOT_FOUND",
                "error",      e.getMessage() != null ? e.getMessage() : "Unknown"
            );
        }
    }

    /**
     * Sends a manual-approval signal to a payment stuck in PENDING_FRAUD_REVIEW.
     */
    @PostMapping("/approve/{uetr}")
    public Map<String, Object> approve(@PathVariable String uetr,
                                       @RequestBody Map<String, String> body) {
        String approvedBy = body.getOrDefault("approvedBy", "unknown");
        String workflowId = "payment-" + uetr;
        PaymentWorkflow workflow = workflowClient.newWorkflowStub(
            PaymentWorkflow.class, workflowId);
        workflow.approveManualReview(approvedBy);
        log.info("[{}] Manual approval sent uetr={} by={}", svc, uetr, approvedBy);
        return Map.of("uetr", uetr, "approved", true, "by", approvedBy);
    }
}
