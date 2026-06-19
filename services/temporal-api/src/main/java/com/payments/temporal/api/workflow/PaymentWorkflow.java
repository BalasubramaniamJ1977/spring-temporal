package com.payments.temporal.api.workflow;

import com.payments.temporal.api.model.PaymentLifecycle;
import com.payments.temporal.api.model.PaymentRequest;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface PaymentWorkflow {

    @WorkflowMethod
    PaymentLifecycle processPayment(PaymentRequest request);

    /** Sent by a compliance officer to unblock a payment flagged for manual fraud review. */
    @SignalMethod
    void approveManualReview(String approvedBy);

    /** Returns the current stage of the workflow (queryable without blocking execution). */
    @QueryMethod
    String getStatus();
}
