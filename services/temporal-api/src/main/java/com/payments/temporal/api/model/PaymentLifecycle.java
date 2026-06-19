package com.payments.temporal.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class PaymentLifecycle {
    private String uetr;
    private String status;          // COMPLETED | REJECTED | FAILED_COMPENSATED
    private String trackingId;
    private String routingRail;
    private String messageFormat;
    private String sanctionsId;
    private String sanctionsResult;
    private String fraudEventId;
    private String fraudRiskLevel;
    private int    fraudRiskScore;
    private String journalId;
    private String postingRef;
    private String dispatchRef;
    private String networkEndpoint;
    private String networkAck;
    private String settlementStatus;
    private String settlementRef;
    private String rejectionReason;
}
