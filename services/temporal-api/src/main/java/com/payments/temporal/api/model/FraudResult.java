package com.payments.temporal.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class FraudResult {
    private String eventId;
    private String riskType;
    private int    riskScore;
    private String riskLevel; // LOW or REVIEW
}
