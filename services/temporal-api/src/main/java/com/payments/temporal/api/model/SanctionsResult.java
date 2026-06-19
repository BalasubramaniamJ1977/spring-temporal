package com.payments.temporal.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class SanctionsResult {
    private String screeningId;
    private String result;    // PASS or REVIEW
    private String database;
    private int    matchScore;
}
