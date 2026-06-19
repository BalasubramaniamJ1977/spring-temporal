package com.payments.temporal.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class JournalResult {
    private String journalId;
    private String debitEntry;
    private String creditEntry;
}
