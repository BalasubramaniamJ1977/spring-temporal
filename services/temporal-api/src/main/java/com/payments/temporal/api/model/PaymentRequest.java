package com.payments.temporal.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentRequest {
    private String uetr;
    private double amount;
    private String currency;
    private String channel;

    @JsonProperty("debtor_account")
    private String debtorAccount;

    @JsonProperty("creditor_account")
    private String creditorAccount;
}
