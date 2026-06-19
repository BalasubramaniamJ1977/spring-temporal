package com.payments.temporal.api.activity;

import com.payments.temporal.api.model.DispatchResult;
import com.payments.temporal.api.model.JournalResult;
import com.payments.temporal.api.model.NetworkResult;
import com.payments.temporal.api.model.PaymentRequest;
import com.payments.temporal.api.model.PostingResult;
import com.payments.temporal.api.model.RouteResult;
import com.payments.temporal.api.model.SettlementResult;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface PaymentActivities {

    @ActivityMethod
    RouteResult routeAndTransform(PaymentRequest request);

    @ActivityMethod
    String acceptPayment(String uetr);

    @ActivityMethod
    JournalResult generateJournalEntry(String uetr);

    /** 1% chance of throwing ApplicationFailure — Temporal will retry per the configured policy. */
    @ActivityMethod
    PostingResult postToLedger(String uetr, String journalId);

    /** Compensation activity: reverses a journal entry when posting fails permanently. */
    @ActivityMethod
    void reverseJournalEntry(String uetr, String journalId);

    @ActivityMethod
    DispatchResult dispatchToNetwork(String uetr, String routingRail, String postingRef);

    /** 3% chance of REJECTED — not retried; workflow treats it as a legitimate business outcome. */
    @ActivityMethod
    NetworkResult simulateNetworkSettlement(String uetr, String dispatchRef, String routingRail);

    @ActivityMethod
    SettlementResult processSettlement(String uetr, String networkAck, String settlementStatus);
}
