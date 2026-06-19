package com.payments.temporal.api.activity;

import com.payments.temporal.api.model.FraudResult;
import com.payments.temporal.api.model.SanctionsResult;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface ComplianceActivities {

    @ActivityMethod
    SanctionsResult screenSanctions(String uetr);

    @ActivityMethod
    FraudResult detectFraud(String uetr, double amount);
}
