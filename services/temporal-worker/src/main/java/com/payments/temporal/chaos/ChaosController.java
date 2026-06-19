package com.payments.temporal.chaos;

import com.payments.temporal.activity.PaymentActivitiesImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/chaos")
@Slf4j
public class ChaosController {

    @PostMapping("/posting/break")
    public Map<String, Object> breakPosting() {
        PaymentActivitiesImpl.FORCE_POSTING_FAILURE.set(true);
        log.warn("[chaos] Posting adapter BROKEN -- every postToLedger will throw LedgerError");
        return Map.of(
            "posting", "BROKEN",
            "effect",  "all postToLedger calls throw LedgerError until /chaos/posting/fix is called"
        );
    }

    @PostMapping("/posting/fix")
    public Map<String, Object> fixPosting() {
        PaymentActivitiesImpl.FORCE_POSTING_FAILURE.set(false);
        log.info("[chaos] Posting adapter RESTORED -- postToLedger functions normally");
        return Map.of(
            "posting", "HEALTHY",
            "effect",  "postToLedger functions normally (1% natural failure rate remains)"
        );
    }

    @GetMapping("/status")
    public Map<String, Object> chaosStatus() {
        return Map.of(
            "posting", PaymentActivitiesImpl.FORCE_POSTING_FAILURE.get() ? "BROKEN" : "HEALTHY"
        );
    }
}
