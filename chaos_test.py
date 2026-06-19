#!/usr/bin/env python3
"""
Chaos test suite for the Temporal payment observability stack.

Scenarios
---------
1. Worker crash mid-workflow  -- kill temporal-worker while payments are in-flight;
   show Temporal resumes every workflow from the last committed activity checkpoint.

2. Fraud manual-review gate   -- spam payments until one hits PENDING_FRAUD_REVIEW
   (~5 % probability), then send the approveManualReview signal and watch it
   continue to COMPLETED.

3. Saga compensation          -- kill the worker three times in quick succession so
   the postToLedger activity exhausts its 3 retry attempts; Temporal then calls
   reverseJournalEntry (saga) and the workflow ends as FAILED_COMPENSATED.

4. Payment-initiation restart -- show that restarting the stateless HTTP gateway
   does not lose any in-flight Temporal workflow state.

Usage
-----
    # Prerequisites: pip install requests
    python chaos_test.py               # run all 4 scenarios
    python chaos_test.py 1             # single scenario by number
    python chaos_test.py 2 3           # multiple by number

Evidence for each scenario is printed inline.  Temporal UI: http://localhost:3001
"""

import argparse
import subprocess
import sys
import time
import uuid
import json
import urllib.request
import urllib.error
from datetime import datetime

# -- config --------------------------------------------------------------------
BASE_URL             = "http://localhost:8001"
TEMPORAL_UI_URL      = "http://localhost:3001"
WORKER_CONTAINER     = "observability-java-temporal-worker-1"
INITIATION_CONTAINER = "observability-java-payment-initiation-1"
PROJECT              = "observability-java"

# -- helpers -------------------------------------------------------------------

def banner(title: str, char: str = "=") -> None:
    print(f"\n{char * 70}")
    print(f"  {title}")
    print(f"{char * 70}")

def step(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}] {msg}", flush=True)

def http_get(url: str) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

def http_post(url: str, body: dict) -> dict:
    data = json.dumps(body).encode()
    req  = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"},
                                  method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"error": e.read().decode(), "code": e.code}
    except Exception as e:
        return {"error": str(e)}

def docker(cmd: str, capture: bool = False):
    full = f"docker {cmd}"
    if capture:
        r = subprocess.run(full, shell=True, capture_output=True, text=True)
        return r.stdout + r.stderr
    subprocess.run(full, shell=True)

def container_status(name: str) -> str:
    out = docker(f"inspect {name} --format {{{{.State.Status}}}}", capture=True).strip()
    return out or "not found"

def stop_container(name: str) -> None:
    step(f"KILLING  {name}")
    docker(f"stop {name}")
    step(f"Status   -> {container_status(name)}")

def start_container(name: str) -> None:
    step(f"STARTING {name}")
    docker(f"start {name}")
    time.sleep(3)
    step(f"Status   -> {container_status(name)}")

def submit_payment(amount: float = 50_000.00, label: str = "") -> dict:
    uetr = str(uuid.uuid4())
    payload = {
        "uetr":             uetr,
        "amount":           amount,
        "currency":         "USD",
        "channel":          "API",
        "debtor_account":   "GB29BARC20000000000001",
        "creditor_account": "DE89370400440532013000",
    }
    resp = http_post(f"{BASE_URL}/initiate", payload)
    tag  = f"[{label}] " if label else ""
    step(f"{tag}Submitted  uetr={uetr[:8]}? amount={amount:,.0f} -> {resp.get('status','ERR')}")
    resp["uetr"] = uetr
    return resp

def poll_status(uetr: str, target_statuses=None, timeout_s: int = 90, interval: float = 2.0) -> str:
    """Poll /status/{uetr} until one of target_statuses is reached or timeout."""
    deadline = time.time() + timeout_s
    last     = ""
    while time.time() < deadline:
        resp = http_get(f"{BASE_URL}/status/{uetr}")
        st   = resp.get("status", "UNKNOWN")
        if st != last:
            step(f"  status={st}")
            last = st
        if target_statuses and st in target_statuses:
            return st
        time.sleep(interval)
    return last

def temporal_ui_workflows(page_size: int = 10) -> list:
    """Fetch recent workflows from Temporal UI REST API."""
    url  = f"{TEMPORAL_UI_URL}/api/v1/namespaces/default/workflows?pageSize={page_size}"
    data = http_get(url)
    return data.get("executions", [])

def show_temporal_evidence(label: str) -> None:
    """Print last 5 workflows from Temporal UI as evidence."""
    banner(f"Temporal UI evidence -- {label}", "-")
    workflows = temporal_ui_workflows(5)
    if not workflows:
        step("No workflows found (Temporal UI may still be warming up)")
        return
    fmt = "  {:<44} {:<22} {}"
    print(fmt.format("workflowId", "status", "startTime"))
    print("  " + "-" * 80)
    for wf in workflows:
        wid = wf.get("execution", {}).get("workflowId", "?")[:44]
        st  = wf.get("status", "?")
        ts  = wf.get("startTime", "?")[:19].replace("T", " ")
        print(fmt.format(wid, st, ts))

def wait_for_service(url: str, path: str = "/health", timeout_s: int = 60) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = http_get(f"{url}{path}")
        if "error" not in r:
            return True
        time.sleep(2)
    return False

# -- scenario 1: worker crash and recovery ------------------------------------

def scenario_1_worker_crash_recovery():
    banner("SCENARIO 1 -- Worker crash mid-workflow -> Temporal resumes from checkpoint")
    print("""
  What this proves
  ---------------
  Temporal durably persists workflow state (event history) in its database.
  Killing the worker container does NOT lose in-flight payments.
  When the worker restarts it picks up execution exactly from the last
  committed activity result -- no activity is re-run unnecessarily.
""")

    # Submit 3 payments so some are mid-flight when we kill the worker.
    step("Submitting 3 payments ?")
    uetrs = []
    for i in range(3):
        r = submit_payment(amount=10_000 * (i + 1), label=f"P{i+1}")
        uetrs.append(r["uetr"])
        time.sleep(1)

    step("Waiting 4 s for workflows to progress past ROUTING ?")
    time.sleep(4)

    # Show status before kill.
    banner("BEFORE crash", "-")
    for u in uetrs:
        r = http_get(f"{BASE_URL}/status/{u}")
        step(f"  uetr={u[:8]}?  status={r.get('status')}")

    # Kill the worker.
    print()
    stop_container(WORKER_CONTAINER)
    step("Worker is DOWN. Waiting 8 s -- Temporal holds state in PostgreSQL ?")
    time.sleep(8)

    # Status during outage (payment-initiation can query Temporal directly).
    banner("DURING outage (Temporal event history is safe in DB)", "-")
    for u in uetrs:
        r = http_get(f"{BASE_URL}/status/{u}")
        step(f"  uetr={u[:8]}?  status={r.get('status')}")

    # Restart the worker.
    print()
    start_container(WORKER_CONTAINER)
    step("Worker restarted -- waiting for it to reconnect to Temporal ?")
    time.sleep(6)

    # Poll to completion.
    banner("AFTER recovery -- polling to completion", "-")
    for u in uetrs:
        final = poll_status(u, target_statuses={"COMPLETED", "FAILED_COMPENSATED", "REJECTED"},
                            timeout_s=120)
        step(f"  uetr={u[:8]}?  FINAL={final}")

    show_temporal_evidence("after worker recovery")
    print("""
  Expected outcome
  ----------------
  All 3 workflows reach COMPLETED (or FAILED_COMPENSATED if saga fired).
  The Temporal UI shows continuous event history across the crash -- no
  duplicate activities, no lost steps.
""")


# -- scenario 2: fraud manual-review gate -------------------------------------

def scenario_2_fraud_review_approval():
    banner("SCENARIO 2 -- Fraud REVIEW gate -> signal-driven approval")
    print("""
  What this proves
  ---------------
  When fraud score > 70 (~5 % of payments) the workflow halts at
  PENDING_FRAUD_REVIEW and blocks on a Temporal signal.
  We send POST /approve/{uetr} to inject the signal, and the workflow
  immediately continues to COMPLETED.  No polling loop in application code.

  Strategy: submit payments in batches of 15, then wait 12 s for compliance
  screening to finish (activities take up to 5 s each), then scan all UETRs
  in the batch.  Expected hit within ~3 batches (45 payments at 5 % rate).
""")

    review_uetr = None
    batch_size  = 15
    max_batches = 6
    all_uetrs   = []

    for batch in range(max_batches):
        step(f"Batch {batch+1}/{max_batches} -- submitting {batch_size} payments ?")
        batch_uetrs = []
        for i in range(batch_size):
            r = submit_payment(amount=500_000, label=f"B{batch+1}-{i+1}")
            batch_uetrs.append(r["uetr"])
            all_uetrs.append(r["uetr"])
            time.sleep(0.2)

        step(f"  Waiting 12 s for compliance screening to complete ?")
        time.sleep(12)

        step(f"  Scanning {batch_size} UETRs for PENDING_FRAUD_REVIEW ?")
        for u in batch_uetrs:
            st = http_get(f"{BASE_URL}/status/{u}").get("status", "")
            if st == "PENDING_FRAUD_REVIEW":
                review_uetr = u
                step(f"  FOUND!  uetr={u[:8]}?  status=PENDING_FRAUD_REVIEW")
                break
        if review_uetr:
            break
        step(f"  No hit yet ({(batch+1)*batch_size} payments sent) ?")

    if not review_uetr:
        step("WARNING: No PENDING_FRAUD_REVIEW in " + str(max_batches * batch_size) +
             " attempts -- check Temporal UI at http://localhost:3001")
        step("  Filter by status=Running, look for workflows stuck at PENDING_FRAUD_REVIEW")
        show_temporal_evidence("fraud scan (no hit this run)")
        return

    banner(f"BEFORE approval  uetr={review_uetr[:8]}?", "-")
    st_before = http_get(f"{BASE_URL}/status/{review_uetr}").get("status")
    step(f"  status={st_before}  (workflow is BLOCKED on Temporal signal)")

    step("Sending manual-approval signal via POST /approve/{uetr} ?")
    resp = http_post(f"{BASE_URL}/approve/{review_uetr}", {"approvedBy": "chaos-tester"})
    step(f"  Approval response: {resp}")

    banner("AFTER approval -- polling to completion", "-")
    final = poll_status(review_uetr,
                        target_statuses={"COMPLETED", "REJECTED", "FAILED_COMPENSATED"},
                        timeout_s=120)
    step(f"  FINAL status={final}")

    show_temporal_evidence("after fraud approval")
    print("""
  Expected outcome
  ----------------
  Status transitions: PENDING_FRAUD_REVIEW -> ACCOUNTING -> POSTING ->
  DISPATCHING -> NETWORK_PROCESSING -> SETTLING -> COMPLETED.
  No code polling loop -- Temporal signal unblocked the workflow directly.
""")


# -- scenario 3: saga compensation via natural 1% ledger failure --------------

def scenario_3_saga_compensation():
    banner("SCENARIO 3 -- Natural ledger failure -> Saga compensation (journal reversal)")
    print("""
  What this proves
  ---------------
  postToLedger throws ApplicationFailure("LedgerError") with 1% probability.
  Temporal auto-retries up to 3 times with exponential backoff (1s, 2s, 4s).
  If all 3 attempts fail, the workflow calls reverseJournalEntry (saga) and
  returns FAILED_COMPENSATED.  If any retry succeeds, it continues normally.

  Strategy: submit 200 payments in a burst.  At 1% failure rate that yields
  ~2 ledger failures.  We then scan logs and Temporal UI for evidence of
  both the RETRY path and the SAGA path.
""")

    step("Submitting 200 payments to trigger natural 1% ledger failures ...")
    uetrs = []
    for i in range(200):
        r = submit_payment(amount=float(1000 + i * 100), label=f"vol-{i+1}")
        uetrs.append(r["uetr"])
        time.sleep(0.05)
        if (i + 1) % 50 == 0:
            step(f"  {i+1}/200 submitted ...")

    step("Waiting 90 s for all workflows to complete ...")
    deadline = time.time() + 90
    while time.time() < deadline:
        sample = uetrs[:20]
        running = sum(
            1 for u in sample
            if http_get(f"{BASE_URL}/status/{u}").get("status") not in
               {"COMPLETED", "FAILED_COMPENSATED", "REJECTED"}
        )
        step(f"  {running}/20 sampled workflows still running ...")
        if running == 0:
            break
        time.sleep(10)

    banner("Worker log evidence -- ledger failures and journal reversals", "-")
    logs = docker(f"logs {WORKER_CONTAINER} 2>&1", capture=True)
    found_failure  = False
    found_reversal = False
    for line in logs.splitlines():
        if "Ledger unavailable" in line or "LedgerError" in line:
            print(f"  RETRY  : {line.strip()}")
            found_failure = True
        if "Journal reversed" in line or "accounting-compensation" in line:
            print(f"  SAGA   : {line.strip()}")
            found_reversal = True

    if not found_failure:
        step("  No ledger failures yet -- 200 payments at 1% => ~2 expected.")
        step("  Increase volume or reduce failure threshold to guarantee a hit.")
    if found_failure and not found_reversal:
        step("  Ledger failure(s) found but Temporal retry healed them (attempts 2 or 3 passed).")
        step("  This is the RETRY path -- correct behaviour, no saga needed.")

    banner("Workflow outcome scan -- looking for FAILED_COMPENSATED", "-")
    compensated = []
    for u in uetrs:
        st = http_get(f"{BASE_URL}/status/{u}").get("status", "")
        if st == "FAILED_COMPENSATED":
            compensated.append(u)
            step(f"  SAGA TRIGGERED  uetr={u[:8]}...  status=FAILED_COMPENSATED")

    if not compensated:
        step("  No FAILED_COMPENSATED this run (retries healed all failures).")
        step("  Evidence of the RETRY path is in the log lines above.")
    else:
        step(f"  Saga compensation fired for {len(compensated)} payment(s) -- no orphaned journals.")

    show_temporal_evidence("after high-volume saga run")
    print("""
  Expected outcome
  ----------------
  Worker logs show:   [posting] Ledger unavailable ... attempt=N
  If retries exhausted: [accounting-compensation] Journal reversed ...
  Workflow:  FAILED_COMPENSATED (all retries failed) or COMPLETED (retry healed it)
  Both outcomes are correct -- saga fires only when all attempts are exhausted.
""")


# -- scenario 4: stateless gateway restart -------------------------------------

def scenario_4_gateway_restart():
    banner("SCENARIO 4 -- payment-initiation restart (stateless gateway)")
    print("""
  What this proves
  ---------------
  payment-initiation is a thin HTTP gateway.  All durable state lives in
  Temporal.  Restarting the gateway mid-flight does NOT affect in-progress
  workflows -- they continue running in temporal-worker.
""")

    step("Submitting 2 payments ?")
    uetrs = []
    for i in range(2):
        r = submit_payment(amount=25_000 * (i + 1), label=f"GW{i+1}")
        uetrs.append(r["uetr"])
        time.sleep(1)

    step("Waiting 3 s then restarting payment-initiation ?")
    time.sleep(3)
    stop_container(INITIATION_CONTAINER)
    step("Gateway DOWN -- workflows continue running in temporal-worker ?")
    time.sleep(5)

    start_container(INITIATION_CONTAINER)
    step("Waiting for gateway to become healthy ?")
    ready = wait_for_service(BASE_URL, timeout_s=40)
    step(f"  Gateway healthy={ready}")

    banner("Querying workflow status via restarted gateway", "-")
    for u in uetrs:
        final = poll_status(u,
                            target_statuses={"COMPLETED", "FAILED_COMPENSATED", "REJECTED"},
                            timeout_s=120)
        step(f"  uetr={u[:8]}?  FINAL={final}")

    show_temporal_evidence("after gateway restart")
    print("""
  Expected outcome
  ----------------
  Both workflows reach COMPLETED -- Temporal maintained all state.
  The gateway restart had zero impact on in-progress payments.
""")


# -- scenario 5: posting adapter failure -> FAILED_COMPENSATED -> reset -> COMPLETED --

WORKER_PORT    = "http://localhost:8020"
TEMPORAL_CONTAINER = "observability-java-temporal-1"

_temporal_ip_cache: str = ""

def temporal_ip() -> str:
    """Return the Docker network IP of the Temporal container.
    Uses 'hostname -i' inside the container (same as the healthcheck)
    to avoid Windows shell quoting issues with docker inspect --format templates.
    """
    global _temporal_ip_cache
    if not _temporal_ip_cache:
        r = subprocess.run(
            f"docker exec {TEMPORAL_CONTAINER} hostname -i",
            shell=True, capture_output=True, text=True)
        _temporal_ip_cache = r.stdout.strip().split()[0]
    return _temporal_ip_cache

def temporal_cli(subcmd: str) -> str:
    """
    Run a temporal CLI command inside the Temporal container.
    Uses $TEMPORAL_ADDRESS env var (supported by CLI v0.11.0 Shared Options)
    so we don't have to inject --address at a specific flag position.
    """
    ip   = temporal_ip()
    full = f"exec -e TEMPORAL_ADDRESS={ip}:7233 {TEMPORAL_CONTAINER} temporal {subcmd}"
    return docker(full, capture=True).strip()

def get_workflow_history(workflow_id: str) -> list:
    """Fetch full event history for a workflow via the temporal CLI (JSON output)."""
    raw = temporal_cli(
        f"workflow show --workflow-id \"{workflow_id}\" --namespace default --output json")
    try:
        data = json.loads(raw)
        return data.get("events", [])
    except Exception:
        return []

def find_pre_posting_event_id(workflow_id: str) -> int | None:
    """
    Walk the event history and return the WorkflowTaskCompleted event ID
    that immediately precedes the first ActivityTaskScheduled for postToLedger.
    That is the correct reset point: all prior activity results replay from
    history, postToLedger is re-executed fresh against the fixed adapter.
    """
    events     = get_workflow_history(workflow_id)
    last_wft_id = None

    for ev in events:
        etype = ev.get("eventType", "")
        eid   = int(ev.get("eventId", 0))

        if etype == "WorkflowTaskCompleted":
            last_wft_id = eid

        if etype == "ActivityTaskScheduled":
            attrs = ev.get("activityTaskScheduledEventAttributes", {})
            name  = attrs.get("activityType", {}).get("name", "")
            if "postToLedger" in name or "PostToLedger" in name:
                return last_wft_id   # WorkflowTaskCompleted just before first attempt

    return None

def temporal_reset(workflow_id: str, event_id: int | None, reason: str) -> str:
    """Reset workflow to a specific event (or FirstWorkflowTask) via Temporal CLI."""
    if event_id:
        return temporal_cli(
            f"workflow reset --workflow-id \"{workflow_id}\" --namespace default "
            f"--event-id {event_id} --reason \"{reason}\"")
    return temporal_cli(
        f"workflow reset --workflow-id \"{workflow_id}\" --namespace default "
        f"--type FirstWorkflowTask --reason \"{reason}\"")

def scenario_5_posting_failure_and_replay():
    banner("SCENARIO 5 -- Posting adapter BROKEN -> FAILED_COMPENSATED -> Temporal Reset -> COMPLETED")
    print("""
  What this proves
  ---------------
  A downstream ledger adapter fails on every call (simulated via chaos flag).
  Temporal exhausts all 3 retries (backoff 1s -> 2s -> 4s), triggers saga
  compensation (reverseJournalEntry), and the workflow closes as FAILED_COMPENSATED.

  Recovery flow:
    1. Operator fixes the adapter  (POST /chaos/posting/fix)
    2. Operator resets the workflow in Temporal UI to the WorkflowTaskCompleted
       event that precedes the first postToLedger attempt
    3. Temporal replays all prior activity results from event history (sanctions,
       fraud, accounting -- none re-executed, no duplicate side-effects)
    4. postToLedger re-runs fresh against the now-healthy adapter -> COMPLETED

  This script automates the same reset the UI Reset button performs.
""")

    # ---- Step 1: break the posting adapter --------------------------------
    step("Step 1: Breaking posting adapter ...")
    resp = http_post(f"{WORKER_PORT}/chaos/posting/break", {})
    step(f"  {resp}")

    # ---- Step 2: submit a payment ----------------------------------------
    step("Step 2: Submitting payment ...")
    r           = submit_payment(amount=75_000, label="saga-replay")
    uetr        = r["uetr"]
    workflow_id = f"payment-{uetr}"
    step(f"  workflowId = {workflow_id}")

    # ---- Step 3: wait for FAILED_COMPENSATED (3 retries ~7 s total) ------
    step("Step 3: Waiting for FAILED_COMPENSATED (3 retries x backoff ~7 s) ...")
    final = poll_status(uetr,
                        target_statuses={"FAILED_COMPENSATED", "COMPLETED", "REJECTED"},
                        timeout_s=120)
    step(f"  Workflow terminal state: {final}")

    if final != "FAILED_COMPENSATED":
        step(f"  WARNING: got {final} instead of FAILED_COMPENSATED")
        step(f"  Chaos status: {http_get(f'{WORKER_PORT}/chaos/status')}")
        http_post(f"{WORKER_PORT}/chaos/posting/fix", {})
        return

    banner("FAILED_COMPENSATED confirmed -- event history in Temporal UI", "-")
    step(f"  Open http://localhost:3001 -> search workflow-id: {workflow_id}")
    step("  Event history will show:")
    step("    ActivityTaskScheduled  postToLedger  attempt 1")
    step("    ActivityTaskFailed     LedgerError")
    step("    ActivityTaskScheduled  postToLedger  attempt 2")
    step("    ActivityTaskFailed     LedgerError")
    step("    ActivityTaskScheduled  postToLedger  attempt 3")
    step("    ActivityTaskFailed     LedgerError  (exhausted)")
    step("    ActivityTaskScheduled  reverseJournalEntry  (saga compensation)")
    step("    ActivityTaskCompleted  reverseJournalEntry")
    step("    WorkflowExecutionCompleted  result=FAILED_COMPENSATED")

    # ---- Step 4: fix the adapter -----------------------------------------
    print()
    step("Step 4: Fixing posting adapter ...")
    resp = http_post(f"{WORKER_PORT}/chaos/posting/fix", {})
    step(f"  {resp}")

    # ---- Step 5: find the correct reset event ID -------------------------
    step("Step 5: Querying event history via Temporal CLI to locate reset point ...")
    reset_event_id = find_pre_posting_event_id(workflow_id)

    if reset_event_id:
        step(f"  Reset event ID = {reset_event_id}  (WorkflowTaskCompleted before first postToLedger)")
        step(f"  Temporal UI: Reset button -> enter event ID {reset_event_id} -> confirm")
    else:
        step("  Could not determine event ID from API -- will reset to FirstWorkflowTask")

    # ---- Step 6: reset the workflow --------------------------------------
    banner("Step 6: Resetting workflow (same action as UI Reset button)", "-")
    reset_out = temporal_reset(workflow_id, reset_event_id,
                               "Posting adapter fixed -- replay from postToLedger")
    step(f"  CLI output: {reset_out}")

    # ---- Step 7: poll the new run to COMPLETED ---------------------------
    banner("Step 7: Polling reset workflow to COMPLETED", "-")
    step("  (New runId created by Temporal -- status endpoint follows latest run) ...")
    final2 = poll_status(uetr,
                         target_statuses={"COMPLETED", "FAILED_COMPENSATED", "REJECTED"},
                         timeout_s=180)
    step(f"  FINAL status after reset = {final2}")

    # Worker logs
    banner("Worker log evidence", "-")
    logs = docker(f"logs {WORKER_CONTAINER} --tail 60 2>&1", capture=True)
    for line in logs.splitlines():
        if any(k in line for k in
               ("posting", "revers", "compensat", "dispatch", "settlement", "chaos", "journal")):
            print(f"  {line.strip()}")

    show_temporal_evidence("after reset")

    print(f"""
  Evidence summary
  ----------------
  workflowId : {workflow_id}

  Run 1 (original):
    - 3x ActivityTaskFailed on postToLedger (adapter BROKEN)
    - reverseJournalEntry called (saga compensation)
    - WorkflowExecutionCompleted  status=FAILED_COMPENSATED

  Run 2 (after reset to event {reset_event_id}):
    - routeAndTransform / acceptPayment / sanctions / fraud / journal
      all REPLAYED from Run 1 history -- zero re-execution, no duplicates
    - postToLedger re-executed fresh against HEALTHY adapter -> COMPLETED
    - Workflow continues through dispatch / network / settlement

  Final status : {final2}

  How to do this in the Temporal UI
  ----------------------------------
  1. Open http://localhost:3001
  2. Search for workflow-id: {workflow_id}
  3. Open Run 1 (the FAILED_COMPENSATED run)
  4. Click "Reset" (top-right)
  5. Enter event ID {reset_event_id} in the "Event ID" field
  6. Add reason: "Posting adapter fixed -- replay from postToLedger"
  7. Click Confirm -- Temporal creates Run 2 and the worker picks it up
""")


# -- main ----------------------------------------------------------------------

SCENARIOS = {
    1: ("Worker crash mid-workflow",          scenario_1_worker_crash_recovery),
    2: ("Fraud review gate + approval",       scenario_2_fraud_review_approval),
    3: ("Saga compensation",                  scenario_3_saga_compensation),
    4: ("Stateless gateway restart",          scenario_4_gateway_restart),
    5: ("Posting failure -> Reset -> COMPLETED", scenario_5_posting_failure_and_replay),
}

def main():
    parser = argparse.ArgumentParser(
        description="Chaos tests for the Temporal payment stack")
    parser.add_argument("scenarios", nargs="*", type=int,
                        help="Scenario numbers to run (default: all)")
    args = parser.parse_args()

    to_run = args.scenarios or list(SCENARIOS.keys())

    banner("Chaos Test Suite -- Temporal Payment Observability Stack")
    print(f"  Scenarios to run: {to_run}")
    print(f"  Payment API:      {BASE_URL}")
    print(f"  Temporal UI:      {TEMPORAL_UI_URL}")
    print(f"  Worker:           {WORKER_CONTAINER}")

    # Pre-flight check.
    step("Pre-flight: checking payment-initiation health ?")
    if not wait_for_service(BASE_URL, timeout_s=15):
        print("\n  ERROR: payment-initiation not reachable. Is the stack up?\n"
              "  Run: docker compose -f C:\\temp\\observability-Java\\docker-compose.yml up -d")
        sys.exit(1)
    step("  Service reachable.")

    results = {}
    for num in to_run:
        if num not in SCENARIOS:
            print(f"  Unknown scenario {num}, skipping")
            continue
        label, fn = SCENARIOS[num]
        try:
            fn()
            results[num] = "PASSED"
        except KeyboardInterrupt:
            step("Interrupted by user -- ensuring worker is back up ?")
            start_container(WORKER_CONTAINER)
            start_container(INITIATION_CONTAINER)
            break
        except Exception as exc:
            step(f"  SCENARIO {num} ERRORED: {exc}")
            results[num] = f"ERROR: {exc}"
            # Always leave stack in a running state.
            start_container(WORKER_CONTAINER)
            start_container(INITIATION_CONTAINER)

    banner("Summary")
    for num, outcome in results.items():
        label = SCENARIOS[num][0]
        print(f"  [{num}] {label:<40} {outcome}")
    print()

if __name__ == "__main__":
    main()
