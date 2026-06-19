"""
Load generator — simulates channel-initiated payments with random UETRs.

Each payment creates a root OTEL span whose trace_id equals the UETR (hex,
no dashes).  Payment Processor now returns immediately with ACCEPTED status;
downstream processing (accounting, posting, sanctions, fraud, settlement)
happens asynchronously via Kafka and Solace.
"""
import asyncio
import contextvars
import os
import random
import uuid
from datetime import datetime
from typing import Optional

import httpx
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import IdGenerator, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

TARGET_URL          = os.getenv("TARGET_URL",          "http://localhost:8001")
OTEL_ENDPOINT       = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
PAYMENTS_PER_MINUTE = int(os.getenv("PAYMENTS_PER_MINUTE", "10"))
INTERVAL_SECONDS    = 60.0 / PAYMENTS_PER_MINUTE

CURRENCIES      = ["USD", "EUR", "GBP", "SGD", "HKD", "JPY", "AUD"]
CHANNELS        = ["MOBILE", "INTERNET_BANKING", "BRANCH", "API", "SWIFT_GPI"]
DEBIT_ACCOUNTS  = [f"GB{random.randint(10,99)}BARC{random.randint(10**14,10**15-1)}" for _ in range(20)]
CREDIT_ACCOUNTS = [f"DE{random.randint(10,99)}1001{random.randint(10**14,10**15-1)}" for _ in range(20)]

_uetr_trace_id: contextvars.ContextVar[Optional[int]] = contextvars.ContextVar(
    "uetr_trace_id", default=None
)


class UETRIdGenerator(IdGenerator):
    def generate_span_id(self) -> int:
        return random.getrandbits(64)

    def generate_trace_id(self) -> int:
        tid = _uetr_trace_id.get()
        if tid is not None:
            _uetr_trace_id.set(None)
            return tid
        return random.getrandbits(128)


def _setup_otel() -> trace.Tracer:
    resource = Resource.create({
        "service.name": "load-generator",
        "deployment.environment": "payment-system",
    })
    provider = TracerProvider(
        resource=resource,
        id_generator=UETRIdGenerator(),
    )
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True))
    )
    trace.set_tracer_provider(provider)
    return trace.get_tracer("load-generator")


_propagator = TraceContextTextMapPropagator()
completed   = 0
failed      = 0


async def send_payment(client: httpx.AsyncClient, tracer: trace.Tracer) -> None:
    global completed, failed

    uetr   = str(uuid.uuid4())
    amount = round(random.uniform(100, 1_000_000), 2)
    payload = {
        "uetr":             uetr,
        "amount":           amount,
        "currency":         random.choice(CURRENCIES),
        "debtor_account":   random.choice(DEBIT_ACCOUNTS),
        "creditor_account": random.choice(CREDIT_ACCOUNTS),
        "channel":          random.choice(CHANNELS),
    }

    uetr_hex = uetr.replace("-", "")
    _uetr_trace_id.set(int(uetr_hex, 16))

    ts = datetime.utcnow().strftime("%H:%M:%S")
    with tracer.start_as_current_span("payment.initiate") as span:
        span.set_attribute("payment.uetr",   uetr)
        span.set_attribute("payment.amount", amount)

        carrier: dict = {}
        _propagator.inject(carrier)

        try:
            resp = await client.post(
                f"{TARGET_URL}/initiate",
                json=payload,
                headers=carrier,
                timeout=30.0,
            )
            resp.raise_for_status()
            data       = resp.json()
            status     = data.get("status",     "N/A")
            tracking   = data.get("trackingId", "N/A")
            completed += 1
            print(
                f"[{ts}] {status} | uetr={uetr[:8]}… trackingId={tracking} | "
                f"trace={uetr_hex[:16]}… | "
                f"{amount:>12,.2f} {payload['currency']} | "
                f"done={completed} err={failed}",
                flush=True,
            )
        except Exception as exc:
            span.record_exception(exc)
            failed += 1
            print(
                f"[{ts}] ERR | uetr={uetr} | {exc} | done={completed} err={failed}",
                flush=True,
            )


async def wait_for_service(url: str, retries: int = 30, delay: float = 5.0) -> None:
    print(f"Waiting for {url} to be ready...", flush=True)
    async with httpx.AsyncClient() as client:
        for attempt in range(retries):
            try:
                resp = await client.get(f"{url}/health", timeout=5.0)
                if resp.status_code == 200:
                    print(f"Service ready after {attempt + 1} attempt(s).", flush=True)
                    return
            except Exception:
                pass
            await asyncio.sleep(delay)
    raise RuntimeError(f"Service at {url} did not become ready after {retries} attempts.")


async def main() -> None:
    tracer = _setup_otel()
    await wait_for_service(TARGET_URL)

    print(f"\nLoad generator started — {PAYMENTS_PER_MINUTE} payments/min → {TARGET_URL}", flush=True)
    print("=" * 80, flush=True)
    print("Architecture: payment-initiation → Temporal workflow → temporal-worker activities", flush=True)
    print("Paste the uetr hex (trace_id) into Grafana Tempo to see the end-to-end trace.", flush=True)
    print("View workflow progress at http://temporal-ui:8080 (or localhost:3001 from host).", flush=True)
    print("=" * 80 + "\n", flush=True)

    async with httpx.AsyncClient() as client:
        while True:
            asyncio.create_task(send_payment(client, tracer))
            await asyncio.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
