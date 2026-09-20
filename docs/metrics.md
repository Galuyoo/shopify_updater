# Production metrics

## Scope
Measure reconciliation cycles and per-store runs: products/variants inspected, discrepancies, updates attempted/succeeded/failed, retries, API/rate-limit failures, processing duration and release version.

## Storage
Events are append-only JSONL under `data/metrics/events.jsonl` by default. Override with `METRICS_PATH` when production storage requires a different persistent path.

## Correlation
Each service cycle gets a cycle ID; each store run gets a workflow ID. Retries are counted separately from successful updates so reprocessing cannot be mistaken for additional business work.

## Privacy
Do not store access tokens, customer data, raw supplier feeds or full API responses.

## Export
The JSONL stream can be queried with Python/Excel/Power Query. Treat StoreFeeder/order revenue as a separate source of truth; updater telemetry measures operational work, not revenue caused.
