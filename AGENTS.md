# AGENTS.md

## Production metrics requirement

Any new production feature or meaningful workflow change must consider telemetry as part of implementation.

A feature is not complete until the developer has explicitly determined:
1. which meaningful operational event(s) it creates;
2. whether success/failure should be recorded;
3. whether throughput/counts should be recorded;
4. whether execution duration matters;
5. whether operator/channel/store/job/batch context is required;
6. whether the metrics schema/registry must change; and
7. whether tests verify expected telemetry.

Do not add vanity metrics or log every UI interaction. Metrics must describe meaningful system behaviour, support operations/product analysis, and enable defensible real-world impact measurement.

Do not store unnecessary PII, credentials, tokens, secrets, full customer payloads, or confidential source data. When metric semantics change, version/document the schema rather than silently redefining an existing event. Where practical, production events must identify the application/release that emitted them.

Telemetry must be best-effort: failure to record metrics must not normally break the business workflow.
