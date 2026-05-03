# Phased execution plan

- Phase 7A — encoded-read probe plan contract: **current**.
- Phase 7B — metadata-only fixture/open transition: **next**.

Phase 7A is contract-only and does not execute scans, read data, decode/materialize, perform object-store/write/spill IO, or allow fallback execution.
