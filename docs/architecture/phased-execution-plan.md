# Phased execution plan

- Phase 7A — encoded-read probe plan contract: **complete**.
- Phase 7B — feature-gated local Vortex metadata-only fixture/open transition: **current**.
- Phase 8 — first controlled encoded-read execution spike: **not started**.

Phase 7A is contract-only and does not execute scans, read data, decode/materialize, perform object-store/write/spill IO, or allow fallback execution.

- Phase 7A: complete (encoded-read probe plan contract).
- Phase 7B: complete (feature-gated local metadata-only open transition).
- Phase 8: current (first controlled encoded-read execution spike).


## Phase 9A update (current)

- Phase 8 is complete once PR #82 is merged.
- Phase 9 is now the current stream.
- Phase 9A introduces minimal `Vortex` query primitives and a metadata-count path (`CountAll`).
- This phase keeps scan/decode/materialization disabled and preserves no-fallback execution policy.

## Phase 9B status update
- Phase 9A is complete.
- Phase 9B is current: metadata-filtered `CountWhere` primitive.
