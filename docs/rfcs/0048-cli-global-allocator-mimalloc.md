<!-- SPDX-License-Identifier: Apache-2.0 -->

# RFC 0048: CLI Binary Global Allocator Selection (`mimalloc` Opt-In Policy)

## Status

Proposed (`E2E-PERF-00` preparation, November 22, 2026) under the
[phased execution plan](../architecture/phased-execution-plan.md#reconciled-e2e-queue-before-version-130)
and canonical `PERF-01` / `PERF-03`. Documents provenance, strict binary-only crate isolation, and
ship/drop measurement criteria ahead of introducing `mimalloc` into the offline build cache / `Cargo.lock`.

## Summary

Workload attribution under ClickBench `Full43` and high-cardinality streaming aggregations spends a
significant fraction of `system_cpu_seconds` and minor page faults inside default OS `malloc`/`free`
paths. Under this RFC:

1. **Binary-only isolation**: `#[global_allocator]` may be compiled **strictly inside `shardloom-cli/src/main.rs`** behind an explicit feature gate (`cli-mimalloc`), never inside workspace library crates (`shardloom-core`, `shardloom-exec`, `shardloom-vortex`, `shardloom-plan`, `shardloom-runtime`, `shardloom-python`), which retain `#![forbid(unsafe_code)]`.
2. **License & provenance**: `mimalloc` (`libmimalloc-sys`) is MIT-licensed (see companion [mimalloc dependency review](../dependencies/mimalloc-dependency-review.md)), Apache-2.0 compatible, and adds zero query-engine fallback capability.
3. **Ship / drop gate before default activation**:
   - No release binary enables `cli-mimalloc` by default until a paired `E2E-VERIFY-01` / `evaluate_perf_ship_drop.py` screen proves net `engine_execution_seconds` / `seconds` reduction plus non-regressing Peak RSS, `system_cpu_seconds`, and minor/major page faults on both `cold_process_cli` and `warm_resident_session` lanes.
   - `default-features = false` (`override = false` on Apple Clang/musl unless explicitly verified per platform wheel/homebrew build).

## Contract Guarantees

- `fallback_attempted=false`, `external_engine_invoked=false`.
- All library crates retain `unsafe_code = "forbid"`.
- Reversible by disabling the feature flag without schema or public API impact.
