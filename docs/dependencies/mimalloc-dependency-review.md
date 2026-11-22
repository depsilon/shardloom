<!-- SPDX-License-Identifier: Apache-2.0 -->

# Mimalloc Global Allocator Dependency Review (`E2E-PERF-00`)

## Purpose

This document records Apache-2.0 license compatibility and security/isolation constraints for
adding `mimalloc` behind an opt-in binary feature in `shardloom-cli` under [RFC 0048](../rfcs/0048-cli-global-allocator-mimalloc.md).
It authorizes the dependency policy ONLY when paired with offline crate vendoring/cache intake; it
does not alter `Cargo.lock` prematurely or add any execution fallback.

## Dependency & License Posture

- Planned direct crate (pending offline intake): `mimalloc` (`0.1.x`) in `shardloom-cli/Cargo.toml`, optional behind feature `cli-mimalloc`.
- Upstream transitive FFI crate: `libmimalloc-sys` (Microsoft `mimalloc` C library).
- Upstream license: `MIT` (explicitly allowed by workspace `deny.toml` and compatible with ShardLoom `Apache-2.0`).
- Zero GPL/AGPL/SSPL/BUSL code, zero external SQL/execution runtime dependencies.

## Safety & Isolation Boundary

- All workspace library crates (`shardloom-core`, `shardloom-exec`, `shardloom-vortex`, `shardloom-plan`, `shardloom-runtime`, `shardloom-python`) continue to enforce `#![forbid(unsafe_code)]` and receive **no** `mimalloc` dependency.
- `#[global_allocator]` resides strictly in `shardloom-cli/src/main.rs` (`#[cfg(feature = "cli-mimalloc")]`).
- Default macOS/Linux builds remain unaffected until paired `E2E-VERIFY-01` / `evaluate_perf_ship_drop.py` Full43 telemetry passes system-CPU, RSS, and page-fault gates.

## Validation Requirements Upon Intake

- `cargo deny check licenses bans advisories sources`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo clippy -p shardloom-cli --features cli-mimalloc --all-targets -- -D warnings`
- Paired before/after `run_clickbench_query_uat.py` + `evaluate_perf_ship_drop.py` screen.
