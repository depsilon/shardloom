<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL Parser Dependency Review (`sqlparser`, `E2E-ARCH-02`)

## Purpose

This document records the Apache-2.0 license, provenance, security, and no-fallback isolation review
for adding `sqlparser = "=0.64.0"` to `shardloom-plan` under
[RFC 0049](../rfcs/0049-pure-ast-sql-parser-and-plan-binder.md) and `E2E-ARCH-02` in
[the phased execution plan](../architecture/phased-execution-plan.md#reconciled-e2e-queue-before-version-130).
It authorizes **only syntax-only SQL lexing and AST construction** inside `shardloom-plan`. It does
not authorize any query-engine execution fallback, planner delegation, C/FFI dependency, or
performance claim.

## Dependency & License Posture

| Field | Reviewed Value |
| --- | --- |
| Crate | `sqlparser` |
| Pinned version | `=0.64.0` (`default-features = false, features = ["std"]`) |
| Workspace placement | `[workspace.dependencies]` in `Cargo.toml`; direct dependency **only** in `shardloom-plan/Cargo.toml` |
| License | `Apache-2.0` (compatible with ShardLoom `Apache-2.0` and allowed by `deny.toml`) |
| Upstream repository | <https://github.com/apache/datafusion-sqlparser-rs> |
| Crates.io archive SHA-256 (`Cargo.lock`) | `92e0d273b4f8ddd14a35ed36862acbc9cc96cec578e36ee1881a9d42e0e0f2c8` |
| Bundled `LICENSE.TXT` SHA-256 / bytes | `a60eea817514531668d7e00765731449fe14d059d3249e0bc93b36de45f759f2` (`10,847` bytes) |
| Bundled `NOTICE.TXT` SHA-256 / bytes | `bb3ca1682483387eb1f4e538b226f83856212488f11ebad1bd71bb928a44f5e9` (`184` bytes; Copyright 2024-2026 The Apache Software Foundation) |
| Transitive runtime dependencies | `log = "0.4"` (already locked in `Cargo.lock` at `0.4.29`, `MIT OR Apache-2.0`, zero new transitive crates added) |
| Proc-macros / build scripts / `unsafe` | `build = false`, zero proc-macros (optional `sqlparser_derive` disabled), `#![forbid(unsafe_code)]` |

## Safety & Isolation Boundary

- **Syntax-only scope**: Although maintained under the Apache DataFusion GitHub organization
  (`apache/datafusion-sqlparser-rs`), `sqlparser` is the standalone pure-Rust SQL tokenizer and AST
  crate. Enabling it introduces **zero** `datafusion*`, `vortex-datafusion`, `duckdb`, `spark`,
  `polars`, `velox`, `trino`, `dask`, `ray`, or `calcite` crates into `Cargo.toml` or `Cargo.lock`.
- **Single-crate encapsulation**: Only `shardloom-plan` imports `sqlparser`. All other workspace
  crates (`shardloom-core`, `shardloom-exec`, `shardloom-vortex`, `shardloom-cli`, `shardloom-python`)
  consume SQL inspection and logical plan binding through `shardloom_plan::sql_binder` with zero
  direct `sqlparser` dependency.
- **No unsafe code & no build scripts**: `sqlparser 0.64.0` enforces `unsafe_code = "forbid"` in its
  published `Cargo.toml`, sets `build = false`, and compiles with pure safe Rust.
- **Bounded parsing**: Recursive features (`recursive-protection`) are disabled in favor of
  ShardLoom's explicit byte-size (`8 MiB`), paren/bracket nesting (`24`), and `Parser::with_recursion_limit(24)` guards.
- **No fallback execution**: `fallback_attempted=false`, `external_engine_invoked=false`,
  `claim_gate_status=not_claim_grade`.

## Validation Requirements

- `cargo deny check licenses bans advisories sources`
- `python3 scripts/check_python_license_bundle.py`
- `python3 scripts/check_dependency_audit.py --no-json`
- `cargo test -p shardloom-contract-tests --test no_fallback_invariants --test license_metadata`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p shardloom-plan --all-targets`
