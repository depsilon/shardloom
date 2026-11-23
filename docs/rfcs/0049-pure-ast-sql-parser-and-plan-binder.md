<!-- SPDX-License-Identifier: Apache-2.0 -->

# RFC 0049: Pure-AST SQL Parser and Canonical `shardloom-plan` Binder (`sqlparser`)

## Status

Accepted (`E2E-ARCH-02`, November 23, 2026) under the
[phased execution plan](../architecture/phased-execution-plan.md#reconciled-e2e-queue-before-version-130)
and canonical RFCs [0011](0011-modular-extensibility-sql-udf-unstructured-llm-api-embeddings.md),
[0012](0012-diagnostics-explain-estimate-capabilities.md),
[0021](0021-expression-engine-kernel-registry.md), and
[0022](0022-plan-ir-substrait-compatible-interoperability.md), plus canonical `PERF-02` and `PERF-10`.

## Summary

ShardLoom previously parsed SQL across `shardloom-cli` and `python/src/shardloom/query.py` using
multiple handwritten string splitters and ad-hoc keyword scanners (`sql_syntax.rs`,
`public_workflow_route.rs`, `sql_local_source_runtime.rs`, and `query.py`). That duplicated
syntactic complexity, drifted across boundaries (comments, quotes, `BETWEEN ... AND`,
`IS [NOT] DISTINCT FROM`, nested parens), and left `shardloom-plan` without a canonical
SQL AST-to-logical-plan frontend ahead of `E2E-PERF-05A`/`05B`/`05C`.

This RFC approves integrating **`sqlparser = "=0.64.0"`** (`default-features = false, features = ["std"]`)
strictly as a **syntax-only SQL lexer and AST parser** isolated inside `shardloom-plan`, backing
one canonical `shardloom_plan::sql_binder` module and retiring the handwritten SQL splitters
across Rust and Python.

## Context

`E2E-ARCH-02` in [the phased execution plan](../architecture/phased-execution-plan.md#reconciled-e2e-queue-before-version-130)
positions pure-AST SQL parsing and canonical logical plan binding directly ahead of
`E2E-PERF-05A`/`05B`/`05C` (canonical plan execution, compiled columnar expressions, `vortex_middle`
pushdown, native window/set/CTE/subquery convergence, and large-block spill). Before `E2E-PERF-05A`
can wire SQL and `LazyFrame` workloads through `shardloom-plan` -> `shardloom-vortex`, SQL syntax
analysis must be unified behind one audited parser and deterministic logical binder without adding
any external query-engine dependency or fallback path.

## Goals

1. **Single authoritative SQL tokenizer & AST parser**: Use `sqlparser = "=0.64.0"`
   (`Apache-2.0`, `#![forbid(unsafe_code)]`, `build = false`) inside `shardloom-plan` only.
2. **Preserve standalone no-fallback invariants**: Zero external execution engines (`DataFusion`,
   `DuckDB`, `Spark`, `Polars`, `Velox`, `Trino`, `Dask`, `Ray`, `Calcite`) in `Cargo.toml` or
   `Cargo.lock`; `fallback_attempted = false`, `external_engine_invoked = false`.
3. **Canonical `SQL AST -> shardloom-plan` logical & expression binder**: Bind `SELECT`, `FROM`,
   single-quoted file path table references (`'path/to/file.vortex'`), `WHERE`, `GROUP BY` /
   `GROUPING SETS` / `ROLLUP` / `CUBE`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`, `JOIN` (including
   ShardLoom `ASOF [LEFT] JOIN ... MATCH_CONDITION (...) [ON ...] [TOLERANCE ...]`), set
   operations (`UNION [ALL|DISTINCT]`, `INTERSECT`, `EXCEPT`), window specs (`OVER (...)`),
   CTEs (`WITH [RECURSIVE]`), subqueries (`IN`, `EXISTS`, scalar), named `:param` parameters, and
   source-free `VALUES`/`range()`/`generate_series()` into typed `LogicalRelation`, `LogicalExpr`,
   and `NativePlanDocument` (`PlanLayer::Logical`) structures.
4. **Deterministic `RFC 0012` diagnostics**: Reject unsupported SQL statements (DML/DDL, multiple
   statements), positional `?` / `$1` parameters, unclosed quotes, unbalanced parens/brackets, and
   intake/depth overflows explicitly with stable `ShardLoomError` / `Diagnostic` payloads.
5. **Retire handwritten splitters**: Replace duplicated handwritten character-by-character SQL
   scanners across `shardloom-cli/src/sql_syntax.rs`, `shardloom-cli/src/public_workflow_route.rs`,
   `shardloom-cli/src/sql_local_source_runtime.rs`, and `python/src/shardloom/query.py` with calls
   backed by `shardloom_plan::sql_binder` (and a single Python lexer/AST companion `_sql_ast.py`
   plus optional `_native.inspect_sql_json`).

## Non-goals

- Executing logical plans directly in `shardloom-plan` (`E2E-PERF-05A` wires execution into
  `shardloom-vortex`).
- Adding DDL/DML execution (`INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `COPY`, `ATTACH`).
- Pulling any DataFusion runtime, physical planner, crate (`datafusion*`, `vortex-datafusion`), or
  query-engine integration into the dependency graph (`apache/datafusion-sqlparser-rs` is solely
  the upstream repository for the standalone `sqlparser` parser crate; no DataFusion runtime crate
  is depended upon or linked).
- Introducing Python PyPI runtime dependencies (`python/pyproject.toml` keeps `dependencies = []`).
- Establishing any performance or benchmark claim (`claim_gate_status=not_claim_grade`).

## Decision

1. **Crate selection & provenance**:
   - Add `sqlparser = { version = "=0.64.0", default-features = false, features = ["std"] }` in root
     `Cargo.toml` `[workspace.dependencies]` and depend on it **only** in `shardloom-plan/Cargo.toml`.
   - License: `Apache-2.0`; repository: `https://github.com/apache/datafusion-sqlparser-rs`;
     compiler safety: `#![forbid(unsafe_code)]`, `build = false`, zero proc-macros; single transitive
     dependency: `log 0.4` (already present in `Cargo.lock` at `0.4.29`). See companion
     [sqlparser dependency review](../dependencies/sqlparser-dependency-review.md).
2. **Custom `ShardLoomSqlDialect` and bounded pre-scan**:
   - Implement `ShardLoomSqlDialect` enabling double-quoted identifiers, CTE materialization flags,
     aggregate `FILTER (...)`, `GROUP BY` expressions, `REPLACE`/`EXCLUDE` wildcards, trailing commas,
     numeric literal underscores, single quotes in `EXTRACT`, and `VALUES` as a table factor.
   - Normalize ShardLoom's `ASOF LEFT JOIN ... TOLERANCE <u64>` token modifiers purely via the
     `sqlparser::tokenizer::Tokenizer` stream (never ad-hoc string slicing) before invoking
     `sqlparser::parser::Parser` with bounded intake (`8 MiB`) and recursion depth (`24`).
3. **Canonical binder & structural extractor API in `shardloom-plan`**:
   - Expose `shardloom_plan::sql_binder::{ShardLoomSqlDialect, BoundSqlPlan, LogicalRelation, LogicalExpr, SqlSingleSourceShape, SqlSelectClauseSlices, SqlSourceRefSpan, SqlSetOperatorSpan, SqlCteParts, SqlInspectionSummary, bind_sql, inspect_sql, inspect_sql_json, split_top_level_csv, ...}`.
   - Provide `BoundSqlPlan::to_native_plan_document(PlanId)` emitting `NativePlanDocument` (`PlanLayer::Logical`, explicit `fallback_attempted = false`).
4. **Python & CLI integration**:
   - Expose `inspect_sql_json` via `shardloom-python` (`python-native` feature) so Python callers can
     query the canonical Rust AST inspection boundary when `_native` is loaded, backed by
     `python/src/shardloom/_sql_ast.py` when running pure-Python without native extensions.

## Detailed design

- `shardloom-plan/src/sql_binder.rs`: owns `ShardLoomSqlDialect`, `LineIndex` (1-based `line:col` ->
  exact UTF-8 byte offsets), bounded tokenizer wrapper, token-stream phrase/operator/CSV/CTE/source
  extractors, `ASOF LEFT JOIN` + `TOLERANCE` token adapter, AST-backed `extract_single_source_shape`,
  `bind_sql` -> `BoundSqlPlan`, `BoundSqlPlan::to_native_plan_document`, and `inspect_sql_json`.
- `shardloom-plan/src/sql_binder_expr.rs`: lowers `sqlparser::ast::Expr` trees into `LogicalExpr`
  and `shardloom_core::Expression`, rejecting unknown/effectful syntax with deterministic `RFC 0012`
  diagnostics (`DiagnosticCode::NotImplemented` / `InvalidOperation`, `fallback.attempted = false`).
- `shardloom-cli` callers (`sql_syntax.rs`, `sql_native_parameters.rs`, `sql_relation_scopes.rs`,
  `public_workflow_route.rs`, `sql_local_source_runtime.rs`) delegate CSV splitting, clause/keyword
  location, operator scanning, source-reference extraction, CTE header splitting, parameter extraction,
  and single-source route shape detection to `shardloom_plan::sql_binder`.
- `python/src/shardloom/query.py` delegates all SQL keyword, source-ref, CSV, `LIMIT`, and cast
  splitting to `python/src/shardloom/_sql_ast.py` (which delegates to `_native.inspect_sql_json`
  when available and keeps pure-Python execution dependency-free via one unified lexer).

## Alternatives considered

1. **Extend existing handwritten Rust/Python splitters**: Rejected because maintaining four separate
   ad-hoc scanners does not produce a typed SQL AST for `E2E-PERF-05A`/`05B` and repeatedly creates
   edge-case bugs around quoted strings, comments, `BETWEEN ... AND`, and `IS DISTINCT FROM`.
2. **Build a brand-new full ANSI/DuckDB/Postgres SQL parser from scratch**: Rejected because writing
   and maintaining a full SQL grammar from scratch adds high defect risk while `sqlparser`
   (`Apache-2.0`, `#![forbid(unsafe_code)]`, `build = false`, zero C dependencies) already exists
   with wide SQL grammar coverage.
3. **Depend on `vortex-datafusion` / `datafusion-sql` / `duckdb`**: Prohibited by ShardLoom's
   non-negotiable standalone architecture (`no_fallback_invariants`).

## Risks

- **SQL dialect/custom syntax mismatches**: ShardLoom supports quoted local file paths (`FROM 'path.csv'`),
  `ASOF LEFT JOIN ... MATCH_CONDITION (...) [TOLERANCE n]`, `GAP_FILL(...)`, `DISTINCT_ROWS(...)`,
  and `:param` named placeholders. Mitigation: inspect `sqlparser` tokenization and handle ShardLoom
  syntax explicitly in `ShardLoomSqlDialect` and token-stream adapters tested against the entire
  existing `sql_syntax_tests` and `public_workflow_route` suites.
- **Error-message or envelope drift**: Existing CLI/snapshot tests may assert existing diagnostic
  wording on malformed SQL inputs. Mitigation: preserve existing boundary validation messages while
  attaching structured AST binder diagnostics and run full `shardloom-cli` contract & snapshot suites.

## Compatibility impact

- External CLI, Python SDK (`shardloom.query`), and typed JSON envelopes (`shardloom.native_batches*`,
  `typed_envelope_*`) remain backward compatible.
- `python/pyproject.toml` remains zero-dependency (`dependencies = []`).
- Workspace MSRV remains `1.96`, workspace version remains `"1.1.0"`.

## Acceptance criteria

1. `docs/rfcs/0049-pure-ast-sql-parser-and-plan-binder.md`,
   `docs/dependencies/sqlparser-dependency-review.md`, and `docs/architecture/rfc-phase-traceability.md`
   exist and match repository doc/license governance.
2. `sqlparser = "=0.64.0"` is isolated solely to `shardloom-plan`; `Cargo.lock`,
   `python/THIRD_PARTY_LICENSES/manifest.json`, and `deny.toml` pass `cargo deny check licenses bans advisories sources`,
   `check_python_license_bundle.py`, `check_dependency_audit.py`, `license_metadata`, and
   `no_fallback_invariants` (zero forbidden fallback packages).
3. `shardloom_plan::sql_binder` parses and binds ShardLoom SQL queries into canonical logical plans
   and `NativePlanDocument`, with deterministic `RFC 0012` diagnostics on unsupported/malformed inputs.
4. Handwritten SQL splitters across `sql_syntax.rs`, `public_workflow_route.rs`,
   `sql_local_source_runtime.rs`, and `python/src/shardloom/query.py` are retired to the canonical
   parser/lexer path.
5. `cargo fmt --all -- --check`, Clippy across default & `release-user-surfaces`, `--workspace --all-targets`,
   Python SQL/builder/API tests, `write_public_api_reference.py --check`, and
   `run_all_checks.py --suite docs` all pass.

## Verification plan

- Run `cargo deny check licenses bans advisories sources`, `python3 scripts/check_python_license_bundle.py`,
  and `python3 scripts/check_dependency_audit.py --no-json`.
- Run `cargo test -p shardloom-contract-tests --test no_fallback_invariants --test license_metadata --test release_readiness_metadata`.
- Run `cargo test -p shardloom-plan --all-targets` and `cargo test -p shardloom-cli --features release-user-surfaces --lib --test public_workflow_route --test typed_envelope_compatibility_lock --test typed_envelope_contract_snapshots`.
- Run full `cargo fmt --all -- --check`, Clippy (`--workspace` and `release-user-surfaces`),
  `cargo test --workspace --all-targets`, Python unittest suites, `write_public_api_reference.py --check`,
  and `scripts/run_all_checks.py --suite docs`.

## Open questions

- Future `E2E-PERF-05A`/`05B` follow-up will retire the remaining imperative execution bindings in
  `sql_native_relational.rs` and `sql_local_source_runtime.rs` once `LogicalRelation` lowers
  directly into compiled columnar Vortex operators.
