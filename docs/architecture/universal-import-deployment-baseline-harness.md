# Universal Import, Deployment, and Baseline Harness

## Purpose

This document records the CG-18 maturity contract for reproducible local, CI, container, optional
Foundry, and optional benchmark-extra harnesses.

The harness is a release-readiness and comparison surface. It does not publish packages, deploy
services, invoke Foundry, run external engines, materialize comparison datasets, or execute fallback
work.

## Harness environments

`UniversalHarnessReport` now declares these required environment rows:

- `local`: package import smoke plus CLI binary resolution and JSON status output.
- `ci`: workspace checks plus Python-wrapper smoke in the CI environment.
- `container`: container smoke contract for `shardloom --version` and JSON status output.
- `foundry_optional`: optional Foundry transform smoke using the Conda package path and certificate
  output.
- `benchmark_extras_optional`: optional local benchmark-extra environment isolated from the core
  runtime dependency graph.

Each environment row requires an environment file or equivalent contract, clean import evidence, CLI
binary resolution evidence, typed output-envelope fixtures, and artifact roots. The report keeps
`harness_execution_performed=false`, `external_engine_invoked=false`, and `fallback_attempted=false`
until those harnesses are explicitly run and certified.

## Baseline environments

Optional baseline environments are comparison-only:

- Spark
- DataFusion
- Polars
- DuckDB
- Dask
- pandas

Baseline engines may be installed in isolated benchmark environments, but they must not become
ShardLoom runtime dependencies and must not execute unsupported ShardLoom work as fallback.

## CLI surface

`universal-harness-plan --format json` exposes:

- `universal_harness_execution_gate_status`
- `universal_harness_execution_allowed=false`
- `universal_harness_execution_attempted=false`
- `universal_harness_required_evidence_refs`
- `universal_harness_attached_evidence_refs`
- `universal_harness_missing_evidence_refs`
- `harness_environment_count`
- `harness_environment_kind_order`
- `local_harness_required`
- `ci_harness_required`
- `container_harness_required`
- `foundry_optional_harness_required`
- `optional_benchmark_environment_required`
- `external_engines_as_runtime_dependencies_allowed=false`
- `baselines_comparison_only_runtime_dependency_free=true`

## Execution gate

`GAR-0030-A` adds an explicit execution-admission gate to the report. The gate status is
`blocked_missing_evidence` until the harness attaches capability evidence, execution certificate
evidence, Native I/O certificate evidence, policy/no-fallback evidence, output-envelope evidence,
output-artifact evidence, correctness evidence, and benchmark evidence.

The gate is intentionally separate from environment readiness. Local, CI, container, Foundry
optional, and benchmark-extra environment rows can be present while
`universal_harness_execution_allowed=false`. External baseline environments remain comparison-only
and cannot satisfy ShardLoom execution evidence or act as fallback.

## Release posture

This closes the CG-18 planning/maturity surface only. Real harness execution, package publication,
container publication, Foundry execution, benchmark execution, comparison dataset materialization,
and production release claims remain blocked until their evidence rows are populated.
