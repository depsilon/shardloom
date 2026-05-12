# Correctness Differential Harness

This document defines the CG-5 aggregate correctness/differential harness surface.
It is report-only until ShardLoom has broader native execution paths and real
decoded-reference, property, fuzz, and external-oracle result artifacts.

## Purpose

`CorrectnessDifferentialHarnessReport` combines the existing CG-5 fixture
manifest, golden fixture coverage, edge-case inventory, external oracle policy,
unsupported-diagnostic expectations, and benchmark-claim blockers into one
machine-readable surface.

The report answers:

- which correctness surfaces exist now
- which surfaces are still evidence gaps
- which validation modes are missing
- which external engines are comparison-only oracles
- whether production or competitive benchmark claims are blocked
- whether the harness performed query execution, decoded-reference execution,
  external-engine execution, data reads, object-store IO, writes, or fallback

## Current Report Surfaces

- `fixture_manifest`
  - Tracks the declared correctness fixture inventory.
- `golden_fixtures`
  - Tracks checked-in source-backed fixture/reference output coverage.
- `source_backed_edge_fixtures`
  - Tracks checked-in edge fixture source manifests without reading or
    executing those sources.
- `decoded_reference_outputs`
  - Required before broad encoded execution can claim correctness parity.
- `differential_oracles`
  - Tracks external engines as test/comparison oracles only.
- `external_oracle_result_artifacts`
  - Tracks declared external-oracle result artifact slots for source-backed
    edge fixtures without invoking external engines.
- `semantic_edge_cases`
  - Tracks required null, nested, dictionary, sparse-validity, run-length,
    temporal, and unsupported-plan-shape fixture families.
- `unsupported_diagnostics`
  - Tracks diagnostic and unsupported-feature expectations.
- `property_fuzzing`
  - Required before broad optimizer/kernel claims.
- `benchmark_claim_gate`
  - Keeps competitive claims blocked until correctness evidence is complete.

## Acceptance Boundaries

- The aggregate report must not execute queries.
- The aggregate report must not execute decoded references.
- The aggregate report must not invoke Spark, DataFusion, DuckDB, Polars,
  pandas, Dask, Velox, or any other external engine.
- External engines are correctness or benchmark baselines only.
- The aggregate report must not read data, perform object-store IO, write
  outputs, probe providers, or create artifacts.
- `fallback_execution_allowed=false` and `fallback_attempted=false` are
  invariant fields.
- `production_claim_allowed=false` remains in force until decoded-reference,
  differential, property/fuzz, edge-case, and benchmark-gate evidence is
  complete.

## Current Evidence State

- Test-only decoded reference output artifacts are present for every current
  executable fixture family.
- Generated executable edge-case fixtures now cover empty input, single-row,
  all-null, mixed-null sparse validity, duplicate low-cardinality,
  high-cardinality, sorted dictionary, unsorted run-length, and temporal
  primitive cases with test-only decoded reference artifacts.
- The current executable edge-case fixtures are backed by the checked-in
  `docs/fixtures/correctness/source-backed-edge-fixtures.json` manifest.
- Future executable fixture families must add decoded reference output artifacts
  before they can support correctness, benchmark, or production claims.
- Generated property fixture families are present for encoded filter selection
  vectors, encoded projection ordering, and encoded filter-project composition.
- Reproducible fuzz seeds are present for the same primitive families, but fuzz
  execution remains deferred.
- Declared external-oracle result artifact slots are present for each current
  source-backed edge fixture across Spark, DataFusion, DuckDB, Polars, pandas,
  Dask, and Velox comparison oracles.
- External-oracle artifact slots are `declared_not_executed`, comparison-only,
  and record `external_engine_invoked=false` and `fallback_attempted=false`.
- Several fixture families still have `NotYetDefined` expected outcomes.
- Current external oracle coverage is policy-only; no external engines are
  invoked by the harness.

## Next Implementation Direction

The next CG-5 work should populate and/or execute the declared
external-oracle artifacts, add property/fuzz execution, and resolve remaining
`NotYetDefined` fixture families. Those fixtures and runs should come before
new benchmark claims and should explicitly record no-fallback boundaries,
semantic profile, materialization boundaries, and reproducible inputs.
