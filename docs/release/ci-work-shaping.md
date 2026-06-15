# CI Work Shaping

## Purpose

`shardloom.ci_work_shaping_plan.v1` is the metadata-first CI planning surface for ShardLoom. It
maps changed files to capillary work families, records pulseweave-style evidence fingerprints, and
recommends which fast, hard, and release-proof lanes are relevant for the change.

This is a planning and evidence contract only. It does not execute runtime code, run benchmarks,
publish packages, create release tags, upload artifacts to package channels, probe networks, or
invoke fallback engines.

```text
runtime_execution=false
benchmark_run_performed=false
publication_attempted=false
tag_created=false
package_upload_attempted=false
fallback_execution_allowed=false
fallback_attempted=false
external_engine_invoked=false
side_effect_free=true
```

## Design

The planner is exposed through the Rust CLI:

```bash
cargo run -q -p shardloom-cli -- ci-work-shaping-plan \
  --mode pull_request \
  --changed-paths-file target/ci-work-shaping-changed-files.txt \
  --format json > target/ci-work-shaping-plan.json
```

The command is intentionally Rust-backed so path classification, CI source selection, and
machine-readable evidence can live next to the CLI and typed-envelope contracts instead of another
Python-only validator. It remains metadata-first: it reads the changed-path manifest and a small set
of contract files to compute a deterministic non-cryptographic fingerprint, then emits a typed
envelope with no execution side effects.

## Capillary Families

Changed paths are classified into these ordered work families:

```text
rust_runtime
rust_tests
python_surface
website_docs
benchmark_harness
benchmark_artifact
release_packaging
ci_workflow
dependency_security
docs_only
other
```

The family map is intentionally conservative. Runtime, Python surface, benchmark harness, release
packaging, CI workflow, and dependency/security changes escalate to hard-gate or release-proof
recommendations. Docs-only changes keep benchmark recomputation out of the recommended fast path
while still keeping no-fallback, unsupported-row, claim-grade, CI drift, and release-boundary
metadata gates always on.

Unknown paths classify as `other` and fail closed into the hard lane. They are not treated as
docs-only candidates, because repo helper scripts, generated manifests, or new source trees can be
merge-critical before the classifier knows their exact family.

## Lane Split

The CI architecture remains three-lane:

- `pr_fast_lane`: `ci-work-shaping`, `ci-gate-matrix`, focused Rust/Python/website/benchmark
  validators selected from the changed-file family map.
- `merge_hard_lane`: full Rust/Python/release evidence producers and aggregate readiness checks.
  When this lane is required, `recommended_job_order` includes the upstream producer jobs needed by
  readiness aggregators, including Python shard/package evidence, runtime core evidence, benchmark
  claim evidence, website evidence, package governance evidence, user-surface evidence, and final
  release readiness.
- `release_proof_lane`: dependency/security, package governance, release readiness, SBOM/checksum
  and publication-boundary proof. The planner recommends the producer jobs before the downstream
  governance/readiness jobs, but it does not authorize publication.

The fast lane never authorizes merge by itself:

```text
hard_gate_preserved=true
fast_lane_authorizes_merge=false
release_lane_authorizes_publication=false
```

## Source-Aware Benchmark Policy

Benchmark work is selected by source family:

- Runtime, Python surface, fixture, benchmark runner, route timing, or benchmark harness changes:
  `benchmark_rerun_required=true`.
- Published benchmark artifact changes:
  `benchmark_artifact_scan_required=true` and publication metadata gates run, but the planner does
  not declare that a rerun happened.
- Docs, README, and website copy changes:
  benchmark recomputation is not recommended; metadata and claim gates remain always on.

The benchmark policy preserves ShardLoom's claim discipline. A performance claim still requires
reproducible benchmark evidence, timing-surface labels, claim-grade rows, and no-fallback metadata.

## Pulseweave Evidence

The planner emits:

```text
pulseweave_incremental_evidence_status=enabled_with_content_fingerprint
pulseweave_cache_fingerprint_kind=fnv1a64_non_crypto_change_set_and_contract_inputs
pulseweave_cache_key=ci-work-shaping-...
```

The fingerprint is for CI evidence reuse and drift detection, not security. It includes the
changed-path list plus stable CI/release contract inputs:

```text
.github/workflows/ci.yml
docs/release/ci-gate-matrix.md
Cargo.toml
python/pyproject.toml
website/assets/benchmarks/latest/manifest.json
```

## GitHub Actions Contract

The `ci-work-shaping` job runs early, uploads `ci-work-shaping-evidence`, and produces:

```text
target/ci-work-shaping-plan.json
target/ci-work-shaping-changed-files.txt
```

`release-readiness` downloads that artifact with the rest of the release evidence bundle and
verifies `target/ci-work-shaping-plan.json` exists before final aggregate gates run.

## Acceptance

- The CI planner is generated by the Rust CLI and validated by CLI regression tests.
- Always-on metadata gates remain listed in every emitted plan.
- Benchmark rerun recommendations are source-aware and do not treat docs-only changes as runtime
  evidence changes.
- The hard gate and release proof gate remain preserved; no public package or performance claim is
  authorized by the planner.
