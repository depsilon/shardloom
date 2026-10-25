# CI Work Shaping

## Purpose

`shardloom.ci_work_shaping_plan.v1` is the metadata-first CI planning surface. The Rust CLI
classifies changed paths and recommends work families and jobs. A conservative Python adapter
applies narrow selection only for verified change classes, and an always-running summary validates
the outcome of every workflow job.

Planning does not execute runtime work, run benchmarks, publish packages, create release tags,
upload artifacts to package channels, probe networks, or invoke external engines. Narrow validation
does not invent release evidence or authorize release claims.

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

## Planner and execution adapter

The Rust CLI emits the recommendation envelope:

```bash
cargo run -q -p shardloom-cli -- ci-work-shaping-plan \
  --mode pull_request \
  --changed-paths-file target/ci-work-shaping-changed-files.txt \
  --format json > target/ci-work-shaping-plan.json
```

The Python adapter validates the plan schema, PR mode, base and head revisions, workflow digest,
changed-path list, current job inventory, and planner family flags. It never removes a Rust
recommendation.

Narrow selection requires every path to be an existing regular file and match one of these cases:

- `README.md` or Markdown under `docs/getting-started/` with plain wording changes only; code,
  directives, front matter, inline code, and structured snippets must be unchanged.
- Website source under `website-src/src/components/`, `layouts/`, `pages/`, or `styles/`
  ending in `.astro`, `.css`, `.ts`, `.tsx`, or `.js`.

Added, deleted, renamed, mode-changed, symlinked, ambiguous, unknown, or unsupported paths require
broad validation. This includes fixtures, manifests, configuration, dependencies, CI, and
documentation outside the supported wording locations. Push/release mode, uncertain revisions,
invalid planner/schema/path evidence, unknown recommendations, and hard/release planner flags also
keep the full job list.

For eligible narrow PRs, these jobs always remain selected, in addition to all Rust recommendations:

```text
ci-work-shaping
ci-gate-matrix
rust-baseline
python-test-shards
python-tests
website-docs
release-readiness
```

That preserves default Rust formatting/lint/tests, Python shard and aggregate checks, website
validation, and the CI contract check. Omitted jobs carry a reason and must be reported as skipped.

## Always-running outcome summary

The always-running `release-readiness` job depends on the complete workflow job inventory. The
summary binds the plan to the current revision and workflow digest, checks selected/omitted
accounting, required dependencies and unconditional jobs, then verifies outcomes: selected jobs
must succeed and omitted jobs must be skipped. Missing, duplicate, invented, or unclassified jobs
fail validation.

```bash
python scripts/ci_execution_plan.py plan --plan target/ci-work-shaping-plan.json \
  --base "$CI_BASE_REVISION" --revision "$CI_HEAD_REVISION" \
  --mode "$SHARDLOOM_CI_WORK_SHAPING_MODE" --output target/ci-execution-plan.json
python scripts/ci_execution_plan.py summary \
  --plan target/downloads/ci-execution-plan/ci-execution-plan.json \
  --output target/ci-execution-summary.json
```

The `shardloom.ci_execution_summary.v1` evidence records revision, full-validation state,
selected jobs, omitted jobs and their reasons, and results. Release evidence is required for a full
plan; a narrow plan proves only its selected checks and outcome accounting. It does not fabricate
package, governance, native wheel, or release evidence omitted by the selection.

The Rust planner's job list is advisory; the verified execution summary establishes job outcome
coverage. Neither planning nor summary alone authorizes merge or publication.

## Capability families and benchmark policy

The planner classifies paths into:

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

Unknown paths classify as `other` and fail closed. Runtime, Python, fixture, benchmark harness,
release packaging, CI, and dependency/security changes recommend hard or release-proof work.
Benchmark rerun recommendations are source-aware: runtime and benchmark implementation changes
require reruns; artifact changes require artifact scans but do not prove a rerun; docs and website
copy do not recommend recomputation. Claim metadata checks remain applicable.

The pulseweave fingerprint is a non-cryptographic evidence reuse and drift key, not a security
mechanism. Its contract inputs include:

```text
.github/workflows/ci.yml
docs/release/ci-gate-matrix.md
Cargo.toml
python/pyproject.toml
website/assets/benchmarks/latest/manifest.json
```

## GitHub Actions evidence and local checks

The workflow fetches complete Git history, collects changed files without renames, and records
planner outputs and adapter selection evidence:

```text
fetch-depth: 0
Collect changed files
SHARDLOOM_CI_WORK_SHAPING_MODE=pull_request
SHARDLOOM_CI_WORK_SHAPING_MODE=merge
--changed-paths-file target/ci-work-shaping-changed-files.txt
git diff --no-renames --name-only
selected_jobs:
full_validation:
target/ci-work-shaping-plan.json
target/ci-work-shaping-changed-files.txt
target/ci-execution-plan.json
ci-work-shaping-evidence
retention-days: 14
```

The final summary is uploaded as `ci-execution-summary`. Narrow checks do not create benchmark
results, runtime output, release proof, or external-engine evidence.

Local focused checks are developer guidance only:

```bash
python3 scripts/run_focused_checks.py --list
python3 scripts/run_focused_checks.py --profile rust-cli-bin --filter route_infers_vortex_manifest_as_native_vortex_input
python3 scripts/run_focused_checks.py --profile rust-cli-test --target public_workflow_route --filter partitioned
python3 scripts/run_focused_checks.py --profile python-unittest --filter python.tests.test_query_builder.LazyWorkflowBuilderTests.test_context_sql_vortex_manifest_source_binds_native_vortex_collect
python3 scripts/run_focused_checks.py --profile current-native-vortex
```

The runner records `shardloom.focused_check_evidence.v1` with
`fallback_attempted=false` and `external_engine_invoked=false`. Rust unit-test filters target
the exact `--lib` or `--bin` surface; integration tests name an explicit `--test` target.
