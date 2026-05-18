# Adoption And Commercial-Readiness Friction Reduction

## Purpose

This document is the report-only architecture reference for `GAR-COMMERCIAL-1`. It turns
ShardLoom's existing public-preview, release dry-run, website/status, evidence, and Foundry proof
work into a practical adoption-readiness plan.

The goal is to reduce friction without overclaiming:

- one documented local install and smoke path
- package-channel readiness matrix
- buyer-facing compatibility and maturity status
- enterprise evidence export pack
- Foundry dev-stack starter kit
- workflow recipes library

This document does not authorize package publication, release tags, runtime expansion, external
service invocation, Foundry production support, performance claims, or fallback execution.

## External Channel Grounding

Package channels have different publication contracts. The ShardLoom release gate must treat each
channel as separately proven.

Reference docs:

- PyPI Trusted Publishers: `https://docs.pypi.org/trusted-publishers/`
- TestPyPI Trusted Publisher usage: `https://docs.pypi.org/trusted-publishers/using-a-publisher/`
- GitHub Releases: `https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository`
- GitHub Container Registry: `https://docs.github.com/packages/getting-started-with-github-container-registry/about-github-container-registry`
- Homebrew formula cookbook: `https://docs.brew.sh/Formula-Cookbook`
- Homebrew taps: `https://docs.brew.sh/Taps`
- Windows Package Manager manifests: `https://learn.microsoft.com/en-us/windows/package-manager/package/manifest`
- Windows Package Manager repository submission: `https://learn.microsoft.com/en-us/windows/package-manager/package/repository`
- Scoop buckets and manifests: `https://github.com/ScoopInstaller/Scoop/wiki/Buckets`
- conda-forge staged-recipes: `https://conda-forge.org/docs/maintainer/understanding_conda_forge/staged_recipes/`

## Current ShardLoom State

Current local proof exists, but public distribution is not complete:

- `scripts/release_dry_run_proof.py` builds local artifacts, installs the local wheel in a clean
  virtual environment, resolves the local CLI, runs smoke checks, runs scoped generated-source local
  output smokes, runs a tiny compatibility/prepared-Vortex benchmark smoke, runs release provenance
  dry-run evidence, and records no-publication safety fields.
- `docs/getting-started/first-10-minutes.md` describes a source-checkout path.
- `docs/release/package-name-readiness.md` tracks PyPI, TestPyPI, Conda, and crates.io readiness
  posture.
- `docs/release/package-channel-readiness-matrix.md` and
  `docs/release/package-channel-readiness-matrix.json` track channel-specific install, uninstall,
  clean-install, smoke, SBOM/checksum/provenance, rollback/yank, and authorization evidence.
- `docs/release/enterprise-evidence-export-pack.md` and
  `docs/release/enterprise-evidence-export-pack.json` define the opt-in local-first evidence export
  pack contract for ShardLoom JSON, OpenLineage facet payloads, OpenTelemetry span/metric payloads,
  optional Markdown summaries, and redaction reports.
- `website/status.html` is a public posture board with a generated buyer-facing "Can I use this?"
  matrix sourced from the universal compatibility scoreboard and package-channel readiness matrix.
- Real package publication, release tags, OCI pushes, Homebrew/Scoop/winget/conda-forge submission,
  crates.io publication, lineage/telemetry backend export, and managed observability integration
  remain blocked until release and opt-in evidence gates pass.

## One-Command Local Proof Target

The adoption path should become a single documented command that proves:

```text
install or local build
smoke check
tiny generated/source-free example
tiny prepared/native example
evidence inspection
claim boundary inspection
```

Current generated/source-free runtime is intentionally narrow rather than absent:
`ctx.from_rows(...).write(local_jsonl)` and `ctx.range(...).write(local_jsonl)` can run scoped local
fixture-smoke output paths with generated-source and output evidence. The one-command proof must run
those paths from the clean installed wheel and keep them separate from no-dataset smoke.

It must not pretend no-dataset smoke is generated-output execution, and it must not promote the
scoped local JSONL smokes into SQL `VALUES`, broad DataFrame runtime, object-store/lakehouse output,
Foundry output, production support, or performance claims.

## Package Channel Readiness Matrix

The source of truth is `docs/release/package-channel-readiness-matrix.json` with schema
`shardloom.package_channel_readiness_matrix.v1`. Validate it with:

```powershell
python scripts\check_package_channel_readiness.py
```

| Channel | Target | Current status | Required proof before ready |
| --- | --- | --- | --- |
| GitHub pre-release | Source archive plus built artifacts | `report-only` / blocked for public claim | Tag/release approval, checksums, SBOM, provenance, install/smoke transcript, rollback/delete policy. |
| TestPyPI | Python package `shardloom` | `blocked` | TestPyPI Trusted Publisher or scoped human credential proof, clean install, uninstall, smoke, no token committed. |
| PyPI | Python package `shardloom` | `blocked` | PyPI Trusted Publisher/OIDC, maintainer approval, clean install, uninstall, smoke, SBOM/checksum/provenance, yank policy. |
| Homebrew tap | CLI formula | `blocked` | Tap/formula proof, versioned artifact checksum, install/uninstall, smoke, rollback/deprecate policy. |
| Scoop | Windows CLI manifest | `blocked` | Bucket manifest, checksum, install/uninstall, smoke, update policy. |
| winget | Windows package manifest | `blocked` | winget manifest, repository submission validation, install/uninstall, smoke, update/rollback policy. |
| conda-forge | `shardloom-cli`, `shardloom-python`, `shardloom` | `blocked` | staged-recipes/feedstock proof, clean Conda install, smoke, no fallback dependencies, maintainer policy. |
| GHCR container | OCI image | `blocked` | image build, SBOM, provenance, digest pin, vulnerability scan, smoke, pull/run docs. |
| crates.io | future public Rust API crates only | `blocked` | extracted stable public crates, API stability gate, publish dry-run, no internal crate publication. |

No channel is ready until the specific channel has install, uninstall, clean-install, smoke,
provenance, and rollback/yank evidence.

Package access does not imply production readiness.
PyPI and TestPyPI require Trusted Publisher/OIDC posture for release-grade proof. Current internal
Rust crates remain unpublished; crates.io is limited to future stable public API crates after API
stability evidence exists.

## Compatibility And Buyer-Facing Status

Users should be able to answer "Can I use this for X?" without reading the architecture docs.

The public status surface should reuse `docs/architecture/universal-compatibility-coverage-scoreboard.md`
and show:

```text
supported
smoke-supported
report-only
blocked
planned
not planned
```

Unsupported paths must remain visible, including production SQL/DataFrame, object-store/lakehouse,
Foundry, external databases/warehouses, REST/Flight/ADBC, performance/superiority claims, and Spark
replacement claims.

The current public status board renders a first-class "Can I use this?" matrix with:

- status vocabulary for `runtime-supported`, `smoke-supported`, `report-only`, `blocked`,
  `planned`, and `not-planned`
- rows sourced from `docs/architecture/universal-compatibility-coverage-scoreboard.json`
- package-channel rows sourced from `docs/release/package-channel-readiness-matrix.json`
- report-only enterprise evidence export and Foundry dev-stack starter rows plus a planned workflow
  recipe row
- explicit not-planned rows for hidden fallback execution, Spark replacement claims, and production
  SQL/DataFrame/object-store/lakehouse/Foundry claims
- visible `fallback_attempted=false`, `external_engine_invoked=false`, and
  `public_package_release_claim_allowed=false` evidence where applicable

This matrix is a maturity map, not a runtime-support expansion.

## Enterprise Evidence Export Pack

The enterprise export pack should make ShardLoom-native evidence usable in common governance and
observability workflows without creating network side effects by default.

The source of truth is `docs/release/enterprise-evidence-export-pack.json` with schema
`shardloom.enterprise_evidence_export_pack.v1`. Validate it with:

```powershell
python scripts\check_enterprise_evidence_export_pack.py
```

Pack contents:

- ShardLoom JSON evidence bundle
- OpenLineage custom facets
- OpenTelemetry spans and selected metrics
- optional Markdown summary
- redaction report

Rules:

- Export is opt-in.
- No network calls by default.
- No backend integration is implied by docs alone.
- Secret, credential, local path, query text, schema name, and sample-value redaction policy must be
  explicit.
- Export does not upgrade runtime support or claim status.

Current report-only defaults:

```text
export_pack_runtime_supported=false
export_pack_enabled_by_default=false
opt_in_required=true
network_calls_by_default=false
backend_integration_configured=false
lineage_event_emitted=false
telemetry_trace_emitted=false
telemetry_metric_emitted=false
telemetry_log_emitted=false
claim_gate_status=not_claim_grade
fallback_attempted=false
external_engine_invoked=false
```

The planned local artifact layout is:

```text
target/enterprise-evidence-export-pack/<run-id>/
  manifest.json
  shardloom-evidence.json
  openlineage-facets.json
  opentelemetry-trace.json
  summary.md
  redaction-report.json
```

## Foundry Dev-Stack Starter

The Foundry starter kit is a personal dev-stack proof path, not production certification.

The source of truth is `docs/foundry/dev-stack-starter-kit.json` with schema
`shardloom.foundry_dev_stack_starter_kit.v1`. Validate it with:

```powershell
python scripts\check_foundry_dev_stack_starter.py
```

It should show:

- import package
- resolve CLI
- source-free generated-output posture
- staged input example
- evidence dataset output
- no-fallback/no-external-compute fields

Required fields:

```text
foundry_runtime_invoked=false
foundry_compute_invoked=false
foundry_spark_invoked=false
foundry_output_api_invoked=false
foundry_result_dataset_written=false
foundry_evidence_dataset_written=false
fallback_attempted=false
external_engine_invoked=false
```

Future real Foundry generated-output smoke must write through Foundry output APIs, not direct S3 or
object-store paths.

## Workflow Recipes Library

The workflow recipe library is a practical, copyable, claim-safe entry point for common local
smokes, prepared/native direction, generated-output smokes, messy-data fixtures, result-sink proof,
blocked object-store diagnostics, Foundry-style local proof, and benchmark interpretation.

The source of truth is `docs/use-cases/recipes/recipe-index.json` with schema
`shardloom.workflow_recipe_library.v1`. Validate it with:

```powershell
python scripts\check_workflow_recipes.py
```

Initial recipe families:

- generated reference table
- dirty CSV cleanup
- nested JSON extraction
- CDC overlay
- prepared Vortex query
- local result-sink replay
- unsupported diagnostic example
- object-store blocked example

Each recipe must include:

- user goal
- command or code
- expected output
- evidence fields
- claim boundary
- no-fallback/no-external-engine fields

Current recipe-library posture:

```text
status=report_only_documentation_surface
claim_gate_status=not_claim_grade
fallback_attempted=false
external_engine_invoked=false
```

Recipes are documentation/adoption surfaces. They do not add runtime support, publish packages,
rerun benchmarks, invoke object stores, invoke Foundry runtime, or create production/performance
claims.

## Acceptance

- Users can run the local smoke path without reading architecture docs.
- Public docs distinguish local proof from public package release.
- No package channel is marked ready without proof.
- Compatibility/status surfaces hide neither unsupported nor blocked paths.
- Enterprise export remains opt-in and no-network by default.
- Foundry starter docs make clear there is no production Foundry, Marketplace, or external compute
  claim.
- Recipes show real workflows and claim boundaries.

## Verification

Current report-only validation:

```powershell
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python scripts/check_website_readiness.py
git diff --check
```
