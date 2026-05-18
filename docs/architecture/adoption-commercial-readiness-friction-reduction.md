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
- `website/status.html` is a public posture board with a generated buyer-facing "Can I use this?"
  matrix sourced from the universal compatibility scoreboard and package-channel readiness matrix.
- Real package publication, release tags, OCI pushes, Homebrew/Scoop/winget/conda-forge submission,
  and crates.io publication remain blocked until release gates pass.

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
- planned adoption rows for enterprise evidence export, Foundry starter, and workflow recipes
- explicit not-planned rows for hidden fallback execution, Spark replacement claims, and production
  SQL/DataFrame/object-store/lakehouse/Foundry claims
- visible `fallback_attempted=false`, `external_engine_invoked=false`, and
  `public_package_release_claim_allowed=false` evidence where applicable

This matrix is a maturity map, not a runtime-support expansion.

## Enterprise Evidence Export Pack

The enterprise export pack should make ShardLoom-native evidence usable in common governance and
observability workflows without creating network side effects by default.

Pack contents:

- ShardLoom JSON evidence bundle
- OpenLineage custom facets
- OpenTelemetry spans and selected metrics
- optional Markdown summary

Rules:

- Export is opt-in.
- No network calls by default.
- No backend integration is implied by docs alone.
- Secret, credential, local path, query text, schema name, and sample-value redaction policy must be
  explicit.
- Export does not upgrade runtime support or claim status.

## Foundry Dev-Stack Starter

The Foundry starter kit is a personal dev-stack proof path, not production certification.

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
fallback_attempted=false
external_engine_invoked=false
```

Future real Foundry generated-output smoke must write through Foundry output APIs, not direct S3 or
object-store paths.

## Workflow Recipes Library

Recipes should be practical, copyable, and claim-safe.

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
