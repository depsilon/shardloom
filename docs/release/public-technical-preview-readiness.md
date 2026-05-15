<!-- SPDX-License-Identifier: Apache-2.0 -->

# Public Technical-Preview Readiness

Status: final public technical-preview readiness pass.

Date: 2026-05-15.

This pass checks whether the repository and website are safe to share publicly as a pre-release
technical preview. It does not authorize package publication, release tags, production support,
performance claims, platform claims, external execution, or fallback execution.

## Result

ShardLoom is positioned as a public technical preview when all validation commands in this document
pass. The public posture is:

- pre-release technical preview,
- Vortex-first and no-fallback,
- independent downstream workflow layer over Vortex-native data,
- evidence-certified local execution foundation,
- benchmark evidence and workflow attribution, not a speed leaderboard,
- no production platform, SQL/DataFrame, object-store/lakehouse, Foundry, package-publication,
  Spark-replacement, performance, or superiority claim.

## Scope Reviewed

- `README.md`
- `website/`
- `docs/getting-started/`
- `docs/benchmarks/`
- `docs/architecture/compute-engine-flow-reference.md`
- `docs/release/`
- `SECURITY.md`
- `CONTRIBUTING.md`
- `Cargo.toml`
- `python/pyproject.toml`

## Public Claim Boundaries

- README first screen states pre-release, Vortex-first, no-fallback, evidence-certified local
  execution, and no production/platform/performance/package claim.
- Website pages state technical-preview or pre-release posture and keep benchmark evidence separate
  from public rankings.
- Compatibility import rows are framed as certification/workflow lanes, not pure query speed.
- Prepared/native Vortex rows are framed as the current runtime-development lanes.
- Native batch runner evidence is scoped process/runtime structure evidence, not a hidden fast mode.
- External engines are baseline context only and never ShardLoom execution or fallback.
- Old local benchmark artifacts are presented only as contextual snapshots; they are not the latest
  performance proof or public ranking basis.

## Vortex And Foundry Boundaries

- ShardLoom is an independent downstream Vortex-first workflow layer.
- ShardLoom is not an official Vortex project, and no Vortex endorsement is implied.
- The website must not use Vortex logos unless explicit permission is recorded.
- Foundry remains a future validation target only.
- No Palantir endorsement, Foundry-native status, Foundry-certified status, or production Foundry
  support is claimed.

## Repo Discoverability

- README links to `https://shardloom.io`.
- README links to getting-started docs.
- README links to compute-flow and benchmark docs.
- Recommended GitHub topics are listed in `docs/release/github-topic-recommendations.md`.

## Security And Legal

- `SECURITY.md` exists.
- `LICENSE` and `NOTICE` exist at the repository root.
- The Python package also carries `python/LICENSE` and `python/NOTICE`.
- Public docs must not include private memo, trade-secret, patent-strategy, governed data, or
  employer-involvement content.
- Contributor wording requires right-to-submit and employer/client contribution clearance without
  implying employer sponsorship or involvement.

## Website Gate

- No runtime `raw.githubusercontent.com` fetches.
- All referenced local assets exist.
- Canonical URLs are extensionless and consistent.
- Sitemap includes the expected public pages.
- Open Graph metadata exists.
- Favicon remains in the global nav corner and full/trimmed ShardLoom logo assets are used for page
  headers and social images.

## Exact Edits Made In This Pass

- Tightened the README first-screen technical-preview posture.
- Added explicit independent downstream Vortex-first and no-endorsement wording.
- Clarified Foundry as a future validation target only.
- Updated Cargo and Python package descriptions to include pre-release/Vortex-first posture.
- Added public brand guidance for Vortex/Palantir/Foundry endorsement boundaries.
- Added the website footer Vortex independence boundary.
- Added this readiness report.

## Validation Commands

Run before sharing the public technical preview:

```powershell
python scripts/check_website_readiness.py
node website/validate_static_assets.js
python -m compileall -q python/src python/tests scripts examples benchmarks/traditional_analytics
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
git diff --check
```

## Current Validation Result

Validated on 2026-05-15:

| Command | Result |
| --- | --- |
| `python scripts/check_website_readiness.py` | passed |
| `node website/validate_static_assets.js` | passed |
| `python -m compileall -q python/src python/tests scripts examples benchmarks/traditional_analytics` | passed |
| `cargo fmt --all -- --check` | passed |
| `cargo clippy --workspace --all-targets -- -D warnings` | passed |
| `cargo test --workspace --all-targets` | passed |
| `git diff --check` | passed |

Cargo emitted known Windows incremental cleanup warnings while deleting stale target files, but the
Rust commands exited successfully.
