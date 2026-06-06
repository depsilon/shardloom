# Website Current-State Public Reference

Status: accepted current contract after the Astro/Starlight migration and PERF-INNOV-5 timing
cleanup.

## Summary

ShardLoom's website is a static interpretation layer over repository evidence. It is not a second
documentation system, a support matrix source of truth, a benchmark leaderboard, or a product claim
surface. The repository remains authoritative for implementation plans, RFCs, architecture
contracts, use-case records, release gates, and benchmark artifacts.

The current site source is `website-src/`. The generated deployable output is `website/`. The
canonical public benchmark artifact is `website-public/assets/benchmarks/latest/`; `website-src`
syncs that artifact into the build and the generated `website/` mirror.

## Current Public State

- Site runtime: static Astro with Starlight docs and local Pagefind search.
- Source of truth: repository docs and committed benchmark artifacts.
- Benchmark profile: promoted public artifact, not ad hoc local reruns.
- Current promoted benchmark SHA: the SHA recorded in
  `website-public/assets/benchmarks/latest/manifest.json`.
- Performance claim posture: `performance_claim_allowed=false`.
- Claim gate posture: benchmark evidence is `not_claim_grade` for public performance, production,
  and replacement claims unless the manifest says otherwise.
- PERF-INNOV-5 hot-runtime metadata rows and full-local publication-proof rows are represented in
  the promoted public benchmark bundle. Future scoped optimization artifacts remain phase-plan
  evidence until they are promoted through the benchmark publication gate.

## Public Routes

| Route | Job | Source |
| --- | --- | --- |
| `/` | First-viewport product identity, route/evidence posture, next action | `website-src/src/pages/index.astro`, benchmark manifest |
| `/about` | Short claim-safe overview of what ShardLoom is, is not, and where current evidence lives | `website-src/src/pages/about.astro`, README, compute-flow reference |
| `/start` | Local proof path without package or production claims | repo getting-started docs and local scripts |
| `/field-guide` | Dense ShardLoom vocabulary atlas | generated docs content and exact source references |
| `/use-cases` | "Can I use this?" support/browser surface | `docs/use-cases/use-case-index.yml` |
| `/benchmarks` | Route timing, timing surfaces, claim gates, and optimization direction | promoted benchmark artifact |
| `/architecture` | Human-readable route architecture | `docs/architecture/compute-engine-flow-reference.md` |
| `/status` | Capability/support posture | `docs/status/runs-today-support-matrix.json` |
| `/docs` | Curated source-doc entry point | repository docs |

Compatibility aliases such as `/about.html`, `/benchmarks.html`, `/architecture.html`, and `/compute-engine-flow`
may remain when static hosting or historical links need them, but they must render the same current
content or redirect to it.

## Benchmark Page Contract

The benchmark page must lead with current timing semantics:

1. Promoted artifact freshness and claim gate.
2. Hot runtime route surfaces.
3. Publication-proof route surfaces.
4. Optimization targets from route-share attribution.
5. Stage inclusion and attribution.
6. Collapsed raw ledgers.

Rules:

- `hot_runtime` rows drive the primary ShardLoom route grid.
- `publication_proof` rows remain visible and slower because they include proof/output work.
- Result sink replay and human evidence render must never silently redefine hot runtime totals.
- External engines are baseline context only, never ShardLoom fallback execution.
- The page may describe optimization direction from committed artifacts, but it must not make
  performance, production, Spark-replacement, or superiority claims while the manifest has
  `performance_claim_allowed=false`.

## Documentation Cleanup Contract

The current website architecture is owned by this file plus source code in `website-src/`. Historical
planning docs for the old website redesign should not stay in the active reference index. If a
historical path is still mentioned in the completed ledger, treat it as provenance, not current
guidance.

Current references:

- `website-src/package.json`
- `website-src/astro.config.mjs`
- `website-src/src/content.config.ts`
- `website-src/scripts/sync-content.mjs`
- `website-src/scripts/postbuild-static.mjs`
- `docs/legal/static-website-third-party-assets.md`
- `NOTICE`

## Non-Goals

- No package-publication claim.
- No production support claim.
- No Spark, DataFusion, DuckDB, Polars, Velox, or Vortex query-engine fallback.
- No external search SaaS.
- No runtime GitHub raw fetches.
- No public benchmark claim without a promoted claim-grade artifact.
- No duplicate active implementation queue outside `docs/architecture/phased-execution-plan.md`.

## Validation

Use bundled Node when `npm` is not available in the desktop environment:

```bash
cd website-src
/Users/dylan/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node scripts/sync-content.mjs
/Users/dylan/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node node_modules/.bin/astro check
/Users/dylan/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node node_modules/.bin/astro build
cd ..
/Users/dylan/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node website/validate_static_assets.js
python3 scripts/check_website_readiness.py
git diff --check
```

For visual QA, inspect `/`, `/about`, `/benchmarks`, `/architecture`, `/field-guide`, `/use-cases`,
`/status`, and `/docs` at desktop and mobile widths. Benchmark charts and tables must be readable without
hover, and no essential evidence may depend on client-side JavaScript.
