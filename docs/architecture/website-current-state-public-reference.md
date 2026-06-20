# Website Current-State Public Reference

Status: accepted current contract after the `WEB-CLEANSLATE-1` public IA cleanup.

## Summary

ShardLoom's website is a static interpretation layer over repository evidence. It is not a second
documentation system, a support matrix source of truth, a benchmark leaderboard, or a product claim
surface. The repository remains authoritative for implementation plans, RFCs, architecture
contracts, use-case records, release gates, and benchmark artifacts.

The current site source is `website-src/`. The generated deployable output is `website/`. The
public benchmark page is a ClickBench handoff, not a local artifact renderer. Local benchmark
artifacts remain repository evidence and must not be mirrored into a shardloom.io leaderboard.

## Current Public State

- Site runtime: static Astro with Starlight docs and local Pagefind search.
- Source of truth: repository docs and local validation artifacts.
- Benchmark profile: ClickBench handoff for public comparison; local artifacts stay in the repo as
  engineering evidence.
- Current public benchmark artifact: none rendered on shardloom.io.
- Performance claim posture: `performance_claim_allowed=false`.
- Claim gate posture: benchmark evidence is `not_claim_grade` for public performance, production,
  and replacement claims unless a future approved public comparison artifact says otherwise.
- The old internal benchmark dashboard is retired from the website so stale local rows cannot read
  as a current public leaderboard.

## Public Routes

| Route | Job | Source |
| --- | --- | --- |
| `/` | First-viewport product identity, route/evidence posture, next action | `website-src/src/pages/index.astro`, benchmark manifest |
| `/about` | Short claim-safe overview of what ShardLoom is, is not, and where current evidence lives | `website-src/src/pages/about.astro`, README, compute-flow reference |
| `/start` | Local proof path without package or production claims | repo getting-started docs and local scripts |
| `/field-guide` | Starlight docs shell for start, Python surface, benchmark methodology, limitations, and vocabulary | generated docs content and exact source references |
| `/benchmarks` | ClickBench handoff and claim-safe public benchmark posture | `website-src/src/pages/benchmarks.astro` |
| `/compute-engine-flow` | Human-readable route architecture | `docs/architecture/compute-engine-flow-reference.md` |

Removed public routes are redirected intentionally: `/architecture` to `/compute-engine-flow`,
`/docs` to `/field-guide`, `/status` to `/field-guide/limitations`, and `/use-cases` to
`/field-guide/python-surface` or repository use-case records. The generated use-case and status
matrices remain repository evidence, not public website IA.

## Benchmark Page Contract

The benchmark page must lead with the public comparison handoff:

1. ClickBench is the public comparison surface.
2. ShardLoom does not host a local leaderboard on shardloom.io.
3. Local benchmark artifacts remain engineering evidence, not public ranking evidence.
4. Performance claims require named dataset, route, format, and evidence path.
5. External engines remain comparison baselines only.

Rules:

- The page must link to `https://benchmark.clickhouse.com/`.
- The page must not render the retired internal benchmark dashboard.
- Local benchmark rows must not be presented as a current public leaderboard.
- External engines are baseline context only, never ShardLoom fallback execution.
- The page may describe the comparison process, but it must not make performance, production,
  Spark-replacement, or superiority claims while `performance_claim_allowed=false`.

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
- `docs/architecture/website-clean-slate-ia.md`
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

For visual QA, inspect `/`, `/about`, `/start`, `/benchmarks`, `/compute-engine-flow`,
`/field-guide`, `/field-guide/python-surface`, and `/field-guide/limitations` at desktop and
mobile widths. Benchmark charts and tables must be readable without hover, and no essential
evidence may depend on client-side JavaScript.
