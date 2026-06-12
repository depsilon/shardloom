# ShardLoom Website Public-Post Readiness

Status: P8.7I public-post readiness gate.

This document is the go/no-go checklist for sharing `shardloom.io` publicly. It is a website
quality and claim-safety gate only. It does not authorize package publication, runtime expansion,
performance claims, production support, external-service execution, or fallback execution.

## Readiness Command

Run the local gate before a public post or site-changing PR:

```powershell
python scripts/check_website_readiness.py
```

The command writes:

```text
target/website-readiness-report.json
```

The report must have:

```text
status=passed
```

## Go Criteria

- All committed static pages exist:
  - `website/index.html`
  - `website/about/index.html`
  - `website/start/index.html`
  - `website/field-guide/index.html`
  - `website/benchmarks/index.html`
  - `website/compute-engine-flow/index.html`
  - `website/field-guide/*/index.html`
  - `website/404.html`
  - canonical `.html` compatibility pages for about, start, field-guide, benchmarks, and
    compute-engine-flow
  - redirects for retired architecture, docs, status, and use-case aliases
- Local logo/favicon assets exist and every page links the favicon.
- All referenced local website assets exist in `website/assets/`.
- Runtime website files do not fetch `raw.githubusercontent.com`.
- `website/assets/data/compute-engine-flow-reference.md` is committed as the local compute-flow
  snapshot.
- Canonical URLs and Open Graph metadata are present on public pages.
- `website/sitemap.xml` includes expected public pages.
- Benchmark copy frames evidence as attribution/workflow coverage, not a speed leaderboard.
- Claim boundaries remain visible:
  - no performance or superiority claim,
  - no Spark-displacement claim,
  - no production SQL/DataFrame claim,
  - no production object-store/lakehouse/Foundry claim,
  - no package-publication claim,
  - no hidden fallback claim.
- `fallback_attempted=false` and `external_engine_invoked=false` semantics remain visible where
  benchmark/evidence claims are discussed.
- If CSS uses animation or transitions, `prefers-reduced-motion` is present.

## No-Go Criteria

Do not publish or promote a site-changing PR if the readiness report finds:

- missing assets or broken local page references,
- runtime GitHub raw fetches,
- missing canonical or Open Graph metadata,
- missing sitemap entries,
- positive performance/superiority language,
- Spark-displacement or production-platform language,
- copied third-party brand/trade-dress references in website runtime files,
- package-publication language without the release gate allowing it,
- private memo/internal-only references.

## Manual Review

The script is a gate, not a substitute for visual review. Before a public post, rebuild the site
from `website-src/` with `npm run build` and `npm run check`, then also check:

- desktop and mobile screenshots for the homepage, About, Start, Benchmarks, Compute Flow, and
  Field Guide;
- first viewport clearly shows ShardLoom identity without duplicating the header logo treatment;
- Benchmarks reads as evidence, not a leaderboard;
- Compute Flow reads as a mission map before the detailed reference;
- the Field Guide limitations page clearly separates supported local smoke, fixture-smoke,
  report-only, blocked, planned, and not-claimed surfaces;
- keyboard focus remains visible and logical;
- copy remains readable at mobile width.

## Claim Boundary

Passing this gate means the website is public-post safe for the current pre-release posture.

It does not mean ShardLoom is release-launch safe, production ready, faster than another engine, a
Spark-displacement platform, a production SQL/DataFrame engine, a production object-store/lakehouse
engine, a Foundry production integration, or published as a package.
