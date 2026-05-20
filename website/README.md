# ShardLoom Website

This directory contains the static Cloudflare Workers Static Assets site for ShardLoom.

The current public site is light-mode and evidence-console oriented:

- `/`: route/evidence console overview, current posture, local start link, Field Guide link,
  benchmark link, and GitHub link.
- `/start`: first local proof entry point.
- `/field-guide`: compact technical vocabulary atlas with generated dossier pages.
- `/use-cases`: filterable use-case browser generated from `docs/use-cases/use-case-index.yml`.
- `/benchmarks`: committed benchmark artifact interpretation. Evidence, not a leaderboard.
- `/architecture` and `/compute-engine-flow`: human-readable route translation of the canonical
  compute-flow reference.
- `/status`: filterable support/status matrix with blocked and report-only rows visible.

Detailed RFCs, phase history, recipes, and source-of-truth docs remain in the repository. The
website translates the current route/status/evidence model for human readers; it does not replace
the repo docs or upgrade runtime claims.

## Build

Regenerate committed static pages locally:

```powershell
python website\build_static_pages.py
```

The generator:

- copies `docs/architecture/compute-engine-flow-reference.md` to
  `website/assets/data/compute-engine-flow-reference.md`;
- renders `index.html`, `start.html`, `field-guide.html`, `use-cases.html`, `benchmarks.html`,
  `architecture.html`, `compute-engine-flow.html`, `status.html`, `404.html`, and `sitemap.xml`;
- renders generated dossier/use-case detail pages under `field-guide/` and `use-cases/`;
- snapshots `docs/use-cases/use-case-index.yml` to `website/assets/data/use-case-index.json`;
- preserves the committed benchmark artifacts under `website/assets/benchmarks/latest/`;
- writes `_headers`, `_redirects`, and `robots.txt`.

Cloudflare serves committed files from `website/`; it does not run the generator during deployment.

## Validation

```powershell
python scripts\check_website_readiness.py
node website\validate_static_assets.js
git diff --check
```

Use the bundled Node runtime if system `node` is blocked in the local environment.

## Claim Rules

Website copy must preserve these boundaries:

- ShardLoom is pre-release.
- ShardLoom is Vortex-first and no-fallback by design.
- Benchmark evidence is not a public speed, superiority, or best-default claim.
- External engines are baseline context only.
- Do not claim Apache Spark replacement.
- Do not claim production SQL/DataFrame support.
- Do not claim production object-store, lakehouse, Foundry, distributed, or managed-platform
  support.
- Do not claim package-publication readiness.

## Files

- `index.html`: public overview.
- `start.html`: local proof entry point.
- `field-guide.html`: public technical atlas.
- `use-cases.html`: public use-case browser.
- `benchmarks.html`: rendered committed benchmark evidence.
- `architecture.html`: preferred architecture route map alias.
- `compute-engine-flow.html`: human-readable compute-flow route map.
- `status.html`: public support/status matrix.
- `404.html`: simplified 404 page.
- `assets/site.css`: shared light-mode visual system.
- `assets/site.js`: small static filter behavior for atlas/use-case/status pages.
- `assets/logo/`: ShardLoom logo/favicon assets.
- `assets/data/compute-engine-flow-reference.md`: local static snapshot of canonical compute-flow
  docs.
- `assets/data/benchmark-evidence.json`: local static benchmark evidence snapshot.
- `assets/data/use-case-index.json`: local static snapshot of the machine-readable use-case index.
- `assets/benchmarks/latest/`: authoritative committed benchmark publishing artifact.
- `validate_static_assets.js`: static asset and claim-safety validator.
- `build_static_pages.py`: local maintainer generator.
