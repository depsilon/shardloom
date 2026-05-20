# ShardLoom Website

This directory contains the static Cloudflare Workers Static Assets site for ShardLoom.

The public site is intentionally small and light-mode:

- `/`: concise overview, current posture, benchmark link, compute-flow link, GitHub link.
- `/benchmarks`: committed benchmark artifact interpretation. Evidence, not a leaderboard.
- `/compute-engine-flow`: human-readable route translation of the canonical compute-flow reference.
- `/field-guide`, `/use-cases`, `/status`, `/readme`, and old `*.html` routes redirect to the
  smaller public surface or the GitHub repository.

Detailed docs, use cases, recipes, field-guide content, RFCs, and phase history remain in the
repository. They are not mirrored into the website.

## Build

Regenerate committed static pages locally:

```powershell
python website\build_static_pages.py
```

The generator:

- copies `docs/architecture/compute-engine-flow-reference.md` to
  `website/assets/data/compute-engine-flow-reference.md`;
- renders `index.html`, `benchmarks.html`, `compute-engine-flow.html`, `404.html`, and
  `sitemap.xml`;
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
- `benchmarks.html`: rendered committed benchmark evidence.
- `compute-engine-flow.html`: human-readable compute-flow route map.
- `404.html`: simplified 404 page.
- `assets/site.css`: shared light-mode visual system.
- `assets/logo/`: ShardLoom logo/favicon assets.
- `assets/data/compute-engine-flow-reference.md`: local static snapshot of canonical compute-flow
  docs.
- `assets/data/benchmark-evidence.json`: local static benchmark evidence snapshot.
- `assets/benchmarks/latest/`: authoritative committed benchmark publishing artifact.
- `validate_static_assets.js`: static asset and claim-safety validator.
- `build_static_pages.py`: local maintainer generator.
