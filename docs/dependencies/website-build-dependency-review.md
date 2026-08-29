# Website Build Dependency Review

## Purpose

This document records dependency posture for the static Astro/Starlight website build. It is a
build-time dependency ledger only. It does not authorize package publication, benchmark
publication, runtime execution fallback, public performance claims, or production readiness.

## 2026-08-29 Website Audit Closeout

- Trigger: PR #1402 website/docs validation failed on `npm audit --audit-level=low` after the
  dependency-intake branch refreshed CI/security action pins.
- Decision: update the website Astro family together rather than forcing a partial transitive
  lockfile patch.
- Updated build dependency family:
  - `astro = ^7.2.9`.
  - `@astrojs/starlight = ^0.41.10`.
  - `@astrojs/mdx = ^7.0.8`.
  - `@astrojs/check = ^0.9.10`.
  - `@astrojs/sitemap = ^3.7.3`.
- Lockfile result:
  - `astro 7.2.9`.
  - `@astrojs/starlight 0.41.10`.
  - `@astrojs/mdx 7.0.8`.
  - `sharp 0.35.4`.
  - `fast-uri 3.1.6`.
  - `js-yaml 4.3.2`.
  - `nanoid 3.3.18`.
  - `postcss 8.5.26`.
  - `svgo 4.1.0`.
- Validation:
  - `npm --prefix website-src ci` passed.
  - `npm --prefix website-src audit --audit-level=low` passed with zero vulnerabilities.
  - `npm --prefix website-src run check` passed.
  - `npm --prefix website-src run build` passed and regenerated checked-in static website output.
  - `python3 scripts/check_public_status_docs.py` passed after the new dependency phase items were
    added to `docs/release/v1-inclusion-scope-matrix.md`.
  - `python3 scripts/check_website_readiness.py` passed.
  - `node website/validate_static_assets.js` passed.

## Runtime Boundary

- Astro, Starlight, MDX, sitemap, Pagefind, TypeScript, and related packages are website-only build
  dependencies.
- They are not ShardLoom runtime dependencies, query planners, execution providers, adapters,
  residual evaluators, or fallback engines.
- They introduce no Spark, DataFusion, DuckDB, Polars, pandas, Velox, Vortex query-engine
  integration, or other external execution fallback.
