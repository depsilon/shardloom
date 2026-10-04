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

## 2026-10-04 Static Website Cache Dependency

The registry published `http-cache-semantics` 4.3.0 on October 4. Update only
that transitive lockfile entry; Astro's existing `^4.2.0` requirement admits it.
The package retains its BSD-2-Clause license and adds no dependency. The registry
integrity and [upstream source revision](https://github.com/kornelski/http-cache-semantics/commit/b1d4bd682fbab0252985de45219f4e7497c0067c)
identify the selected release. No other package entry changes.

Local validation passed on this main-based change: dependency installation,
`npm audit --audit-level=low` with zero vulnerabilities, website build, Astro
checks with zero errors/warnings/hints, all eight link tests, public-status
validation, website readiness and static assets. The audit gate is unchanged
and no exception is introduced. Hosted checks and production deployment remain
required before publication is complete.
The clean audit is not evidence that the earlier reported `max-stale` behavior
changed: upstream [disputed that report](https://github.com/kornelski/http-cache-semantics/issues/56#issuecomment-5975759591),
and the 4.3.0 changes address Vary matching and expose response status. The site
uses Astro static output; this update remains a build-time dependency change.

## Runtime Boundary

- Astro, Starlight, MDX, sitemap, Pagefind, TypeScript, and related packages are website-only build
  dependencies.
- They are not ShardLoom runtime dependencies, query planners, execution providers, adapters,
  residual evaluators, or fallback engines.
- They introduce no Spark, DataFusion, DuckDB, Polars, pandas, Velox, Vortex query-engine
  integration, or other external execution fallback.
