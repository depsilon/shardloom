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

## 2026-10-03 Unpatched HTTP Cache Advisory

PR #1506's website job failed on
[GHSA-ch52-4w7c-c8xp](https://github.com/advisories/GHSA-ch52-4w7c-c8xp).
The local audit reproduces five high-severity dependency-chain entries for this
single advisory. As of October 3, `http-cache-semantics` 4.2.0 was the registry's latest release;
Astro 7.3.5 also depends on it. The audit's proposed Astro 2.10.9 downgrade is not
a compatible remediation for this Astro 7 site. No dependency fix is claimed.

A local probe reproduces cached-response reuse with a client `max-stale` header
in the installed library. The installed Astro caller at
`dist/assets/build/remote.js` invokes only the constructor, `storable` and
`timeToLive`. Its initial-fetch and revalidation paths both complete with the
vulnerable request-evaluation methods replaced by throwing sentinels. The helper
generates conditional outbound headers; it does not accept public request headers.
The deployed application is static: `astro.config.mjs` selects static output and
both Wrangler configurations deploy the `website` assets directory without a
server entrypoint. This is application reachability counterevidence, not a claim
that the installed package is fixed or that every Astro deployment is unaffected.

The proposed exception in `website-src/dependency-audit-exception.json` was disabled
pending explicit maintainer approval. The proposal would expire at
2026-10-10 00:00 UTC and admits only this exact advisory and its transitive reports.
The complete lockfile, Astro helper, vulnerable library source, Astro config and
both deployment configs must match their reviewed SHA-256 fingerprints. A new
advisory, missing report/link/file, changed fingerprint or expiry fails the gate.
The command still runs `npm audit --audit-level=low --json`; accepted output says
`reviewed_exception` and `dependency_vulnerability_fixed: false`. It does not
report a clean dependency audit. Seven deterministic tests cover acceptance and
denial conditions. Reassess and remove the exception when an upstream remedy is
available; renewal requires a new explicit decision.

## 2026-10-04 Registry Update

The registry published `http-cache-semantics` 4.3.0 on October 4. Update only
that existing transitive lockfile entry; Astro's existing `^4.2.0` requirement
admits it. The package retains its BSD-2-Clause license and adds no dependency.
The registry integrity and
[upstream source revision](https://github.com/kornelski/http-cache-semantics/commit/b1d4bd682fbab0252985de45219f4e7497c0067c)
identify the selected release. No other package entry changes.

The exact revised dependency graph has a clean `npm audit` result. The unused
exception proposal, helper and helper tests are removed; CI, its command registry
and documentation again run the standard `npm audit --audit-level=low`. The update
does not establish that the earlier `max-stale` behavior changed: upstream
[disputed the report](https://github.com/kornelski/http-cache-semantics/issues/56#issuecomment-5975759591),
and the 4.3.0 source changes address Vary matching and expose response status.
Retain the static-deployment and build-helper reachability boundaries above;
do not describe this as a general shared-cache vulnerability fix.

Local validation passed for this lockfile update: clean dependency installation,
the unchanged dependency audit gate, website build and type/content checks,
public-status validation, all eight link regressions, website readiness and
static-asset validation. Hosted checks must pass for the updated commit before
merge; production deployment follows the existing Cloudflare integration.

## 2026-10-05 Build Parser Updates

PR #1523's website validation found three newly reported advisories. Update only
the affected existing transitive packages:

- `smol-toml` 1.8.0 → 1.9.0, the upstream fix for
  [GHSA-r4xh-jqrq-34v2](https://github.com/advisories/GHSA-r4xh-jqrq-34v2).
- `source-map-js` 1.2.1 → 1.2.2, the upstream fix for
  [GHSA-68fv-2mgg-jv7q](https://github.com/advisories/GHSA-68fv-2mgg-jv7q).
- `postcss-selector-parser` 6.1.4 → 7.1.6, the upstream fix for
  [GHSA-rj75-hqrm-r3gf](https://github.com/advisories/GHSA-rj75-hqrm-r3gf).

The first two updates satisfy their callers' existing version ranges. The CSS
parser needs an override scoped to `postcss-nested`, whose 6.2.0 release still
requests parser 6. The latest Expressive Code core, 0.44.2, retains that parent
range. Keep the installed Astro/Starlight family and `postcss-nested` unchanged;
the audit's proposed Starlight downgrade does not fit this site. Remove the
override when the parent dependency admits a patched parser itself.

The parser's [upstream changelog](https://github.com/postcss/postcss-selector-parser/blob/main/CHANGELOG.md)
identifies insertion-during-iteration behavior as the version 7 change. The
installed parent uses parsing, cloning, node replacement, iteration and
serialization; compatibility acceptance includes rebuilding the complete site
and comparing the generated CSS bytes. The parser remains MIT licensed; the
other two packages remain BSD-3-Clause licensed. No package or execution
dependency is added. These are build-time parser advisories, not evidence of a
vulnerable public request handler in the deployed static site.

The updated lockfile passes the unchanged audit with zero reported
vulnerabilities. Clean installation, type/content checks, the full site rebuild,
public status/readiness/static checks and all 166 focused release/link tests pass.
All nine generated CSS files retain identical paths and SHA-256 hashes; the
rebuild changes no checked-in website output. Hosted checks must pass for the
revised commit before merge.

## Runtime Boundary

- Astro, Starlight, MDX, sitemap, Pagefind, TypeScript, and related packages are website-only build
  dependencies.
- They are not ShardLoom runtime dependencies, query planners, execution providers, adapters,
  residual evaluators, or fallback engines.
- They introduce no Spark, DataFusion, DuckDB, Polars, pandas, Velox, Vortex query-engine
  integration, or other external execution fallback.
