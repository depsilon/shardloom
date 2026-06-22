# Website Clean-Slate IA

Status: active for `WEB-CLEANSLATE-1`.

## Decision

ShardLoom's public website is a static product, docs, and evidence surface. It should not expose
internal audit matrices, phase-plan process pages, generated support atlases, or historical cleanup
scaffolding as first-class public routes.

The clean public route contract is:

| Route | Owner | Purpose |
| --- | --- | --- |
| `/` | Astro product surface | First-screen ShardLoom identity, route console, current evidence posture, and next actions. |
| `/about` | Astro product surface | Concise no-fallback, Vortex-first, pre-release overview. |
| `/start` | Astro product surface | Source-checkout local proof and current Python ETL scenario shape. |
| `/benchmarks` | Astro evidence surface | ClickBench handoff, local-evidence boundary, timing-surface vocabulary, and proof boundaries. |
| `/compute-engine-flow` | Astro architecture surface | Human-readable rendering of the canonical compute-flow reference. |
| `/field-guide/` | Starlight docs shell | Durable docs, vocabulary, quickstart, Python surface, benchmark methodology, and limitations. |

## Removed Public Surfaces

The following are no longer first-class public pages:

| Old route | New handling | Reason |
| --- | --- | --- |
| `/architecture` | Redirect to `/compute-engine-flow` | Duplicate rendering of the same compute-flow contract. |
| `/docs` | Redirect to `/field-guide/` | Starlight owns durable docs and search. |
| `/status` | Redirect to `/field-guide/limitations/` | Generated support matrices are repo evidence, not public product IA. |
| `/use-cases` and `/use-cases/*` | Redirect to `/field-guide/python-surface/` or repository use-case records | The generated atlas is noisy pre-release scaffolding. |
| `.html` route source files | Redirect where needed, otherwise generated only by static output mechanics | Avoid duplicate authored pages. |

## Kept Evidence

Keep these current assets visible:

- ShardLoom logos and favicon.
- ClickBench handoff and local benchmark/UAT evidence boundary.
- Python ETL scenario snippet using the primary ShardLoom route.
- No-fallback and Vortex-native route identity.
- Compute-flow diagram derived from `docs/architecture/compute-engine-flow-reference.md`.

## Non-Goals

- Do not publish package install claims.
- Do not claim production readiness, Spark displacement, or broad performance superiority.
- Do not expose the active phase plan as public product documentation.
- Do not preserve pages merely because they existed before this clean slate.

## Validation Contract

Website readiness and static asset validation must enforce the route contract above. Removed public
surfaces should fail validation if they remain generated as pages. Redirects are allowed when they
point to a current canonical route.
