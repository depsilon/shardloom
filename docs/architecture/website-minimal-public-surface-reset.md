# Website Minimal Public Surface Reset

Status: superseded by the compact Astro/Starlight website source in `website-src/`.

## Design Thesis

ShardLoom's public website should be a simple interpretation layer, not a second documentation
system. The repo remains the source of truth for phase plans, RFCs, use cases, recipes, and support
matrices. The website now renders a compact Starlight surface from repository source data, but the
same reset principle still applies: generated pages interpret current evidence and route posture;
they do not become independent support or release sources of truth. The website should answer these
questions quickly:

- What is ShardLoom?
- What can a reader run or inspect today?
- How does work move through the compute engine?
- What does the current benchmark evidence show?
- Which claims remain blocked?

## External Structure Reference

The structural reference is Modal's GPU Glossary at `https://modal.com/gpu-glossary`: a light,
concise, table-of-contents-led technical page that makes terms scannable before a user reads deeper
docs. The technique transfer is density, clarity, compact sections, and restrained styling. The
ShardLoom implementation must not copy Modal code, CSS, text, typography, layout code, or brand
trade dress.

## Public Site Scope

Keep:

- `/` as the main public overview.
- `/start` as the first local proof path.
- `/field-guide` as a compact vocabulary atlas.
- `/use-cases` as the generated "can I use this?" browser.
- `/benchmarks` as the benchmark artifact interpretation page.
- `/architecture` and `/docs` as shallow entry points back to source docs.
- `/compute-engine-flow` as the route/diagram translation page.
- `/status` as the generated support/posture matrix.
- GitHub as the primary source-docs destination.

Do not reintroduce:

- the old Python static-site generator;
- a generated README mirror;
- hand-edited generated site data;
- a sprawling docs duplicate that hides repo source docs;
- public pages that imply production support, performance claims, package publication, or fallback
  execution.

The underlying repository docs remain authoritative. `website-src/scripts/sync-content.mjs` copies
approved source docs, use-case/status rows, and committed benchmark artifacts into the Astro build.

## Information Architecture

```text
Home
  -> Start
  -> Field Guide
  -> Use Cases
  -> Compute flow
  -> Benchmark evidence
  -> Status
  -> GitHub repository

Start
  -> first-10-minutes local proof
  -> release dry-run path
  -> local Python smoke

Use Cases
  -> runnable local smokes
  -> deterministic blockers
  -> expected evidence fields

Benchmark evidence
  -> artifact lane availability
  -> claim-gate distribution
  -> local timing context
  -> scoped ShardLoom timing rows
  -> source-backed scan and encoded predicate evidence

Compute flow
  -> front door vs route
  -> UniversalIngress / SourceState / vortex_ingest
  -> VortexPreparedState / execution modes
  -> OutputPlan / evidence / claim gate
  -> raw Mermaid source as expandable reference

Status
  -> runtime-supported / smoke-supported / report-only / blocked / planned posture
  -> package-channel and compatibility boundaries
```

## Visual Direction

- Light mode first.
- Use the ShardLoom logo as the primary brand asset.
- Use restrained technical typography, compact headings, and visible whitespace.
- Avoid cyberpunk dashboard sprawl, nested cards, ornamental effects, and oversized page headers.
- Keep every page visually consistent: same header, same footer, same card/table/detail language.

## Claim Boundary

The site must not imply:

- performance or superiority;
- Apache Spark replacement;
- production SQL/DataFrame support;
- production object-store/lakehouse/Foundry/distributed support;
- package publication readiness;
- hidden external fallback.

Benchmarks remain local evidence and external engines remain baseline context only.

## Validation

Required local checks:

```powershell
Push-Location website-src
npm run build
npm run check
Pop-Location
python scripts\check_website_readiness.py
node website\validate_static_assets.js
python -m compileall -q scripts website-src
git diff --check
```

For visual QA, serve `website/` locally and inspect `/`, `/start`, `/use-cases`, `/benchmarks`,
`/compute-engine-flow`, and `/status` at mobile and desktop widths.
