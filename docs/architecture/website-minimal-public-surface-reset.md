# Website Minimal Public Surface Reset

## Design Thesis

ShardLoom's public website should be a simple interpretation layer, not a second documentation
system. The repo remains the source of truth for phase plans, RFCs, use cases, recipes, and support
matrices. The website should answer three questions quickly:

- What is ShardLoom?
- What does the current benchmark evidence show?
- How does work move through the compute engine?

## External Structure Reference

The structural reference is Modal's GPU Glossary at `https://modal.com/gpu-glossary`: a light,
concise, table-of-contents-led technical page that makes terms scannable before a user reads deeper
docs. The technique transfer is density, clarity, compact sections, and restrained styling. The
ShardLoom implementation must not copy Modal code, CSS, text, typography, layout code, or brand
trade dress.

## Public Site Scope

Keep:

- `/` as the main public overview.
- `/benchmarks` as the benchmark artifact interpretation page.
- `/compute-engine-flow` as the route/diagram translation page.
- GitHub as the primary source-docs destination.

Remove from the public site:

- generated Field Guide pages;
- generated Use Case Atlas pages;
- generated status board;
- generated README mirror;
- Pagefind search bundle;
- large multi-page atlas navigation.

The underlying repository docs remain intact.

## Information Architecture

```text
Home
  -> Benchmark evidence
  -> Compute flow
  -> GitHub repository

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
python website\build_static_pages.py
python scripts\check_website_readiness.py
node website\validate_static_assets.js
python -m compileall -q scripts website
git diff --check
```

For visual QA, serve `website/` locally and inspect `/`, `/benchmarks`, and
`/compute-engine-flow` at mobile and desktop widths.
