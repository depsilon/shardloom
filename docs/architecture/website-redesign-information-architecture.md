# Website Redesign Information Architecture

Status: accepted direction for `GAR-WEB-REDESIGN-2B`.

## Decision

The rebuilt public site should expose the concepts users need without requiring them to read the
phase plan, RFCs, or benchmark internals first.

Primary navigation:

```text
Home
Start
Field Guide
Use Cases
Benchmarks
Architecture
Status
Docs
GitHub
```

This is a public learning/navigation model. GitHub and repo docs remain the source of truth for deep
implementation detail.

## Route Map

| Public route | Primary job | Source data |
| --- | --- | --- |
| `/` | Understand ShardLoom, choose next action | route definitions, benchmark summary, claim boundary |
| `/start` | Complete a local proof path | README, getting-started docs, Python/CLI examples |
| `/field-guide` | Learn terms and route vocabulary | content-model terms and source-linked references |
| `/field-guide/<slug>` | Understand one concept | dossier template |
| `/use-cases` | Answer "Can I use ShardLoom for X?" | use-case index, status rows, reference files |
| `/use-cases/<id>` | Follow a recipe or blocker explanation | use-case page template |
| `/benchmarks` | Interpret evidence and route timing | committed benchmark artifact manifest/results |
| `/architecture` | See the compute route map | compute-flow reference and route vocabulary |
| `/status` | Filter capability/support state | capability/status matrix |
| `/docs` | Find README and deeper docs | curated repo links |

Compatibility aliases:

- `/compute-engine-flow` should redirect to or render the same content as `/architecture` unless a
  separate compatibility page is intentionally kept.
- `/benchmarks/index.html` and other directory indexes should continue working under static hosting.
- Retired URLs should redirect, not 404, when there is an obvious replacement.

## Homepage Structure

The homepage should be compact but product-grade:

1. Header with logo, nav, GitHub link.
2. Hero route/evidence console.
3. "Why ShardLoom exists" three-column explanation.
4. Execution route cards:
   - Certified import/stage route.
   - Vortex ingest / prepare once route.
   - Prepared Vortex route.
   - Native Vortex route.
   - Source-free generated route.
   - Direct one-shot route.
5. Supported code example.
6. Evidence output preview.
7. Mini support/status matrix.
8. Benchmark-evidence preview.
9. Footer with claim boundary and repo links.

The hero should show the actual path:

```text
Source -> UniversalIngress -> vortex_ingest -> VortexPreparedState -> Execution
-> OutputPlan -> Evidence -> ClaimGate
```

## Field Guide Structure

`/field-guide` should be the dense technical atlas:

- search box;
- reading paths;
- category table of contents;
- compact term rows;
- status chips;
- route/evidence metadata;
- links to use cases and reference files.

Required categories:

- Start Here
- Execution Routes
- UniversalIngress
- Vortex Ingest
- Prepared/Native Vortex
- Evidence + Certificates
- Benchmarks
- I/O + Outputs
- Scale + Resource Envelope
- Platform Boundaries
- Unsupported Diagnostics

Dossier pages use the same structure:

- Plain-English meaning.
- Why it matters.
- How ShardLoom uses it.
- Current support.
- Evidence fields.
- What it does not claim.
- Try it / related use cases.
- Related concepts.
- Reference files.

## Use Case Structure

`/use-cases` should be a workflow browser, not an internal architecture index.

Filters:

- status;
- input type;
- output type;
- execution route;
- evidence level;
- platform.

Every use-case card must show:

- user goal;
- status;
- execution route;
- input/output;
- quick command/code or blocker explanation;
- expected evidence fields;
- claim boundary;
- reference files.

Blocked and report-only states must remain visible.

## Benchmark Structure

`/benchmarks` should lead with interpretation, then raw evidence.

Top order:

1. What this benchmark is.
2. What this benchmark is not.
3. Route timing cards.
4. Claim-gate summary.
5. Competitor lane completeness.
6. Scenario/format coverage.
7. Collapsed raw timing tables.
8. Artifact manifest.

Route cards:

- Certified cold ingest/stage route.
- Prepared warm query route.
- Native Vortex route.
- Direct transient route.
- External baseline context.

Rules:

- Raw tables are collapsed by default.
- External baselines are labeled `external_baseline_only`.
- `compatibility_import_certified` cold timing is not pure query speed.
- `prepared_vortex` warm timing starts after `VortexPreparedState` exists unless the artifact says
  preparation is included.

## Architecture Structure

`/architecture` should translate the compute-flow doc into a readable route map:

```text
Access surface
-> Source route
-> Ingress route
-> Preparation route
-> Execution route
-> Output route
-> Evidence route
-> Claim gate
```

Mandatory callouts:

- `prepared_vortex` executes from `VortexPreparedState`.
- Non-Vortex input reaches `prepared_vortex` only through `vortex_ingest`.
- `compatibility_import_certified` is a certified cold ingest/stage route.
- External engines are baselines only.

## Status Structure

`/status` should answer "Can I use this?" without hiding unsupported paths.

Required visible rows:

- local CSV;
- local JSONL/NDJSON;
- local JSON;
- local Parquet;
- Arrow IPC;
- Avro;
- ORC;
- Vortex input;
- generated/source-free output;
- Python;
- SQL/DataFrame;
- S3/GCS/ADLS;
- Iceberg/Delta/Hudi;
- Foundry;
- benchmarks;
- package/release.

Every status row:

- capability;
- status;
- what works;
- what is blocked;
- evidence required;
- reference docs.

## Technology Direction

Default implementation: keep the current Python static generator for the next implementation slice.

Framework migration remains gated:

- create a decision doc first;
- preserve Cloudflare static asset deployment;
- preserve route/redirect behavior;
- preserve validation;
- avoid runtime GitHub raw fetches;
- avoid external search SaaS.

Astro/Starlight/Pagefind can be reconsidered after the content model proves the current generator is
too costly to maintain.

