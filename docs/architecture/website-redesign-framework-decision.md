# Website Redesign Framework Decision

Status: accepted for `GAR-WEB-REDESIGN-2G`.

Decision status: `report_only`.

Recommendation: keep the current Python static generator, committed static HTML/CSS/JS output, and
Cloudflare Workers Static Assets deployment. Do not migrate to Astro, Starlight, Pagefind, or any
other website framework/search bundle in this slice.

Migration status: `blocked_pending_explicit_approval`.

This decision does not add ShardLoom runtime behavior, website runtime services, npm dependencies,
benchmark data, package publication, performance claims, production claims, or fallback execution.

## Current Site Evidence

The rebuilt public website now has enough structure and validation to stay on the current stack:

- `website/build_static_pages.py` renders the public pages and generated detail pages.
- `wrangler.toml` serves `./website` as Cloudflare static assets.
- The generated site currently includes 67 committed HTML files:
  - public shell pages: `/`, `/start`, `/field-guide`, `/use-cases`, `/benchmarks`,
    `/architecture`, `/compute-engine-flow`, `/status`, and `/404`;
  - 32 generated Field Guide dossier pages;
  - 19 generated use-case detail pages;
  - extensionless and directory-index compatibility pages.
- `docs/use-cases/use-case-index.yml` remains the use-case source of truth.
- `website/assets/benchmarks/latest/manifest.json` remains the benchmark publishing source of
  truth.
- `scripts/check_website_readiness.py` now emits `shardloom.website_readiness.v3` and validates
  metadata, landmarks, navigation, labeled controls, image stability, status-chip text labels,
  runtime fetch boundaries, and claim-safety language.
- `website/validate_static_assets.js` validates the generated static artifact shape and public
  copy requirements.
- `shardloom-contract-tests/tests/release_readiness_metadata.rs` pins the public website posture.
- There is no current Pagefind runtime bundle under `website/pagefind/`.

## Decision

Keep the current generator for the next runtime work.

The site is now cohesive enough that a framework migration would mostly add process risk:

- new npm dependency and lockfile review;
- new Cloudflare build/output decisions;
- validator porting or wrapping;
- generated route parity risk;
- claim-safety regression risk;
- time diverted from the compute-engine runtime queue.

Astro custom remains the preferred future migration candidate if the generator becomes too
expensive to maintain. Starlight remains deferred because the current site is a custom product/docs
surface, not a standard docs portal.

## Options

### Option A: Keep Current Python Static Generator

What this means:

- keep `website/build_static_pages.py`;
- keep committed generated HTML under `website/`;
- keep vanilla CSS and the small static filter script;
- keep Cloudflare serving `./website`;
- keep readiness/static validators as the site safety gate.

Strengths:

- no new dependency footprint;
- deployment path is already proven;
- generated output is easy to inspect in PRs;
- ShardLoom-specific claim-safety checks already exist;
- the visual system remains custom and cohesive;
- runtime work can resume after this decision.

Weaknesses:

- templates are Python strings;
- authoring ergonomics are custom;
- schema validation is split across focused scripts rather than a single framework content model;
- a larger future knowledge base may eventually benefit from content collections or MDX.

Verdict: accepted current path.

### Option B: Astro Custom Site

What this would mean:

- add a Node/Astro build pipeline and lockfile;
- model route definitions, Field Guide terms, use cases, status rows, and benchmark summaries as
  content/data collections;
- preserve custom ShardLoom templates and CSS rather than adopting a generic docs theme;
- decide whether built output is committed or generated during deployment;
- port or wrap all current website validators.

Strengths:

- stronger content schema and authoring model;
- better component/template ergonomics;
- future MDX support if the Field Guide grows substantially.

Weaknesses:

- added dependency and supply-chain review;
- route/output parity migration risk;
- Cloudflare build/deploy decision required;
- no immediate user-visible improvement over the rebuilt static site.

Verdict: defer until migration triggers are met and explicit approval is granted.

### Option C: Astro Starlight

What this would mean:

- adopt a docs-first framework on top of Astro;
- rely on Starlight conventions for navigation, sidebars, docs typography, and search posture;
- customize heavily to preserve ShardLoom's product-console identity.

Strengths:

- strong docs defaults;
- familiar documentation contributor model.

Weaknesses:

- too likely to make the current product/benchmark/status site feel generic;
- still requires the same dependency, deployment, route parity, and validator work;
- less appropriate than custom Astro for the current command-center website.

Verdict: defer.

## Migration Revisit Triggers

Reopen the decision only when at least one trigger is true:

- generated HTML page count exceeds 150 and the Python templates become difficult to review;
- Field Guide or use-case authoring needs Markdown/MDX workflows beyond the current index files;
- content validation would materially improve by moving to framework-level schema collections;
- contributors need hot reload and component-level authoring to maintain public site quality;
- search needs exceed the current local filters and can be met without external search SaaS;
- dependency/license review and Cloudflare output strategy are prepared before implementation.

## Migration Blockers

No framework migration may begin until a later planned item explicitly resolves:

- dependency/license review for npm packages;
- lockfile and supply-chain policy;
- Cloudflare build command and output directory;
- whether generated output is committed or built by deployment;
- route and redirect parity;
- sitemap, canonical, Open Graph, and metadata parity;
- readiness/static validator parity;
- benchmark artifact ingestion parity;
- no runtime `raw.githubusercontent.com` fetches;
- no external search SaaS;
- no Pagefind/static-search bundle unless explicitly reapproved;
- no package-publication, performance, production, SQL/DataFrame, object-store/lakehouse, Foundry,
  Spark-displacement, or broader platform claim expansion.

## Rollback Path

Because this decision keeps the current stack, rollback is simply:

1. keep `wrangler.toml` pointing at `./website`;
2. regenerate with `python website/build_static_pages.py`;
3. validate with `python scripts/check_website_readiness.py` and `node website/validate_static_assets.js`;
4. do not add npm package files or framework output directories.

If a future framework migration is approved and fails, the rollback target is this current
Python-generated static output model.

## Claim Boundary

This is a website implementation decision only. It does not imply runtime support, benchmark
superiority, package publication, production readiness, Spark replacement, broad SQL/DataFrame
runtime, object-store/lakehouse runtime, Foundry production support, or external fallback execution.
