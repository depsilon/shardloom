<!-- SPDX-License-Identifier: Apache-2.0 -->

# Website Atlas Framework Decision

## Status

`GAR-WEB-ATLAS-1H`

Decision status: `report_only`

Recommendation: keep the current Python static generator, vanilla HTML/CSS/JS, committed Pagefind
bundle, and Cloudflare Workers Static Assets deployment for the next website slices.

Migration status: `blocked_pending_explicit_approval`

This decision does not add runtime behavior, dependencies, build tooling, package publication,
benchmark data, support claims, or fallback execution.

## Decision Summary

ShardLoom should not migrate the website to Astro or Starlight yet.

The current site is already functioning as a generated technical atlas:

- the Python generator builds the homepage, Field Guide, Use Case Atlas, benchmarks, compute-flow,
  status, and rendered README pages;
- Pagefind is committed as static assets and indexes the built HTML without server infrastructure;
- validators enforce local assets, no runtime GitHub raw fetches, claim-safe copy, sitemap/canonical
  metadata, generated use-case coverage, reverse Field Guide links, and source-linked citation
  blocks;
- the deployment path stays simple: Cloudflare serves the committed `website/` directory.

Astro and Starlight remain credible future options, but the next ShardLoom work should finish the
current atlas polish and public-readiness gates before adding a frontend build system.

## Current Site Evidence

Current generated-site shape as of this decision:

- static generator: `website/build_static_pages.py`
- content sources:
  - `website/content/field-guide-index.json`
  - `docs/use-cases/use-case-index.yml`
  - `docs/use-cases/recipes/recipe-index.json`
  - benchmark manifest and evidence snapshots under `website/assets/benchmarks/latest/`
- generated pages:
  - 99 HTML files discovered by Pagefind
  - 97 indexed pages
  - Field Guide entries exceed the current 50-entry gate
  - Use Case Atlas covers 16 capability families
- validation:
  - `scripts/check_use_case_index.py`
  - `scripts/check_use_case_coverage.py`
  - `scripts/check_use_case_backlinks.py`
  - `scripts/check_website_readiness.py`
  - `website/validate_static_assets.js`
  - `shardloom-contract-tests/tests/release_readiness_metadata.rs`
- deployment:
  - `wrangler.toml` serves `[assets] directory = "./website"`
  - no runtime `raw.githubusercontent.com` fetch is allowed
  - no external search service is used

## Sources Reviewed

| Source | What it contributes to this decision |
| --- | --- |
| `website/build_static_pages.py` | Current generator and static page model. |
| `website/README.md` | Current Cloudflare Workers Static Assets and Pagefind publishing workflow. |
| `docs/legal/static-website-third-party-assets.md` | Pagefind static-search license and non-runtime posture. |
| `scripts/check_website_readiness.py` | Existing public-readiness and claim-safety gate. |
| `website/validate_static_assets.js` | Existing local-asset and runtime-reference gate. |
| Astro content collections docs | Astro can validate content with loaders and Zod-backed schemas and build static output. |
| Starlight docs | Starlight provides docs-site navigation, search, SEO, dark mode, i18n, code highlighting, and customization hooks. |
| Pagefind docs | Pagefind indexes built static HTML and supports metadata/filter attributes without server infrastructure. |

External source references:

- `https://docs.astro.build/en/guides/content-collections/`
- `https://docs.astro.build/en/reference/cli-reference/#astro-build`
- `https://starlight.astro.build/`
- `https://starlight.astro.build/guides/site-search/`
- `https://pagefind.app/docs/`
- `https://pagefind.app/docs/running-pagefind/`

## Options

### Option A: Keep Current Python Static Generator

What this means:

- keep `website/build_static_pages.py`;
- keep generated committed HTML under `website/`;
- keep Pagefind as a generated static bundle under `website/pagefind/`;
- keep Cloudflare Workers Static Assets deployment pointed at `./website`;
- improve the existing generator and validators in the remaining atlas slices.

Strengths:

- zero new build-system dependency;
- same deployment model currently used by Cloudflare;
- strong control over ShardLoom's command-deck visual identity;
- validators already encode ShardLoom-specific claim-safety and no-fallback rules;
- generated pages are committed, easy to inspect, and do not require Cloudflare to install tools;
- Pagefind already provides static search with metadata/filter support.

Weaknesses:

- page templates live in Python strings;
- richer authoring may become tedious if the atlas grows substantially;
- schema validation is split across custom scripts instead of one content-collection model;
- contributor ergonomics are more custom than standard documentation frameworks.

Verdict:

Recommended for the next slices.

### Option B: Astro Custom Site

What this means:

- add a Node/Astro build pipeline;
- move Field Guide and Use Case Atlas content into Astro content collections or data collections;
- use Astro templates/components for pages while keeping ShardLoom's custom visual system;
- run Pagefind after `astro build`;
- deploy the generated `dist/` or copied static output through Cloudflare.

Strengths:

- Astro content collections can validate frontmatter/data with schemas;
- Astro can generate a static site and still support custom pages/components;
- custom Astro pages would preserve ShardLoom's command-deck identity better than a strict docs
  theme;
- better long-term authoring model if content becomes mostly Markdown/MDX.

Weaknesses:

- adds Node dependency management, lockfiles, dependency review, and build steps;
- requires reworking Cloudflare deployment or committing built output intentionally;
- requires porting current validators or wiring them into the build;
- introduces migration risk for no-runtime-fetch, claim-safety, and generated artifact checks.

Verdict:

Best future migration candidate if the current generator becomes a bottleneck.

### Option C: Astro Starlight

What this means:

- adopt Starlight as the docs framework on top of Astro;
- use Starlight navigation/sidebar/search/SEO/dark-mode defaults;
- customize visual identity and components where needed.

Strengths:

- strong documentation-site defaults;
- built-in or integrated search posture;
- navigation/sidebar/table-of-contents conventions are mature;
- good fit if ShardLoom becomes primarily a docs portal.

Weaknesses:

- likely fights the current custom command-deck / telemetry / status-board visual system more than
  custom Astro;
- Starlight defaults may make the site feel generic unless carefully customized;
- still adds the Node/Astro dependency and build pipeline;
- migration could delay runtime/engine work without directly improving current public safety.

Verdict:

Defer. Reconsider only if documentation conventions become more important than custom product-site
identity.

## Decision Criteria

| Criterion | Current generator | Astro custom | Astro Starlight |
| --- | --- | --- | --- |
| Current delivery risk | Lowest | Medium | Medium-high |
| New dependencies | None | Node/Astro | Node/Astro/Starlight |
| Content schema validation | Custom scripts | Strong with content collections | Strong with Astro/Starlight content model |
| ShardLoom visual identity | Strongest | Strong | Medium unless heavily customized |
| Search | Current Pagefind bundle already works | Pagefind after build | Starlight search/Pagefind posture available |
| Claim-safety gates | Already encoded | Must be ported or wrapped | Must be ported or wrapped |
| Cloudflare static deployment | Already working | Needs build/output decision | Needs build/output decision |
| Contributor familiarity | Custom but simple | Common web framework | Common docs framework |
| Migration cost | None | Medium | Medium-high |

## Recommendation

Keep the current generator through:

- `GAR-WEB-ATLAS-1I visual density and readability refinement`;
- `GAR-WEB-ATLAS-1J Field Guide / Use Case public-readiness gate`;
- near-term website/status/benchmark publishing work.

If a future migration is approved, prefer this order:

1. Astro custom site with content collections.
2. Keep Pagefind as the static search layer unless Starlight search is explicitly selected.
3. Consider Starlight only after proving that the site should behave more like a standard docs
   portal than a custom ShardLoom technical atlas.

## Migration Revisit Triggers

Reopen this decision only when at least one trigger is true:

- generated website page count exceeds 150 and Python templates become hard to maintain;
- Field Guide or Use Case content needs Markdown/MDX authoring beyond the current JSON/YAML model;
- content validation would be materially simpler with Astro content collections than with current
  scripts;
- contributors need local preview/hot-reload ergonomics that the current generator does not provide;
- Starlight's docs conventions become more valuable than the current custom command-deck identity;
- dependency, license, and Cloudflare build-output review has a committed implementation plan.

## Migration Blockers

No Astro/Starlight migration may begin until a later implementation slice explicitly resolves:

- dependency/license review for new npm packages;
- lockfile and supply-chain policy;
- Cloudflare deployment command and output directory;
- whether generated output is committed or built in deployment;
- Pagefind indexing strategy after build;
- validator parity with current ShardLoom-specific website gates;
- parity for current website validators;
- parity for source-linked citation blocks;
- parity for status/use-case filters;
- no runtime GitHub raw fetches;
- no package-publication, performance, production, SQL/DataFrame, object-store/lakehouse, Foundry,
  Spark-displacement, or broader platform claim.

## Claim Boundary

This decision is about website maintainability only.

It does not imply:

- production readiness;
- package publication;
- benchmark or performance claims;
- Spark replacement;
- SQL/DataFrame runtime support;
- object-store/lakehouse runtime;
- Foundry production support;
- external fallback execution.

All public pages must continue to preserve `fallback_attempted=false`,
`external_engine_invoked=false`, blocked/report-only visibility, and workload-scoped claim gates.

## Follow-Up

The next website work should stay on the current stack and complete:

- `GAR-WEB-ATLAS-1I` for visual density/readability;
- `GAR-WEB-ATLAS-1J` for atlas-specific public-readiness gates.
