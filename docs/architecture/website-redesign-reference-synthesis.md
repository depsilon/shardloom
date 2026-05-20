# Website Redesign Reference Synthesis

Status: accepted direction for `GAR-WEB-REDESIGN-2A`.

## Decision

ShardLoom's public website should move beyond the minimal reset into a cohesive light-mode
technical product/docs surface:

```text
command-center homepage
-> dense technical Field Guide
-> use-case/status browser
-> benchmark evidence dashboard
-> compute-route architecture map
-> GitHub/repo docs as source of truth
```

The public experience should communicate one core idea:

```text
ShardLoom is the command center for evidence-gated compute.
```

This direction intentionally supersedes the prior minimal public-surface constraint where it
conflicts with the richer website rebuild request. The minimal reset remains useful as a claim-safety
baseline, not as the final information architecture.

## Technique Transfer Sources

These sources are structural references only. Do not copy their code, text, CSS, assets, animations,
layout code, typography, brand identity, or trade dress.

| Reference | Technique to transfer | ShardLoom translation |
| --- | --- | --- |
| `https://modal.com/gpu-glossary` | Dense technical atlas with categories and atomic terms | Field Guide category table of contents and compact route/evidence entries |
| `https://linear.app/` | Product-world storytelling and polished system narrative | Homepage route console showing source, ingest, prepared state, execution, output, evidence |
| `https://vercel.com/` | Clear platform lanes and confident CTAs | Start locally, prepare Vortex once, run prepared/native workflows, inspect evidence |
| `https://docs.stripe.com/` | Use-case-first developer documentation | Start/use-case pages organized by user jobs, not internal phase-plan names |
| `https://resend.com/` | Code-first developer proof | Supported Python/CLI snippets and concise evidence output examples above the fold |
| `https://supabase.com/` | Broad capability map with instant-start confidence | Capability/status grid with explicit runtime/smoke/report-only/blocked labels |
| `https://www.cursor.com/` | Immersive product-console style hero | Evidence Console hero based on real ShardLoom fields, not a generic dashboard |
| `https://mintlify.com/` | Docs as a knowledge product for humans and agents | Source-linked Field Guide and use-case pages with machine-readable status metadata |
| `https://starlight.astro.build/` | Polished docs infrastructure | Framework option if current generator becomes a bottleneck |
| `https://pagefind.app/` | Static search without backend infrastructure | Search option for Field Guide / Use Cases / Status if the atlas returns |

## Product Posture

Primary audience:

- technical evaluators deciding whether ShardLoom can handle a workflow;
- local users trying the Python/CLI surface;
- reviewers comparing benchmark evidence and route timing;
- agents or contributors that need deterministic status and reference links.

First five-second outcome:

```text
ShardLoom prepares admitted inputs into Vortex-backed state, runs no-fallback compute routes,
writes outputs, and emits evidence that states what happened and what is not claimed.
```

The site should feel:

- precise;
- calm;
- technical;
- human-readable;
- claim-safe;
- confident without being promotional.

## Visual Direction

The user explicitly prefers light mode, and the transparent ShardLoom logo reads best on light
surfaces. Use the committed transparent logo assets under `website/assets/logo/` as primary brand
signals.

Recommended style:

- light technical base with white and near-white surfaces;
- graphite text and borders;
- cyan/teal accents for active route and Vortex preparation;
- violet accent used sparingly for ShardLoom brand continuity;
- amber for warnings/report-only states;
- green for supported/certified states;
- rose/red for blocked/unsafe states;
- precise tables, route rails, evidence chips, and diagrammatic flows;
- restrained shadows and borders;
- compact headings and readable prose.

Avoid:

- dark-only design;
- excessive neon/cyberpunk styling;
- decorative orbs/blobs;
- nested card walls;
- raw benchmark tables above the fold;
- third-party visual identity copying;
- Fallout/Bethesda/Vault-Tec/Pip-Boy trade dress;
- Modal/Linear/Vercel/Stripe/Supabase/Resend/Cursor trade dress.

## Canonical Route Story

Every page should reinforce this model:

```text
Access Surface
-> UniversalIngress
-> SourceState
-> vortex_ingest
-> VortexPreparedState
-> ExecutionPlan
-> OutputPlan
-> SinkArtifact
-> Evidence
-> ClaimGate
```

Required wording:

- `prepared_vortex` executes from `VortexPreparedState`.
- Non-Vortex input reaches `prepared_vortex` only through `vortex_ingest`.
- `compatibility_import_certified` is a certified cold ingest/stage route.
- `native_vortex` starts from existing Vortex input.
- `direct_compatibility_transient` is a direct one-shot route and not Vortex-native.
- External engines are baselines only, never ShardLoom fallback execution.

## Homepage Intent

The homepage should not be a generic OSS landing page. It should be a product-console entry point:

- headline: `Evidence-gated compute over Vortex-prepared data.`
- route/evidence console hero;
- status chips: technical preview, no fallback, Vortex-first, claim-gated, local-first, not
  production claim;
- CTAs: Start local proof, Read Field Guide, View benchmark evidence;
- supported code example;
- compact evidence output block;
- mini "Can I use it?" status preview;
- benchmark-evidence-not-leaderboard framing.

## Quality Bar

Performance and accessibility targets:

- Core Web Vitals targets: LCP <= 2.5s, INP <= 200ms, CLS <= 0.1.
- WCAG 2.2 AA target.
- Keyboard navigable.
- Visible focus states.
- Reduced-motion support.
- No hover-only information.
- Status colors must include text labels; never rely on color alone.
- Mobile and desktop must both be first-class.

## Claim Boundary

The redesigned website must not imply:

- production readiness;
- package publication readiness;
- performance/superiority;
- Spark replacement;
- production SQL/DataFrame support;
- production object-store/lakehouse/Foundry/distributed support;
- hidden fast mode;
- external-engine fallback.

Benchmark pages remain local evidence dashboards, not leaderboards. Field Guide and Status pages
must keep blocked and report-only rows visible.

## First Implementation Recommendation

Complete the website in these slices:

1. Reference synthesis, information architecture, and content model.
2. Homepage product-console rebuild.
3. Field Guide / Use Case / Status atlas rebuild.
4. Benchmark evidence dashboard rebuild.
5. Performance/accessibility/claim-safety gate.
6. Framework migration decision only if the current generator becomes a proven bottleneck.

