# ShardLoom Website Visual Identity System

Status: P8.7A completed visual-system planning reference.

This document defines the public website visual direction for ShardLoom. It is a design and
claim-safety reference only. It does not authorize runtime behavior, benchmark claims, package
publication, production support, external service execution, or fallback execution.

## Design Thesis

ShardLoom is the command deck for auditable compute.

The website should make the core contract understandable at a glance: ShardLoom does not hide
execution behind magic. It shows the route, mode, evidence, materialization boundary, and claim
boundary for a run.

The visual system should combine:

- Retro-future technical field guide structure.
- Spaceborne command deck information hierarchy.
- Cyberpunk telemetry console accents.
- Evidence dashboard framing instead of marketing-dashboard framing.

The result should feel serious, technical, slightly cinematic, and highly readable. It should be
memorable without becoming game cosplay or dark-mode eye strain.

## Visual Mood

Use this mood language for website implementation:

- Space and orbital command systems.
- Mission-control panels and signal routing.
- Neon telemetry and terminal evidence trails.
- Technical field manual chapters and dossiers.
- Artifact review console, not product-hype dashboard.

Avoid gimmicks that make the page harder to read. The visual identity should help users understand
the system state, not distract from it.

## Forbidden Copying

The identity must be original ShardLoom branding.

Do not copy or imitate protected trade dress, assets, fonts, layouts, characters, terms, or source
from:

- Fallout.
- Vault-Tec.
- Pip-Boy.
- Bethesda.
- Modal.
- Vortex.
- Palantir.
- Apache project branding.
- Any third-party product, site, game, or platform brand.

Allowed technique transfer:

- Field manual structure.
- Terminal cards.
- Mission-control dashboard layout logic.
- Status labels.
- Technical dossiers.
- Readable concept pages.
- Evidence dashboard organization.

Not allowed:

- Copied source, CSS, images, icons, typography, brand language, or visual trade dress.
- Terminology that could imply affiliation with another brand.
- Use of ShardLoom logo or icon in a misleading way.

## Design Tokens

Implementation should use stable tokens before one-off colors.

| Token | Suggested value | Meaning |
| --- | --- | --- |
| `--void` | `#050816` | Deepest page background. |
| `--deep-space` | `#071229` | Primary section background. |
| `--panel` | `#0b1020` | Main panel surface. |
| `--panel-soft` | `#10182d` | Secondary panel surface. |
| `--starlight` | `#eef8ff` | Primary text. |
| `--muted-starlight` | `#9fb2c7` | Secondary text. |
| `--plasma-cyan` | `#00d9ff` | Routing, links, mode labels. |
| `--ion-teal` | `#00ffc6` | Supported local evidence. |
| `--nebula-violet` | `#7c5cff` | Vortex-native and prepared Vortex accents. |
| `--warning-amber` | `#ffb84d` | Report-only, blocked, or caution status. |
| `--danger-rose` | `#ff5d73` | Forbidden or not-claimable status. |
| `--terminal-green` | `#8dff9a` | Certified or passed checks. |
| `--grid-line` | `rgba(0, 217, 255, 0.14)` | Technical grid and separators. |

Color semantics:

- Cyan: information, routing, execution modes.
- Teal: supported local evidence.
- Violet: Vortex-native and prepared Vortex paths.
- Amber: blocked, report-only, or caution.
- Rose: forbidden, not claimable, or unsafe.
- Green: certified, passed, or complete.
- Gray: unavailable, unsupported, or intentionally absent.

## Component Vocabulary

The website should converge on reusable components rather than page-specific decoration.

| Component | Purpose |
| --- | --- |
| `command-shell` | Global page shell with mission-control framing. |
| `mission-nav` | Header navigation for site sections. |
| `status-ribbon` | Compact current public posture and claim boundary line. |
| `terminal-panel` | Technical panel for command, policy, or evidence state. |
| `signal-card` | Small state card for mode, policy, evidence, or claim. |
| `telemetry-card` | Benchmark/evidence metric card that avoids leaderboard framing. |
| `evidence-chain` | Visual chain from request to mode to provider to result to claim gate. |
| `claim-badge` | Standard badge for claim states. |
| `mode-dossier` | Execution-mode explainer card. |
| `field-guide-card` | Concept summary card for the Field Guide. |
| `raw-data-drawer` | Collapsible raw evidence table or JSON detail. |
| `mission-log-entry` | Release, completed-ledger, or evidence-history row. |

Standard status labels:

- `supported`
- `fixture-smoke`
- `claim-grade`
- `not-claim-grade`
- `report-only`
- `blocked`
- `external-baseline-only`
- `unsupported`

## Page Mapping

Use clear public labels while allowing branded page framing.

| Public nav label | Branded framing | Primary job |
| --- | --- | --- |
| Home | Command Deck | Explain ShardLoom's value and current posture. |
| Field Guide | Technical Dossiers | Explain concepts and evidence vocabulary. |
| Benchmarks | Benchmark Evidence | Present local evidence without leaderboard claims. |
| Compute Flow | Mission Map | Show the execution route and claim gates. |
| Status | Launch/Readiness Board | List supported, report-only, blocked, and planned posture. |
| Docs | Archive / Source Docs | Link back to README and architecture references. |
| GitHub | Source Repository | Send engineers to the repo. |

## Homepage Direction

The homepage should feel like entering a command deck.

Preferred hero line:

```text
Auditable compute over Vortex-native data.
```

Preferred supporting copy should include:

- Pre-release status.
- No silent fallback.
- What executed.
- What materialized.
- What stayed native.
- Which claims are supported.

The homepage should expose a command console with:

- Mode.
- Policy.
- Evidence.
- Claim.

The mission map should read:

```text
request -> mode -> source/preparation -> provider -> result -> evidence -> claim gate
```

## Field Guide Direction

Field Guide pages should read like short technical dossiers.

Each concept page should include:

- What is X?
- One-sentence answer.
- Why it matters.
- How ShardLoom uses it.
- What it proves.
- What it does not prove.
- Evidence fields.
- Related concepts.
- Source docs.
- Claim boundary.

The structure may use field-manual technique, but it must not copy a specific game, product, or
third-party documentation brand.

## Benchmark Evidence Direction

The benchmark page must be framed as evidence, not a leaderboard.

Preferred page title:

```text
Benchmark Evidence, Not A Leaderboard
```

Preferred top cards:

- Local smoke evidence.
- Performance claim not allowed.
- `fallback_attempted=false`.
- External engines baseline-only.
- Prepared/native batch smoke.

Use safer labels:

- `Fastest rows` becomes `local fastest count`.
- `Relative bar` becomes `local timing context`.
- `Engine Timing Overview` becomes `Local Timing Context`.

Raw tables should appear after explanation and should be collapsible when large.

## Accessibility And Readability

Accessibility constraints are part of the identity.

- Maintain readable contrast for body text, small labels, links, and badges.
- Do not rely on tiny terminal-only text.
- Do not require animation to understand page state.
- Honor `prefers-reduced-motion`.
- Avoid excessive neon glow behind body text.
- Preserve keyboard navigation and visible focus states.
- Keep mobile layouts readable without horizontal scrolling.
- Use semantic HTML before decorative wrappers.
- Keep text containers wide enough for long technical terms such as
  `compatibility_import_certified`.

## Motion Rules

Motion should be subtle and nonessential:

- Slow starfield or grid drift is acceptable when it does not affect readability.
- Soft neon hover states are acceptable.
- Terminal cursor blink is acceptable only when reduced-motion is honored.
- Avoid heavy animation, parallax dependency, rapidly flashing states, or interaction traps.

## Claim Safety

Website copy must stay claim-safe:

- ShardLoom is pre-release.
- Benchmark evidence is not a leaderboard.
- No performance, superiority, or best-default claim.
- No Spark replacement claim.
- No production SQL or DataFrame claim.
- No production object-store, lakehouse, or Foundry claim.
- No package-publication claim unless release gates explicitly allow it.
- External engines are baselines or oracles only.
- `fallback_attempted=false` and `external_engine_invoked=false` semantics stay visible.

Use these phrases when evidence is incomplete:

```text
claim_gate_status=not_claim_grade
support_status=report_only
support_status=unsupported
```

## Implementation Guardrails

Future website implementation slices must not:

- Change ShardLoom runtime behavior.
- Change benchmark results.
- Add package publication state.
- Add external JavaScript frameworks without explicit approval.
- Add runtime GitHub fetches.
- Add secrets or remote service dependencies.
- Weaken the no-fallback policy.
- Add unsupported production claims.

Future implementation slices should:

- Keep URLs stable or add redirects.
- Validate local assets.
- Preserve canonical links.
- Keep OG metadata and favicon assets current.
- Run website readiness checks once P8.7I exists.

## Checklists

Brand safety checklist:

- [x] Original ShardLoom identity.
- [x] No Fallout, Vault-Tec, Pip-Boy, Bethesda, Modal, Vortex, Palantir, or Apache copying.
- [x] Technique transfer is limited to information architecture patterns.
- [x] ShardLoom logo and name stay governed by `website/BRAND.md`.

Claim-safety checklist:

- [x] No performance or superiority claim.
- [x] No Spark replacement claim.
- [x] No production SQL/DataFrame claim.
- [x] No production object-store/lakehouse/Foundry claim.
- [x] No package-publication claim.
- [x] Benchmark posture remains evidence-only.

Accessibility checklist:

- [x] Readability is a design requirement, not an afterthought.
- [x] Motion is optional and reduced-motion aware.
- [x] Neon accents must not reduce contrast.
- [x] Mobile text wrapping must be preserved for long technical terms.
