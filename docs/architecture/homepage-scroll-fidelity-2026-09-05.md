# Homepage Scroll And Fidelity Review

Date: 2026-09-05. Scope: homepage assets, Astro integration, additive README differentiators,
and browser/static validation. These are website measurements, not ShardLoom engine benchmarks.
Changes are local and have not been deployed as part of this work.

## Reference And Reproduction

The supplied `ShardLoom_Parallax_Experience.html` is the visual reference. The deployed
`https://shardloom.io/assets/parallax-home.js` matched the repository's pre-change asset byte
for byte. The original and deployed renderer both had the same approximately 30 FPS paint cap;
the difference was not an Astro framework animation replacing the reference implementation.

The deployed Engine anchor landed 185 px from the viewport top beneath a 77 px header. Root
`scroll-padding-top:100px` and section `scroll-margin-top:85px` were additive. This left the
previous chapter visible and displaced the pinned scene's bottom controls out of view. A second
problem was the pinned scene's minimum height on short desktop windows.

## Corrections

- Preserve every original canvas geometry routine, seeded particle population, color, detail
  level, parallax multiplier, pointer response, and device-pixel-ratio limit. No animation library,
  framework island, remote asset, tracker, or new package dependency was added.
- Paint visible animated canvases on display-aligned animation frames instead of a 31 ms gate.
  Pause in hidden tabs, avoid continuous work without a visible scene, and allocate/resize canvas
  backing stores only when nearby. Recheck geometry when revisiting a previously resized scene.
- Batch DOM measurements before style writes; cache references and font strings. Idle animation
  reuses measured geometry. Update a dedicated header progress element rather than an inherited
  root custom property that invalidates styles throughout the document.
- Use one header-sized anchor offset. Desktop pinned Engine cancels that offset because its
  inner stage already reserves header space. Mobile, reduced-motion, and short unpinned layouts
  use the normal header offset. Native anchors, history, and smooth scrolling remain native.
- At short desktop heights, reduce the scene's visual allocation so the five labels and their
  explanatory text stay visible. At 520 px high or below, use a normal scrolling chapter with
  manual stage selection, matching the small-screen interaction model instead of clipping a
  fixed-height scene. Preserve desktop pinning above that limit.
- Keep focus out of the closed mobile menu and show the active navigation chapter.
- Remove homepage draft labels from visible copy, metadata, and the install-note state. Keep
  scope links and the distinction between illustrations and actual execution evidence.

## Additive Differentiators

README additions clarify explicit admitted/unsupported outcomes, reusable preparation versus
cached query answers, and declared timing boundaries. They do not claim that the unfinished
resident-runtime performance plan is complete or that the engine has general production support.

Two small Astro components extend existing editorial diagrams without changing section order:

- `RouteEvidence.astro`: admitted native work versus a diagnostic stop. The required false
  fallback/external-engine fields remain visibly labeled as illustrative, not live receipts.
- `TimingBoundaries.astro`: runtime, replay proof, and publication proof with cumulative
  inclusion indicators. No invented durations or benchmark comparisons appear in the diagram.

## Measured Browser Behavior

Headed Chrome on the same Mac, 1440 x 900 CSS pixels, DPR 1, normal motion, warmed page. Compare
the live site with the production static build served by `astro preview`, not the development
server. The idle sample wraps `clearRect` on the hero canvas and `getBoundingClientRect` for
1.4 seconds, counting actual paints against requestAnimationFrame callbacks. The scroll sample
uses native `scrollTo` with CSS smooth behavior temporarily disabled: one call per animation
frame for 3.5 seconds, `y = elapsedMilliseconds * 0.35`. Chrome CDP Performance metrics are
subtracted before/after this interval.

| Measurement | Deployed Before | Local Build After |
| --- | ---: | ---: |
| Idle hero paints / display frames | 43 / 86 | 86 / 86 |
| Idle DOM geometry reads | 516 | 0 |
| Scroll frame intervals observed | 210 | 211 |
| Scroll p95 frame interval | 17.4 ms | 16.8 ms |
| Scroll maximum frame interval | 17.7 ms | 17.6 ms |
| Style recalculations | 412 | 256 |
| Style recalculation duration | 267.879 ms | 11.436 ms |
| Layout count | 15 | 15 |
| Layout duration | 1.060 ms | 1.551 ms |
| Script duration | 401.611 ms | 495.763 ms |
| Total task duration | 821.139 ms | 601.944 ms |

Interpretation: idle animation now paints at display cadence, and scroll style work is much
lower. Script work increased with the fuller paint cadence; layout duration did not improve.
This single-machine diagnostic is not a universal FPS or battery-life guarantee. Neither version
showed large dropped frames in this particular scroll sample. High-refresh-rate devices and
real low-power phones remain field-validation targets.

The scheduling approach follows browser guidance on
[display-aligned requestAnimationFrame](https://developer.mozilla.org/en-US/docs/Web/API/Window/requestAnimationFrame)
and [batching DOM reads and writes](https://web.dev/articles/avoid-large-complex-layouts-and-layout-thrashing).

## Fidelity And Responsive Proof

At 1440 x 900, DPR 1, reduced motion enabled from navigation, scroll each scene into view and
compare SHA-256 hashes of `getImageData(...).data` with the original standalone HTML. All seven
were nonblank and byte-identical. These checks compare the canvas artwork, not the intentional
anchor, label, or new diagram changes around it.

| Scene | Canvas Pixels | Matching SHA-256 Prefix |
| --- | --- | --- |
| Orbital | 1181 x 815 | `d650331808cc1c98` |
| Work avoidance | 1290 x 300 | `5dc76306cc2633fe` |
| Vortex plates | 864 x 727 | `e3cb487f90095f92` |
| Capillary work | 1253 x 989 | `36e0ebaba77cdfc6` |
| PulseWeave | 735 x 315 | `abce0503ceb11bad` |
| Artifact | 610 x 540 | `705ce25fe52a3b75` |
| Horizon | 1440 x 759 | `41a3c1ea9def2a39` |

The executable browser regression entry is `scripts/check_parallax_browser.js`. Open the built
homepage in the existing Playwright CLI session, then run:

```bash
playwright-cli run-code --filename scripts/check_parallax_browser.js
```

Coverage includes all header anchors, deep links and browser back, 1920 x 1080, 1440 x 900,
1280 x 600, 768 x 600, 1024 x 500, 768 x 1024, 390 x 844, 360 x 740, and 320 x 700. Test all
five work-avoidance stages, all three pressure modes, all seven artifact layers, both route
outcomes, three timing boundaries, keyboard code tabs, motion-disabled interaction, mobile
menu focus, actual paint cadence, and absence of idle DOM measurements. The work-avoidance
labels and explanation must remain inside every tested pinned viewport at every stage.

Additional manual browser probes verified visible no-JavaScript mobile navigation, static code
and heading content, functioning native links, and no horizontal overflow in either new diagram
at 320 px. Screenshots were inspected for desktop opening/Engine/PulseWeave, both added diagrams,
mobile opening/PulseWeave/artifact/navigation, and the 768 x 600 Prune stage. QA images and JSON
reports are under the local non-iCloud `/Users/dylan/LocalData/shardloom/` directory.

## Gates And Limits

- `npm run build`: 45 static pages. Run independently of `npm run check`; both perform cleanup
  passes on the same generated output. An overlapping attempt hit a stale file-handle error in
  duplicate-artifact cleanup; the subsequent standalone build succeeded.
- `npm run check`: 20 files, zero errors, warnings, or hints.
- `npm audit --audit-level=low`: zero vulnerabilities.
- `python3 scripts/check_website_readiness.py`: passed, including interaction hooks and removal
  of obsolete homepage draft labels.
- `node website/validate_static_assets.js`: passed.
- `node --check website-public/assets/parallax-home.js` and `git diff --check`: passed.
- `python3 scripts/check_public_status_docs.py`: blocked by existing unclassified PERF-01 through
  PERF-13 entries in the separate, uncommitted runtime plan. Public claim language checks within
  that report pass. This homepage change does not classify or complete those runtime items.
- Chromium was exercised; Safari/WebKit and physical touch devices were not available in the
  installed Playwright browser set. No cross-browser performance claim is made.
- The live deployment injects a Cloudflare analytics beacon rejected by its existing CSP. That
  pre-existing console warning was not treated as the scroll cause; no CSP weakening or tracking
  configuration change was made.
- No Rust runtime behavior, package publication, deployment, PR, or merge was performed here.
