<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite whole-file refresh reuse — October 20, 2026

Status: the unconditional whole-file refresh adapter is **rejected**. All 84
workflows pass exactness, activation and ownership checks. Both frozen cohorts
fail the initial-preparation control in every case, the complete-workflow
control in all three controls, and the downstream engine control in all three
reuse targets. Moderate reuse also misses the complete-workflow benefit gate
in both. The experimental adapter is removed; public changed-generation
rejection remains intact.

This completes the finite row-13 investigation under `PERF-02.changed_source`.
It does not close the broader owner, establish safe replacement, or rule out
other admission policies or reuse mechanisms. The
[conditional sequence](native-conditional-investigations-2026-10-19.md) continues
with multiway joins. Eligible pre-writer idle attribution remains a prerequisite
to revisit before the 37 required substeps resume. RFC 0044 and CG-1 through
CG-23 retain their separate obligations.

## Mechanism and prerequisites

Classification: `use_vortex_native_provider`. Pinned Vortex 0.85.0 provides the
ordered array scan and native layout writer. The test-only adapter authenticates
a previous native artifact against its successful publication digest, validates
an embedded complete file-to-row map, and hashes every current source file.
Same-name/content files reuse complete original source batches. Changed or new
files use ordinary conversion and the admitted small-source writer child.
Removed files contribute no rows. Every refresh writes a complete new native
artifact, recomputing statistics and metadata.

The [intake](native-changed-source-reuse-intake-2026-10-20.md) preserves provider
refusals and their revisions. Shared wrappers are explicitly removed through
the native accessor and checked parent constructors. A cross-boundary Slice
wrapper remains unsupported; it is not silently canonicalized. The
[region proof design](native-changed-source-region-proof-design-2026-10-20.md)
requires full content identity, compatible schema/build/provider/recipe,
whole-file deletion semantics, bounded ownership and pre-publication validation.

Ten focused tests pass, including actual batch-map refusal, forged/incompatible
bindings, content/generation races, damaged old artifacts, cancellation,
credit denial and failed-publication cleanup. Four separate processes then
produce, refresh, consume/rewrite and consume again. That prerequisite passes
all ordered values, exact schema, native statistics and 36 native queries.
The parent verifies full hashes and closed handles, moves outputs after producer
exit, and retires all nineteen generated payloads. It remains a separate
prerequisite; timed workflows do not claim separate-process handoff.

An ordinary-route audit aligns the candidate child with the actual public
4,096-row, 1-MiB, P1 small-source recipe. The original no-advisor lifecycle and
its corrected lifecycle remain distinct records. No raw-segment copy, external
query engine or new dependency is involved.

## Frozen comparison and result

The [prospective protocol](native-changed-source-comparison-design-2026-10-20.md)
is unchanged. Both roles compile from the same frozen 1,910-file source manifest
and locked dependency graph. The ordinary role calls
`prepare_local_source_as_vortex_for_public_workflow_with_schema` with its normal
fingerprint/advisor/publication behavior. The candidate remains an experimental
adapter; a positive screen would have required public integration and renewed
acceptance. This result supplies no new public preparation route.

Six fixed cases cover high reuse, changed/added/removed files, moderate reuse,
all-changed input, tiny files and a single changed file. Paired roles use the
same nullable integer/text formulas and identical source bytes. Both perform
initial preparation, close/move, a fresh native prepared filtered COUNT/SUM twice,
source update, refresh/rebuild, close/move, another fresh prepared COUNT/SUM twice,
then output retirement. The complete-workflow objective includes proof creation
and authentication, full preparation and teardown, native consumer setup,
execution and serialization, and retirement. Independent validation and fixture
generation are outside both operation clocks. Neither role claims a cold cache.

Twelve unscored workflows pass before two complete 36-workflow cohorts. The
second cohort reverses role order. All 84 workflows pass, covering 336 timed
native query calls and 1,596 additional full-value projection calls. Independent
ordered-row verification covers 12,160,512 rows. All actual reuse counts, native
credit limits, zero final credits, source generations and output identities pass.
The output-byte control passes in every pair and stage.

| Case | Role | Complete-workflow gate A / B | Refresh benefit gate A / B | Engine change A / B |
| --- | --- | --- | --- | ---: |
| `high-change1` | Target | Pass / pass | Pass / pass | +10.39% / +10.56% |
| `mixed` | Target | Pass / pass | Pass / pass | +9.66% / +8.02% |
| `moderate` | Target | Fail / fail | Pass / pass | +6.44% / +8.39% |
| `all-changed` | Control | Fail / fail | Not a benefit target | −2.13% / −1.45% |
| `small` | Control | Fail / fail | Not a benefit target | +2.87% / −0.22% |
| `single` | Control | Fail / fail | Not a benefit target | +0.15% / −0.95% |

Engine changes compare the median four-call engine sum for each case and role;
they are not Full43 results. Every target exceeds the 5% downstream engine
regression limit in both cohorts. Every case also exceeds the separate 5%
initial-preparation limit in both. Complete refresh gains do not override those
controls. All samples, other clocks, byte costs, medians and exact thresholds
remain in the [result packet](../benchmarks/evidence/changed-source-results-816.json).
No case is omitted, cutoff added, gate relaxed or replacement cohort substituted.

These observed failures support dropping the finite unconditional adapter; they
do not isolate a universal CPU cause. The runner did not retain host-load or
per-process CPU/RSS samples for these workflows. A later guard audit found that
its legacy name filter omitted underscore-named Rust test binaries and did not
inspect its declared UAT-lock path. Controlled children ran serially and drained,
but comprehensive competing-test/lock exclusion cannot be retroactively claimed.
Future jobs use the corrected guard and host sampling; the original guard and
results remain preserved. No CPU-isolation, positive admission or performance-
superiority claim follows from this screen.

The native grant is not an RSS ceiling or complete Parquet allocator coverage.
Broader input admission, partial-region reuse and generation-conditional
replacement remain separate work.

## Restoration and verification

After packet readback and source reconstruction, eight temporary Rust modules
and their two parent hooks are removed. The public Parquet/Arrow partition test
remains. It checks lexical file order, NULL/empty/Unicode/NUL/long values,
successful unchanged-source reuse, held-generation invalidation after a new
directory member, refusal to overwrite the existing target and final credit
release. Its optimized native-feature execution passes for both formats.

The restored workspace passes `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings` and all 3,235 tests under
`cargo test --workspace --all-targets` (offline, locked dependency resolution).
The [restoration receipt](../benchmarks/evidence/changed-source-restoration-816.json)
checks all 1,895 remaining Rust/Cargo files against the base: the public test is
the only source delta. Runtime code is unchanged.

The experimental source passes strict native-feature Clippy and ten focused
release tests. The independent checker rejects six corrupted records: wrong
values, missing query calls, false reuse, incorrect source hashes/schema and a
nonfinite clock. Portable replay reconstructs all 1,910 source inputs from Git
plus the exact three-file delta and eleven added source/design files, verifies
all 517 packet members and repeats every case and decision check. Exact rational
threshold comparisons agree with the recorded decision.

The [retirement receipt](../benchmarks/evidence/changed-source-retirement-816.json)
records 510 redundant result/source members and six obsolete test executables
removed after exact hash and closed-handle checks. All compact observations and
failures remain in the verified packet. The
[post-retirement replay](../benchmarks/evidence/changed-source-after-retirement-verification-816.json)
passes without those originals, experimental sources or binaries. The
[closeout packet](../benchmarks/evidence/changed-source-closeout-816.json)
preserves restoration, cleanup, current gates and corrected future guard sources.

One first portable-check attempt incorrectly required bit-identical Rust and
Python aggregate clock sums. The repaired replay uses the original scorer's
component-sum boundary; its independent rational gate comparisons alter no
observation or threshold. That checker failure and its source are retained in
closeout evidence. Original provider, schema-oracle, recipe-declaration,
batch-fixture and build failures remain in the main packet.

The packet contains compact results, protocols, sources and failures, with no
executable or native-payload backup. The
[portable checker](../benchmarks/evidence/verify_changed_source_packet_816.py)
needs only the packet, Git base and a local scratch directory:

```bash
python3 -B docs/benchmarks/evidence/verify_changed_source_packet_816.py \
  --packet docs/benchmarks/evidence/changed-source-results-816.json.xz \
  --repository . --scratch /path/to/local-only-scratch \
  --output /path/to/new-verification.json
```

Packet SHA-256:
`a3f257a567fc218a347d91a93e6d52295894591ea3351e5ab0642313bb70e0d2`.
Replay checks recorded evidence; physical reproduction requires regenerating
the frozen artifacts and rebuilding the bound sources under the current guards.
