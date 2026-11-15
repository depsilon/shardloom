<!-- SPDX-License-Identifier: Apache-2.0 -->

# Per-measure metadata experiment: dropped

Status: the first bounded experiment in the
[metadata investigation sequence](native-metadata-encoded-consumer-investigations-2026-11-13.md)
is complete and dropped. This experiment restored `046488dc3`, whose
runtime is the [November 14 activation unit](native-optimization-activation-2026-11-13.md).
The subsequent [regional unit](native-regional-aggregates-2026-11-14.md) is now
retained locally at `c9baefd00`. The subsequent
[constant-key candidate](native-constant-key-aggregates-2026-11-14.md) is dropped
under its own complete-cohort rule. The subsequent
[dictionary-domain N unit](native-dictionary-domain-2026-11-14.md) and subsequent
[source-domain C unit](native-source-domain-facts-2026-11-14.md) are retained
locally. Predicate-specific native Bloom admission, then necessary text-zone
evidence, is next. Broader
constant-key support and the item-20/RFC-0044 queue remain open. Those separate
decisions do not change this experiment's drop disposition.

## Implementation and provider contract

The experimental source is preserved locally at `6f9650473`, with all 13 changed
or added files and its exact patch in the
[portable evidence](../benchmarks/evidence/native-metadata-measures-2026-11-14/README.md).
Its bytes match the tested runtime fingerprint
`179e9f90c26bfc9c12e794af1a1a81c581eed5dc992d4479bb7e6972c22e6404`.
The preservation commit was made after testing; it did not rebuild or relabel the
runtime's recorded parent commit and dirty source snapshot.

The candidate used pinned Vortex 0.93.0 footer statistics, exact typed scalars
and the existing native scan provider. It staged independently proved integer
COUNT/MIN/MAX measures, removed completed states from the residual updater,
selected only remaining columns and restored original positions before the
shared finalizer. The complete original request was bound first. Optional
reservation refusal left the ordinary native path unchanged. Alias, dtype,
null, HAVING, cancellation, source-generation and ownership contracts remained
required; a residual error could not expose a successful partial answer.

SUM and AVG remained residual. Converting each integer to F64 before compensated
accumulation is not equivalent to converting an exact integer sum afterward;
`[2^53 + 1, -2^53]` distinguishes those semantics. No Arrow execution substrate,
external execution fallback or new provider dependency was introduced.

This code is **not retained** in the working implementation. The existing
whole-request footer completion is unchanged. The original prospective contract,
tests, source and every observation remain in the packet.

## Decision and evidence

The [complete comparison](../benchmarks/native-metadata-measures-2026-11-14.md)
contains 637 declarations per runtime: 343 exact accepted baseline observations,
119 previously omitted global-aggregate declarations and 175 versioned controls.
All 1,274 complete results and 40 lifecycle operations replay correctly, including
resource, native no-fallback and cleanup checks. Default, native and installed
validation passed with 3,345, 7,223 and 450 tests respectively; the native suite
also preserves its 39 declared ignored tests. Five public test methods cover 60
new interface subcases. There were no installed-test skips.

The complete fresh-session total increased from **11.827542699** to
**11.858516037 engine seconds**. Both calls in ten prepared lifecycles improved
in aggregate from **0.197566507** to **0.103567627 engine seconds**, but that does
not override the independent fresh-session gate. All 278 individual fresh
regressions remain recorded.

The required read-reduction witness also failed in all ten large-input controls.
Five text-distinct controls recorded equal requested-read bytes; the five
separate-column controls recorded increases of 500–652 bytes. Both sides
delivered 128 provider arrays in each control. These counters measure completed
requested ranges, including repeats and coalescing gaps, not unique or device
bytes. No dynamic per-measure scalar-update count was available.

The fixture profile named `columnar` describes its declared workload, not a
proved column-addressable physical layout. Its ordinary native writer uses
sequential Flat leaves; the pinned Flat reader requests a leaf before evaluating
its projection. That source contract explains why selecting fewer logical
columns alone cannot establish fewer physical reads. The exact cause of the
small byte increases was not decomposed, and no timing difference is attributed
solely to that layout observation.

The frozen replayer therefore retains `incomplete_or_invalid_evidence` /
`not_admitted` for its unmet work gate. A separate final disposition records
`drop`, citing both that gate and the higher complete fresh engine total.
Neither thresholds nor observations were revised; no timing repeat was run.
Twenty-seven saved-evidence negative controls and 33 packet controls passed.
Relocated replay also passed with reads of original source, input, runtime and
evidence paths forbidden. The original control-driver and packaging failures
remain documented.

## Regional handoff

The subsequent regional work uses actual zoned layouts and proves disjoint,
exhaustive row coverage across all required columns. For each region it
distinguishes no qualifying rows, fully qualifying rows with exact compatible
partials and unresolved rows that retain the native scan. Its separately
versioned controls exercise missing metadata, independent zone boundaries,
the shared state/finalizer, held generation and native segment source. The
[regional evidence](../benchmarks/native-regional-aggregates-2026-11-14.md)
establishes its bounded scalar acceptance; constant-key grouping remains open.

The dropped per-measure prototype is not a prerequisite for that work. Its
source and validation can inform a distinct design, but its observations cannot
be relabeled as acceptance for regional partials or constant-key grouping.
No Full43, full10k, competitor pass, merge, release or package publication is
implied by this closeout.
