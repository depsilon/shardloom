<!-- SPDX-License-Identifier: Apache-2.0 -->

# Per-measure metadata comparison — November 14

The fixed candidate is **dropped**. Complete answers, native execution,
resources and cleanup passed. The frozen read-reduction gate failed, and the
complete fresh-session engine total increased. The
[architecture disposition](../architecture/native-metadata-measure-completion-2026-11-14.md)
and [portable packet](evidence/native-metadata-measures-2026-11-14/README.md)
preserve the implementation, full results, every regression and original failures.

| Declared workload | Baseline engine seconds | Candidate engine seconds | Decision boundary |
| --- | ---: | ---: | --- |
| All 637 fresh-session declarations | 11.827542699 | 11.858516037 | Fails strict lower-total gate |
| Both calls in ten prepared lifecycles, 20 operations per runtime | 0.197566507 | 0.103567627 | Lower total; cannot override fresh or work gates |

This is one frozen observation per declaration or lifecycle call, with 343 exact
accepted baseline packets reused once. The remaining 294 baseline and all 637
candidate fresh observations executed once. The cohort includes all selected
current aggregate owners, the 119 omitted original global aggregates, and 175
versioned controls across five public interfaces. Controls cover empty, all-null,
wide-integer, small and large inputs; shared/separate columns, HAVING, all/no
metadata, filtering and text distinct. No Full43, full10k or competitor run was
performed for this experiment. Operating-system cache state was uncontrolled.

All 1,274 complete fresh results and all 40 lifecycle operations replay against
the original independent references. Candidate activation/refusal checks pass
for all 175 new controls. There are 278 slower fresh cases; their positive
differences sum to 0.296974377 engine seconds. That sum is a regression inventory,
not the complete-cohort difference and not an independent timing claim.

All ten large separate-column/text-distinct controls fail the required strict
requested-read reduction. Each baseline records 10,317,092 bytes. Text-distinct
candidates record the same number; separate-column candidates record
10,317,592–10,317,744 bytes. All deliver 128 arrays on both sides. Logical
projection and metadata-state evidence cannot substitute for that missing work
witness. Read counters include requested-range repeats/coalescing gaps and do not
measure unique or storage-device bytes. Scalar measure-update invocations were
not dynamically counted.

The exact candidate source passed formatter, default/native strict lint,
feature-footprint checks, 3,345 default tests, 7,223 native tests with 39 declared
ignored tests, 450 installed-package tests without skips, and five selected
public test methods. The packet retains 27 saved-evidence negative controls,
including synthetic fixtures that independently reject equal/slower totals and
equal/increased reads. Another 33 controls test transport, source identities,
validation breadth, resources, native execution and process completion.

The immutable replay decision remains `not_admitted`, with status
`incomplete_or_invalid_evidence` because its required work witness failed.
The separate closeout decision is `drop`; it does not turn the original replay
into a valid performance acceptance. All earlier failed checks and the failed
temporary-cleanup control are retained. No threshold change, selective subgroup
acceptance, cache manipulation or timing repeat was used.
