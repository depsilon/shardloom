<!-- SPDX-License-Identifier: Apache-2.0 -->

# Exact regional aggregate comparison — November 14

The fixed regional candidate is **retained locally** at `c9baefd00`. Complete
answers, native execution, resource/ownership checks and cleanup pass. All ten
frozen requested-read witnesses improve, and both complete engine-time gates
pass. The [architecture disposition](../architecture/native-regional-aggregates-2026-11-14.md)
and [portable packet](evidence/native-regional-aggregates-2026-11-14/README.md)
preserve the original contract, exact implementation, every result and regression.

| Declared workload | Baseline engine seconds | Candidate engine seconds | Decision boundary |
| --- | ---: | ---: | --- |
| All 987 fresh-session declarations | 14.828472105 | 13.836163897 | Passes strict lower-total gate |
| Both calls in ten prepared lifecycles, 20 operations per runtime | 0.399843288 | 0.221834044 | Passes separate strict lower-total gate |

All 637 compatible baseline observations from the preceding experiment are
reused byte-for-byte: 343 accepted activation packets and 294 freshly observed
baseline packets. The new 350 baseline requests and all 987 candidate requests
execute once. Each prepared plan executes its two declared calls once per
runtime. No warmup, timing repeat, cache manipulation, Full43, full10k or
competitor execution was added. Operating-system cache state was uncontrolled.

The 350 new cases cover fourteen actual native-layout profiles and five query
shapes across CLI SQL, embedded workflow, Python SQL, DataFrame and typed plans.
The profiles pair 71-row public inputs and 1,048,577-row work inputs, with
separate empty inputs: complete and independent zones, all-null data, nullable
predicates, missing extrema, flat metadata-poor controls and empty data.
Shapes cover complete qualification, partial coverage, narrow predicates,
HAVING rejection and unsupported SUM controls. All 350 activation/refusal and
coverage expectations pass. Independent references use complete values and
schema; all 1,974 fresh results and 40 lifecycle operations replay exactly.

All 429 slower fresh cases remain in the packet; their positive differences
sum to 0.404957248 engine seconds. Those individual differences are an
inventory, not another complete-cohort result. The full 350-case regional
subgroup decreases from 3.000929406 to 1.874564918 seconds. The preceding
637 declarations increase from 11.827542699 to 11.961598979 seconds. Grouped
aggregate and predicate families are slower in this observation; no independent
speedup is claimed for them. Only the frozen complete-cohort decision admits
this candidate.

The ten strict read witnesses cover unfiltered and partially qualifying work
inputs on every public surface. For example, the unfiltered CLI SQL witness
decreases completed requested bytes from 17,091,496 to 5,176, with zero payload
arrays and two auxiliary metadata arrays delivered. Exact metadata covers all
1,048,577 rows in that witness. Partial cases retain their unresolved native
range scans. Counters include metadata reads, requested repeats and coalescing
gaps; they measure neither unique bytes nor device I/O. Residual source-range
lengths, delivered payload rows and auxiliary metadata rows have separate
scopes. Dynamic per-measure update invocations are not claimed.

The exact executed source passes formatter, strict default/native workspace
Clippy, feature-footprint checks, 3,344 default tests, 7,230 native tests plus
39 declared ignored tests, 779 reviewed layout-provider tests with strict lint,
451 installed-package tests with no skips, and six selected public test
methods. Test identities and every test binary are reconciled with their full
inventories. The source fingerprint is
`55506cdaf0b2d835d868f754b9a72dc26016c315cb9373891114763741863620`;
the preservation commit's 5,745 captured files match the build snapshot exactly.
The build retains its original `43e99d582` parent and dirty-source identity;
the later commit does not masquerade as the build's original revision.

Thirty-four saved-evidence controls reject incomplete results, incorrect region
proofs, invalid resources/native execution and failed retention gates. Another
36 controls cover packet transport, source identities, complete validation,
native reports and process completion. Relocated replay passes with original
source, runtime and evidence paths forbidden and no query execution.

All original compile/lint/fixture/verifier failures are retained. A guard/output
filename collision affected only a derived replay report; its guard receipt
was preserved and the report recomputed offline from unchanged raw packets.
The initial packaging attempt selected the predecessor's outer closeout
manifest; the corrected attempt uses its inner portable manifest. Both failures
and their corrections remain visible. No acceptance threshold or observation
was rewritten and neither correction ran a query.

This is the scalar integer COUNT/MIN/MAX regional decision. Constant-key
grouping, wider aggregate semantics and the broader investigation queue remain
open. Writer defaults, package publication, PR merging and competitive claims
are outside this local acceptance.
