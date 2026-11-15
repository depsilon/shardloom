<!-- SPDX-License-Identifier: Apache-2.0 -->

# Constant-key aggregate comparison — November 14

The fixed constant-key candidate is **dropped**. Complete results, schemas,
native execution, resources, ownership and cleanup pass, as do every frozen
coverage/refusal check and all ten strict requested-read witnesses. The full
fresh engine total is higher, which fails the prospective retain rule despite
the separate lifecycle improvement. The
[architecture disposition](../architecture/native-constant-key-aggregates-2026-11-14.md)
and [portable packet](evidence/native-constant-key-aggregates-2026-11-14/README.md)
preserve the exact rejected implementation and all observations. The accepted
scalar-regional code at `e5f6728a2` is restored unchanged.

| Declared workload | Baseline engine seconds | Candidate engine seconds | Decision boundary |
| --- | ---: | ---: | --- |
| All 5,014 fresh-session declarations | 556.592642154 | 564.033758513 | Fails strict lower-total gate |
| Both calls in ten prepared lifecycles, 20 operations per runtime | 3.325826500 | 1.998268543 | Passes separate strict lower-total gate |

All 987 compatible observations from the accepted regional candidate are reused
byte-for-byte as baseline observations. The remaining 4,027 baseline requests and
all 5,014 candidate requests execute once. Each prepared plan executes its two
declared scored calls once per runtime. Execution order is fresh baseline, fresh
candidate, baseline lifecycles, candidate lifecycles, with other task-owned work
idle during queries. No warmup, repeat, cache manipulation, Full43, full10k or
competitor execution was added. Operating-system cache state was uncontrolled.

The population contains all 3,291 original grouped COUNT/MIN/MAX declarations
with nonempty grouping keys, including 983 integer-key and 2,308 bool/text-key
declarations. Their union with the entire accepted 987-case regional population
has 74 overlaps and 4,204 distinct cases. The 810 new declarations complete the
5,014-case set. Selection was frozen before candidate observations without using
per-case timings. Every reused raw packet, complete job, reference, input identity,
command, worker and accepted runtime binding was checked for exact compatibility.

The new cases cover nine native-layout profiles at public and work sizes, nine
query shapes and all five public interfaces: CLI SQL, embedded workflow, Python
SQL, DataFrame and typed relation. Nonempty inputs have 71 or 1,048,577 rows;
empty inputs are separate. Profiles cover exact constant regions, independently
zoned columns, mixed-null and mixed-value keys, all-null keys, missing extrema,
flat metadata-poor input, empty input and nonnullable input. Shapes include
complete and partial qualification, COUNT(*), COUNT(value), unsorted and incomplete
ordering, HAVING/limit/offset, aliases and unsupported SUM controls. All 18 files
were read back completely and checked against independent literal rows before
the 810 complete references were frozen. All 10,028 fresh results and 40 lifecycle
operations replay exactly, including schemas and ordered or tied-result contracts.

All 3,211 slower fresh cases remain; their positive differences sum to
36.945931145 engine seconds. The complete fresh loss is 7.441116359 seconds.
Subgroups explain where the observed difference lies without replacing the
frozen complete-cohort decision:

| Complete subgroup | Cases | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Original declarations | 3,649 | 503.971547868 | 526.529268475 |
| New constant-group declarations | 810 | 50.415283459 | 35.278733609 |
| Inherited regional declarations | 350 | 1.874564918 | 1.890927510 |
| Inherited per-measure controls | 175 | 0.324779200 | 0.328246585 |
| Inherited mechanism controls | 30 | 0.006466709 | 0.006582334 |

Among original cases, 3,621 report no constant-group proof and increase from
502.485712911 to 524.737837726 seconds; the other 28 report no applied grouped
metadata contribution. Of the new cases, 460 report applied contributions and
improve, while absent/unapplied controls are slower in aggregate. The saved
attribution retains the twenty largest regressions and comparisons of their
reported non-clock work. The largest original null-aware anti-join cases report
no constant-group activation. Source/runtime/input identities and supervision
pass, but this single observation does not establish the cause of those timing
differences or prove controlled host/cache state. No repeat or narrower gate was
used to change the drop decision.

The ten read witnesses cover complete and partially qualifying work inputs on
every interface. Unfiltered completed requested reads fall from 40,632,440 to
12,680 bytes on each interface, with payload arrays reduced from 129 to zero.
The CLI SQL partial witness falls from 37,842,256 to 1,546,776 bytes, with payload
arrays reduced from 96 to four. Metadata covers exact disjoint regions, and
unresolved source ranges still use native scans. Counters include metadata,
requested repeats and coalescing gaps; they are not unique bytes or device I/O.
Residual source rows, delivered payload rows and auxiliary metadata rows retain
their separate scopes. Dynamic weighted-update invocation savings are not claimed.

The exact candidate passes formatter, strict default/native workspace Clippy,
feature-footprint checks, 3,345 default tests, 7,245 native tests plus 39 declared
ignored tests, 779 reviewed layout-provider tests with strict lint, 453 installed
package tests without skips, and six selected public test methods. Complete test
names and all test binaries match their inventories. The candidate fingerprint is
`5456aa37099fba9328cc293b1eb59eab0afcaaf9dca8cb0d6ac4b005371edf1a`.
All 5,762 captured source files match preservation commit
`b6e9d5d5d6bbd00ad48890795b8fe95b27a1d088`; its build retains the actual
`e5f6728a2` parent and dirty-source identity. The baseline is the unchanged
accepted regional runtime with source fingerprint
`55506cdaf0b2d835d868f754b9a72dc26016c315cb9373891114763741863620`.
Its original build identity and later documentation-only differences remain explicit.

Forty-seven saved-evidence controls reject bad complete results, coverage/group
proofs, resources, native-execution claims and retention-gate failures. Another
36 controls cover transport, source identities, validation inventories and
process/report completion. Relocated replay succeeds with original source,
runtime and evidence reads forbidden and no engine or subprocess execution.
The twelve-part packet preserves complete raw ledgers, references, exact candidate
source/patch, all validation failures and external payload identities.

The first candidate runtime's public CLI checks exposed omitted constant-group
evidence fields. Its failed results and exact dirty-source archive remain; the
corrected runtime passed before the protocol freeze and measured comparison.
A repeated pre-execution freeze invocation was refused because its successful
receipt already existed; it ran no helper or query and replaced nothing.
No failed observation, timing value or acceptance threshold was rewritten.

The rejected branch remains available, while active runtime source is verified
equal to the accepted scalar-regional baseline. That restoration requires no
new benchmark or rebuild of identical accepted code. Dictionary-domain work is
next; broader constant-key support, the parent metadata family and the wider
optimization queue remain open. This local disposition does not merge a PR,
publish packages, authorize a release, change writer defaults or establish a
competitive performance claim.
