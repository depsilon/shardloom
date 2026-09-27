# Lookup interleaving admission — C3

Decision: drop a new AMAC prototype for the currently measured paths at bounded
admission. No runtime candidate was timed, no negative speedup is asserted, and
no minimum-percentage retention rule is applied. Reopen for a measured stable
lookup whose dependent accesses dominate; mutable-state experiments need their
own growth and ordering contract.

[AMAC](https://www.pure.ed.ac.uk/ws/files/23617672/AMAC_VLDB16_1.pdf)
keeps independent lookup states so another lookup can progress while one waits
on a dependent access. That mechanism is distinct from batching complete opaque
map calls. This screen adapts the concept only; it imports no code or dependency.

The source and retained Full43 inventory separates these phases:

| Current family | Actual fixed-key work | Admission result |
| --- | --- | --- |
| Q18 source-order COUNT | Ten retained groups, three interned strings; dense dictionary-code slots | Existing candidate filtering reduces the accessor input to 65,563 rows across nine chunks. All group updates together take 0.343–0.375 ms in the three saved calls, versus 223.162–237.678 ms in accessor preparation. The measured candidate phase is not the dominant cost. |
| Q31–Q33 late measures | Ten retained numeric-pair keys in the later pass | The millions of reported candidate groups belong to mutable first-pass aggregation; they do not establish a large immutable probe table. Later-pass lookup timing is not separately instrumented. |
| Q13/Q15/Q17/Q22/Q23/Q34/Q35 | Partition or first-pass exact completion | All three saved runs per query elide the corresponding exact recount. Do not credit interleaving an unexecuted pass. |
| Dictionary predicates | One truth value per dictionary value, then direct code indexing | Dense indexing already avoids a variable pointer chain. Construction or decoding remains a separate target. |
| Prepared traditional dimension lookups | Range check or dense membership; a sparse-domain standard HashMap is also implemented | The existing native join fixtures exercise two-key dense membership. They do not establish a large sparse-map bottleneck. The component exposes complete map calls, with no staged probe state. |

The Q18 times are disjoint caller elapsed scopes, not exclusive CPU time or a
complete-query speedup. Provider work may overlap. Numeric decode/copy timing is
nested/partly shared work and must not be added to those scopes. The source-order
candidate directory is inside the measured group-update scope. The separate C4
comparison owns its complete-query measurements; these timings do not explain
or add to C4's observed change.

The readonly interner probes and sparse dimension map are real potential future
targets. This audit does not claim all lookups are tiny, that bandwidth is
saturated, or that AMAC can never help. Existing evidence supplies neither
dependent-stall measurements nor a current costly stable probe for a same-table
interleaving prototype. Creating a new hash-table representation merely to make
AMAC applicable would change the admitted experiment.

No unsafe references, new runtime, stored result, external execution provider,
format change or package publication is introduced. PERF-INTAKE remains the
queue; RFC 0044 ownership and CG-5/CG-6 evidence boundaries remain in force. The
next ranked candidate is C6, with its Vortex provider check already collected.

The [portable evidence](../benchmarks/lookup-packed-provider-admission-2026-09-27.json)
preserves the source hashes/excerpts, all 33 referenced complete prior Full43
envelopes, and caller timing extraction. Those prior calls are attribution;
this decision introduces no runtime change or new query timing. C6's provider
proof is validated in the same cohesive batch.
