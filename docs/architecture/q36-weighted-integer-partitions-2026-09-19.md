# Q36 weighted integer partition screen

Status: candidate implementation and validation in progress. No speedup is
claimed. This follows the selected next screen in
[remaining admission](remaining-performance-admission-2026-09-19.md), under the
existing PERF-02/03/07/08/09/11/12 obligations; broad CG completion remains paused.

The retained Q36 complete run is 4.144340 seconds, including 3.330456 seconds
merging partials on the caller. Existing chunk reduction produces 21,678,299
weighted entries from 99,997,497 rows. Preserve that reduction and native integer
width, then append weighted entries to leased complete-key partitions. At EOF,
sort and sum each complete partition on the same workers, retaining a bounded
union of partition winners for the existing renderer. No per-chunk Top-K is safe.

Admission requires the existing nonnullable identity physical-key proof, COUNT(*),
one descending COUNT order, no HAVING or spill, and positive OFFSET+LIMIT at most
128. Dependent outputs remain limited to the existing identity, integer constant
and AddOffset proof. Each sorted partial's minimum and maximum are validated
through the existing reconstruction function before routing: checked addition
is monotone on each admitted integer domain, so these endpoints cover errors
in losing groups too. Final output still uses the existing comparator and
derived-value reconstruction.

Native Constant input keeps its complete multiplicity. On a native Dict chunk,
drain prior jobs and transfer every accumulated weighted entry into the existing
global state before using the existing native dictionary path; disable persistent
integer partitions for that query. This transfer never truncates candidates.
Committed allocation or source failure cancels, joins and fails; it does not
invent a spill or pressure replay route. Vector capacity and growth overlap,
task output and coordinator ownership stay leased; legacy output-map allocations
remain a separately reported scope, not a claim of process RSS enforcement.

Vortex-first provider check: `implement_shardloom_kernel`. The pinned Vortex
0.85 native scan, primitive and Constant providers already supply the admitted
source/count boundaries. This changes ShardLoom's cross-chunk grouped-state
ownership and exact Top-K completion, which the current provider inventory does
not expose as a certified grouped executor. Reuse native scan and owned partials;
add no upstream dependency, Arrow conversion, alternate engine or persistence
format. Native certificates and `fallback_attempted=false` remain required.

Verification: renamed scalar keys, signed/unsigned extrema, native Constant and
mixed Dict, empty inputs, cross-chunk global winners, ties/OFFSET, non-winning
expression errors, rejected schemas/shapes, cancellation, reservation denial and
native-file source faults. A candidate must prove actual route activation before
timing. Retain only for at least one second of comparable complete-query savings
or at least 30% lower OS peak RSS with nonregressing time. Preserve all samples and
use the fastest valid run symmetrically. Winners receive Full43 UAT, broad gates,
independent review and a PR; failed prototypes are removed with evidence retained.
