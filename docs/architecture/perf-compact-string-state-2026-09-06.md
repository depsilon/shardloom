# Compact exact string state experiment

This isolated PERF-03/PERF-04/PERF-05 experiment addresses the supplied review's
compact offsets and slab-storage suggestions. It adds no production dispatch,
unsafe code, dependency, approximate result, local top-K pruning or new worker.
Owner-partition scheduling and control-byte probing are separate experiments.

## Source-grounded scope

The retained `local_primitives/string_count_partitions.rs::Slot` has a full
64-bit hash/count and two machine-width offset/length fields (32 bytes on the
measured 64-bit target). `Partition::insert` grows at 50% occupancy, allocates a
replacement table while the original remains charged, and grows its byte arena
by copying all prior strings into a larger vector. The new compound domain
interner has the same byte-arena growth in
`compound_count_partitions.rs::Partition::intern`; it already counts those copies.
Retained block entry credits and local comparison counters are existing work.

The candidate keeps the full hash, exact equality and 50% load factor, using two
checked u32 fields for text addressing. The resulting slot is 24 bytes on the
measured target. The alternative arena uses fixed power-of-two slabs; a key is
an arena-local logical offset and length, not a Vortex dictionary code or an ID
that can move between owners. Equal supplied hashes still require full UTF8 byte
equality. Callers resolve referenced native dictionary entries with the correct
dictionary generation before using this table. Null grouping remains on the
existing nullable route; the candidate only accepts actual nonnullable `&str`.

Each string fits within one slab. A longer string, or exhaustion of the u32
logical address range, returns `OutsideCompactRange`; it does not truncate,
wrap, split a string or change equality. An initial 64 KiB slab is a candidate
configuration, not an imposed default. Slab-size sensitivity belongs in the
paired measurement because unused tails and fixed slabs can waste memory.
The existing wide/native representation remains required outside this scope.

## Ownership and pressure

Reserve table capacity, slab payload capacity, slab Arc/control metadata and
the slab-directory vector before allocation. Replacing either table/directory
retains old plus new allocation credits during the move. String payloads are
never relocated by arena growth. Directory growth moves Arc owners, separately
counted from copied UTF8 bytes. Returned `OwnedSlabText` values retain complete
slab capacity; dropping a table/arena/session handle cannot free their credits.
Once a slab has an external slice owner, further insertion starts a new slab
instead of mutating shared storage. Empty strings require no payload slab.

Byte denial returns `Pressure` with prior complete counts usable. A cancelled
rehash drops the new table and preserves the old one. Successful capacity growth
may remain owned if a later allocation is denied, but it never becomes an
unowned retained payload. Counts and rows are checked before committing an
update; zero weights and overflow fail explicitly. On error/drop, every owned
allocation retains its lease until its actual last owner disappears.

The candidate counts table/directory capacity and slab data/Arc metadata. It
does not claim process RSS, source accessor buffers, allocator bookkeeping,
the surrounding partition object, oracle/output allocations or provider scratch.
Existing shared entry-credit admission is separate from these byte leases and
must remain around any future per-partition integration. The isolated table
does not independently replace that query-wide entry limit or implement spill.

## Executable comparison boundary

`compact_string_state_benchmark.rs::compare_retained` accepts weighted strings
and an actual retained-reducer callback plus its frozen source identity. It
accepts an explicit retained-first/candidate-first order so paired iterations
can alternate which kernel actually executes first. It
executes and verifies the complete retained groups, runs the candidate's updates
over the identical input, and checks both against an independent checked
BTreeMap oracle outside kernel clocks. It records retained/candidate kernel
times, actual payload-copy counts, available key-lookup probes, owned bytes, slot size, slab count,
directory moves and table rehash moves. It checks zero candidate-owned bytes
after release. It emits no score if either complete result disagrees or if the
candidate cannot admit the input. Synthetic callback tests validate this hook;
they are explicitly not a retained-runtime benchmark.

The registered test-only callback in `string_count_partition_benchmark.rs`
constructs the actual `StringCountPartitions` and calls its production `reduce`
method on native UTF8 partials containing the supplied complete hashes and
weights. `StringCountPartial::benchmark_weighted` prepares those inputs before
state clocks; it does not duplicate the old count table. The callback visits
every occupied partition slot after reduction, without selecting top-K.
Test-only counters at the actual retained arena copy sites count both new
strings and all prior payload relocated by arena growth. Retained lookup probes
are unavailable (`null`); full-hash equality comparisons are reported separately.
These copy counters add instrumentation to the test binary, with no production
field or branch. For eventual
runtime measurement, switch only the partition table/arena under the existing
complete-key workers, ordered pressure handoff, native source replay and final
global selection. Preserve global entry credits; outside-range/pressure must
take the existing exact transition at an uncommitted entry, with no row replay
or loss. Reuse the arena in the compound interner only after its own complete
numeric/string domain and tie-order checks pass.

Tests cover all-key counts with deliberately identical hashes, non-URL strings,
Unicode/NUL/empty values, differently ordered dictionary domains, exact weighted
overflow, cancellation during growth and long collision chains, shared-budget
denial/retry including slab denial after successful table growth, reduced test
address limits and slices surviving arena/pool-handle drop. The new actual
retained callback and paired runner pass root's native compilation and ordinary
correctness gate. The ignored release matrix remains unmeasured; passing these
tests is not performance evidence.

The ignored `compact_string_state_actual_retained_paired_release` test adds one
complete warm pair and seven alternating measured pairs for repeated, skewed,
and high-cardinality UTF8 values, each at 16/64/256 KiB slabs. A profile has
131,072 weighted entries, at most 65,536 complete groups, a checked 16 MiB logical
input payload cap, a finite 128 MiB state pool per side, and an input-entry-count
global entry limit. Both use 64 content-hash partitions and the existing entry
credit implementation. The exact BTreeMap oracle, complete output comparison,
input/hash/weight checksum and output checksum are outside state clocks. Every
successful pair requires zero outstanding entry credits and zero owned state
bytes after last-owner release. Source fixture/partial ownership is separately
released and checked; its native provider payload is outside state-pool scope.

This runner measures one caller with no background workers. It records separate
construction, update, routing and teardown clocks. Retained update calls the
actual production reducer and includes its routing, locks and evidence; the
compact adapter routes supplied entries before its separately timed updates.
Native input construction, source counting, hashing, oracles and exports are
excluded. The actual retained reducer consumes and disposes its native partial
inside its reduce clock, whereas the compact adapter's borrowed routing vectors
dispose outside its update clock. This asymmetry is explicitly emitted in each
record. Both complete, independently exported BTreeMaps remain alive across the
paired kernels in either order, so alternating order does not change which
case retains a prior full output. The routing and orchestration differ, so these are representation
experiment measurements, not an isolated instruction-level speed ratio or a
public query improvement. The source digest covers the compiled retained table,
partial, credits, candidate and helper files; the root runner must still attach
the immutable binary/commit and machine identity. Allocator overhead, provider
scratch, fixture/oracle/output allocations and process RSS are not bounded or
measured by the reported state credits. No threshold assertion promotes the
candidate, and a pressure, value mismatch or incomplete refund fails the pair.

Measure low/high cardinality, all-unique, skew and long-string distributions
at fixed 1/2/4/8 worker grants, alternating retained/candidate order with warmup
and at least three timed samples. Record all values before any top-K selection,
then complete public output and offset/tie behavior. Compare realistic 16/64/256
KiB slabs as well as state pressure; unchanged load factor isolates this work
from a different probing policy. Retain only a demonstrated CPU/memory/lifecycle
benefit. Slab slack, extra directory access and narrower admission may justify
dropping this candidate even when it copies fewer payload bytes.
