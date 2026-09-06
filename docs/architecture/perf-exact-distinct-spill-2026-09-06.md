# Exact integer grouped DISTINCT native runs

This PERF-06 packet follows RFC 0044's explicit caller-owned temporary workspace
decision. The first checkpoint is a private executable adapter and native run
tests. Public dispatch follows only after the shared store and adapter pass their
serial validation gates. No-policy grouped DISTINCT behavior stays unchanged.
No external execution dependency, Arrow bridge, answer cache or synthetic spill
payload is introduced. Existing numeric-sort namespace and recovery stay intact.

The pinned Vortex 0.85 providers are the shared `QueryRunStore` native push writer,
`SequentialNativeFlatLayout`, and one exact row-range scan task per run block.
The store owns exclusive files, input/output disk overlap, held-generation
checksums/reads, metadata credits and cooperative owned cleanup. The adapter owns
complete-pair semantics, positive contribution weights, run ordering, compaction,
working geometry and final group ordering. Store blocks retain metadata/work
owners until native consumers finish. This is cleanup recovery, not crash resume.

The admitted group and distinct value are nonnullable identity integer columns,
with the current complete-pair family accepting all original integer widths and
arbitrary field names. Spill keys store exact U64 bit patterns plus a U8 signedness
signature; signed values are not converted through floats. Positive U64 row
weights preserve source accounting. Equality compares both complete integer keys
and the admitted signature. Dictionary codes are resolved in their own native
domain before these pairs enter the adapter. Nullable, transformed, text, mixed
measure and composite DISTINCT families are outside this packet.

One query-wide registry holds sorted Vortex runs. Initial memory admission
reserves a bounded pair buffer, merge/conversion scope, store scratch and final
offset-plus-limit selection before partition state can consume remaining credits.
All scopes use the same LiveMemoryPool. The private checkpoint uses at least a
2 MiB envelope, 1,024-row native blocks and four-way merges; byte admission and
simultaneous run-footer accounting may still reject a workload. The pair buffer
has an explicit maximum of 65,536 entries in addition to byte admission. These
limits describe owned operator work, not arbitrary provider allocations or RSS.

At pressure the coordinator must stop admission, cancel/join outstanding jobs,
and send every committed pair and untouched deferred contribution exactly once
to this adapter before releasing the old epoch. The adapter sorts bounded runs
by exact group then exact distinct value. Level-balanced compaction preserves
duplicate pair records and their weights: the native store requires a known row
count before writing, so output rows equal the sum of input run rows. This avoids
a hidden counting pass or unbounded merged buffer. Only the final global merge
deduplicates pairs. Each unique pair contributes one to its complete group count;
all duplicate row weights are checked and accumulated separately. The final
bounded heap uses the existing count-descending/key-ascending order after each
group is complete, with offset applied only after global selection. No local
group top-K or sketch can discard a future winner.

Runs validate full dtype, signature, positive weight, exact row geometry,
monotonic pair order, checksum and held file generation. Any failure is terminal
for the attempt. Cleanup removes only owned files and preserves unknown/replaced
paths under the store's documented cooperative identity contract. Cancellation
is checked at bounded run/sort/merge boundaries; ordinary blocking filesystem
operations are not advertised as synchronously interruptible. A source replay
must first drop all jobs, partition state, readers, run blocks and spill-attempt
files; it may not mix a prior epoch's runs with the restarted source.

The intended public admission is optional `spill` inside the existing explicit
simple-aggregate JSON, with workspace, quota_bytes and memory_bytes. Only this
exact family may admit it. Its absence creates no spill workspace; explain,
route, estimate and capability inspection must remain side-effect-free. The
namespace/marker are explicitly exact-integer-distinct, never borrowed from
numeric sort. Source/policy/cache-key and certificate changes will be reviewed
as one production integration before this option becomes callable.

Acceptance requires real native run reopens and an independent complete-pair
BTreeMap/BTreeSet oracle across all widths, extrema, repeated pairs, skew,
renamed/reordered columns, global winners, ties and large offsets. Tests must
force many runs and compaction, denied overlapping disk/footer memory, corrupt
bytes/schema/order/weights, cancellation, held-source generation change, and
complete owned cleanup/refund after errors and successful final-result release.
The six private native-run tests pass. The public query and measurements remain
pending; private tests alone cannot
close the entire PERF-06 aggregate/distinct/join queue.
# Public integration checkpoint

The next integration adds an optional `spill` object to the existing public
`--vortex-aggregate` JSON payload and the equivalent Rust aggregate request. Its
fields are `workspace`, `quota_bytes` and `memory_bytes`. Absence preserves the
current execution path. Parsing is side-effect-free and rejects malformed or
unknown policy fields. The policy admits only the already-tested nonnullable
integer identity group / integer identity COUNT DISTINCT family, with descending
distinct count ordering, explicit bounded result size and checked offset. Group
names, integer widths and dictionary domains remain arbitrary.

Execution rejects unsupported request/schema, unavailable native write support,
incompatible materialization policy and an operator envelope exceeding the query
budget before temporary data effects. The declared operator envelope is reserved
from the shared query pool before creating its finite child accounting pool. The
entire parent lease survives through the owned final result; this intentionally
conservative lifetime is not reported as just the small final selection size.
Source arrays and their native numeric execution retain the configured query
context and query budget. Bounded run I/O reuses the same runtime and provider
registry with the child allocator. This is one runtime and one run registry,
with no independent uncharged operator budget.

Allowing spill does not force native runs for a small input. The pair buffer is
reduced in memory when it fits, and the workspace/run store is created only on a
required flush. Emergency merge/footer/selection capacity is still admitted
before counting. Once runs exist, every contribution follows the validated
complete-pair merge and final global group selection. All attempt-owned files
must be removed before either successful result return or same-source retry.
The final retained source-generation validation remains outside the accumulator;
failure destroys the provisional result and cannot return a certificate.

Public reporting matches the exact policy, namespace, quota, memory reservations,
actual run counts and cleanup status. The certificate exception is specific to
this operator family; it is not a generic write allowance. Numeric sort retains
its existing diagnostic text. New exact-distinct errors name the actual operator
and preserve native source/corruption/cancellation causes.

Following this family, the next coherent reusable-store slice is exact weighted
string and numeric+string COUNT runs. Those runs must persist complete native UTF8
values with optional typed numeric key parts and positive u64 weights, resolve
dictionary domains before persistence, merge every equal complete key globally,
and select bounded output only after EOF. Variable-width byte admission must
bound each block and maximum individual key; collisions may never substitute for
complete equality. The same explicit workspace, one registry, parent envelope,
typed source retry, quota-overlap and owned cleanup contracts apply. It needs
its own actual native multi-run/public exact-value, skew/global-winner/tie and
long-key pressure tests. Generic join spill is outside this packet.
