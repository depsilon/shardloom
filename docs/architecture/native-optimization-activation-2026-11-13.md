# Native optimization activation and execution evidence

Status: implementation contract; acceptance is pending. The November 13
maintainer instructions authorize both connecting existing mechanisms and
creating missing native mechanisms. This work remains under item 20, RFC 0044,
and the existing CG-14/CG-17 contracts. It does not close those competitive gates
or authorize publication. The metadata and encoded-consumer investigations stay
after the active coverage work.

The comparator is a maintainer of an encoded columnar engine: every claim must
join a public declaration to an actual shared execution owner, preserve complete
values and validation obligations, and account for retained state. A native
pipeline label alone does not establish optimization coverage.

## Vortex-first decision

The pinned Vortex 0.93 source supplies `BoundExpression`, scan filter/projection,
pruning, `SegmentSource`, `SharedSegmentSource`, `FileSegmentSource`, and credited
host buffers. Use these providers without adding an execution dependency.
`ScanBuilder` carries an optional metrics registry but its current prepare path
does not forward it into the repeated scan. File request metrics do not provide
per-column bytes or independent segment-pruning decisions. The existing
ShardLoom native reader and held source generation therefore own the missing
read observations; scan owners supply their executed logical contracts.

Keep whole-file exclusion separate from internal Vortex pruning. Unknown
physical leaf counts, decode bytes and per-segment exclusions remain unavailable.
Do not derive avoided work from the number of requested logical columns.

## Executed scan contracts

Add typed evidence to the existing native I/O certificate and shared renderer.
An executed contract records its source ordinal, route, logical projection,
pushed-predicate dependencies, residual dependencies, available logical width,
source rows, invocation count and delivered rows. Record it only when the owner
executes; do not manufacture records by traversing an unexecuted plan.

Repeated invocations with the same source and column contract aggregate counters.
Do not retain predicate literal values or one event per correlated row. Retained
names, vectors and source records acquire metadata credit before allocation.
The resulting certificate retains that credit through its native result owner.
An empty/pruned scan still records its completed decision; errors do not produce
a success certificate.

Binding and dependency analysis release their scratch credit before scan
execution. Only the copied contract names and vectors remain reserved through
downstream work. The existing 1,025-column nested export exposed a regression
when that temporary analysis allowance survived into Parquet writer admission;
all seven writers again complete within the same 64 MiB test grant after the
lifetime correction.

The common relational scan and embedded projection kernel must contribute to
the same evidence. Primitive Count/CountWhere/Aggregate/Sort kernel reports must
remain represented when nested inside another operator and when their root
certificate is selected. Metadata COUNT has zero data-scan invocations; an
executed projection must not be inferred from an aggregate's output names.

Aggregate data passes forward the actual bound filter and projection through a
small adapter around the pinned Vortex scan builder, which exposes no public
getters for those expressions. Its provider iterator remains the producer.
`aggregate_pass` records each sample, COUNT, winner-measure and exact-recount
invocation when it executes. Columns describe the logical pass, not internal
layout-reader requests. Delivered rows include partial attempts before an
explicitly admitted native retry. Equal source/column contracts merge across
sample ranges and retries; neither literal values nor an event trace is retained.
The ordinary `aggregate` route remains for footer/pruning completion with zero
provider scans. These records do not count decoded bytes or prove per-leaf reads.

Instrument successful positional reads in `ResidentFileReadAt`. Aggregate by
the held source generation within one `NativeExecutionContext`. Report completed
read requests and bytes, with repeated reads/coalescing gaps included; these are
not device reads or unique bytes. Keep segment requests separate. Drain readers
before taking the completed snapshot. Cancellation, replaced source generation,
consumer error, reservation denial and last-owner release remain test obligations.

The resident projection stores its original unbound filter as well as the bound
scan filter. Its projection is a selection of source fields, so a file-statistics
proof can complete an empty result before constructing a provider scan. Use the
same upstream `VortexFile::can_prune` proof already consumed by the other native
scan owners. Preserve source validation, cancellation, typed empty results and
the distinction between an attempted proof, a refused proof and a proved
exclusion. Missing statistics must use the ordinary scan. This does not authorize
pruning arbitrary computed projections or suppressing their validation.

## Remaining shared connections

The direct grouped COUNT prototype needs a newly valid declaration/transport
contract before acceptance. Its preserved invalid baseline is not a speed
rejection. Reuse the native grouped owner and its finalizer, including uint64
COUNT transport and the established nullable floating SUM representation.

The archived prototype is now connected to the unified compiler, preserving its
immediate total-sort and nonnullable integer admission. Its old provider-field
diagnostic is versioned separately for the current embedded-kernel plan. Native
production evidence records admitted batch inputs, reduced groups, constant
batches, unique-key declines and optional-capacity declines; summed partial
capacity is explicitly not peak or live memory. Capacity admission now uses the
pool's atomic optional reservation instead of parsing an error string. The
original failed baseline and unadmitted proposal remain immutable evidence;
correctness tests do not supply the pending complete-cohort performance decision.

Extend cyclic semi-join admission to eligible prepared native-file scans using
the semantic declaration retained by `Node::semantic_kind()`. Execute the actual
compiled nodes, preserving source order, generation checks, explicit spill
precedence, null/duplicate behavior and existing predicate/computation barriers.
Add production witness/probe observations; thread-local test counters alone do
not establish public activation.

The public matrix exposed two additional cyclic admission gaps. SQL and
DataFrame lowering retain harmless field-selection/alias stages around the
inner join and source scans. Resolve the two cyclic edges backwards through
only bound column projections, including flat stages; execute the original
source nodes with their declarations intact. Computed expressions, filters,
limits, observations and application stages remain barriers. A Vortex DataFrame
join may retain validation obligations for additional inner payload fields even
after a parent selects only the two cyclic edges. Preserve those obligations:
index the inner join key, validate each matching input row through the existing
selected-payload validator, and avoid constructing the Cartesian output. Rows
with no inner match remain outside that selected-value domain. A credited
per-key completion mask avoids validating a right-hand duplicate group again
for another matching left row. Complete this work before reading the outer
source, including when no outer row could have a witness. The outer gather also
honors the existing retained-payload mask without skipping hidden validation.
Record actual left/right validation rows and cells separately from witness
search work. This is mandatory validation, not another successful result or a
claim of decoded bytes avoided.

Admitted finite local compatibility readers also feed the same native scans.
Permit their held local source owners and detach retained build batches through
the existing join ownership boundary. Arbitrary external batch producers remain
excluded. The old 128-row heuristic remains for known memory/Vortex populations;
local compatibility sources with unavailable populations are not assigned an
invented row count or sampled before execution. Their finite readers, complete
drain, cancellation, generation validation and memory grant remain authoritative.
These additional connections require public parity and local-source failure
tests before the complete affected-cohort decision.

## Runtime filters

Create an executable native runtime-filter path from completed build-side state.
The first shared path uses conservative membership information and exact native
key semantics, with ordinary equality still proving every surviving match.
Apply only where row preservation and the producer's validation/effect domain
are proved. Outer and anti joins must not lose unmatched rows.

A filter applied after complete producer/key validation may report rejected
probe rows and avoided join work. It must not claim file I/O pruning. Actual
Vortex filter pushdown requires a separate proof that source/predicate/key errors
and effects cannot be suppressed. Do not relabel an internal hash-table probe
as a scan runtime filter. Report build provenance, admission/decline, application
boundary, evaluated rows, rejected rows and actual avoided work independently.

## Skew handling

Use an observed build-key distribution to select a native mitigation. The
existing RowIndex owns exact keys and stable duplicate row order; reuse it.
Investigate sealing heavy duplicate chains into contiguous row-ordinal ranges
after build completion. Optional storage must be credited before allocation,
preserve duplicate order, and decline cleanly without sufficient headroom.
No insert may silently invalidate sealed ranges. Report measured hot groups,
the chosen mitigation, rows represented, actual uses and admission declines.
Do not add distributed salting or synthetic post-merge labels as local execution.

The first concrete join adaptation seals only completed keyed join builds.
Unsealed RowIndex users (grouping, windows and incrementally populated sets) keep
their insertion contract. A sealed build cannot accept more rows. At least 128
distinct keys admit a three-bit Bloom membership test, sized at 16 bits per key
and rounded to a power of two. It consumes the existing exact native key hashes;
hash collisions and Bloom positives still require the existing equality check.
The filter runs after producer and key validation, including for conditional,
outer and membership joins. Cross and as-of joins retain their own algorithms.

For duplicate-preserving builds, groups with at least 64 rows may acquire a
contiguous ordinal range in stable insertion order. The sparse range directory
does not enlarge every exact-key entry. Both optional owners require at most
one sixteenth of currently available memory and reserve before allocation.
An atomic optional reservation decline keeps the original exact index; allocation
errors and cancellation remain errors. Only actual range iteration contributes
visited-row counters. Completion publishes the counters once through the parent
query, after consumer success. These are prospective candidates pending the
complete-cohort acceptance protocol, not accepted performance improvements.

## Cross-query reuse

Create real reuse across completed calls. A source-segment implementation must
retain only immutable native segment buffers from the same held generation and
layout, under the existing source/session lifetime and memory pool. Reuse cannot
retain an old operation's reader or I/O scope. Generation checks surround every
new call; changed inputs fail or invalidate before use. Independent source owners
stay cold, and effects/dynamic producers do not enter this cache.

Bound optional retention and evict before it can starve necessary query work.
Record actual hits, misses, retained bytes, evictions and avoided reader work on
the new call. Prepared-plan reuse, OS cache and action-local materialization are
different mechanisms. This contract authorizes scoped native segment reuse;
result or join-build reuse needs its own semantic-key/invalidation proof and
must not be implied by a source-cache hit. Preserve the prior dropped warm-source
candidate and compare any new implementation as a distinct prospective candidate.

The concrete store belongs to the resident runtime and uses weak held-source
identity plus SegmentId, deliberately keeping separately prepared owners cold.
Each new call retains its own reader and I/O scope. Optional copies retain known
full allocation capacity under both the session grant and a bounded retention
grant, even after eviction while a consumer still holds a slice. The default
prepared-source path uses this automatically. Bound the store to 128 entries,
8 MiB per segment and at most min(current grant / 16, 32 MiB) retained capacity.

Only a successfully completed call publishes its retained copies for another
call. Same-call requests retain upstream live-future sharing. Failed or cancelled
calls discard their pending entries, while completed prior entries remain usable
under the same generation checks. A different projection or predicate still
executes its own scan and expression work over the immutable compressed bytes.
Retention uses a bounded least-recently-used directory with one admission rule:
do not evict a segment already read or stored by the current call just to retain
another first-use segment. Otherwise a scan larger than the cache would replace
every earlier segment before the next complete scan could reuse it. Once this
call's protected entries fill the bound, further optional copies decline; query
execution continues through the ordinary native reader. Other calls may replace
entries they have not used. Mandatory pressure always retains eviction priority.

The memory pool admits one weak optional-memory reclamation endpoint. An already
occupied endpoint declines cache enablement for another runtime; it never
prevents that runtime from constructing or executing. On a mandatory reservation
refusal or any allocation shrink, invoke the endpoint once before final denial.
It must use a nonblocking cache lock and only drop existing immutable buffers
and their leases; it cannot allocate, reserve, execute a query or reenter policy.
Do not hold the registration mutex during this callback. Optional reservations
do not invoke reclamation. A cache epoch prevents in-flight copies from
republishing after reclamation. Memory snapshots continue to include every
cache-owned byte; tests must explicitly account for the new persistent owner.

An empty operation's drop also revokes pending copy admission when it refunds
the table reservation. Explicit serving calls may overlap: a different call
can be copying outside the store lock while the table still has no entries.
Advance the same invalidation epoch before releasing that table, so a pending
copy cannot publish an entry without its table credit. The deterministic
interleaving test preserves the separate pending capacity credit and proves
that a later fresh admission still acquires and refunds both owners.

## Breadth and acceptance

Extend the fixed 10k with composed multi-window, correlated-subquery, melt/pivot,
sampling and vector/tensor/Variant cases through their supported public APIs.
Use independent complete values/schemas and positive/negative activation checks,
including nulls, empty input, skew, unknown generations, late errors, denial,
cancellation, changed inputs and retained output owners. Do not infer SQL support
from a typed-plan spelling or sampling correctness from an unfixed seed.

Freeze each selected performance cohort before measuring it. Use one isolated
execution per declaration, preserve every observation, and keep other task work
idle during query execution. Report engine time only in routine comparisons.
Correctness, ownership, resource and no-fallback proof precede the existing binary
complete-cohort gain/loss decision for performance candidates. Required default
and native checks, installed-package proof, complete results and source-bound
receipts remain mandatory. Instrumentation establishes evidence availability,
not a speedup or universal optimization completion.
