# Native complete-pair grouped distinct

This is the next PERF-04/05 family under RFC 0044. Native aggregate dispatch and
the public request path are integrated. Native compilation and correctness tests
pass, including all original integer widths and complete public results; fresh
benchmark retention remains pending. Compound COUNT validation is independent.
No generic aggregate spill claim follows.

The first admission is one nonnullable identity integer group and one
COUNT(DISTINCT nonnullable identity integer), with bounded ordered output.
Both columns may use any of the eight integer physical widths and arbitrary
names. Existing native paths handle nullable, text, transformed, multi-measure
and composite-distinct requests. Pinned Vortex 0.85 provides numeric execution
and original-width PrimitiveArray owners; ShardLoom owns exact pair equality,
resource admission and reconciliation. No Arrow row vectors or external engine
are introduced.

Q9 currently misses the existing integer pair-preunion route because
`grouped_count_distinct_integer_pair_preunion_inputs_for_accessors` requires
more than one measure. That is source evidence, not a measured bottleneck.
The new partial emits every `(group bits, group signedness, value bits, value
signedness)` pair. Hashes only select buckets; full keys establish equality.
Dictionary numeric codes execute through their native values, never become
cross-chunk identities. Typed loops dispatch once per source array.

Complete pairs share a deterministic partition across the query. All equal
pairs are deduplicated there before the partition emits per-group distinct
contributions. Summing those contributions is exact because a complete pair
occurs in one partition. A large group can span partitions through different
distinct values. No partial group top-K is permitted, including at the first
partition reduction; final ordering occurs after the second group reduction.

Pair-set storage reserves vector capacity and simultaneous old/new growth.
Global entry admission uses the existing exact entry-block credits;
reservation ownership survives queued, active and completed jobs. Pressure
drains workers and replays exact committed pairs plus unconsumed input pairs to
the existing per-group exact sets. It must not replay only distinct counts,
because those would lose cross-batch duplicate identity. Source-generation,
typed resource-error and cancellation handling reuse the retained native query
boundary. Real spill remains a separately admitted Vortex-run family.

The implementation bounds primary complete-pair entries and final
group entries separately. Both tables share one byte pool, including their
simultaneous capacity while the EOF group reduction runs. The final reduction
keeps all pair identities unchanged until it succeeds; a final-group entry or
byte denial can therefore hand off exact pairs rather than incomplete counts.
Both entry bounds use the admitted group-state item budget. Byte reservation
denial before job admission leaves the current source chunk untouched and takes
the same exact handoff. Typed native allocator denial retries untouched arrays
at most once after partition release; corruption remains an error even if an
unrelated allocation was denied concurrently. A scan-level typed denial discards
the entire attempt and replays once through the same retained source generation.

The EOF group task ranks only complete counts, using a charged heap of at most
`offset + limit` entries and the existing numeric count-descending/key-ascending
comparator. It returns an explicit finalized distinct-count owner to aggregate
state. It never changes the aggregate function or fabricates distinct sets.
The heap allocation is reused as a sorted retained vector; the owner survives
until bounded result rendering finishes. A denied final group table or selection
reservation leaves all complete pairs available for exact-set handoff.

The original source schema decides whether an externally owned aggregate CPU
pool is eligible. Unsupported nullable or non-integer schemas restore provider
CPU drivers without retaining an aggregate pool. Native predicates keep their
existing semantics; residual predicates do not enter the worker family. Source
generation checks remain around the complete execution, including final counts.

Acceptance includes all widths/extrema, dictionary reordering, collisions,
duplicate pairs across source batches, one giant group, global group winners,
ties/offset, exact byte/entry pressure handoff, cancellation and source change.
Complete results require an independent Rust set oracle and public native
query tests. Worker time, pair counts, probes, retained bytes, source reads and
second-reduction work must be reported separately. Provider allocator bypasses,
legacy handoff/output maps and RSS are excluded from owned-capacity claims.

Focused coverage includes cancellation inside long collision probes and table
rehashing, final heap denial/lifetime, native files with reordered renamed schema,
source pressure/corruption separation, source replacement and same-inode mutation.
These tests have been authored but not yet executed for this packet.
