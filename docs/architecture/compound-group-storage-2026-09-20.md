# Compound group storage attribution

Status: diagnostic screen and conditional candidate, not retained performance.

This follows PR #1453 under PERF-03/06/09/12 and RFC 0044. Q17 already
uses exact complete-key partitions; adding partitions or removing a recount
would duplicate shipped work. Broad CG-5/6/8/14 and the 116 unchecked phase
items remain open.

The retained binary is `b6b92497d7ec1fff26caa4411512320d9f0fc97f`, from
merged main `606808b6cf21899cc7dcb467bc80a4858f46245d`. Latest Full43
Q17 calls are 9.345954, 8.920590 and 9.266660 seconds, with the fastest
call peaking at 4,918,951,936 OS RSS bytes. All three complete values pass.
These unpaired samples establish current observations, not a regression.

## Attribution and conditional design

Three all-worker stack samples of that retained binary passed full-size
complete-result validation in `full43_20260920T111911266024Z`. The
reducer's inlined code remains unresolved. Blocking samples are separated;
sampled stack residence is not exclusive CPU time or additive wall time.
The external `q17-stack-attribution.json` preserves all samples and hashes.

The source stores complete records in every slot of a half-full open-addressed
table. Three diagnostic calls in `full43_20260920T112254013613Z` establish
the post-drain storage before selection. Compiler layout is 48 bytes per group
and 32 bytes per string-table slot. There are 67,108,864 group slots for
24,070,560 groups and 33,554,432 string slots for 8,825,862 strings.
These two tables occupy 4,294,967,296 reserved bytes; the separate text arena
retains 532,256,559 live bytes. The calls pass complete-value validation.
The external `q17-storage-diagnostic-audit.json` verifies binary, archive and
result hashes. Diagnostic timing is excluded from retention evidence.

Keeping the measured record widths and existing sparse directory capacities,
dense payloads plus `usize` indices model 2,243,120,832 bytes: a
2,051,846,464-byte opportunity before page metadata and transient growth.
At 1,024 items per page, unused final-page payload is bounded by 5,237,760
bytes across both types and all partitions. This is a storage model, not
measured RSS. It admits the following bounded prototype.

Keep complete group and interned-string records in dense, reservation-owned
pages and store native `usize` indices in the existing sparse directories.
The now-unnecessary string-slot occupied flag is removed. Retain the
same hash routing, partition count, load factor, integer signedness, full-width
offsets, lengths and counts, exact byte equality, interning and entry credits.

Pages avoid moving records when the lookup directory grows. The first page
starts at 16 records and doubles through 1,024; later pages hold 1,024 records.
Ordinals remain stable through first-page relocation. Reserve metadata, payload and
simultaneous old/new directories before allocation. Complete page admission
before interning and publishing a group. Denial preserves committed state for
the existing COUNT pressure handoff; committed UTF8 DISTINCT pressure remains
an explicit error after draining workers. Cancellation and overflow remain explicit.
Final selection and replay iterate committed records. The same storage must
preserve the already admitted UTF8/integer DISTINCT consumer.

Vortex-first decision: `implement_shardloom_kernel` inside the existing
ShardLoom-owned exact aggregation directory. The pinned Vortex 0.85 native
scan, Primitive, Dict and VarBinView providers remain unchanged. Upstream
providers supply typed arrays and owned buffers, not this mutable exact
aggregate directory and its pressure contract. No new engine, dependency,
decode boundary, output format, query dispatch or public capability is added.
The universal compact-state replacement remains parked; this screen concerns
measured empty-slot payload storage in one existing compound family.

## Acceptance

The extra directory-to-payload indirection can cost CPU time. Page metadata
and allocator overhead can weaken the modeled memory saving. Retain only
after fastest valid paired complete calls save at least one second, or reduce
OS peak RSS by at least 30% without regressing complete time. Preserve every
sample symmetrically. Diagnostic timing is excluded from that comparison.

Verify collisions, signed/unsigned extrema, duplicate domains, page and
directory growth, exact Top-K ties and offsets, DISTINCT output, reservation
denial before publication, pressure prefix/suffix replay, cancellation,
overflow and release. A winner requires full 43-query UAT, the workspace and
native gates, independent review, PR and merge. A failed prototype is removed
and its source-bound evidence retained before selecting the next target.
