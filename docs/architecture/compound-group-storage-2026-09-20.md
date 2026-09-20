# Compound group storage attribution

Status: retained candidate; paired material gates, Full43 and broad checks passed.
[PR #1454](https://github.com/depsilon/shardloom/pull/1454) carries this change.

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

## Paired candidate result

Frozen candidate `dc4ce81a54b89540d71edaa1e0c86b5dab7a5223`, SHA-256
`494ae33284ed864b1387f40c36b139a04455748a1676464c73cf6e25e2706e26`,
passes all 24 complete outputs in `paired43_20260920T113330374131Z`.
The unchanged 99,997,497-row native Vortex artifact is 18,591,586,804 bytes.
The machine is Apple M5 with 16 GiB physical RAM and macOS 27; requested
parallelism is 12, the host ceiling is 10, and the query policy is 24 GiB.
The policy is not an OS RSS limit. Cache and concurrent load are uncontrolled.
The comparison includes fresh native CLI startup, complete output and exit.

| Query | Fastest control | Fastest candidate | RSS at those calls, control / candidate |
| --- | ---: | ---: | ---: |
| Q17 | 4.072863 s | 2.579536 s | 5,345,722,368 / 3,097,083,904 bytes |
| Q14 | 1.521034 s | 1.411049 s | 3,700,244,480 / 1,822,998,528 bytes |
| Q15 | 1.413437 s | 1.298642 s | 2,576,072,704 / 1,460,715,520 bytes |
| Q11 | 0.751381 s | 0.730331 s | 578,043,904 / 289,161,216 bytes |

Q17 saves 1.493328 seconds (36.7%) and 42.1% of OS peak RSS at the
independently selected fastest calls. Both gates pass. Its candidate user CPU
is 19.586794 versus 18.738044 seconds, while system CPU falls from 5.034320
to 0.560405 seconds. Thus the evidence does not establish lower user-instruction
cost: extra indirection is a tradeoff. The complete-call and memory gains
justify retention with the validation below.

Both roles preserve Q17's 99,997,497 committed rows, 24,070,560 complete
groups, 8,825,862 interned strings, 64 partitions, and zero handoffs/retries.
The three other queries cover the same current COUNT/DISTINCT family. Their
observations are retained without attributing every RSS difference to table
storage. All samples, timing/OS records, output archives and source generations
are preserved and independently replayed by `audit-q17-dense-paired.py`.

## Full-size regression evidence

Full43 `full43_20260920T113531997317Z` passes all 129 complete results
on the same frozen candidate. Only Q11/Q14/Q15/Q17 use this compound
storage family. Q11 and Q14 preserve 1,188,468 and 10,681,408 complete
integer/text pairs respectively; their final distinct group counts remain
165 and 6,019,102. All source generations, archives, complete results and
timing/RSS records are replayed by `audit-q17-dense-full43.py`.

The observed best-of-three query sum is 64.551416 seconds. This full-suite
run is unpaired and does not establish a whole-suite causal speedup. Q17's
complete-call retention uses the paired comparison above. The input artifact,
ingest evidence and physical storage format are unchanged.

After this candidate merges, profile Q10 mixed-measure exact DISTINCT,
observed at 4.447893 seconds in the new Full43. It already uses packed-pair
preunion and chunk group partials; neither is a new proposal. Attribute pair
construction/union, measure updates, ownership and final reduction before
admitting a further candidate. Preserve the same material gates and scoped
drop policy. Q29/Q19/Q34/Q35 and the wider profiling inventory remain open;
this selected next screen does not close those obligations.

## Validation and limits

All 34 focused compound cases pass, including six new page-growth, exact
pair/domain, pressure and cancellation cases. The workspace passes 3,424
tests; the native `release-user-surfaces` suite passes 1,865 with nine existing
benchmark fixtures ignored. Formatting, workspace Clippy and native CLI/Vortex
all-target Clippy pass. Independent runtime and paired-evidence reviews found
no remaining actionable issues. A Clippy follow-up changes one rustdoc line
and test-only style/assertion code; execution logic remains identical to the
measured source. The exact overlay and proof are preserved in the
[machine-readable evidence](../benchmarks/compound-group-storage-2026-09-20.json).

Dense pages and both directories retain allocation leases, including simultaneous
old/new capacity during growth. These reservations exclude allocator overhead,
provider allocations outside the owned hook, legacy handoff maps and total
process RSS. COUNT pressure replay remains exact; broader compound/DISTINCT
spill transitions are not added. No new ingest, storage-format, production
fairness, whole-family completion or competitor claim follows from these results.
