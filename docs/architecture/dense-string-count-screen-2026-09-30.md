# R10: dense single-string COUNT payloads

Status: **retain** after full local acceptance; PR/CI acceptance is pending.
Current PERF-INTAKE /
PERF-03/04/06; CG-1 through CG-23 and V1 candidate scope remain unchanged.

The current complete-key string partitions keep a 32-byte record in every sparse
hash slot. Compound partitions already separate compact ordinal directories from
leased dense payload pages. Investigate reuse of that existing implementation,
preserving complete UTF8 equality, checked counts and the existing worker/entry
credit/pressure-handoff protocol. No generic replacement state framework.

First record simultaneous post-drain live slot capacity, occupied payload bytes,
arena used/capacity bytes, and partition-local old-plus-new growth overlap.
Distinguish summed final capacities from a maximum individual growth overlap,
cumulative growth counts and the existing shared-pool high-water mark. Temporary
diagnostic timing is not a ship result. Use Q34/Q35 on the retained artifact and
verify every complete value before deciding whether to prototype.

Vortex-first: `implement_shardloom_kernel` for ShardLoom-owned exact grouping
storage. Keep the existing Vortex 0.85 scan, encoded/native string access and
owned partial boundary. Vortex's grouped count accumulator accepts already
grouped list values; it does not replace this directory's complete-key partition,
lease, spill handoff and output-order contracts. No new dependency, fallback,
decode boundary, provider version or external capability.

The temporary diagnostic `6825f771` passes 17 focused tests and native all-target
Clippy. All 12 complete results in `paired43_20260930T045746743962Z` pass. Both
queries retain 18,342,019 groups and 3,374,058,173 string bytes. Every diagnostic
call ends with 2,147,483,648 live sparse-slot bytes, of which 586,944,608 bytes
hold occupied records: 27.33% occupancy. Simultaneous arena capacity is
4,332,453,888–4,387,241,984 bytes. The maximum single-partition old-plus-new growth
overlap is 179,044,352–182,976,512 bytes; pool-wide peak credits are 6.69–6.73 GB.
The same snapshot covers all drained partitions before release; these are not
cumulative initialization bytes. No handoff occurred. Diagnostic timings are
excluded from speed claims and its runtime counters are removed before screening.

An eight-byte ordinal directory plus the existing 32-byte dense payload has a
calculated end-state cost near 1.12 GB before page slack/metadata, versus 2.15 GB
now. This roughly 1.02 GB structural opportunity admits the prototype, not a
measured RSS or elapsed-time win. Extra lookup indirection is a concrete risk.

Reuse dense pages with stable ordinals and a compact full-width
directory. Preserve cancellation, allocation-before-publication, old-plus-new
growth credits, full-hash/full-byte collisions, overflow diagnostics, null/schema
admission, EOF-only selection, no replay after fatal source corruption, and the
existing typed source-allocation-denial retry. Keep the exact in-memory
prefix/deferred-suffix pressure handoff; explicit disk spill is dispatched
separately. Test actual ordinary/prepared/owned output
routes and resource failure/recovery, then compare complete Q34/Q35 calls against
the uninstrumented dff85c33 executable. Useful small gains are eligible; memory
benefits must retain acceptable complete-call time. Successful runtime work needs
workspace/native checks, Full43, independent review, PR and exact-head CI.

## Prototype screen

Runtime `a33da94f131125b4119603bc9f032c5687e8df6b`, binary SHA-256
`23cca93e0345e7b184dc5991088f8ca2868b41e91ba34f7460e79f7239c93f78`,
compares with the final merged R3.b runtime `dff85c33`. The diagnostic counters
are absent. `DensePages` and exact leased vector allocation are shared privately
by compound and single-string partitions. Compound behavior is unchanged;
single-string directories now hold full-width ordinals into stable dense records.
Existing full hashes, byte equality, checked counts and exact output ties remain.
Test-only benchmark accounting includes dense pages and reports the new directory
slot width, preventing its capacity evidence from silently omitting records.

The alternating cohort `paired43_20260930T051052993489Z` verifies all 12 complete
results. Each route accounts for 99,997,497 rows and 18,342,019 complete groups;
no native handoff occurred.

| Query | Control calls (seconds) | Dense calls (seconds) | Best reduction |
| --- | --- | --- | ---: |
| Q34 | 4.015959, 3.354424, 3.828589 | 3.512020, 2.337639, 2.380164 | 30.31% |
| Q35 | 3.979027, 3.584530, 3.535295 | 2.178589, 2.182601, 2.246142 | 38.38% |

Every matched candidate call is faster. Q34 median falls from 3.828589 to
2.380164 seconds; Q35 from 3.584530 to 2.182601. Shared modeled peak reservations
fall from 6.66–6.75 GB to 5.57–5.68 GB. RSS is mixed for Q34: control
5.28/5.53/5.63 GB versus candidate 4.74/5.65/5.52 GB. Q35 observes control
5.80/6.00/5.90 GB versus candidate 5.62/5.68/5.69 GB. Do not equate the roughly
1 GB reservation reduction with a uniform process RSS reduction.

Before this screen, 800 native local-primitive tests pass, with 11 existing
manual/regeneration fixtures ignored. They include two new cross-page collision
and allocation-denial tests, existing ordinary/prepared owned native results,
typed source-denial replay and corruption failure. Existing in-memory
cancellation/refund tests pass; no new active file-backed UTF8 cancellation
fixture is claimed. Formatting and native CLI/Vortex all-target Clippy pass.
Two test-only Clippy warnings were corrected before the frozen build.

This is the same retained optimized input and Apple M5 Mac17,3/10-CPU/16-GiB
host as R3.b. Complete clocks include CLI startup, all output and exit. Requested
24 GiB is not a physical/RSS cap. Caches are uncontrolled; unrelated host work is
accepted. Preserve all observations, including the slower first candidate Q34
sample. The two-query screen and the suite below remain separate cohorts.

## Full acceptance

The same frozen runtime passes formatting, workspace and native all-target
Clippy, 3,436 workspace tests, 2,029 native tests and 1,520 CLI tests. These
counts overlap; 22 existing manual native fixtures remain ignored. All 258
complete results in `paired43_20260930T051504426196Z` match the retained full-value
oracle. The sum of each query's best of three complete calls falls from
55.420075 to 53.611191 seconds (3.26%). Q34 falls from 3.433073 to 2.266653
seconds (33.98%); Q35 from 3.516848 to 2.377824 (32.39%). Every Q34/Q35 pair is
faster. This is a local cohort result, not a hardware-independent guarantee.

Review includes the negative observations: Full43 Q19 best is 6.08% slower,
Q17 8.42% slower, and Q13 2.18% slower. Q19's caller routing span rises from
0.618–0.622 to 0.953–0.960 seconds, although this patch does not change its
triple-key routing source. A bounded reverse-order follow-up on Q13/Q17/Q19
(`paired43_20260930T052508268460Z`) passes all 18 complete comparisons. Q19's
routing spans then overlap (control 0.588–0.594, candidate 0.592–0.596 seconds),
and its best gap narrows to 0.51% (4.145501 versus 4.166778 seconds). Q17's best
gap narrows to 1.15%; Q13 is 1.16% faster. Both cohorts are preserved; the
follow-up does not replace any Full43 sample or establish a particular cause.
Q13 also retains lower observed RSS, around 1.27–1.29 GB versus 1.67–1.69 GB.

Retain the shared dense pages and leased vector allocator: both target queries
improve materially in the initial and full cohorts, complete values and memory
failure contracts pass, and the larger non-target slowdown does not recur in
the bounded follow-up. The private abstraction keeps existing worker scheduling,
full-key equality, checked updates, order/ties and pressure behavior intact.

The [portable evidence](../benchmarks/evidence/dense-string-count-2026-09-30.json.xz)
contains 300 strict complete comparisons: 12 diagnostic, 12 screen, 258 Full43
and 18 follow-up. It preserves raw member identities, complete sanitized result
envelopes, source manifests/patches, all validation logs and the assembly script.
An independent mechanical audit verifies all 300 complete values/hashes, 50
archives and 1,200 members, 13 source files and two declared deletions, 21 portable
text records and the documented metrics. Primary review owns semantic acceptance;
the retained references are regression oracles, not a new independent SQL oracle.
Temporary capacity counters are absent from the accepted runtime. R2.b remains
the next candidate after this PR lands; the release and format pulse stay closed.

PR review also updates both ignored string-state benchmark fingerprints to include
the shared dense-page helper. This changes test-only source identity, not the
frozen release implementation or any recorded timing. The Full43 source manifest
already includes that helper; historical evidence remains immutable.
