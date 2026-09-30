# R10: dense single-string COUNT payloads

Status: capacity attribution admits a bounded dense-page prototype. Current PERF-INTAKE /
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
