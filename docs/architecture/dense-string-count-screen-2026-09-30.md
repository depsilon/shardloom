# R10: dense single-string COUNT payloads

Status: capacity attribution before runtime admission. Current PERF-INTAKE /
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

If admitted, reuse dense pages with stable ordinals and a compact full-width
directory. Preserve cancellation, allocation-before-publication, old-plus-new
growth credits, full-hash/full-byte collisions, overflow diagnostics, null/schema
admission, EOF-only selection, no replay after fatal source failure, and the
certified native pressure handoff. Test actual ordinary/prepared/owned output
routes and resource failure/recovery, then compare complete Q34/Q35 calls against
the uninstrumented dff85c33 executable. Useful small gains are eligible; memory
benefits must retain acceptable complete-call time. Successful runtime work needs
workspace/native checks, Full43, independent review, PR and exact-head CI.
