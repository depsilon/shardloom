# Derived dictionary persistence — R1.a

Status: admission and prototype in progress; `claim_gate_status=not_claim_grade`.
This is the first experiment in the [September 26 intake](performance-candidate-intake-2026-09-26.md).
The maintainer authorized the complete queue. No retained speedup is claimed yet.

## Frozen workload and acceptance

Control source is merged `6db17c9f`, portable release profile with
`shardloom-cli/release-user-surfaces`. The source is the resident 99,997,497-row
`hits.parquet`; ingest uses P4 and 24 GiB, with the existing guarded runner.
The retained 18,591,586,804-byte native artifact is the value/schema/statistics
comparison reference. The candidate changes existing dictionary persistence;
source text Zstd, numeric data compression, worker counts and input batch policy
remain the comparison controls.

Retention requires one of the intake's frozen ingest, storage or suite gates:
10% lower complete durable ingest with no larger artifact; 15% fewer artifact
bytes with no ingest/affected-query regression; or 10% lower matched Full43
best-sum and 5% lower geometric mean without material family regression.
Charge any slower preparation to a demonstrated reuse workload before conditional
retention. Retain every sample and compare symmetric fastest valid complete calls.
All values, schema, row order, required statistics, native reopen and full query
outputs must pass, alongside independent renamed/null/Unicode/dictionary-epoch
fixtures and resource/error cleanup. Actual RSS is separate from reservations.

## Source and provider evidence

`EmbeddedUrlDomainInt32Builder` already emits Arrow dictionary arrays, and native
Arrow conversion preserves their Vortex Dict representation. The retained writer
does not preserve it end to end. Vortex 0.85 `RepartitionStrategy` canonicalizes
its emitted ChunkedArray even with `canonicalize=false`; that flag controls an
earlier per-input canonicalization. The default leaf enters this repartition
before its dictionary probe. The probe then uses an empty edition whitelist,
so enabling a dictionary probe alone cannot reuse the producer's dictionary.

The existing native Flat serializer accepts Dict arrays, the existing native
Zoned writer computes required statistics without replacing its input, and the
aggregate accessor already consumes Vortex Dict values/codes. Use those providers
before inventing a layout or another execution path. A candidate can preserve an
economical existing UTF8 dictionary before repartition, compress only its codes,
and keep the same dictionary values/epoch. No global dictionary, answer sidecar,
foreign encoding, dependency upgrade or external engine is involved.

Admit only existing UTF8 Dict chunks within the current row-block bound and a
conservative encoded-buffer cost bound. Other chunks use the retained native
strategy. Each preserved chunk has its own native zone, whose block size covers
exactly that chunk; no statistics are reused across dictionary epochs. Native
buffer owners and their allocator reservations survive through serialization.
The existing bounded source-batch writer controls outstanding input lifetimes;
this experiment adds no cross-batch queue.

## Evidence log

- Exact retained-file metadata confirms 18,591,586,804 bytes and full local
  allocation. Read-only physical inspection first hit its explicit 100,000 Flat
  reference limit. Add a bounded caller-selected inspection limit and repeat;
  do not treat the failed partial inspection as a completed inventory.
- Completed repeat: SHA-256
  `7181c2e578659910da176ff6c0dcfe7ce563405337f3ae88cd44e7932d92a266`,
  160,132 unique Flat segments, 18,577,006,684 segment bytes. Both derived URL-domain
  fields are plain `vortex.varbinview`: 1,781,937,792 and 1,770,608,144 bytes,
  totaling 3,552,545,936 bytes. This is current-artifact evidence, replacing the
  older file's attribution for this experiment.
- Recovered the rejected recipe: native Zstd of expanded helper text, 100.521644 s
  complete ingest and 15,466,554,020 bytes. Complete values/statistics and all 129
  outputs passed, but ingest and the unpaired query screen were slower. Preserve
  its historical drop; dictionary survival is a different mechanism.
- Independent design review found signed Arrow codes would miss the unsigned
  native accessor, and shared leaf callers without bounded input ownership would
  admit unbounded chunk scheduling. The prototype uses a checked native unsigned
  cast and is installed only inside the source-batch-bounded stream route.
- Evidence root: `/Users/dylan/LocalData/shardloom/performance-candidates-20260926`.
  Individual receipts will record binary hashes, source identity, full command,
  physical encoding counts and complete process timing.
