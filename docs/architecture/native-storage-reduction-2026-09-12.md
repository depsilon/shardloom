# Native artifact storage reduction

Status: active exploration requested September 12, alongside the remaining
phased work. This is part of PERF-08/09/10/12 and the existing production writer
codec-portfolio item, not a new engine, scheduler or canonical phase.

The maintainer explicitly requests retention of the faster canonical numeric
probe path. Its two candidate-only full ingests are 90.303309 and 93.945037
seconds versus the previous accepted 95.447305-second control; all three outputs have
the same 18,591,586,804-byte physical representation. Do not reject the small
native change merely because its measured benefit varies. Normal correctness,
resource and final-tree validation still apply.

Storage reduction is a separate experiment. Freeze source/reference identities
and comparison settings within each experiment, then advance controls as faster
retained versions complete validation. The
[control ledger](performance-control-progression-2026-09-12.md) separates the
retained numeric observations, pending combined acceptance and query artifact
profiles. A changed representation cannot borrow
the old file-SHA proof: it must pass complete native value/schema/order comparison,
required physical statistics checks and native query acceptance before retirement
or promotion. No public field, derived metadata or query may be removed to reduce
bytes. One logical native artifact and explicit no-fallback execution remain.

## Execution checklist

- [ ] Attribute physical bytes from the existing footer/layout/segment inventory
  by column and encoding, accounting for shared segments, metadata and gaps.
  Distinguish stored bytes from decoded buffer estimates and compressor work.
- [ ] Review the retained and rejected portfolio decisions before selecting a
  different candidate. Current text uses framed Zstd; current post-coalescing
  numeric compression excludes new dictionary roots. Determine whether actual
  low-cardinality numeric/text inputs or shared derived representations offer
  meaningful byte reductions through existing admitted Vortex encodings.
- [ ] Screen a bounded representative numeric/text/null/skew matrix using the
  product strategy and native readers. Record encoded bytes, encode/decode work,
  exact values, physical statistics, ownership and any lost encoded consumer.
  Compare codec or layout choices individually; do not simultaneously vary CPU
  ownership, worker counts, physical region topology and compression.
- [ ] Implement a capability- and measured-cost-derived admission only for a
  promising profile. Do not select by benchmark query number or column name,
  add speculative metadata, force Arrow materialization or add another writer.
- [ ] Measure the complete 100M-row candidate with existing storage/process
  guards. Record size, complete ingest time, memory and all 129 Full43 execution
  results; classify which scan/filter/group/order workloads benefit or regress.
  Validate complete results and independent held-out renamed schemas.
- [ ] Retain useful gains with explicit lifecycle tradeoffs, or record the scoped
  measured rejection. Smaller storage alone does not establish faster decoding
  or queries; equally, a byte reduction with meaningful lifecycle benefit need
  not be discarded solely because ingest time is unchanged.

Start with physical evidence and bounded feasibility. Preserve failed samples;
do not regenerate an unchanged control merely to populate a fresh table. Promote
completed, validated faster ingest/query versions under the control ledger.
Continue independent phased implementation while screening storage candidates.

## Initial physical evidence

The existing complete physical inspection matches the protected reference's
SHA-256 `93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2`.
It inspected 82,908 unique Flat segments. Summed column/role bytes equal the
global unique total for this artifact, so the following categories are additive
here; that is not a general assumption for shared Vortex segments.

The stored segments include 7,594,925,412 bytes of framed Zstd text and
3,530,957,728 bytes in two plain `varbinview` derived domain fields. Their names
identify the observed evidence, not future admission rules. Statistics account
for only 9,985,968 bytes; eliminating required statistics is neither authorized
nor a material size opportunity. Footer/padding/postscript account for another
7,557,564 bytes. The first candidate should therefore examine compact native
encoding of already-generated text, including dictionary/constant results that
currently fail to survive the probe/coalescing boundary.

This inventory belongs to the protected 18.643-GB reference, not a new reading
of the 18.592-GB ingest output. It is suitable for selecting a bounded feasibility
experiment, not attributing a current timing difference. The source inventory and
derived attribution are retained in `perf-drop-ship-20260905/candidate-5-physical-encodings.json`
and `perf-all-20260906/storage-reduction-inventory-20260912.json` under the existing
machine-local evidence root. No control ingest was rerun.
