# Incoming source dictionary screen — R1.b

Status: **drop at bounded native admission**; no production policy change.
This follows the R7 drop under PERF-INTAKE / RFC 0044. R1.a's derived dictionaries
and R6.a's native sort consumer are already retained. No new release is authorized.

## Opportunity and control

`universal_format_io.rs::parquet_dictionary_schema_plan` disables the existing
dictionary schema hint for sources with at least ten million rows. The official
99,997,497-row source therefore supplies plain source strings. Smaller sources
already request dictionaries for string fields used by derived length/domain
handling. Arrow-to-Vortex conversion preserves those dictionaries; it is an
explicit compatibility-input boundary, not external query execution.

R1.a added a bounded default writer that can preserve economical incoming UTF8
dictionaries before native repartition canonicalizes them. Original source text
uses explicit Zstd field writers that bypass that default; merely enabling the
reader hint does not preserve source dictionaries. Its admission bound compares native
buffer bytes against 16 bytes per row, not the retained Zstd artifact size. Thus
an incoming dictionary can qualify for preservation yet be a worse persistence
choice. Measure actual output before removing the large-source guard.

Pinned Vortex 0.85.0 already supplies Arrow dictionary conversion, Dict arrays,
native code compression and Flat/Zoned persistence. The provider decision is
`use_vortex_native_provider`; no new codec or cross-column layout is needed.
The existing derived-expression code already transforms dictionary values and
remaps codes. Any benefit from reaching that code must be charged to the whole
ingest, not presented as newly implemented expression logic.

## Bounded screen

Compare the retained plain Arrow schema with the existing Int32/UTF8 dictionary
hint for the five fixture columns URL, Referer, SearchPhrase, Title and OriginalURL.
These are experiment projections, not new name-based runtime admission rules.
Read the first 131,072 rows from row groups 0, 113 and 225, matching the current
budgeted source batch. Keep the native writer row-block bound at 262,144 and its
byte target at 8 MiB. Current R1.a full-ingest evidence (`172832Z`) records 817
source batches and the 131,072-row source size; the configured large-source
default alone does not establish the applied batch size. Alternate role order by region.
Keep all measurements; the sample is not randomized or a complete ingest.

Compare three paths: plain input through retained source-text Zstd; dictionary
input through that unchanged writer; and dictionary input with bounded preservation
wrapped around the source-text Zstd override. The third is a test-only composition
of existing native strategies, not a production policy change. Ineligible inputs
still use the retained Zstd strategy. Alternate its two writer cases by region.
Record reader/conversion time once per role and region, and each column's native
encoding, owner bytes, dictionary domain size, admission inputs, actual serialized
artifact bytes and write span. Reopen every output and compare all values, nulls,
dtype and row count. Record physical encoding evidence and final native reservation
release. Provider input allocations remain separate from native reservations.

The ignored benchmark requires an explicit source path. It caps input rows,
individual in-memory outputs, writer reservations and report size; it creates no
on-disk data payload. Run only through the existing local UAT watchdog, with no
concurrent build or engine benchmark. Freeze the test executable, source revision,
script/fixture identity and source generation. Its timings are native sample
observations, not complete-ingest or query-performance claims.

## Admission and retention

The first screen retained exact values in all 45 outputs but raw dictionary
preservation grew the sampled artifacts from 44,434,676 to 46,595,352 bytes.
Before disposition, add one bounded fourth arm: compress only the incoming
dictionary's value domain with the same native Zstd provider, level and frame
bound as the retained text writer, retain its original codes, then apply the
existing preservation admission. Charge domain canonicalization, compaction and
compression separately, alongside writing and source reading. Re-evaluate
admission on the transformed input; do not relax the bound. Keep the raw-domain
arm and all first-screen observations. This is test-only provider composition,
not a codec sweep or production policy. Exact reopened comparison against the
original source remains mandatory for every arm.

## Recorded disposition

Both frozen runs passed the local watchdog with unchanged source/executable/fixture
generations. The initial three-arm screen verified 45 native artifacts; the
four-arm refinement verified 60. Every output's dtype, row count, UTF8 bytes and
validity matched the original native input, both reader roles matched exactly,
and all native reservations released after each region. These are sampled native
artifacts held in memory, not replacement full-size data files.

The refinement at `b6a46fd8044937249024840a4badc6f3e0b790fa` produced:

| Input / writer | Sample bytes | Domain preparation | Native write spans |
| --- | ---: | ---: | ---: |
| Plain / retained Zstd | 44,434,676 | 0 ms | 226.167 ms |
| Dictionary / retained Zstd | 44,434,676 | 0 ms | 206.087 ms |
| Dictionary / raw-domain preservation | 46,595,352 | 0 ms | 196.762 ms |
| Dictionary / Zstd-domain preservation | 42,982,272 | 136.543 ms | 409.889 ms |

Each row sums fifteen column/region artifacts. Dictionary reader/conversion time
was 220.053 ms versus 194.748 ms for plain input; metadata preparation was
4.274/5.281 ms respectively. Reader time is shared across that role's writer arms
and must be charged once, not once per column or arm. These single observations
have uncontrolled host load and cache state; they establish no complete-ingest
speedup or regression. The first screen's raw-domain artifact sizes agree exactly
with the refinement; its timings remain separately recorded, not spliced.

Raw-domain preservation grows bytes by 4.86%. Compressing the values reduces the
sample by only 3.27%; preparation plus writing totals 546.432 ms, versus
226.167 ms for the retained plain writer. Rejected transformed dictionaries pay
domain compression and then whole-column decode/compression, explaining substantial
extra work. The existing bound admits some growing outputs too: SearchPhrase grows
in all three regions, and OriginalURL grows in region 0. A native-buffer admission
bound is not a prediction of persisted Zstd bytes.

Neither variant supplies a credible material storage/ingest case to justify a
full replacement ingest and Full43. Drop these two tested preservation compositions
and the reader-hint-only variant; retain the large-source reader guard and source
text writer. No failed production prototype exists to remove. Keep the explicitly
ignored bounded harness for reproducibility. This does not establish that every
possible source-dictionary policy is unprofitable: the sample covers five columns
and three prefixes. Reopening needs new whole-lifecycle evidence, not removal of
the same guard unchanged. Shared expression work remains the separate R1.c item.

The [receipt](../benchmarks/source-dictionary-screen-2026-09-26.json) records all
column outcomes, guard and build identities, hardware, checks and limitations.
Frozen binaries, fixture sources, full reports and guard receipts are retained
under `/Users/dylan/LocalData/shardloom/performance-candidates-20260926`; no bulk
data artifact was added. Next is R6.b, FSST paired with its encoded predicate
consumer. No complete UAT or runtime speedup is claimed by this dropped screen.

## Gate for a materially different future candidate

Only advance a materially promising result to a production candidate. Reusing
incoming codes must include reader construction, dictionary values/code ownership,
derived expression work, remapping, statistics and persistence. Reject or retain
plain handling for uneconomical columns; do not reinstate the previously dropped
text-codec replacement unchanged.

For any candidate, freeze matched complete ingests against the retained writer
and 15,682,956,116-byte artifact. Require one of the existing gates: at least 10%
lower complete durable ingest with no byte growth or query regression; at least
15% lower whole-artifact bytes with nonregressing ingest and affected queries;
or the Full43 suite gate. Slower preparation needs demonstrated lifecycle
break-even. Sample byte percentages alone cannot meet a retention gate.

Before retention: exact full native value/schema/order/statistics comparison,
complete paired Full43, independent renamed/null/Unicode/dictionary-epoch and
error/resource tests, normal workspace/native checks, review and PR. Preserve
failed evidence and remove rejected prototypes. Logical schemas, no-fallback
execution, native output and explicit unsupported behavior remain unchanged.
