# Incoming source dictionary screen — R1.b

Status: bounded native admission experiment, not retained runtime behavior.
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
