<!-- SPDX-License-Identifier: Apache-2.0 -->

# Resident join attribution and validated variable copying

Status: finite local acceptance and original/relocated portable replay pass at
`ffe0d077a676fd8b23cfebed9e11bf189e1ae459`; hosted integration remains pending.
This closes one attribution and copy investigation within the
[propagation follow-on](native-optimization-propagation-2026-11-15.md), not the
broader optimization-coverage program. The [complete report](../benchmarks/native-resident-join-attribution-2026-11-16.md)
retains every observation and the original decision.

## Observed cost and selected owner

The earlier 500.383376588-second join-family total is complete-query time. It
does not establish exclusive join cost. A separately frozen, test-only native
observer now attributes 54 complete requests across IPC, Parquet and Vortex.
All 1,659 result rows match their original independent references. The
instrumented engine total is 7.736501631 seconds: probe input accounts for
2.491412065, output gathering/validation 2.123419020, matching/pairs 1.719474851
and downstream consumption 0.810047332. These observations are diagnostic;
they are not a production performance comparison or a weighted estimate for
the full join population.

Three additional debug work observations select the largest gathering request
per format. Their six inner nodes copy 4,715,742 variable-width cells,
4,929,885 numeric cells and 1,274,287 scalar cells. Existing direct numeric
gathering handles 1,501,657 numeric cells, all from one physical chunk; no
dictionary-copy path executes. The entire gathering time cannot be attributed
to variable copying from these counts.

## Shared implementation contract

The production change stays in `VariableValues::copy_column` and the existing
`take_column_with_policy` dispatcher. It preserves the generic builder's full
first value-validation/sizing pass, then copies the same immutable native
bytes without constructing typed scalar values a second time. Checked Vortex
arrays already reuse a UTF-8 validation receipt; repeated UTF-8 decoding is
not claimed for those arrays.

Allocation and failure order remain nullable validity, offsets, complete
validation/sizing, payload allocation, ordered null/type checking and copying,
then finalization. Checked arithmetic, both cancellation passes and final
refunds remain required. Receiving buffers are independently charged; the
result does not retain the producer's bytes to avoid its copy. Selection,
casts, canonicalization and the generic callback builder retain their
existing contracts. There is no dependency, public routing or fallback change.

Pinned Vortex 0.93's variable-buffer compactor can retain/slice the source or
return the original array. That does not satisfy this owner's independent
receiving-buffer contract. The implementation therefore reuses ShardLoom's
existing fallible typed-buffer construction and Vortex arrays. This is a
bounded native kernel inside the current owner, not a new execution family.

All cost scopes and work counters are test-only. The attribution observer
requires explicit opt-in and is registered with the native test inventory;
release CLI/library execution contains no diagnostic timing scopes.

## Acceptance and limitations

Six new copy controls cover complete values and physical offsets/validity,
constants/nulls/binary/text, selected encoded inputs, malformed text and error
precedence, allocation refusal, both cancellation passes, escaping children
and independent final refunds. Default/native workspace tests pass
3,344/7,308 cases. All 42 native ignored tests are classified: 39 deliberate
opt-ins and three non-Unix refusal tests. Strict default/native lint,
formatting, all-feature checking, fresh release compilation and all 455
installed-wheel tests pass.

The frozen production comparison executes each of 403 original requests once
per runtime: 197 join and 206 neighboring requests across five interfaces and
three formats. Complete engine time decreases from 60.778284678 to
60.597904154 seconds, retaining the candidate under its original rule. All
186 individual regressions remain recorded. This roughly 0.3% difference is
small and establishes neither material improvement nor repeatability.

Both sides run fresh under the same bounded active supervisor; no baseline
observations are reused. Recursive hashes and inventories occur before/after
execution. That explicit supervision change is separate from the code change.
The baseline build predates the merged status documentation; its complete
source differences are classified, rather than described as an exact merged-
head build. This copy comparison adds no full10k, full11k, Full43 or competitor
execution. The later, separately authorized
[Polars observation](../benchmarks/polars-propagation-once-2026-11-16.md)
has its own frozen requests, failures and clock boundaries.

The first diagnostic attempt failed after 31 passing requests because a
floating value was re-encoded one ULP away from the original native JSONL.
The corrected diagnostic transports the original JSONL text and parses once;
the comparison tolerance is unchanged. Both attempts and all build/test/
packaging failures are preserved. Portable replay reconstructs every complete
production packet, the corrected diagnostics and the gather observations from
archived source and evidence, with original-path reads, native imports,
processes and network forbidden. Original and relocated replay pass, as do
126 fresh-packet corruption controls, seven decision-boundary controls and
19 archive/manifest corruption controls.

## Subsequent performance priority

The November 16 maintainer clarification applies to choosing new work:
**require credible material potential before starting another optimization**.
Useful existing behavior may remain, and a smaller-than-expected positive
result does not by itself force a drop. This does not rewrite the frozen
comparison above or the earlier failed propagation/payload-mapping decisions.

Screen join input and intermediate-result volume, repeated scans/builds,
expensive sorting and spill work first. For each candidate, establish actual
exposure, the shared owner, measured cost and how much work could be removed.
Do not start a small accessor/bookkeeping change merely because it might make
the total slightly lower. Drop unsupported or implausible proposals from the
active investigation pool; do not revive rejected mechanisms unchanged.
No fixed numeric definition of material was supplied, and none is inferred.

The performance priority precedes the previously next spatial unit; required
spatial support and deep traversal remain on the feature roadmap. Optional
summaries, codec choices and new CPU fusion need the same material-potential
screen before new performance work. Item 19, item 20, RFC 0044 and CG-1 through
CG-23 retain their independent obligations.
