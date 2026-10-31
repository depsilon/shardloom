# Exact adjacent Parquet reads

Status: prospective item-20 candidate after compact grouped-state acceptance.
PR #1630 is awaiting hosted acceptance; its retained local 347-query baseline
is 11.997152207 engine seconds with 12,565 complete result rows. No timing or
benefit is claimed for this candidate.

## Current operation and selection

`universal_format_io/demanded/chunks.rs::SelectedChunks::read` validates and
reads each demanded compressed column chunk separately. All selected chunks
for the row group are retained before decoder construction. Each read enters
the existing `OwnedLocalRead` reservation, generation, cancellation and shared
I/O admission path. Metadata inspection of the original Parquet inputs shows
physically adjacent column chunks, including the dimension tables and adjacent
numeric, temporal and nested leaves of the fact table.

The hypothesis is to remove positional read calls and their repeated setup for
exactly adjacent demanded chunks. It does not avoid decoding, reduce selected
compressed bytes, or promise a large grouped-aggregation gain: most single-value
grouped cases demand separated keys and measures. Complete-query evidence is
required even if request counts fall.

The earlier scoped dynamic-demand proposal needs a different contract. Its
global incomplete-analysis guard belongs to static preparation, while deferred
subqueries are created only in execution binders. The two dynamic execution
callers do not run `apply_source_demand`; they copy complete prepared source
owners into an already-created input executor. Changing the static guard alone
therefore does not activate on that path. Execution-phase narrowing remains
deferred until consumed sources, replay, root discovery callbacks and the input
executor's ownership can be addressed explicitly. No patch or query run was
made for that guard-only proposal.

## Provider decision and implementation boundary

Vortex-first decision: `implement_shardloom_kernel`, inside the existing
compatibility-import read owner. Pinned Vortex 0.90.0 exposes positional reads
and `CoalesceConfig`; the admitted native file provider already uses its local
configuration. Its 4 MiB maximum is a suitable upper bound here, but its default
1 MiB gap allowance is not: this candidate permits a gap of exactly zero.
The synchronous Parquet `ChunkReader` consumes existing credited `Bytes` slices;
introducing another async provider or query engine is unnecessary.

Keep `OwnedLocalRead` authoritative for actual reads, memory credit, source
generation and cancellation. Use the current I/O envelope and available memory
to cap a candidate combined read below the local 4 MiB maximum. This capacity
observation is advisory; actual shared admission remains authoritative and may
refuse concurrent pressure. Do not retry an I/O or reservation failure through
an alternate execution path. Add no worker, prefetch, payload cache, dependency,
public setting, data format or external-engine delegation.

## Exact admission and invariants

- Consider selected leaves in their original metadata traversal order. Join only
  a nonempty chunk whose start equals the preceding selected chunk's end. Do not
  sort, overlap, bridge unselected bytes, cross row groups/files, or broaden a
  selected range. Keep zero-length, nonadjacent, reverse-ordered and over-limit
  chunks on the existing individual read path.
- Keep one original descriptor and decoder-visible slice per selected chunk,
  including when multiple slices share one credited allocation. `get_read` and
  `get_bytes` must still reject crossing an original chunk boundary; a combined
  physical read must not broaden decoder access.
- Charge the actual combined capacity once through the existing byte owner.
  Retain that credit until its last slice/alias is released. Metadata remains
  bounded by the already-admitted selected chunk count. Preserve independently
  owned native output and complete final refunds; no unaccounted staging buffer.
- Keep source-generation checks around every actual positional read and
  cancellation at the existing reader boundary. Bound added combined capacity
  and cooperative cancellation latency. An already-large individual chunk keeps
  its original handling; this change does not increase its limit.
- Lookahead must not report a later malformed chunk before completing the
  earlier selected read. An invalid prospective neighbor ends the combined run;
  ordinary traversal then reports that metadata error in its original place.
  Never suppress corruption, shorten required validation, or change selected
  rows, row-group/prefix admission, decoder semantics or result order.

## Proof before performance

Native controls must use real files and verify exact bytes plus actual read
requests for adjacent ranges, separated ranges, metadata order, nested leaves,
multiple files/groups, empty demand and both sides of the combined-size limit.
Check original per-chunk decoder access after coalescing, no reads of gaps,
invalid signed/overflow/out-of-file metadata, and malformed later neighbors.

Cover low-memory refusal, a tight custom I/O envelope, cancellation, source
mutation/truncation, failed-read cleanup, partial owner release, and the final
alias refund. Preserve existing demand, prefix, ranked, replay, join, native I/O
and controlled-Parquet transfer tests. Evidence must distinguish logical
compressed bytes from successful positional request counts; neither is a
physical-device or cold-cache measurement.

Add fresh complete public-interface cases with independent expected values,
multiple source sizes/layouts and three formats. Include adjacent and separated
projections, mixed aggregation, joins, nullable/rich values and retained output
lifetime. Parquet cases must expose the actual request/byte observation; Vortex
and Arrow controls preserve their existing owners. Declare the grid and reference
identities before its native executions. Run required default/native formatting,
lint, workspace and installed-package checks before the performance decision.

## Complete-cohort decision

Freeze the implementation, tested runtime, original unchanged 347-case corpus,
five resident inputs, references, baseline and standard supervisor. Run one
isolated execution of every case after all other task jobs and agents finish.
Keep full hashing and recursive inventories outside query execution. No routine
repeats, cache manipulation, artificial pacing, regenerated cohort, new competitor
execution or Full43 pass is part of this experiment.

Retain only if the complete native engine total is lower than 11.997152207
seconds and correctness, ownership, resource and no-fallback controls pass.
Higher or unchanged total drops the runtime change. Preserve all observations,
case/family regressions, partial attempts, changed supervision and failed gates;
request reduction alone cannot retain a slower complete query cohort. Full43
stays held until item 18. The broader item-20 queue and CG-1 through CG-23 remain
separate obligations, with no publication authorized here.

## Recorded disposition — October 31

The experiment above is **dropped**. Its one complete 347-query observation
passes all 12,565 rows but records 12.028866368 engine seconds against
11.997152207. Requests fall in 16 cases, with unchanged compressed bytes, but
the complete engine total is higher. The runtime changes and experimental
tests are removed; accepted product sources and the baseline are restored.
The [complete record](../benchmarks/native-adjacent-parquet-reads-2026-10-31.md)
retains the implementation, all correctness checks, corrected empty-file
fixture, failed attempts, unchanged protocol, per-case observations and
portable replay. This section records the outcome without rewriting the
original prospective contract or reclassifying the performance gate.
