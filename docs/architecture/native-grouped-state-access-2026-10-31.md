# Compact grouped-state access and count proofs

Status: retained and merged in #1630 as `3b4c7d92` inside item 20, following the
completed
[48-case attribution](../benchmarks/native-grouped-cost-2026-10-31.md). The
[complete acceptance observation](../benchmarks/native-grouped-state-access-2026-10-31.md)
is 11.997152207 engine seconds for 347 queries and 12,565 result rows, against
the unchanged prior observation of 12.055132613 seconds. All 57 hosted checks
pass; the [closeout](../benchmarks/evidence/native-grouped-state-closeout-2026-10-31/closeout.json)
retains hosted acceptance and verified retirement. The original prospective
contract remains retained byte-for-byte in the evidence packet; the requirements
below are unchanged.

## Hypothesis and provider decision

The accepted optimized library still selects a dense page, loads its descriptor
and checks page/element bounds on each typed state access. Paired extrema also
repeat checked count increments after whole-batch headroom and count-equality
admission. Bind a single-page payload as a mutable slice once per measure or
paired traversal, and use one proven next count for the paired extrema and
already-admitted COUNT. Keep ordinary paged access for larger payloads and
ordinary checked measure execution when paired admission declines.

The comparator is a native columnar-engine maintainer reviewing both ordered
semantics and optimized machine code. The proposed saving is less state-access
work, not parallel execution or a different aggregation algorithm. The diagnostic
does not predict a saving across all 347 queries.

Vortex-first decision: `implement_shardloom_kernel`, extending the existing
ShardLoom flat grouped-state owner. The pinned Vortex 0.90.0 provider and the
[provider inventory](vortex-public-api-inventory.md) preserve native array and
validity preparation. Upstream grouped kernels consume already-grouped arrays;
they do not replace this owner's flat input, ordered accumulators, reservations
and first-error contract. No provider, dependency, public feature, Arrow
execution substrate or external-engine delegation is introduced.

## Boundaries that must remain equivalent

- Only the existing typed COUNT, integer SUM/AVG, strict/extended floating
  SUM/AVG and signed/unsigned/floating extrema payloads receive slice binding.
  All arithmetic, selected rows, value order and measure order remain unchanged.
- A slice borrows the current leased page. It allocates, copies and caches
  nothing; no growth occurs during the borrow. Empty or one-page owners may
  bind; multi-page owners retain checked page indexing. Different measure
  widths can cross the page boundary independently. Growth and restoration keep
  their current admission and rollback behavior.
- Paired MIN/MAX uses the existing exact source/domain, filter, validity and
  completed float-policy proof. For every group, starting counts must agree and
  be at most `u64::MAX - incoming_rows`, including any admitted COUNT. After
  admission, each non-null row increments the shared count once and publishes
  that count to the admitted states. Nulls do not increment. No wrapping or
  saturating arithmetic is permitted.
- Preserve strict nonfinite rejection, extended NaN and signed-zero comparison,
  retained value bits, compensated integer summation and output ordering. A
  declined proof retains the original measure-first error and partial state.
- Keep cancellation checks at their existing boundaries and row intervals.
  Failed/cancelled execution must release every owner normally; neither slice
  binding nor proof consumption adds a reservation or retains input ownership.
- Unknown domains, changed schemas/types, filters and encoded/pending validity
  retain their existing execution or deterministic decline. Native Vortex
  input/output, explicit no-fallback certification and current spill boundaries
  are unchanged.

Use a small private compile-time dispatch around the existing loops so that
slice-versus-page selection occurs outside row traversal. Do not replace it
with a per-row enum or trait-object dispatch. Debug-test observations may count
bindings and consumed proof rows; shipping builds must have no such counters.

## Required proof before timing

Extend existing dense-page and typed-state tests with zero/one-page binding,
first-page growth, the exact 1,024-element boundary and multi-page decline;
exercise state widths independently. Compare complete accumulator state and
results with the ordinary native reducer across integer widths, strict/extended
floating values, nulls, filters, changed domains and both extrema orders.
Retain the existing restoration, allocation-denial and last-alias tests.

Add last-safe count-headroom, one-past-headroom, unequal-count and COUNT validity
controls. Cover both admitted and declined paired routes on one and multiple
pages, cancellation after state allocation, full-pool pressure and final refund.
Verify actual route events and inspect the new optimized closures to establish
that the alleged page/count work was removed. Preserve any failure and its
source revision; a test observation alone is not a speedup claim.

Run focused native checks followed by required formatting, default and native
lint/workspace checks, source Python tests and installed-wheel acceptance.
Public-interface controls must compare complete schemas/values across SQL,
query/DataFrame/connection entrypoints and Vortex, Parquet and Arrow files,
including page-boundary sizes, mixed measures and composed projections/renames.
Use the existing independent expectation and package-proof patterns.

## Complete-cohort decision

After correctness/resource/public/package gates, freeze the candidate source,
build receipt, CLI/native-library identities, exact original 347-case corpus,
five resident inputs, all independent references, baseline observation and
protocol. Run exactly one isolated execution of each case using the established
bounded cohort supervisor, 2 GiB grant and maximum parallelism two. Keep other
task jobs and agents idle; perform full hashes and recursive inventories only
before or after queries. No external competitor execution, regeneration, cache
manipulation, artificial pacing or routine repeat is part of this comparison.

Use the unchanged complete-cohort gain/loss rule: lower total engine time ships
after all other gates pass; unchanged or higher drops. Preserve every case,
family regression, failure, partial attempt and original baseline. No minimum
percentage gain applies. If dropped, remove only this candidate's runtime
changes and retain the diagnostic and complete evidence. Full43 stays held
until item 18; CG-1 through CG-23 and release publication remain separate gates.
