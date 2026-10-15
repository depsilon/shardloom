<!-- SPDX-License-Identifier: Apache-2.0 -->

# Capacity growth during native dictionary binding

Status: dropped after the frozen complete-operation screen under
PERF-01/03/04/05/06. All 80 calls preserve complete values, but Q19 improves in
only five of ten target pairs and its median is slightly slower. Q22's control
median also exceeds the original 5% limit. The
[decision report](../benchmarks/native-aggregate-dictionary-growth-2026-10-15.md)
preserves every observation and the removed candidate. The accepted selected-view
runtime is restored; PR #1556 integration is recorded separately.

The contract below was frozen before timing. Its original bytes are retained
with the candidate source overlay; no threshold was changed after measurement.

## Source evidence and provider decision

`aggregate_direct_utf8_dictionary_interner_ids` currently reserves the entire
incoming dictionary cardinality before looking up existing strings. A repeated
dictionary can therefore grow both the global identity table and value directory
without adding a new value. The October 15 Q19 diagnostic sample includes this
binding path and string-table reserve/rehash frames. Sampling does not establish
how much growth is avoidable, and its durations receive no benchmark credit.

The earlier P033-5 rejection stays intact: source-backed ownership, cached chunk
hashes, miss-only ownership promotion and exact preunion already exist. This
candidate changes when capacity grows; it does not claim those earlier mechanisms
again or reopen the slower triple-sort replacement.

Vortex-first decision: `implement_shardloom_kernel`. Reuse the pinned Vortex 0.85
dictionary/native view owners and `Utf8DictionaryValue`. The upstream grouped
accumulator consumes already grouped native arrays; ShardLoom owns this existing
cross-chunk string identity table and its stable IDs. No new provider, encoding,
allocator, scheduler, dependency, Arrow conversion or execution engine is added.
Native input/output, source pruning, capillary units, PulseWeave admission and
late materialization retain their current contracts.

## Finite change and risks

Remove the speculative incoming-cardinality reservation from dictionary binding.
After the existing exact borrowed lookup proves a missing value, reserve one map
entry and one retained-value slot fallibly before promoting ownership or inserting.
Preserve dictionary entry order, exact equality, source-owner lifetime, stable
first-seen IDs and the existing forget/reinsert rule. Declined non-dictionary or
null-bearing accessors keep their current behavior. Other callers of
`intern_dictionary` receive the same capacity check before new insertion.

Incremental growth can require more initial table rebuilds than one bulk
reservation on mostly-new dictionaries. Complete query measurements, including
other shared interner callers, decide retention. The worker partition tables,
routing, codec work, source schema and result format do not change.

Container capacity checks do not establish an allocator-wide or RSS limit.
String payload promotion, returned ID vectors and unrelated metadata retain their
existing resource scope. This work does not close broader memory/spill coverage.

## Regression and frozen comparison contract

Before candidate timing, prove that duplicate dictionary binding preserves IDs,
owners and both container capacities; the capacity assertion must fail on the
baseline. Cover mixed and duplicate entries, empty/declined accessors, source
teardown, forgotten IDs, checked capacity refusal, and the affected grouped,
sketch, exact-histogram and DISTINCT consumers. Preserve complete typed values,
null behavior and deterministic error paths.

Freeze the exact baseline/candidate sources and executable identities, current
resident input, complete query/reference files and driver before timing. The
baseline may reuse the accepted selected-view executable only after proving that
its runtime sources match; the separate notice-description correction changes
package metadata only.

Use ten alternating complete public-call pairs for Q19 at requested parallelism
12. Controls use five pairs each for Q19 at parallelism 1 and Q17/Q22/Q23/Q29/Q2
at parallelism 12. Keep the current 99,997,497-row native input and 24-GiB grant.
These repetitions are the targeted experiment, separate from the final one-call
per-query Full43 acceptance.

Engine execution is primary: require a lower target median, positive median
paired saving and at least nine improved pairs out of ten, without a minimum
percentage or second saving. Target process CPU median may rise at most 5%.
Each control engine median may rise at most the greater of 5% or 5 ms. Each
case's RSS median may rise at most the greater of 25% or 256 MiB. Preserve every
sample, failed run and original threshold. No automatic retries or replacement
observations are admitted.

Validate every complete value, schema, grant, native certificate, source generation
and false fallback field. Keep process wall time, result serialization, remaining
process work, CPU, RSS and supervision separate. Use serialized native jobs under
the existing storage, workload and process-group guards. Do not reingest for this
state-only change, insert pacing, or gate on battery/thermal readings.

A failed gate removes the candidate runtime and preserves its evidence. Retained
code requires focused and broad workspace/native checks, direct/public workflows
and one Full43 pass before a coherent PR. Batch related optimization decisions;
continue the remaining breadth work before native transport. CG-1 through CG-23,
the six-area completion contract and all eight conditional investigations keep
their separate owners. No release is authorized by this candidate.
