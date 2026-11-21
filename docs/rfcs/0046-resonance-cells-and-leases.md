<!-- SPDX-License-Identifier: Apache-2.0 -->

# RFC 0046: Resonance cells and leases for bounded shared live preparation

## Status

Proposed design and Vortex-first provider review (November 21, 2026) under the
[current queue](../architecture/phased-execution-plan.md#current-execution), following
local retention of [RFC 0045 (Chrysalis)](0045-partial-native-source-coverage.md) and the
[Chrysalis and Resonance design intake](../architecture/native-chrysalis-resonance-cells-2026-11-16.md).
This RFC records architecture, provider classification, bounded interfaces, and the
prospective attribution gate. It includes no runtime implementation, automatic request
coalescing, fallback execution, package publication, performance claim, or closure of
items 19/20 or CG-1–CG-23.

## Summary and context

Two explicitly composed consumers executing against the same bounded immutable native input
within one admitted operation or explicit composition scope can repeat identical pure
column preparation before their private operators run: resolving lazy `ScalarFn` / wrapper
layers (`NativeField` in `local_primitive_unary_values.rs`), validating strict floating
domains or UTF-8 domains, building `KeyColumn` / `SharedKeyColumn` (`Arc<Budgeted<KeyColumn>>`)
and `VariableColumn` / `VariableHashes` owners (`local_primitives/native_relational_keys.rs`),
or computing identical deterministic unary projections over the same batch coordinates.

Unlike Chrysalis (RFC 0045), which retains completed column fragments in the session across
sequential operations for local Parquet sources, **Resonance** governs live, bounded
in-flight sharing over immutable native regions as chunks flow between explicitly composed
consumers.

Three existing mechanisms stay distinct and are not duplicated or rerun unchanged:

1. **`write_produced_many_controlled` and `replay::Recorder`** (`local_primitive_produced_fanout.rs`,
   `local_primitive_relational_batch_replay.rs`) capture the complete stream of a single
   producer query before replaying finished batches to multiple output sinks. Resonance
   shares live input regions and pure preparation across composed consumer queries while
   regions flow, without waiting to materialize a whole-source copy up front.
2. **R8 eager shared-producer screen** (`docs/architecture/concurrent-source-reuse-screen-2026-09-26.md`)
   eagerly materialized the full input up front across four concurrent serving calls
   without sharing demand-scoped key/field preparation and was rejected. Resonance never
   revives that eager whole-source fanout default.
3. **Completed-operation segment / coverage caches** (`source_segment_cache`, RFC 0045
   `columnar_coverage`) retain bytes or columns after completion. Resonance releases each
   live region and its shared pure preparations as soon as its final consumer lease drops.

## Goals and non-goals

- Start with **two explicitly composed consumers** reading an immutable validated native
  input (`ResidentMemorySource` / `PreparedVortexSource` / admitted in-flight `ArrayRef`
  region) where attribution demonstrates repeated pure preparation cost.
- Share the bounded live native region and demand-scoped pure deterministic preparation
  (`NativeField` resolution and `SharedKeyColumn` across `Values` / `Hash` / `HashOnDemand`)
  while keeping filter selections, mutable aggregate/join/ranking state, cursors, errors,
  and outputs strictly per consumer.
- Charge every region buffer, metadata lease, and shared prepared artifact through its
  **last owner** on the caller's existing `LiveMemoryPool`.
- Isolate consumer cancellation, `LIMIT` early completion, slow readers, and demand-scoped
  errors so neither consumer silently corrupts or terminates the peer.

Non-goals for this RFC:

- No automatic cross-request coalescing, global broker, or background speculative read-ahead
  past active consumer leases.
- No durable files, sidecars, remote/distributed exchange, or separate compute scheduler
  (`AdaptiveWorkSizer` and PulseWeave scheduling ceilings remain authoritative).
- No sharing of non-deterministic, stateful, or mutable operator state (`OrderedAggregate`,
  hash/build tables, ranking heaps, or user callbacks).
- Exact vector consumers remain a later explicit consumer application of the same contract,
  with independent exact accumulation/ranking and no approximate-search or new arithmetic claim.

## Vortex-first provider review

Classification: **`use_vortex_native_provider`** for immutable native `ArrayRef`,
`StructArray`, `DictArray`, `ChunkedArray`, `VarBinViewArray`, `ExecutionCtx`, and
existing `PreparedVortexSource` / `ResidentMemorySource` intake; **`wrap_vortex_concept`**
for ShardLoom's `ResonanceCell`, `ResonanceLease`, demand-scoped preparation table, bounded
slow-reader window, and isolation semantics.

Pinned graph: Vortex **0.93.0** and Arrow/Parquet **59.3.2** with the repository's
recorded local patches; no new external crate or query-engine integration (`vortex-datafusion`,
`vortex-duckdb`, `vortex-spark`, Polars, Ray, etc.) is permitted.

| Existing owner / provider | Role in Resonance | Explicit boundary / gap wrapped |
| --- | --- | --- |
| `vortex::array::ArrayRef`, `StructArray`, `ExecutionCtx`, `execute_until::<NativeField>` | Immutable regional column storage and non-canonical wrapper resolution (`local_primitive_unary_values.rs`). | Upstream `ArrayRef` shares backing buffers cleanly, but tracks neither `LiveMemoryPool` credit to last drop nor peer cancellation isolation. |
| `SharedKeyColumn` (`Arc<Budgeted<KeyColumn>>` in `native_relational_keys.rs`) | Immutable domain-validated numeric/UTF-8/binary/decimal/temporal key owner with exact normalization under `FloatPolicy` (`Strict` vs `Extended`). | Currently instantiated per `NativeBatch` / operator without cross-consumer demand scoping, `Preparation` upgrade/compatibility rules, or shared window accounting. |
| `ResidentVortexSession`, `PreparedVortexSource`, `NativeExecutionContext` | Owns `LiveMemoryPool`, `CancellationToken`, `FloatPolicy`, `RecursionLimit`, `SourceIdentity::validate()`, and operation admission. | Must validate source generations across every cell delivery; never store `PreparedVortexSource` or `ResidentVortexSession` cycles inside long-lived session caches. |
| `AdaptiveWorkSizer` & `NativeIoCertificate` | Bounds chunk sizing inside caller CPU/memory ceilings and emits exact reuse/work counters with `fallback_attempted=false`, `external_engine_invoked=false`. | Resonance must add explicit live-cell / shared-preparation attribution fields rather than overstating zero-decode or zero-copy. |

## Detailed design

### 1. Compatibility and identity

Consumers may join one composed Resonance stream only when all of the following match
explicitly:

- Identical validated source identity (`SourceIdentity` + held `FileGeneration`, or shared
  admitted immutable in-memory source instance).
- Identical immutable execution policies: `FloatPolicy` (`Strict` vs `Extended`) and
  `RecursionLimit`.
- Same logical root schema (`DType`) and natural/sized region sequence `(region_index, first_row, rows)`.
- Explicit caller composition (e.g., an internal multi-consumer relational composition
  adapter); unrelated concurrent queries never silently share a cell.

### 2. Core abstractions (`ResonanceCell` and `ResonanceLease`)

1. **`ResonanceCoordinator`** (scoped to one admitted composed operation):
   - Owns an active sliding window of at most **`MAX_LIVE_CELLS = 4`** in-flight cells and
     shared live-window reservations capped at **`min(16 MiB, grant / 8)`**, inclusive of
     prepared `SharedKeyColumn` and table metadata on `LiveMemoryPool`.
   - Reads the next bounded input region only when live window occupancy and shared budget
     permit; refuses admission cleanly before exceeding `LiveMemoryPool` or active window limits.
2. **`ResonanceCell`**:
   - Represents one bounded immutable native region (`region_index`, `first_row`, `rows`,
     `ArrayRef` projected to the union of active consumers' demanded columns).
   - Retains the backing `MemoryLease`(s) covering the input region and an internal
     demand-scoped table of pure prepared artifacts keyed by `(column_index, PreparationSpec)`:
     - `PreparationSpec::ResolvedField`: output of `resolve_field` (`NativeField`), preserving
       `RecursionLimit`.
     - `PreparationSpec::KeyColumn(Preparation)`: `SharedKeyColumn` (`Arc<Budgeted<KeyColumn>>`)
       under exact `FloatPolicy`. (A consumer needing `Hash` may reuse an already-prepared
       `Hash` artifact if compatible; a `Values`-only artifact never satisfies `Hash` without
       explicit hashing admission.)
   - **Last-owner drop**: when all active consumers drop their `ResonanceLease` for a cell
     (and no peer retains an internal `Arc<Budgeted<_>>` reference beyond its lease), all
     cell buffers and prepared artifacts drop immediately and refund `LiveMemoryPool`. Any
     derived consumer slice retaining an `Arc` keeps its original credit until dropped.
3. **`ResonanceLease`**:
   - Per-consumer handle holding an active cursor over live cell sequence plus its own
     independent `CancellationToken` and private operator state (`MutableGroupState`,
     filter mask, `LIMIT` counter).
   - **Early completion & cancellation isolation**:
     - If Consumer A finishes via `LIMIT` or drops its lease early, the coordinator unregisters
       Consumer A's column union immediately and shrinks future region projections to Peer B's
       remaining demand; Peer B continues undisturbed.
     - If Consumer A cancels via its private `CancellationToken` or hits an error inside its
       own operator state (e.g., overflow inside A's `SUM`), Consumer A drops its lease and
       receives its deterministic error while Peer B finishes normally (unless the shared root
       source generation itself changed or the caller's outer operation token was cancelled).

### 3. Demand-scoped validation and failure boundaries

- Input columns outside a consumer's demanded projection must NOT fail that consumer. If
  Consumer A demands `(id, x)` and Consumer B demands `(id, strict_f)`, only Consumer B
  triggers `KeyColumn::prepare` / validation on `strict_f`:
  - Preparing `PreparationSpec` runs lazily on first demand by an active lease requiring it,
    credited before allocation on `LiveMemoryPool`, under the demanding consumer's
    `CancellationToken` (while respecting outer session cancellation and `SourceIdentity::validate()`).
  - If Consumer B fails on a non-finite float or memory refusal in `strict_f`, Consumer A
    must not inherit Consumer B's validation failure.
- If the shared `SourceIdentity` changes on disk between regions, or if the shared root I/O
  scope fails/cancels externally, the coordinator terminates the cell stream deterministically,
  releases all unreleased leases, and surfaces the source/cancellation diagnostic before any
  consumer completes or publishes output.

### 4. Bounded slow-reader retention and backpressure

- When Consumer A is faster than slow Consumer B, fast Consumer A cannot race arbitrarily
  far ahead while pinning unbounded live cells: once `MAX_LIVE_CELLS` (4) or the live-window
  budget ceiling (`min(16 MiB, grant / 8)`) is reached, production pauses (sequentially
  stepping / yielding demand backpressure to the faster consumer) rather than allocating an
  unbounded queue or spilling silently without explicit spill admission.
- On a single execution lane (cooperative composed stepper) or bounded multi-lane operation,
  backpressure schedules the lagging lease so live windows drain forward without deadlock.
- Under memory pressure or narrowed grant refusal, uncompleted shared preparation drops
  immediately with zero credit leaks; neither consumer returns a partial/truncated result
  claiming completion.

## Prospective attribution and complete-workflow gate

Before writing production Resonance runtime code or declaring any benefit:

1. **Pre-implementation attribution screen**:
   - Measure actual repeated preparation cost (`resolve_field` + `KeyColumn::new_with_float_policy` /
     `VariableColumn` / `VariableHashes`) across immutable 131,072-row numeric and nullable
     UTF-8 native inputs in release mode with an explicit owner observer, proving whether
     shared pure preparation represents a credible removable fraction beyond coordinator/lease
     bookkeeping overhead. Do NOT rerun R8 unchanged and do NOT extrapolate from micro-timings
     alone.
2. **Freeze matched comparison protocol BEFORE candidate timing**:
   - **Arms**:
     - Independent two-consumer execution (existing `ResidentVortexSession` / separate
       prepared operations over the same immutable native input).
     - Composed live-cell two-consumer execution using `ResonanceCoordinator` / `ResonanceCell` /
       `ResonanceLease` with shared demand-scoped `SharedKeyColumn` + `ResolvedField`.
   - **Mandatory controls** (all part of the frozen retain/drop population):
     - Overlapping numeric & UTF-8 key-heavy consumers (`GROUP BY` / `DISTINCT` / `JOIN` key prep).
     - Disjoint-column consumers (testing non-interference and shrinking union after early drop).
     - Single-consumer overhead control (must bound bookkeeping overhead when no peer shares).
     - Mixed completion times (`LIMIT` early finish on Consumer A vs full scan on Consumer B).
     - Slow-consumer bounded window backpressure (proving <= `MAX_LIVE_CELLS` and bounded budget).
     - Active cancellation isolation, demand-scoped error isolation, late source replacement/failure,
       and memory refusal returning `LiveMemoryPool` reserved bytes to exact baseline / `0` after drop.
3. **Retain/drop rule**:
   - Retain only after complete correctness, schema, isolation, zero-leak resource, no-fallback,
     feature-matrix, MSRV, and isolated installed-package checks pass AND the complete frozen
     cohort improves total `engine_nanos` cleanly with explicit regressions preserved verbatim.

## Alternatives and risks

- **Chrysalis-only completion caching**: Cannot avoid repeated `KeyColumn` / `VariableHashes`
  building inside live streams without extra session-level cross-operation caches and risks
  retaining large hash tables past the active window.
- **Full materialization (`replay::Recorder`)**: Duplicates full input buffers when consumers
  only need transient live cells or hit `LIMIT` early.
- **Primary risks**: deadlock under slow-reader backpressure across consumers sharing one
  caller thread; failure leakage across column domains; coordinate overhead erasing key-build
  savings on cheap numeric keys. Explicit sequential/bounded stepping (`MAX_LIVE_CELLS = 4`),
  demand-scoped lazy preparation, and the upfront attribution gate mitigate these risks
  before runtime adoption.

## Open questions

1. Which exact multi-consumer entry point (e.g., explicit internal multi-plan / CTE-over-native
   composition in `local_primitive_prepared_relational.rs`) should serve as the first bounded
   attribution and integration surface without altering public SQL defaults?
2. Should the first candidate share both `NativeField` resolution and `KeyColumn` owners
   (including `VariableDomain` UTF-8 hash tables) or start strictly with `KeyColumn` over
   already-resolved native columns if attribution shows `VariableHashes` dominates?
