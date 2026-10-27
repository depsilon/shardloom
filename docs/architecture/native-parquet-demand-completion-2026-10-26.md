<!-- SPDX-License-Identifier: Apache-2.0 -->

# Parquet source-demand completion and shared broad-query optimization

**Status:** locally accepted October 27; all 347 cases and required runtime/package
gates pass. The [acceptance report](../benchmarks/native-parquet-demand-completion-2026-10-27.md)
records 34.960427343 engine seconds versus 50.877676708, a 31.3% observed reduction,
with no predeclared material case/family regression flags. One observation per
case does not establish statistical confidence or external-engine superiority.

## Maintainer direction and boundary

The October 26 continuation keeps item 20 on Parquet until the remaining
source-demand work and justified shared optimizations are complete. The 347-case
suite is the primary performance and acceptance target. The earlier nine-case
focused timings remain historical evidence, rather than another routine timing
suite. Small targeted correctness and lifecycle tests remain required.

The [first adapter](native-source-demand-2026-10-26.md) is locally accepted in
`57b622b8` and merged in [PR #1612](https://github.com/depsilon/shardloom/pull/1612)
as `81ab0a4d` after all 57 checks completed successfully. This continuation is
isolated on `codex/parquet-demand-completion` for the next PR.
Its [complete evidence](../benchmarks/native-source-demand-2026-10-26.md) remains
immutable. This continuation does not expand to other formats, accept item 18's
unfinished implementation, publish a package, or close CG-1 through CG-23.

The user should use one ordinary query model. Source access determines and
produces the data needed by that query. Explicit creation of a durable Vortex
dataset and full-source validation remain separate supported operations with
their existing guarantees. Native Vortex input and output remain first-class.
Do not require whole-source persistence merely because the reader needs to
decode a compatibility source. Do not select a strategy by benchmark case ID,
public interface, or fixture values.

## Grounded opportunities

The frozen 347-case Parquet observation totals 50.877676708 engine seconds.
Grouped aggregation accounts for 25.859804 seconds across 180 cases; projection
accounts for 7.388781 seconds across 26 cases. These are workload-family totals,
not isolated measurements of grouping or reading CPU.

The wide case reports 273,781 positional reads for 107,525,511 data bytes. The
current `ChunkReader::get_read` exposes an unbuffered cursor, so Parquet page
header reads repeatedly enter source-generation, I/O admission and system-read
boundaries. The pinned Parquet provider's ordinary file reader uses buffering.
Any correction must preserve charged ownership and must not prefetch arbitrary
unselected columns or row groups.

The current bound-source pass also leaves conservative work at residual
predicates and source-order limits. Extend demand only when the fully bound
operator dependencies and evaluation order establish safety. Query result
limits do not justify changing global sort, aggregate, join or window semantics.

## Coherent work unit

1. Complete the physical Parquet read owner: bound reads to admitted selected
   chunks, avoid repeated tiny reads, retain cancellation and source-generation
   checks, and release compressed buffers when their admitted region ends.
2. Complete safe demand propagation through shared bound operators, including
   hidden residual-expression dependencies and source-order row demand where
   semantics permit it. Preserve fallible computation and validation order.
3. Admit currently supported multi-file/directory Parquet query workflows and
   explicit content-digest identity without forcing durable preparation. Reuse
   existing file discovery, schema and identity contracts. Full content hashing
   still reads all source bytes; distinguish hashing from demanded data decoding.
4. Remove routing ambiguity for unsupported Parquet schema overrides: preserve
   the existing authoritative embedded-schema error without pretending that an
   override is an alternate working preparation mode. Do not add new casts or
   schema-evolution semantics as an incidental optimization.
5. Improve shared aggregation or expression work only where the broad evidence
   and source inspection establish avoidable work. Retain native representation,
   exact arithmetic/null behavior, state reservations and spill contracts.

Explicit `prepare_once`, durable outputs and low-level Vortex primitive requests
retain their declared semantics. They are not hidden retries after demand access
fails. An already prepared native dataset remains valid input; these changes do
not establish that eagerly preparing every fresh source is worthwhile.

## Provider and ownership decision

Decision: **use_vortex_native_provider** for owned arrays and existing execution.
The pinned Vortex 0.90.0 array, projection/filter, selection and native-output
surfaces remain behind existing ShardLoom owners. The existing shared
`SourceDemand` contract describes requirements; it is not a new query evaluator.
Parquet 59.3.1 supplies metadata, column chunks, row selections and decoding.
Inspect and use its pinned reader surfaces before adding adapter machinery.

Range buffering is an extension of `OwnedLocalRead`/the admitted source reader,
with native memory credits, reader ownership and observed actual byte/request
counts. It is not the previously rejected Vortex-file request-deferral or
whole-file refresh experiment. No Vortex query-engine integration or external
engine executes residual work. `fallback_attempted=false` remains mandatory.

## Representation and binding follow-up

The maintainer's supplied optical-system analogy is used as a source of testable
software patterns, not as evidence of engine speed or a request for new network
or scheduling machinery. The target problem remains complete first-use query
cost on the existing 347 cases. Software stages share CPU, memory and I/O capacity;
separate physical optical channels do not establish useful software concurrency.

| Supplied pattern | Existing native boundary | Concrete action and required evidence |
| --- | --- | --- |
| Selective add/drop: handle only required channels | Bound `SourceDemand`, Parquet chunks and row-group statistics | Complete hidden dependencies and safe prefixes; certify selected bytes, decoded rows and declared completion. |
| Bypass: retain useful representations | `VariableColumn` retains dictionary codes and its value domain; `RowSet` keeps first-occurrence native keys | Preserve existing dictionary-aware equality across batches. Do not treat dictionary-local codes as global identities. Add cross-dictionary coverage where missing. |
| Bind a stable path before repeated work | `NativeNumericOwner::bind_row_update` already binds physical type and validity for scalar SUM/AVG | Investigate using the same borrowed typed kernel for grouped SUM/AVG. Prove identical row/measure order, NULL, signed/unsigned, nonfinite and overflow behavior; compare the whole aggregation family. |
| Begin downstream work before a complete durable artifact | The demanded reader delivers bounded native batches directly into the shared operators | Keep the existing streaming owner and explicit final-consumption checks; do not introduce another queue or change scheduling in this work unit. |

The source inventory distinguishes actual transitions: Parquet decoding enters
the explicit Arrow compatibility boundary, intake copies into charged native
owners, numeric key preparation uses original-width primitive arrays, string
dictionary keys preserve their compact codes and domain, grouping retains unique
keys, and final output gathers retained keys. The numeric consumer's current
per-row `raw_cell` path remains a concrete dispatch candidate; this is not a claim
that all encodings pass through every operator without decoding. Existing debug
observation counters and native I/O certificates must prove any changed boundary
before a complete-query improvement is claimed.

## Semantic and resource obligations

- Demand includes hidden filter, ordering, grouping, join, window and subquery
  dependencies. Multiple references combine requirements conservatively.
- Source-order row restriction must be derived from ordinary plan semantics.
  Never move it through an order-dependent or fallible operation without proof.
  Document the exact requested input region and certify its complete consumption;
  do not report an unconsumed full-source declaration as complete.
- Query-scoped access validates its declared data region, metadata and source
  generation. Explicit full ingestion continues to validate the whole requested
  source. Corruption inside/outside requested regions must test this distinction.
- Multi-file input preserves deterministic discovery, schema, ordering, global
  LIMIT/OFFSET and source-generation checks. File replacement, membership change,
  incompatible schemas and output aliasing must fail deterministically.
- Buffers and metadata are credited before allocation, do not cross admitted
  source regions, and release on EOF, cancellation, failure and final drain.
  Do not turn upstream decoder workspace estimates into an allocator/RSS claim.
- Preserve decimal, unsigned, temporal, nested, NULL, duplicate and floating
  contracts. Do not narrow existing full-validation paths merely to pass a cost
  target. Preserve existing standalone, native-output and no-fallback boundaries.

## Prospective verification

Freeze the complete 347-case candidate declarations, source/runtime identities,
resources, references, baseline observations and decision before execution.
Reuse the five verified Parquet files and independent expectations from the
accepted checkpoint. The native comparison baseline is the final Parquet
candidate at `57b622b8`; its original source overlay and runtime hashes remain
the recorded build identity. Do not rerun it merely to restate its timing.

Execute each of the 347 cases once per justified candidate, in isolation with
the standard storage, concurrency, deadline, output and process-cleanup guards.
Retain complete typed outputs, schemas, source-demand evidence and every failure.
Report engine-to-engine changes for ShardLoom and preserve complete API costs
for the user-requested first-use comparison. Do not substitute one clock for
another or rank incomplete observations.

Use the whole cohort and all family/case results when deciding retention. A
shared performance change must show a material reduction in its declared target
family or the complete cohort without an unexplained material regression in
controls. Freeze numeric thresholds in the candidate protocol before measuring;
single observations do not establish statistical confidence. Capability coverage
must pass correctness and ownership gates regardless of observed speed.

Retain the original Polars 2.1.0 observations without executing Polars again.
They use Arrow IPC, while the new native path uses Parquet. Compare matching API
boundaries only for prioritization, retain the format distinction, and make no
matched-format superiority claim. Beating every retained case is the target,
not an acceptance fact inferred from the suite total.

Focused correctness includes multi-file/global semantics, identity policies,
source changes, malformed ranges, missing statistics, hidden columns, safe and
unsafe row-limit propagation, late corruption, low memory, cancellation, large
output, replay/spill and all five public interfaces. Run required workspace and
native-feature gates and installed-package proof after the coherent change.
ClickBench remains a regression guard when shared behavior requires it; it is
not the primary optimization target. Do not run the separate nine-case timing
suite or another full-scale ClickBench observation without a specific proof need.

Retain portable complete-result replay and exact runtime/source linkage. Retire
only completed generated payloads after verified replacement under the existing
local storage policy. No new public release or competitive-gate completion is
implied by this work unit.

## Accepted implementation

The completed unit provides charged selected-chunk reads, hidden predicate
dependencies, safe source-order prefixes, sorted directory input, explicit
content fingerprints, source guards across output writers, and shared typed
grouped SUM/AVG updates. Dictionary-local codes remain domain-aware. All five
public interfaces have installed-package directory/prefix/grouping coverage.
The report links complete portable replay, eight corruption controls, source
snapshots, failed attempts and exact runtime/package identities. Historical
focused timings and the original competitor observations remain unchanged.
