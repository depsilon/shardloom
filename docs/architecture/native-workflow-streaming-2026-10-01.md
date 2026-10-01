<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native workflow result streaming

Status: implementation and local acceptance complete under PERF-03/06/07/11/12. The
[phase plan](phased-execution-plan.md) owns sequencing; this document records
the design and finite acceptance contract for its first workflow completion unit.

## Contract

Complete already executable flat-scalar aggregate and ordered results through
one native array boundary. Local writes must consume bounded batches without
constructing a complete JSON/scalar result table or running the query again.
Small in-memory collection keeps its independent row and byte bounds. Source
generation validation, schema, validity, complete values, ordering, cancellation,
spill ownership and atomic publication must survive the producer/consumer handoff.

The reference comparator is an encoded-columnar engine maintainer reviewing
buffer lifetimes, global aggregate selection, precise null/type semantics and
failure before publication. A larger fixture passing is insufficient if retained
output still grows without reservation or a sink silently publishes a prefix.

## Provider decision

Vortex-first provider check: `use_vortex_native_provider`. Reuse pinned Vortex
0.85 `ArrayRef`, `StructArray`, primitive/Boolean/UTF8 arrays, native buffer
ownership, native scan, and the existing sequential Vortex writer. Compatibility
conversion remains at the existing output adapter. No dependency changes or
external execution providers are required.

Inspection of Vortex 0.85 `builders/mod.rs:451-457` shows that
`builder_with_capacity_in` currently discards its allocator argument. Consequently
completed scalar columns use the existing ShardLoom reserved host allocator and
the native array constructors directly. Payload and validity leases attach to
each buffer, so clones and slices retain credit after the producing result and
session are dropped. This is explicit final-result construction, not a new array
representation or a claim that arbitrary provider allocations are accounted.

Retain the existing aggregate state machines and their exact global selection:
finalized integer/UTF8 DISTINCT, numeric COUNT, numeric-pair compact/late measures,
numeric-minute-string COUNT, numeric-UTF8 COUNT, UTF8 COUNT/DISTINCT refinement,
transformed dictionary measures, generic ordered groups and source-order groups.
Do not replace complete-key reduction with local top-K selection. Preserve the
existing floating accumulation/evaluation order and signed/unsigned key identity.

Shared result construction accepts complete typed values directly from those
states. Report rendering is a terminal consumer, rather than an intermediate
execution format. Bounded synchronous handoff supplies backpressure: a producer
cannot advance while its consumer retains the active operation. Later async
queues would require their own admitted capacity and cancellation contract.

Completed DISTINCT and weighted COUNT owners already reserve their exact global
selection. Their final handoff borrows at most 2,048 selected keys/counts at once,
with a separate reservation for that reference window. It does not duplicate the
whole selected result before producing batches. Generic aggregate ordering still
reserves its complete candidate selection separately; this does not add spill
support or complete resource accounting to those state machines.

## Acceptance contract

- Complete values, dtype, validity and order for scalar/grouped/ordered empty and
  nonempty results; repeated calls and independent source generations.
- More than 65,536 output rows, more than 8 MiB overall output, multiple batches,
  and batch/resource boundary failures. Collection limits remain independently
  tested. A single oversized value must fail deterministically before publication.
- Native Vortex and admitted Parquet, Arrow IPC, Avro, ORC, JSON, JSONL and CSV
  sinks, including format-specific UInt64/loss checks and existing-file safety.
- Native aggregate spill and numeric-sort spill output, cancellation, corrupt
  run/pressure failure and cleanup, with no query replay or leaked reservations.
- Downstream native consumption, drop/clone/slice lifetimes, slow-consumer and
  consumer-error behavior. Buffer accounting exclusions remain explicit.
- Public CLI, SQL and Python/DataFrame calls share the same native handlers;
  full source/transform/write/reopen checks use renamed non-ClickBench schemas.

Focused tests precede the required format, strict Clippy and workspace/native
gates. Full43 remains regression evidence. No timing improvement, complete
relational breadth, total RSS ceiling, competitive gate completion, production
certification or package publication follows from this unit alone.

Every accepted execution retains `fallback_attempted=false` and
`external_engine_invoked=false`. Real native payload verification is required;
placeholder artifact paths do not satisfy native output acceptance.

## Local validation

Runtime revision `d6052cbe63a6703ce2671b92169ed3cd1788531f` passes the following
checks with Rust 1.99 on the local Apple Silicon host. Logs and their SHA-256
manifest are retained under
`/Users/dylan/LocalData/shardloom/workflow-completion-20261001/` in
`local-checks-d6052cbe.json`.

| Check | Result |
| --- | --- |
| `cargo test --workspace --all-targets -- --test-threads=2` | 3,436 passed across 102 targets; no failures or ignored tests |
| CLI/Vortex all-target tests with `release-user-surfaces` | 3,630 passed across 86 targets; no failures; 23 existing manual benchmark, environment or fixture-generation tests explicitly ignored |
| Workspace and CLI/Vortex native all-target Clippy | Passed with `-D warnings` |
| No-default `vortex-local-primitives` all-target Clippy | Passed with `-D warnings` |
| Formatting, diff whitespace, public surface/status docs and contribution governance | Passed |
| Release architecture tracker | Passed in its existing `--allow-blocked` audit mode; this does not certify release readiness |

The direct visitor acceptance verifies every value in 70,017 rows under a 2 MiB
result-buffer budget, delivery during visitation, typed empty delivery, immediate
consumer-error propagation and final reservation release. The real integer
DISTINCT spill case verifies 4,097 selected rows across reference-window boundaries
and removes every owned run. Broader tests cover all eight output formats, complete
nullable UTF8 output above 8 MiB, UInt64 format limits, parent cancellation,
writer pressure, atomic publication and native downstream filter/projection.

The Python SDK forwards validated filter → aggregate → HAVING → order → limit
write chains through the same public native SQL lowering, without requiring a
scenario identifier or an artificial output limit. Scalar aggregate writes also
use this path. The chain validator rejects repeated/out-of-order stages rather
than moving an input limit past an aggregate. Bare-source native writes select
all fields through the existing projection primitive. General collection and
arbitrary chained-expression parity remain separate work.

The real Python SQL/DataFrame acceptance runner verifies 20 complete workflows on
a 70,017-row renamed-schema input: aggregate, ordered rows, scalar COUNT/SUM,
measure-alias HAVING and typed empty HAVING output, each written to Vortex and
JSONL. Large results contain 70,004 rows; HAVING removes two groups and writes
70,002 rows without an explicit limit. Every native output is reopened through
the public Python API and every value/order is checked. This does not expand
the existing native HAVING parser to arbitrary group-key predicates.

The accepted receipt is
`workflow-completion-20261001/logs/native_result_stream_20261001T192424930907Z/summary.json`,
supervised by `python-stream-acceptance-7.json`. SDK and harness revision
`146a713a` includes the final legacy-route stage-order check. The receipt records the frozen binary,
Python source, harness and input hashes. Runtime binary SHA-256 is
`8abff1bcd5b92678abbc6b68d116d9ad69aa2f4ee39d92c34c5273c56b918e95`.
The complete Python suite runs 815 tests: 671 pass and 144 existing retired or
environment-dependent tests are skipped; no new skips were added. Earlier failed
UAT receipts are retained, including the SDK routing failures and the explicitly
unsupported group-key HAVING probe. The accepted HAVING case uses the existing
measure-alias contract and verifies actual group removal.

Full43 completes **129/129** executions across all 43 queries, with complete
returned-value regression checks and no guard failures. The receipt is
`clickbench-100m-uat/logs/full43_20261001T191940379896Z/summary.json`, SHA-256
`11c3115a91d046774658fbe132e6cad6d7727a41efce49d5128607b148aef168`.
Each query runs three times in a fresh process using the resident 15,682,956,489-byte
native source. Full input hashing precedes the run; OS page cache and ordinary
host activity remain uncontrolled. The retained native references are regression
evidence, not an independent oracle. Source identity/hash, binary and all 43
reference hashes are frozen, and completed logs are archived losslessly.

The [portable acceptance packet](../benchmarks/evidence/native-workflow-streaming-2026-10-01.json.xz)
contains build identity, validation log hashes, all 20 public workflow records,
all 129 Full43 records, retained reference identities, failed-attempt receipts,
guarded-run receipts and final source/archive verification. Rebind its path
placeholders to resident inputs when replaying the checked-in acceptance runner.
The packet records local acceptance before hosted PR checks; its timestamps and
check scope are immutable. These tests make no speedup or total-RSS claim.
