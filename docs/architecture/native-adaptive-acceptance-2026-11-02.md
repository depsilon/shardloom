<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native adaptive ownership and interface acceptance

Status: local runtime, ownership, public-interface and hosted acceptance passed;
PR #1644 is merged with no review findings. This matrix joins the existing
owner controls to the shared public routes; it does not introduce another
execution policy. The [runtime contract](native-adaptive-execution-2026-11-01.md)
and [phase plan](phased-execution-plan.md#engine-wide-adaptive-execution) remain
authoritative. The original failures and timing observations retain their
original source and scope.

## Acceptance boundaries

The memory grant governs named native reservations and admitted provider work.
Schema estimates select work before the actual reservation; they are not a
complete allocator or process-RSS measurement. Caller CPU limits include the
existing caller/provider progress contracts. A path with no worker queue must
not claim parallel execution from a selected lane count.

Verification is compositional: owner tests force changing headroom and terminal
states, while real public executions verify that adapters preserve those owners,
the caller's grant, complete results and certificate fields. Small public
fixtures alone do not prove pressure adaptation, and mocked command forwarding
does not prove native admission. This separation is explicit in the tests.

## Owner matrix

All paths below are relative to `shardloom-vortex/src` unless another crate is
named. The referenced controls run in the enabled workspace suite. The
production callsites and physical limits determine each row's scope.

| Owner | Applied execution or admission | Executable controls and necessary limits |
| --- | --- | --- |
| Metadata | `universal_format_io/demanded/source.rs` and `metadata_admission.rs` reserve inventory/footer growth before retention. Generic discovery retains each format's held metadata and generation. | `local_primitive_relational_source_demand_metadata_tests.rs`, `local_primitive_relational_source_demand_orc_tests.rs`, and the IPC/Avro owner controls cover denial, cancellation, source changes and refunds. Complete schema/footer facts can be irreducible; a smaller output row count cannot remove their cost. |
| Native scans | `local_primitive_relational_scan.rs` chooses actual row ranges before Vortex scan execution and actual memory/batch projection windows. Prepared primitive scans preserve their held source, provider drivers and admitted scan policy. | `local_primitive_relational_adaptive_tests.rs` and `local_primitive_prepared_scan_tests.rs` cover pressure, retained outputs, source identity and driver cleanup. Specialized encoded scan/worker paths retain their existing provider and materialization contracts; the relational row selector does not establish coverage for them by association. |
| Selected Parquet | `universal_format_io/demanded.rs` reserves selected chunks/decoder workspace, then applies the next row target to the provider before decoding. | `local_primitive_relational_source_demand_adaptive_tests.rs`, `universal_format_io/provider_batch_tests.rs` and `local_primitive_relational_source_demand_transfer_tests.rs` check real buffer shrink/regrowth, masks, nested values, retained ownership and failure refunds. A page/chunk's physical admission is separate from output rows. |
| Other local formats and collections | `demanded/generic.rs` passes actual next-batch limits to Parquet/ORC/Avro; `complete_parquet.rs` selects ordered task windows within the same ingest grant. IPC admits complete physical messages. | `universal_format_io/avro_batch_tests.rs`, `demanded/ipc_tests.rs`, `demanded/generic_schema_tests.rs`, `demanded/complete_parquet_batch_tests.rs` and source ORC/mapped tests cover continuation, ownership, mutation and denial. IPC messages, Avro blocks and retained codec/dictionary state cannot be split by post-read slicing. Controlled ORC LZO remains explicitly unsupported. |
| Text | `universal_format_io/text_stream.rs` selects record/batch geometry using schema costs and current shared headroom before parsing/building the next batch. | `text_stream/tests/adaptive.rs` checks narrow/wide schemas, long records, held results, shrink/regrowth, late denial, cancellation and decoder errors. Complete records, compressed framing and whole-document syntax remain mandatory. |
| Conversion | `local_primitive_columnar_compat_batches.rs` and `local_primitive_columnar_compat_sizing.rs` select conversion windows after the original complete validation boundary. Native intake and ingest conversion retain buffer/schema/structure credits. | `resident_memory_arrow_tests.rs`, `universal_format_io/demanded/generic_schema_tests.rs` and owned complete-ingest controls cover aliases, bufferless structures, hidden invalid values, overlap denial and refunds. A lifetime envelope is not allocation-transfer proof; copying stays required where the existing ownership proof does not admit transfer. |
| Relational state and output | `native_relational_output_buffer.rs` chooses optional compaction or unchanged forwarding; `native_relational_sort_output.rs` adapts validated output gathers. Existing state owners reserve or use explicitly admitted native spill. | Output-buffer, sort-output, join/aggregate/set/window/pivot/subquery pressure suites exercise complete values, stable ties, retained outputs and terminal cleanup. Full candidate/field-major validation, numeric reduction order and lazy expression domains remain semantic boundaries; changing their batch geometry requires separate proof. |
| Source replay and capture | `local_primitive_relational_batch_replay.rs` selects compact copy windows and retains native payload ownership. The completed source reader records delivered rows only after successful consumption. | Batch replay/capture tests cover exactly-once demand, source ordinals, empty/zero-limit behavior, late errors, resident denial, admitted spill, aliases and complete native readback. Typed source evidence does not include output fanout deliveries. |
| Native spill | `native_relational_spill_sizing.rs` chooses physical run blocks with footer, writer and future merge-reader overlap. Existing query-run ownership and quota checks remain authoritative. | Spill geometry, stored-order, family pressure/fault/recovery and query-run-store tests cover mixed block sizes, corruption, cancellation, denied capacity and owned cleanup. Original gather/validation windows and complete merge/key states can remain irreducible even when physical blocks shrink. |
| Writers and fanout | Native completed-record buffering and compatibility conversion share admitted work. Fanout captures one completed producer, stages complete destinations and records each delivered replay in its separate output summary. | `local_primitive_relational_bulk_writer_tests.rs`, writer-pressure tests, source/output replay tests and CLI `tests/support/public_primitive_fanout.rs` verify complete values, all admitted output families, minimum denial, failed staging, shared spill quota and cleanup. Provider high-water workspace remains retained through close; writes already consumed are not retried. |
| Workers and queues | Complete Parquet tasks and `vortex_ingest.rs` conversion prefetch select newly admitted work using current headroom. Existing specialized worker owners reserve before submission and retire/drain under pressure. | Complete-Parquet task tests, ingest queue controls, dictionary/weighted-count/mixed-distinct worker tests and `shardloom-exec/src/compute_pool.rs` test bounds, ordering, partial admission, cancellation and drain. Caller-driven relational execution does not acquire a substitute compute pool. |

The standalone `plan_dynamic_sizing_feedback` utility remains advisory;
`executes_feedback()` remains false. General runtime-filter, skew and certified
cache plans are not executable merely because their planning modules exist.
The output acknowledgment window retains its existing resident Int64
scan/projection scope. This acceptance does not extend its source, type,
composition or spill support.

## Public input/output matrix

`python/tests/native_adaptive_resource_cases.py` uses independent complete
expected rows and real native calls. It covers Vortex, Parquet, Arrow IPC, Avro,
ORC, CSV, TSV, JSON and JSONL as individual sources and in composition with a
native Vortex source. Each case runs through DataFrame, typed relation, Python
SQL, embedded workflow and CLI SQL with both 16-MiB/one-lane and
128-MiB/two-lane grants. This is 180 complete collection executions.

The same test writes a mixed Parquet/IPC query through all five interfaces to
Vortex, Parquet, Arrow IPC, Avro, ORC, CSV, JSON and JSONL under both grants:
80 writes, each followed by complete readback. It checks source-order
independence, nullable numeric values, variable Unicode strings, exact declared
grants, shared native peak reservation and final session refunds. TSV is an
input adapter, not an additional output claim. Existing richer-type and
translation-fidelity suites retain their separate format/type contracts.

`native_metadata_count_cases.py` adds actual count/exclusion assertions to the
same five entry points, including union/Hive Parquet and ORC under two grants.
`test_native_ordered_input_binding.py` checks the typed decision schema and
actual ordering admission/decline within its existing 480 complete cases.
Source and output replay controls verify separate ownership scopes; no report
infers avoided bytes, evaluation savings or RSS from a capability flag.

## Validation and remaining integration

The final local default workspace passes 3,311 tests with none ignored. The
enabled native workspace passes 6,833 tests with 36 existing exclusions: manual
measurement/diagnostic and fixture-generation controls, plus three non-Unix
tests delegated to Windows CI. Both all-target strict Clippy configurations and
the workspace formatting check pass. All 298 focused decision controls pass.

The release CLI/library pair passes all six focused public test methods,
including the 260-operation format/grant matrix, the ORC count/pruning cases
and all 480 ordered-input cases. The isolated installed wheel passes 292 tests
with no failures, errors or skips, verifies the installed CLI/library identities
and passes uninstall checks.

The [acceptance record](../benchmarks/evidence/native-adaptive-decisions-acceptance-2026-11-02/acceptance.json)
binds all 3,370 source-file identities, both runtime binaries and the installed
wheel to the exact local checks. Its
[verified archive](../benchmarks/evidence/native-adaptive-decisions-acceptance-2026-11-02/verification.tar.xz)
retains 27 original receipts and logs, source overlays and blobs, the original
failed attempts, package reports and the partial benchmark checkpoint. All 167
archive members pass complete byte-for-byte readback. No new Full43,
fixed-runtime repeat or timing claim is part of these final checks.

## Hosted integration and retention

[PR #1644](https://github.com/depsilon/shardloom/pull/1644) is merged as
`d3e943add236baf3200fa07d299437cc3dd71314`. All 58 hosted checks pass on
`563279a54e398897584eb2b132d7d0bb017f44a2`; review completes on that same
head with no findings or unresolved threads. The
[closeout record](../benchmarks/evidence/native-adaptive-decisions-closeout-2026-11-02/closeout.json)
retains exact hosted evidence and the generated-payload retirement receipt.
Repeated identity and closed-handle checks retire five superseded runtime
pairs, old wheels and completed package environments. The accepted runtime and
installed-tested wheel, source inputs and all observations remain. No query
rerun, payload backup or storage-limit increase accompanies closeout.

Broader local table readers, the requested preparation/query investigations and
the combined documentation/site/portable-evidence closeout retain their phase
ordering. This document does not close competitive gates or authorize a version
bump or package publication.
