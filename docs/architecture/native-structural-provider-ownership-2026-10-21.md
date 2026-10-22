<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native structural provider ownership

Status: six bounded allocation classes implemented under RFC 0044 and
`PERF-03.providers` / `PERF-03.mixed_load`, with completed local acceptance.
The final isolated Full43 passed at 49.990920425 engine seconds on the unchanged
executable and input. The maintainer accepted that observation and directed
continuation. Earlier slower observations remain in the evidence; this does not
establish their cause or a speed gain.

Native layout and file parsing previously allocated structural containers outside
the shared memory grant. Attaching credit only to a footer or layout shell would
also release it too soon when independently shared tables survived that shell.
The covered containers now obtain credit before construction and retain it with
their actual shared storage. Denial is recoverable and leaves existing owners
valid. The [serialized metadata buffer contract](native-file-metadata-buffers-2026-10-16.md)
remains a separate allocation class.

This covers specific resource-accounting gaps, with complete-result regression
acceptance. It makes no performance, whole-provider or process-RSS claim. The
canonical [queue](phased-execution-plan.md#planned) now determines requirements
from concrete comparison workloads and existing public guarantees. Additional
provider coverage is optional unless such a gap requires it; optional breadth
is last and may never be implemented.

## Vortex-first decision

Decision: `use_vortex_native_provider`. Pinned Apache-2.0 Vortex 0.85.0 owns
the native layouts, readers, statistics and file tables. Its existing
`VortexSession`, `MemorySessionExt`, `HostAllocator::reserve_external` and
`HostMemoryReservation` provide the admission boundary. ShardLoom's
`ReservedHostAllocator` connects that boundary to the existing shared live-memory
pool. Allocators without foreign-storage admission continue returning the
explicit unsupported-reservation error.

The implementation stays in the reviewed vendored `vortex-layout` and
`vortex-file` providers and their ShardLoom adapters. It adds no runtime
dependency, query-engine integration, file-format change or Arrow execution
substrate. Native feature gates and the lightweight default remain intact.
Vortex input/output and the existing certificates preserve
`fallback_attempted=false` and `external_engine_invoked=false`.

## Covered storage and lifetime

| Allocation class | Construction and final owner | Bounded claim |
| --- | --- | --- |
| Layout/reader child caches and mixed chunk classification | Fallible provider construction reserves checked capacity before allocating cells or classification storage. Shared owners retain the reservation. Uniform all/none classification avoids the Boolean array. | Cache cells and classification capacity; independently retained children have separate lifetimes. |
| Chunked cumulative row offsets | Deserialization and native writers construct immutable shared offset storage after checked count, row-sum and allocation admission. Data clones share its reservation; empty layouts use a static zero boundary. | The offset table, independently of the layout-child cache. |
| Reader metadata vectors | Native struct, zoned and debug chunked readers use an admitted indexed metadata factory. It checks cardinalities and all three container layouts before invoking the producer. | Reader cells and DType/name-reference vector capacity, including partial-construction cleanup; referenced schema/text payloads remain separate. |
| Parsed segment specifications | A `SegmentMap` handle owns admitted exact-capacity storage. Footer, source, deferred reader and serializer aliases share that handle. | The segment table. Sources address base segments and metadata locators through a view, avoiding a second combined table. |
| File statistics tables | An admitted `FileStatistics` owner holds exact-capacity `StatsSet` and DType vectors. The parser reads FlatBuffer entries directly; the native writer consumes a lazy exact-size accumulator iterator after admission. | Both table capacities and inline slots, without intermediate view/statistics vectors or vector-to-shared-slice copies. |
| User metadata names and locators | One private `MetadataSegments` owner retains exact vector capacity and copied UTF-8 names. Parser retries, footer/source/serializer clones and native writers share the admitted owner. | Locator entries and owned key bytes under the existing sixteen-key, sixty-four-byte-per-key limits. |

Each owning structure drops covered storage before its reservation. Size and
cardinality checks precede allocation; empty storage makes no fabricated positive
charge. Partial producer/parser failure returns credit. Shared clones keep the
same owner instead of copying its storage or exporting an uncredited raw alias.
The admission contract measures requested payload capacity, not allocator
bookkeeping or process memory.

Metadata names are validated as borrowed references before copying. A bounded
stack array detects duplicate keys without a temporary heap table. Incremental
parsing keeps the same owner through missing-size/missing-data responses and
later retry. Native writer replacement metadata uses the receiving session;
reserialization without replacement preserves the existing owner.

The segment-source work also fixes an actual native buffer alignment error.
A buffer declared 64-byte aligned can contain a valid segment beginning at an
8-byte-aligned offset. The source now uses Vortex's `slice_unaligned`, then
retains the existing actual-alignment check and admitted copy when required.
The failing fixture and exact-grant misaligned-copy coverage are preserved.

## Pinned API compatibility

These are changes to the vendored Rust provider boundary. ShardLoom CLI,
Python, SQL, diagnostic schemas and persisted formats are unchanged.

| Provider surface | Compatibility mapping |
| --- | --- |
| Existing caller-directed cache/offset/vector construction | Remains an explicit caller-owned boundary; native provider paths use fallible session-admitted construction. It does not retroactively admit caller allocations. |
| `Footer::new` and `FileSegmentSource::open` | Continue accepting caller-owned raw segment slices. The admitted source constructor takes the owned `SegmentMap`. |
| `Footer::segment_map` | Returns a borrowed `SegmentMap`; retained callers clone the handle. Existing indexing, iteration, lengths and compressed-size inspection remain available. |
| `FileStatistics::new` / `new_with_dtype` | Keep caller-owned raw-slice inputs. `stats_sets` and `dtypes` return borrowed slices; cloning the whole `FileStatistics` retains admitted table storage. Cloning an entry creates separately owned payload. |
| User metadata inspection | Retains borrowed access. Private parser/writer construction receives its allocator explicitly; unchecked/default construction is confined to format tests. |

## Exclusions and scope

The six classes do not cover schema name text, nested DTypes, raw shared-string
aliases, process-global interned IDs, read-context ID arrays, wide-field lookup
maps, expression caches, arbitrary layout-vtable data, small-vector overflow or
separately owned statistic scalars. Other I/O, file-cache, codec, preparation and
writer allocations remain outside this finite claim. Fixed wrapper
bookkeeping, caller/Python/library retention and whole-process RSS are outside
the payload-credit claim.

Schema work needs a concrete ownership and compatibility contract: a raw
`Arc<str>` can escape its schema, and lazy nested types can allocate repeatedly
through infallible accessors. An outer reservation cannot cover those lifetimes.
Allocation denial must not become an accessor panic.

PulseWeave and Capillary continue using the existing CPU and memory owners.
This change introduces no new topology, worker-count policy, codec reuse, spill
permission or source effect. Existing driver-free sort/filter/projection,
mixed-job, retained-result and cancellation tests are preserved; they do not
close a broader PERF-03 acceptance matrix. Only concrete gaps in the selected
workload or existing declared guarantees block required-area acceptance; extra
allocation classes and wider compositions remain in the optional backlog.

## Verification and preserved failures

The five new native test modules contain 69 regressions: 26 structural-cache
and reader-vector tests, eight offset tests, eleven segment-map tests, thirteen
statistics tests and eleven metadata-locator tests. They exercise real provider
constructors, exact and one-byte-short grants, empty/wide/nested cases, overflow
before callbacks, malformed input, competing owners, denial/retry, lazy children,
partial construction and final-owner release after file/session teardown.

Earlier fixtures attributed all retained credit to one owner. Each correction
preserves the old grant and failure, records the separate allocator requests,
and proves the independently retained alias keeps only its own credit. No grant
is raised to hide a newly admitted owner. A standalone file test allocator now
explicitly forwards external reservations while retaining its original buffer
assertion. Production allocators still reject unsupported admission.

The derived-relational CLI cleanup regression now obtains its retained-memory
baseline from an independent successful session. A cold session exercises the
original cancellation, consumer-failure and generation-change paths against
that baseline. Both sessions return all credit on teardown; the three original
retained-credit checks and failed-execution counters remain. This distinguishes
legitimate lazy metadata retention from failed-operation state.

The package notice validator initially rejected stale vendored modification
descriptions. Only the two provider `local_changes` strings were synchronized
with their reviewed provenance. Dependency identities, versions and license
texts remain unchanged. The original failed package, CLI, lint and attribution
observations remain in the evidence.

Final local checks and portable replay are recorded in the
[companion acceptance report](native-structural-provider-acceptance-2026-10-21.md).
Source identities distinguish the tested dirty base, each corrected
test-only or notice-only input, and the final committed implementation. The
packet retains complete values, original failures and guarded-process outcomes;
it does not retain runtime, environment or dataset backups. New measurements
do not imply a speed gain. No PERF owner, CG-1 through CG-23 gate or release is
closed by this finite batch.
