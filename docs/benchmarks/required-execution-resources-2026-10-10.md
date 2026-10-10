# Required execution resources

Execution now requires an explicit memory allocation and maximum parallelism.
Callers can configure them once on a context/session, supply them on an operation,
or deliberately load validated environment settings. Missing, invalid or
unauthorized configuration fails before input inspection, producer demand,
preparation or output creation. There are no built-in numeric fallback values.
Lazy declarations and side-effect-free discovery remain resource-free.

The [contract](../architecture/required-execution-resources-2026-10-10.md)
defines the implementation and compatibility change. The
[evidence index](evidence/required-execution-resources-2026-10-10.json),
[immutable packet](evidence/required-execution-resources-2026-10-10.json.xz),
[independent inspection](evidence/required-execution-resources-2026-10-10-inspection.json)
and [inspector review](evidence/required-execution-resources-2026-10-10-inspector-review.json)
retain exact source, execution, failure and readback evidence. Local engine,
support and rendered-guide acceptance are complete; hosted integration remains
pending.
Published v0.5.1 packages are unchanged. This unit makes no performance claim.

## Configuration and ownership

Rust and Python share the same resource contract: exact bytes, positive integer
execution lanes, per-field origin and intersected authorization ceilings.
`memory_gb` denotes GiB (2^30 bytes); `memory_bytes` permits exact allocations.
Partial call overrides inherit the other configured field and cannot remove or
exceed an administrator ceiling. SQL continues to describe computation while the
context or request carries resources.

`ExecutionResources.from_env()` and `--resources-from-env` deliberately load the
existing environment variables. Importing Python or supplying explicit execution
arguments does not consult ambient settings. Malformed values receive the stable
configuration diagnostic instead of replacement numbers.

The allocation reaches Python, SQL, CLI, the persistent worker, native session
constructors, ingest/preparation, collection, incremental delivery and writers.
Compatibility preparation uses the existing session memory pool when retained
owners overlap. Native buffers, preparation and output therefore compete for the
same credits. Sequential preparation releases its owner before the next stage
when no retained overlap exists. Dynamic scheduling and admitted native spill
continue inside the declared allocation.

Reports separate declarations from admission and observed use. Instrumented
resident paths report the actual session allocation and live/peak reservations;
the scope includes preparation and retained owners over the session lifetime.
Spill fields distinguish observed events, measured native payload bytes and
unavailable byte counts. Uninstrumented admission and usage remain explicitly
unavailable. Requested memory is not preallocated memory or a process-RSS ceiling;
maximum lanes are permission, not measured utilization. Caller Python objects,
CLI report formatting, allocator overhead and unreviewed provider allocations
remain outside the native reservation claim.

## Complete verification

The rejection tests cover absent and partial configuration, invalid values,
overflow, booleans, strict environment loading, inheritance, field overrides and
authorization ceilings. Recording sources and one-shot producers verify no data
access on rejection. Native tests retain an existing owner across preparation or
writer denial, then release credits and prove successful reuse.

The independent report contract verifies all nine retained workflow families,
including every authoritative resource block and all three partial typed
projections. It validates 66,055 authoritative declarations and 132,110 per-field
origins. These are report blocks, not independent query counts. Required values,
admitted grants, observation scope and no-fallback evidence must agree; unavailable
measurements are not silently interpreted as zero.

| Validation | Accepted result |
| --- | --- |
| Fresh source/build, formatting, lint, feature and MSRV gates | 16 pass; no source-gate reuse |
| Default workspace tests | 3,165 pass |
| Native library tests | 2,531 pass; 24 ignored |
| Native CLI / example tests | 1,337 / 17 pass |
| Python tests | 655 pass |
| Complete public regression | 32,497 cases; 18,595,284 rows compared |
| Direct unary regression | 202 cases; 131,734 rows compared |
| Exact typed workflows | 87 pass |
| Finite streaming / input-growth workflows | 442 / 20 pass |
| Input-pressure controls | All five pass, including expected resident denial |
| Batch / format adapter workflows | 48 / 19 pass |
| Admitted semantics / golden stages | 145 / 9 pass |
| Resource-report controls | 52 pass |
| Reviewed streaming-inspector controls | 1,379 pass |
| Full43 retained-input regression | All 129 complete results pass |

All public workflow families execute freshly against the recorded source. The
ignored native tests remain separately gated benchmark, attribution,
external-fixture or regeneration helpers. Finalization checks complete values,
types, schemas, output identities, failures and completion certificates as well
as resource fields. The typed family alone retains 343 raw reports, 164 complete
value files, 36 recursive-schema proofs and 88 output artifacts; these overlapping
evidence counts must not be added as independent workloads.

## Provenance and timing

Acceptance tests clean commit
`868d2b0cf7ae6cfd0c9f194ad522a3873bc77e97` and 1,037 source assets, identified by
`33757c73b634e1f2292f9ff5030f8c07d29ff49e7fea32f4f1ece26be5d1dd38`.
The Rust 1.99.0 macOS arm64 release executable uses `release-user-surfaces`, with
SHA-256 `c414cd924cb5345d52ccc2b1f153eaa858e82ab77d93d39a3afaac23f30d96dd`.

Full43 runs each query three times against the retained 15,682,956,489-byte Vortex
artifact, with 24 GiB and maximum parallelism 12 explicitly configured. The sum
of query minima is 70.135621916 seconds; medians sum to 72.633668041 seconds.
All 129 native calls total 223.504400873 seconds; the guarded stage takes
255.513512708 seconds. Maximum observed native-process RSS is 4,495,409,152 bytes.
Timing includes native process creation, complete CLI output and exit. OS cache
is uncontrolled, each run uses a new process, and no result cache is used.

This is an unpaired retained-input regression against retained native complete
results, with complete-result comparison and finite-float tolerance 1e-12. It is not an
independent external correctness oracle, fresh ingest, a paired comparison or a
speedup claim. The earlier fresh-ingest checkpoint remains separate.

The immutable packet is 56,119,196 compressed bytes with SHA-256
`f40cb7c3442242f1783a0e98f2963151880ab321e7a795f1c7ebe70133f2f389`.
Its complete decompressed JSON has 5,018,635,157 bytes and SHA-256
`19592adc6aa6c441b22d06ca748508c5b60849e6d1401ef7b880369940000b42`.
Independent inspection validates the complete decompressed identity and content;
invalid-check, invalid-report and fallback counters are zero. The finalizer and
successful inspector finish within their deadlines with child groups drained.

## Preserved failures and evidence review

Full43 storage admission first refuses to run at the unchanged log threshold.
Lossless compaction of 516 closed historical files recovers 3,231,744 accounted
bytes but does not yet pass admission. A second verified compaction archives four
closed profile files; subsequent admission and all 129 query runs pass. The
finalizer independently reopens all 520 archived members. Inputs, failed or
incomplete observations, unrelated files and storage ceilings remain unchanged.

The first finalizer fails because its supervised environment lacks the existing
PyArrow dependency path. The corrected invocation passes without changing engine
source or rerunning accepted queries. Both observations remain in the packet.

The first complete reader verifies the packet identity but rejects its resource
counter: it counts both authoritative declarations and duplicated typed-result
projections, while the expectation counts authoritative blocks once. The separate
finalizer already checks agreement across all three partial projections. The
reviewed inspector fixes counting, preserves rejection of invalid values in every
projection and adds 864 projection controls. The original packet, engine source,
binary, reader and failure observation remain unchanged. A wrong-relative-path
review invocation is also preserved. The subsequent complete readback passes;
the separate review artifact binds both versions and all observations.

## Documentation and review

All 14 documentation/use-case checks and ten site generation/check steps pass.
The site check reports zero errors, warnings and hints. Refreshed mobile
(390 by 844) and desktop (1280 by 720) review verifies the configured-context
example, deliberate environment loading, resource guide navigation and acceptance
link. Searching for `environment` returns the updated Python section and its link
opens the correct example. Neither viewport has page-wide overflow, and no
warning/error logs are captured in the successful review tab. Earlier review
separately covers home-page Python/SQL tabs and the getting-started route. The
[support receipt](evidence/required-execution-resources-support-2026-10-10.json)
binds the checks, generated pages and both browser observations.

## Remaining work

Hosted acceptance remains pending. The next implementation
unit exposes native Python result owners, direct columnar input/output,
in-process operations and shared native expression/prepared-plan handles using
this resource contract. It has no acceptance credit from the resource packet.
Safe full-domain timestamp statistics/compression remains attached to ingest and
pruning. The six remaining areas, eight investigations and CG-1 through CG-23
retain their existing owners and statuses; version 0.5.1 remains fixed.
