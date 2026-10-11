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
retain the initial source, execution, failure and readback evidence. The PR review
corrections described below have separate source and check evidence. Hosted
integration remains pending.
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

## Initial complete-workflow verification

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

All public workflow families execute freshly against the initial recorded source. The
ignored native tests remain separately gated benchmark, attribution,
external-fixture or regeneration helpers. Finalization checks complete values,
types, schemas, output identities, failures and completion certificates as well
as resource fields. The typed family alone retains 343 raw reports, 164 complete
value files, 36 recursive-schema proofs and 88 output artifacts; these overlapping
evidence counts must not be added as independent workloads.

## Provenance and timing

The initial complete-workflow acceptance tests clean commit
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

## PR review corrections

The initial 39 hosted checks passed, but review of PR #1541 found two allocation
bypasses. Native `.vortex` preparation dropped its resolved resources,
and three older top-level encoded report paths evaluated kernels without an
admitted memory owner. Both findings were valid.

Native preparation now forwards the declaration and actual shared pool through
both public entry points. The existing Vortex allocator charges footer reads to
that pool. An 8-KiB copy reservation precedes input inspection; copies additionally
reserve the workspace staging writer's 256-KiB buffer before output creation.
Pass-through and copy reports preserve exact bytes/origins, distinguish declared
parallelism from the single admitted lane, and report reservation use and scope.
Copy and pass-through retain native encoded layouts. Metadata/report objects and
uninstrumented provider allocations remain outside the reservation claim.

The `PreparedEncoded`, `SourceBackedEncoded` and `ReaderBackedEncoded` facade
variants now refuse execution with `encoded_facade_resource_admission` and attach
the declaration without an execution certificate. Their kernels are unbudgeted
legacy report fixtures; ordinary SQL, Python and CLI operators use the admitted
native relational/Vortex primitive paths. Capability discovery changes those
three rows from executable to unsupported: one executable row, six unsupported
rows, with the other categories unchanged.

Regression cases cover one-byte refusal before inspecting a missing input,
shared retained owners, footer denial, output-buffer denial before replacing an
existing destination, exact copied bytes, pass-through, and complete public
preparation evidence. All three facade variants refuse filter, projection, and
combined filter/projection at both one-byte and 4-GiB declarations, without kernel
reports or certificates.
The capability matrix also checks each blocked row.

All 14 fresh correction gates pass: formatting; default and native workspace
lint; 3,166 default workspace tests; 1,338 native CLI tests; 2,534 native Vortex
tests with the existing 24 ignored cases; 17 example tests; lean, write-only,
native-without-write and all-feature checks; Rust 1.96 lean/native checks; and
diff validation. The complete native suite includes its spill/pressure cases.
Python source and dependencies are unchanged by this correction; their original
checks remain separately recorded rather than counted as freshly rerun checks.

The [review correction evidence](evidence/required-execution-resources-review-2026-10-10.json)
binds the six changed source files, final source identity, fresh source checks,
documentation checks, focused observations, and preserved compile/lint failures.
The original 32,497 public cases and 129 Full43 results remain evidence for their
original immutable source; they were not rerun for this isolated correction.
Query kernels, input adapters other than existing-native-file preparation, and
website content are unchanged. Original timings do not measure the changed copy
path, and no new performance claim is made.

## Exact-byte aggregate policy follow-up

Review of the corrected source found another valid issue: aggregate policy used
a 65,536-item lower bound and fixed candidate windows even when the caller's
exact byte grant was smaller. The new policy removes that floor, caps both
heavy-hitter families by the byte-derived estimate, prevents physical route
selection from raising those windows, and removes the transformed dictionary
cache's 16,384-item floor. Grants below one estimated 128-byte grouped-state item
fail before source opening and before constructing grouped state.

This changes policy estimates for small grants; it does not turn uninstrumented
hash-table/tree capacity into a measured reservation or a process-memory bound.
Whole-GiB benchmark allocations keep their previous policy choices. The initial
Full43 and public-workflow packet remains immutable and has not been rerun for
this policy correction; no speed or memory-usage improvement is claimed.

Focused checks cover byte boundaries, string/count-distinct/numeric-string
candidate routes with and without writers, cache/mirror capacities, early
refusal and complete grouped values under a 2-MiB declaration. A first broad
run caught an over-broad guard in runtime setup shared with projection and sort.
The guard now applies only to aggregate payloads; all 77 affected public workflow
and preparation tests pass. All 14 fresh source gates pass on the corrected
source: 3,166 default workspace tests, 2,539 native Vortex tests with 24 explicitly
ignored cases, 1,338 native CLI tests and 17 example tests. Formatting, strict
Clippy, feature-isolation and minimum-Rust-version checks also pass. The
[evidence index](evidence/required-execution-resources-2026-10-10.json) retains a
separate correction packet binding these checks, the failed first attempt and
the unchanged original evidence. Hosted checks for this correction remain pending.

## Python inheritance follow-up

Further review identified two Python gaps: direct context `run` and `prepare`
calls omitted a context-only allocation, and a ceiling-only configuration was
incorrectly treated as an incomplete execution allocation. Direct context calls
now resolve and forward their resources, including partial call overrides.
Clients, contexts, sessions and lazy plans retain ceilings separately from the
job grant. Descendant plans and source factories preserve those ceilings, and
terminals validate them before data conversion, producer demand or dispatch.
Shared client configuration remains unchanged.

All 660 Python tests pass, including the new ceiling-only, direct-call and
early-refusal cases. Eight fresh public Python/native workflow checks verify
complete values, exact bytes, per-field origins and ceiling propagation across
preparation, direct execution, context/client/session collection, a writer and
incremental results. These wrapper checks deliberately use the retained initial
resource-contract release executable identified above; they do not remeasure the
later Rust corrections. The first full Python run caught an unclassified public
property in the static API inventory, now corrected. Two acceptance-driver
failures are preserved: an omitted explicit input declaration, and use of the
wrong result-wrapper accessor. Neither is counted as passing acceptance.

The supplemental packet in the
[evidence index](evidence/required-execution-resources-2026-10-10.json) binds the
seven changed Python source/test files, full test output, all eight raw runtime
reports and their complete-result checks. Rust sources and dependency manifests
are byte-identical to the preceding 14-gate correction, so those expensive Rust
checks are retained rather than described as newly executed. No Full43 rerun,
new performance claim or package publication accompanies this Python correction.
Fresh hosted acceptance remains required.

## Non-Unix admission follow-up

Review found that the older non-Unix metadata-count path could execute through
an unbudgeted Vortex session while reporting the numeric declaration as admitted.
All public primitive entry points now refuse on that platform before opening a
source or changing a destination. The CLI reports declared resources and
unavailable admission. Unix execution continues through its existing shared owner.

Fresh local formatting, default workspace lint and all 3,166 default tests pass.
Native workspace lint and all 1,338 CLI tests pass. Three new non-Unix regression
tests compile but are explicitly ignored on this Unix host: they cover existing
and missing inputs, one-byte and larger grants, direct/partitioned APIs, writer
preservation and the actual CLI envelope. The Windows compatibility lane now
executes those tests; hosted runtime proof is pending. An initial launcher used
an older Python without `hashlib.file_digest`, and the first native lint attempt
caught a test import from the wrong crate. Both failures are retained; the
corrected invocation and test compile pass.

The supplemental packet in the
[evidence index](evidence/required-execution-resources-2026-10-10.json) binds this
source correction, test source, local gate outputs and CI registration. The
preceding complete workflows and native Vortex suite remain historical evidence;
they were not rerun for this platform refusal. No new benchmark or package
publication accompanies the correction.

## Shared reader and writer follow-up

The next review found two valid admission gaps: Unix CSV/JSONL exports could open
an unbudgeted Vortex session, and a writer-only build could materialize a text
source before resource admission. Synchronous primitive sessions now install the
reserved native allocator before footer access and retain that session through
partitioned scans and later materialization. Preparation reserves control and
fingerprint scratch before source inspection; retained preparation identities
keep their metadata credits on the same pool.

Buffered scalar and columnar writers reserve construction estimates before
conversion. Native host allocations, writer buffers and footer reopening use
that pool. Structured Vortex output passes its retained reader's pool into the
buffered writer, so output cannot issue a second grant while input remains live.
Empty streams retain the admitted streaming provider and report its actual
identity. Caller containers, reader internals and provider allocations bypassing
the host allocator remain excluded; this is not a process-memory ceiling.

The `vortex-write` profile without `universal-format-io` has no accounted
compatibility reader and now refuses preparation before source access or output
creation. CI covers all CLI targets in that profile. Regression checks verify
small-grant denial, malformed inputs, existing destinations and absent output
directories, retained reader/writer competition, released-credit reuse, empty
streams and complete structured output values.

All 16 fresh source gates pass: formatting and strict lint; 3,166 default
workspace tests; 1,339 native CLI tests; 2,546 native Vortex tests; 1,104 reduced
writer-profile tests; 17 example tests; lean, native-without-write, write-only and
all-feature checks; Rust 1.96 lean/native checks; and diff validation. The native
Vortex suite retains its 24 existing ignored cases. The three non-Unix CLI cases
are ignored locally; their separate Windows run passed on reviewed `1acbd5d6`
and is evidence for the earlier platform refusal, not this writer correction.

The supplemental packet in the
[evidence index](evidence/required-execution-resources-2026-10-10.json) binds the
final source to these checks, documentation checks and CI registration. It keeps
the initial lint failure, the two native test failures, the reduced-profile
failures and a deliberately stopped broad run. That run was stopped when review
found the structured writer's independent pool; after the shared-pool correction,
the entire native suite was rerun successfully. A zero-test filter attempt
receives no coverage credit. The original public workflows and Full43 results
were not rerun or retimed for this correction. No performance gain or package
publication is claimed; hosted checks for the final source remain pending.

## Remaining work

Hosted acceptance remains pending. The next implementation
unit exposes native Python result owners, direct columnar input/output,
in-process operations and shared native expression/prepared-plan handles using
this resource contract. It has no acceptance credit from the resource packet.
Safe full-domain timestamp statistics/compression remains attached to ingest and
pruning. The six remaining areas, eight investigations and CG-1 through CG-23
retain their existing owners and statuses; version 0.5.1 remains fixed.
