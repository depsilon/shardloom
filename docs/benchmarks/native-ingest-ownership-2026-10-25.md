# Full-ingest ownership: finite measurement protocol

Frozen before the first new complete ingest, October 25, 2026. This protocol
does not claim a measured benefit. Preserve every observation, including failed
admission, correctness, publication and performance observations.

## Contract and controls

Use the accepted Parquet input at
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/sources/hits.parquet`
(14,779,976,446 bytes; verify the full accepted SHA-256 before measurement).
The accepted provider-refresh artifact contains 99,997,497 rows, 105 original
fields in source order, and seven explicitly derived fields. Freeze the full
source/target hashes, source generation, code and executable identities,
resource settings, host observations and current codec/layout recipe in a
machine-readable receipt before execution.

Keep the existing standard ingest command, 24 GiB memory admission, maximum
parallelism 12, source batch policy, compression configuration, derived fields,
publication and storage safeguards. Run through
`scripts/run_clickbench_ingest_uat.sh`. Use a new output destination outside
synced folders. Do not raise storage or concurrency limits. Keep task-owned
builds, tests and other substantial workloads idle during measurements.

## Attribution and candidate selection

1. Build an observation-only control that preserves validated copies. Record
   actual copied buffer bytes, allocation charges, buffer-region counts and
   repeated exact visible regions within each batch. Keep the original owners
   alive while comparing region identities; an address alone is not an owner.
   No cross-batch cache is allowed. Record ownership admission/copy/validation
   as a nested work span separately from the inclusive Arrow conversion span.
   These are overlapping elapsed work spans, not exclusive wall or CPU time.
2. Execute one complete guarded control ingest. If ownership work is below 10%
   of the inclusive conversion work, or no actual copied bytes are observed,
   stop the ownership performance candidate and document the attribution.
3. Prefer exact duplicate removal when repeated bytes reach at least 20% of
   copied bytes. Otherwise investigate the provenance-checked internal transfer
   candidate: consume the input batch, require Arrow's unique standard allocator
   ownership proof, zero allocation pointer offset, explicit alignment, full
   allocation size no larger than the existing copy charge, and retained native
   memory credit. Shared, offset, empty, oversized or opaque owners keep copies.
   Generic external-memory intake keeps its existing copy contract.
4. Admit only one of those mechanisms to the first complete candidate screen.
   Preserve the accepted artifact representation. No new compression portfolio,
   scheduling experiment or re-enabled warm cache belongs in this candidate.
   Tests must establish the full ownership/lifetime/resource contract first.

## Complete-ingest screen and matched confirmation

The observation-only control is the comparison executable. Keep it immutable
after its source and executable identity are recorded. The candidate must use
the same attribution fields and recipe. Initially run the candidate once. Stop
without a repeated cohort if it removes less than 20% of copied bytes, fails
any correctness/publication/resource check, exceeds the control artifact size
by more than 0.5%, or exceeds control complete-ingest process time by more than
5%. Require at least 3% complete-ingest improvement in this first observation
before admitting repeated confirmation. A preliminary result is a screen,
not a speedup claim.

Only a candidate passing that screen receives three fresh matched pairs in
the declared order control/candidate, candidate/control, control/candidate.
Each execution is a complete successful ingest. Keep all observations. Require
at least 5% improvement in the median of the three within-pair percentage
reductions in complete-ingest process time, and no pair with a candidate
regression greater than 5%. These diagnostic process clocks are used only for
this explicitly requested ingestion investigation. Do not substitute them for
query engine clocks or put them into routine query timing updates.

Every measured output must pass complete row/schema/publication and encoding
inventory checks. Its size must stay within 0.5% of its paired control, and
observed peak RSS within 5% of its paired control. Allocation reservations,
cleanup, no-fallback evidence and source generation checks must all pass.
The five-percent benefit threshold selects a meaningful complete-workflow
gain; the historical twenty-percent illustration is not an acceptance gate.

## Independent readback and consumer acceptance

Before accepting a candidate, independently reopen both the observation-control
artifact and the retained candidate output. Compare every original value, type,
null and row order against direct pinned Parquet decoding. Verify all seven
derived columns with independent formulas. Use bounded column-at-a-time,
batch/window comparisons; reject unsupported or unknown columns and incomplete
coverage. Preserve generation checks, concurrency bounds and explicit memory
scope. Parquet/provider allocations outside the credited allocator must remain
identified as excluded; a work grant is not proof of their full ownership.

For later paired outputs, exact size and SHA-256 equality to an already fully
validated output establishes byte identity. Otherwise compare every value
against the independently validated control using the existing bounded native
artifact comparator before retiring redundant large payloads. Preserve every
report and full hash first.
Only one new full-size payload should remain once its checks and replacement
requirements permit retirement; never remove the accepted source or artifact.

For a retained correction, run one isolated standard Full43 pass on the new
artifact: exactly one execution of each query with the complete current
independent result reference. Freeze the executable, all inputs and supervision
before the pass; keep all other task work idle during queries. Report only the
sum of its 43 engine clocks. A complete engine sum more than 10% above the
accepted provider-refresh observation triggers query-level attribution before
queue advancement; do not silently repeat the suite. This is a consumer
regression check, not a matched competitive ranking.

## Stop and retain rules

Reject the optimization on any ownership, value, type/order, generation,
publication, accounting, cleanup or control failure. Preserve the failed
observation and reason; do not relax a gate after seeing the numbers. Fix an
implementation defect before any newly declared run. Retain useful validated
readback coverage and truthful attribution separately if the performance
candidate is rejected. Actual repeated preparation or probe work requires
source evidence and a separate frozen finite mechanism; existing fused derived
passes and numeric-probe avoidance are already implemented.

## Preliminary complete readback

Before the first new complete ingest, the existing accepted provider-refresh
file passes the new direct-Parquet readback: **99,997,497 rows, 112 columns and
11,199,719,664 values**. All 105 original fields and seven independently derived
fields match, including native schema, names, nullability and row order. The
comparison completes 1,367,184 equal spans. Input generations remain unchanged,
both native scan tasks drain, and native plus Arrow work reservations return
to zero. This is a later correctness proof; the original provider-refresh
acceptance did not claim complete readback.

The first attempt fails closed at its Arrow batch limit. A diagnostic replay
identifies `Title`, with 29,511,164 retained bytes for 65,536 rows against a
16,777,216-byte ceiling. Both failed observations are preserved. The complete
pass reduces reference and native batches to 8,192 rows while keeping the
16 MiB Arrow ceiling, 8 MiB value limit, 512 MiB native source admission,
two-task concurrency and 8,388,608-row scan windows. Every row is still checked;
the largest observed Arrow batch is 11,489,688 bytes. Decoder and other upstream
allocations outside the native allocator remain explicitly excluded from its
reservation claim.

The source SHA-256 is
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`;
the accepted Vortex file SHA-256 is
`95a393d153a8c2e1ffeb5084b2656ac61465c6ef9dad435d032365844f1ac057`.
The readback executable SHA-256 is
`acf92e36719eac28a484545a9314adc04900af018c42c3440cf480e1efbcfc47`.
The exact command, build/source identities, supervision, complete report and
earlier failures are retained under
`/Users/dylan/LocalData/shardloom/ingest-ownership-20261025/`, indexed by
`baseline-readback-acceptance-r3.json`. No new ingest or ownership optimization
is accepted by this preliminary readback, and the frozen performance gates
above are unchanged.

## Complete observation control and selected mechanism

The first complete control uses immutable source `39210b43` and its matched
four-tool build. It preserves all copies and passes guarded publication,
complete direct-Parquet readback and physical inventory. All 99,997,497 rows,
112 columns and 11,199,719,664 values match. Its 15,715,228,660-byte output has
the same complete SHA-256 as the accepted provider artifact above. The writer
recipe is unchanged, including all 28 Zstd source columns. Target-bound
physical-design digests differ because the output destinations differ.

Attribution observes 483,417 copied buffer regions, 115,652,962,945 copied
bytes and 115,776,717,698 bytes of copy allocation charges. No repeated exact
held region is observed. The nested ownership work is 99.3703% of inclusive
conversion work; these overlapping spans include observation, allocation and
validation and are not exclusive CPU or pure copy time. The frozen 10% screen
therefore admits controlled-reader transfer, while the duplicate-removal
preference does not activate. Actual numeric probe calls are zero.

The full physical inspection covers 876,721 layout nodes, 1,824,506 array nodes
and 611,520 flat references. Its initial 500,000-reference refusal is preserved;
the completed read-only retry uses the existing one-million-layout-node
ceiling, with unchanged segment-byte and array-node limits. Ingestion is not
rerun for that post-check. Readback source/task credits return to zero. The
ingest report's 4,096-byte retained metadata snapshot matches the preceding
accepted ingest and is not described as a zero-after-drop assertion; process
cleanup is verified.

`control-screen-r1-acceptance.json` under the local evidence root binds all
26 supporting members, full source/output identities, unique report fields,
readback, layout, native accounting and diagnostic ingest clocks. Receipt
assembly preserves its two corrected assertions: target-bound decision digest
comparison and the inventory's explicit `sha256:` prefix. No candidate benefit
or new Full43 result is accepted yet; every prospective gate above is unchanged.

## Candidate screen, retention override and consumer check

The controlled-reader candidate at `20f03396` completes one guarded ingest.
Copied bytes fall from 115,652,962,945 to **44,831,432,362**; **70,821,530,583**
buffer bytes transfer with their complete eligible allocation charges. This is
**61.236% less copying** in the measured full ingest. All original and derived
values, exact schema/order, physical layout, generation, publication and cleanup
checks pass. The 15,715,228,660-byte output has the same SHA-256 as the control
and accepted provider file. Independent readback covers all 11,199,719,664
values, with native/Arrow readback reservations and tasks drained.

The first observation does **not** pass the prospective 3% complete-ingest
speed gate. The immutable `candidate-screen-r1-decision.json` preserves that
rejection; no three-pair confirmation is run. Diagnostic process clocks remain
in the local receipts. The maintainer subsequently asks to retain the concrete
copying improvement and open PR #1605. That decision supersedes the proposed
runtime removal, without changing the failed gate or establishing a repeatable
ingest-speedup claim.

The required consumer check executes each of the 43 queries exactly once under
the existing isolated procedure on the newly written artifact. All independent
values (381 rows), schemas, no-fallback reports, identity and cleanup checks pass.
The engine sum is **47.281897502 seconds**, versus the previous provider-refresh
observation of **49.625514041 seconds**. This is approximately 4.7% lower in one
observation and does not activate the 10% regression trigger. It is not a matched
comparison or proof of a causal query speedup; the persisted input is byte-identical
and host/cache conditions are uncontrolled. No cache manipulation or repeated
query trial was added. `candidate-full43-r1-acceptance.json` reopens every saved
query result and verifies all values, schemas and engine clocks independently.

These observations bind the ownership implementation before the subsequently
requested dependency consolidation. Final combined validation must identify its
own source/dependency state and must not relabel these earlier measurements.

## Combined runtime acceptance

The combined source at `23807cb70e13ba2981140a895798e9aff99fb9da`
retains the ownership implementation with Tokio 1.54.1, prost/prost-derive
0.14.4 and the four reviewed workflow-action updates. The
[dependency intake](../architecture/dependency-intake-2026-10-25.md) records
the exact scope, license/provenance checks and unchanged provider graph.

Nine local gates pass: formatting, strict default and selected native lint,
default and release-user-surfaces workspace tests, all-feature and minimal
checks, and Rust 1.96 minimal/native checks. The two test suites execute
**9,364 passing tests**, with no failures and 35 declared exclusions: three
Windows-only checks plus 32 explicit opt-in benchmark/fixture checks. This is
a test-execution count across feature suites, not a count of unique test names.
Source-byte inventories bind all 2,858 build-relevant files to the final build.
The pre-commit test inventory differs only in seven historical Git index blobs;
working source bytes and modes are identical.

The installed Mac ABI3 wheel passes **140 integration tests without skips**,
isolated import/CLI checks, embedded CLI/library identity, license verification
and uninstall. The native extension uses the existing extension-module build
path; the CLI is built separately without that linkage flag. Both dependency
listings exclude Homebrew/Python-framework links. The earlier package-linkage
failure remains preserved. A subsequent receipt-inventory mismatch is resolved
by rehashing every prior receipt and accounting for the newly added evidence;
its original `NEEDS_REASONING` status is retained alongside primary acceptance.
No package is published, and this local proof does not establish a Linux ABI.

The combined runtime's one isolated Full43 pass validates all 43 independent
results, 381 rows and exact schemas at **46.973255495 engine seconds**.
The earlier ownership-only observation remains **47.281897502 seconds**;
the accepted provider observation remains **49.625514041 seconds**. Each is
one execution of each query. The combined pass does not activate the 10%
regression trigger, but separate runs with uncontrolled host/cache state do
not establish a causal speedup. Full input hashing and recursive inventories
remain outside query execution; every earlier observation stays unchanged.

The [portable evidence packet](evidence/native-ingest-ownership-2026-10-25/README.md)
preserves 735 original files as 592 unique objects. Independent replay checks
both complete ingests/readbacks, unchanged failed speed gates, both Full43
passes, the combined tests/dependency audit and installed-wheel provenance.
All 21 corruption controls reject altered coverage, values, clocks, source
bindings, package identities or acceptance decisions. Replay does not execute
native code or need the historical datasets. Existing audit warnings remain
visible; no waiver or runtime fallback is introduced. GitHub integration is
recorded separately from these completed local checks.
