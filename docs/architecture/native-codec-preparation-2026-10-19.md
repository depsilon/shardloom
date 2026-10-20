<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native codec preparation investigation — October 19, 2026

Status: finite candidate rejected and removed. This records the first decision
in the [21 conditional investigations](native-conditional-investigations-2026-10-19.md).
It evaluates `PERF-03.codec_preparation`; it closes no broader owner or competitive
gate. Future codec retention needs a materially different mechanism and a new
frozen protocol. The separate physical-reader reconstruction investigation under
`PERF-02.physical_cache` remains open.

The [portable evidence index](../benchmarks/evidence/native-codec-preparation-2026-10-19.json)
links the complete protocols, source patches, build identities, raw samples,
failure records and restoration proof. All 101 archived members were reopened and
checked against their size and SHA-256. Frozen executables remain local, with
their identities in the index; they are omitted from the portable archive.

## Production boundary and cost attribution

The admitted provider is the local Vortex Zstd 0.85.0 patch with `zstd` 0.13.3,
`zstd-safe` 7.2.4, `zstd-sys` 2.0.16 and Zstandard 1.5.7. Compression already
constructs one compressor per `compress_values` call and reuses it across frames.
Decompression already constructs one decoder context and optional prepared
dictionary per `decompress_frames` call, then uses them for every selected frame.
Neither path reconstructs those objects for every frame within an invocation.

The first temporary attribution probe preserved the existing algorithms and
ownership. It separated allocation, initialization, frame work, compressor
construction and dictionary training across nullable integer, UTF-8 and binary
fixtures. Compression and full-array observations did not justify retention.
Repeated small slices exposed a setup signal, so a second frozen screen tested
the actual prepared native file path and terminal consumers before selecting a
candidate.

That screen covered 54 cells: three logical types, 4,096/65,536 rows, canonical
or Zstd with/without a dictionary, and native canonical delivery in 2,048-row
batches or JSON delivery in 2,048/64-row batches. File producers closed before
prepared readers opened. Every value, logical type, NULL and order position was
verified. In two fresh processes, allocation plus initialization exceeded the
predeclared 10% threshold in both integer/dictionary/JSON64 cells. This justified
a finite dictionary-preparation experiment, not a general reader cache or a
compressor-retention change.

## Evaluated candidate

The candidate was opt-in experimental code installed in a runtime/session owned
provider extension. It retained at most one idle prepared dictionary and its
exact dictionary bytes under the existing memory pool. It used exact content
equality with the pinned preparation recipe; pointer equality, dictionary IDs,
file paths and generation shortcuts were not cache identities. It did not reuse
source validation, query results or data-derived expression state.

An exclusive lease removed the complete entry before decoding. Concurrent calls
used independent preparation when the slot was busy; only one successful return
could occupy the idle slot. The mutable decoder context remained temporary.
Both retained native buffers were charged, and a weak, nonblocking pressure
callback could release an idle entry before retrying ordinary admission once.
Active leases remained protected; optional cache admission failure used the
existing admitted uncached native path.

Adversarial review added an allocator-owned transient witness before timings:
after freezing the initialized workspace, the candidate checked length,
alignment and exact bytes before deriving a temporary provider pointer. The
added allocation, copy and verification were charged and timed. No raw pointer
was stored in the Rust cache owner. This relied on the audited pinned provider
representation, not a general promise about future Zstandard versions.

The cache changed no serialized format or logical representation. Fixed
session/allocator/synchronization bookkeeping remained outside the finite
buffer-credit claim. Neither those credits nor the tests establish a process
RSS ceiling. There was no external-engine execution or fallback.

## Frozen comparison and decision

The candidate protocol was frozen before implementation. Its safety addendum
preceded the successful build and all candidate measurements; the analysis
implementation was frozen before either comparison cohort. The workload,
cache bound, sample count and thresholds were unchanged after observing results.

Both cohorts ran all 54 cells in fresh sequential processes, alternating
candidate/control order. Each role had one warmup and three samples of seven
operations: 432 records and 2,376 operations per cohort, including warmups.
The comparison used the same executable with independent matched owners and
the same 18 physical files as the prior attribution screen. Complete lifecycle
cost included preparation, first use, native delivery, output release and final
owner cleanup. Full-value verification was outside the timer and mandatory for
every operation.

The frozen gate required at least 2% improvement in both qualifying cells and
in the sum of all 54 cell medians, in both cohorts. It also prohibited any
control regression exceeding both 5% and 100 microseconds per operation.

These are focused lifecycle diagnostics, not query-engine or Full43 timings:

| Diagnostic gate | Cohort A | Cohort B | Result |
| --- | --- | --- | --- |
| Integer/dictionary/JSON64, 4,096 rows | 13.63% improvement | 14.17% improvement | Pass |
| Integer/dictionary/JSON64, 65,536 rows | 8.09% improvement | 8.02% improvement | Pass |
| Complete 54-cell workload, at least 2% | 0.71% improvement | 0.60% improvement | Fail in both |
| No control beyond both regression bounds | One violation | None | Fail |

The violating control was 65,536-row integer/Zstd-without-dictionary/JSON64 in
cohort A: 5.24% and approximately 2.61 ms per operation. The decision follows the
frozen all-cell rule; it does not attribute that control difference to a proven
mechanism. No workload pruning, cache tuning or extra timing repeats followed.

**Reject the candidate.** Targeted gains did not satisfy the complete-workload
or control requirements. The patch and all six new source files were archived,
compared byte-for-byte with the working experiment, and then removed. All 1,895
tracked Rust/Cargo source inputs match baseline `62754f90` exactly at restored
commit `1a898f26`; intervening PR evidence corrections affect Python/docs only.
No experimental cache or attribution probe remains enabled or retained in the
repository.

## Correctness, failure and ownership evidence

Before candidate timing, the memory-pool suite passed 15 tests, the expanded
cache suite passed 14, and the encompassing Zstd regression suite passed 76.
These are overlapping selections, not additive counts. The tests covered exact
dictionary content in distinct owners, equal-length misses, concurrent distinct
trained dictionaries, idle-slot contention, active-lease pressure protection,
limit reduction, ordinary allocation eviction, optional admission denial,
freeze relocation/replacement and last-owner cleanup. Malformed, truncated,
legacy and output-overflow cases retained the exact uncached diagnostics.

A separate writer process exercised all 54 workflows, then exited. An ordinary
reader process with no cache installed verified the hashes and complete values
of all 18 files through the same 54 workflows. Readers closed before fixture
cleanup. Both comparison cohorts verified all values/types/NULLs/order, actual
cache hits where applicable, bounded credits and zero final reservations.
All supervised process groups drained.

Three compile failures are preserved: the first production-attribution harness,
the initial cache build and the expanded test build. Their repairs preceded
candidate timings. They are not successful observations or discarded samples.

The failed finite cost gate stopped promotion to production installation and
broad workspace/native/public/Full43 candidate acceptance. Those broad checks
were not run for this rejected candidate. Exact source restoration is evidence
that the prior runtime was restored, not a new test pass. Any retained runtime
change elsewhere in the conditional pass still requires the repository's
coherent-batch validation gates.

## Replay and remaining scope

The archive contains separate diagnostic and candidate patches, all added
sources, frozen source manifests, commands, guard helpers, raw logs and analysis.
Replay in an isolated baseline checkout, adding the six candidate sources with
the recorded candidate patch. Historical absolute paths must be rebound to new
local evidence paths; never overwrite an original run identity. Preserve the
protocol, exclusive workload guard, process cleanup and storage ceilings.

This decision rejects one idle-dictionary mechanism in the measured local
macOS arm64 scope. It neither proves every codec-preparation strategy unhelpful
nor admits cross-call physical-reader caching. The current temporary workspace
contract remains unchanged, and the conditional-first sequence continues.
