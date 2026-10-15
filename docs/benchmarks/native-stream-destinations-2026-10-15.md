<!-- SPDX-License-Identifier: Apache-2.0 -->

# Streaming input through local destinations: acceptance

Finite streaming input now reaches all eight existing local writers, and one
execution can feed several destinations through credited native result replay.
The direct Python binding also routes independent producers through collection,
incremental output and writes. Vortex remains the native representation and
highest-fidelity persistence target; each compatibility writer retains its
existing type, feature and fidelity limits.

Local runtime acceptance passes: 3,219 default workspace tests, 4,229 native
tests, 84 direct Python tests, 777 public cases across 20 families and one complete
43-query pass. The [portable evidence index](evidence/native-stream-destinations-2026-10-15.json)
records 3,993 verified outer members and the preserved 2,300-member failed public
run. PR #1557 is merged after all 44 hosted checks pass; the
[hosted packet](evidence/native-stream-destinations-hosted-2026-10-15.json)
records three installed-wheel lanes, source identity, and preview/production
verification. These source changes follow the immutable 0.6.0 packages and do
not publish another release.

## Shared execution and ownership

The [implementation contract](../architecture/native-stream-destinations-2026-10-15.md)
extends the existing relational batch adapter, completed-result writers, native
replay recorder and query-run store. Native Vortex, Parquet, Arrow IPC, Avro, ORC,
JSON, JSONL and CSV use their existing writer implementations. ORC's nested,
decimal and temporal exclusions remain explicit.

One destination streams directly. Multiple destinations consume each declared
producer once, execute the relational plan once and retain the completed native
result under the query allocation. Each writer reads that result into private
staging. Every input must finish, every writer must succeed, native replay owners
must be released, and owned spill cleanup and certification must complete before
publication. Input generations and target paths are revalidated before commit.

This result replay is separate from repeated-source replay. Without spill
permission, a result that exceeds the shared grant is refused. With an explicit
policy, source replay, operators and output replay use the same native run store
and disk quota. Repeatable file/resident-source fanout keeps its existing
per-destination execution strategy. Local multi-file publication remains
non-atomic: a late commit failure preserves already published complete targets
and identifies them in its diagnostic.

The Python binding retains one lazy reader per declared producer and routes
demand by source identity and ordinal. Each producer keeps its own schema,
factory, completion and closure state. Reader metadata and conversion scratch
are credited; switching producers releases the previous conversion reservation.
Existing single-source Rust interfaces remain available. No new execution engine,
transport, codec, dependency or public Python method is added.

## Correctness and pressure evidence

| Check | Result |
| --- | --- |
| Workspace formatting | Passed |
| Workspace, native-read and release-user-surfaces Clippy | Passed with warnings denied |
| Default workspace tests | 3,219 passed; none ignored |
| Native CLI/Vortex tests | 4,229 passed; 29 declared manual/platform exclusions |
| Direct native Python tests | 84 passed; no failures, errors or skips |
| Public workflows | 777 passed across 20 families |
| Full43 acceptance | 43/43 complete queries; 381 result rows; one call per query |

Single-writer regressions cover mixed values, empty input, all-null input and
late producer failure across every writer. Fanout covers all eight writers,
typed empty output, exact recursive values, static admission, multiple producers,
repeated sources and dynamic schemas. The direct binding checks 24 composition
combinations across collection, incremental delivery, a single writer and fanout,
plus eight multi-source lifecycle modes. Original Python producer errors are
preserved; opened producers close on completion, failure, cancellation or early
consumer closure, and later session use remains valid.

The native pressure fixture uses 8,193 rows with 2,048-byte text values, a
16-MiB query grant and 251-row producer batches. Resident result retention is
refused. With the same grant and an explicit 128-MiB spill quota, both Vortex and
Parquet outputs contain every expected row. The execution uses multiple native
runs and merge passes, reports one execution and two result replays, stays within
its credited allocation, and refunds owners and removes its runs. Quota refusal,
corruption, cancellation and unknown-file cleanup failures publish no prefix;
unknown files are preserved. These are allocation and ownership checks, not a
whole-process RSS ceiling.

Independent public-evidence inspection reopens 893 complete-value files with
22,491 expected rows, 1,408 native envelopes, 20 pressure files with 677,251 rows,
and 305 artifact references. The exact 777-case list matches the frozen protocol.
Complete values, resource evidence and both false fallback fields are checked.
The local direct tests use an explicit frozen native library with the source
package. Separate installed-wheel and cross-platform results are recorded below.

## Preserved failures and corrections

The packet retains the original failures and their corrections. The first
all-writer regression exposed the old compatibility-output refusal. A later
pressure run showed merged replay batches requesting more transient writer
memory than the original small producer batches; replay now preserves that
batch bound. The multi-source direct baseline reproduced the binding's former
one-producer restriction before source-aware routing was implemented.

Development checks also caught a CSV empty-string expectation, Python dependency
and report-field setup errors, a cleanup assertion placed after compatibility
readback created its preparation directory, Clippy findings, and a stale window
test expecting JSON writers to remain unsupported. Those failures and corrected
checks are retained. The final six native commands all pass. Their first wrapper
postcheck referred to an older manual-test inventory; the completed logs and all
29 current declarations were independently reverified without rerunning those
successful commands. Vendored provider source is unchanged, so its previous
selected-view acceptance is retained separately.

The first full public attempt passed 765 cases, then stopped at the existing
192-MiB retained-log limit before starting the next case. No recorded case failed.
The only subsequent source change replaces indentation in the UAT's JSON evidence
writer with compact separators. An AST comparison proves that this is the sole
executable change; all 2,214 retained JSON files round-trip with identical values,
saving 32,734,169 bytes. No storage limit, case, resource grant or runtime behavior
changed. The failed run remains intact and is included as a verified nested
archive. The complete 777-case rerun passes within the original limit.

The release binaries and 84 direct tests are reused across that evidence-format
correction. Both source manifests are retained: all 2,263 file names match and
only `scripts/run_native_streaming_input_uat.py` differs. The manifest explicitly
links the original build source, current harness source and equivalence receipt;
it does not claim that the binaries were rebuilt.

## Existing-destination publication

All eight writers already reject an existing destination, including a resident
single write with overwrite permission. Their shared `OwnedOutput` publication
boundary has no atomic generation-conditional replacement. The new preflight
moves that refusal before producer demand; it does not remove supported
replacement behavior.

The [review evidence index](evidence/native-stream-destinations-review-2026-10-15.json),
[archive](evidence/native-stream-destinations-review-2026-10-15.tar.xz) and
[manifest](evidence/native-stream-destinations-review-2026-10-15.manifest.json.xz)
retain 185 verified members. Three added CLI integration tests exercise 50
scenarios: 16 absent-target creations, 32 existing-target single-write refusals
and two fanout refusals. Rejected calls preserve existing bytes, create no
staging or other destination, and demand no input. A separate 32-case comparison
of the frozen prior and current CLIs confirms both versions reject existing
targets and create absent ones across all eight formats. All 16 successful
outputs are reopened and compared completely: 48 rows and 60 native envelopes
are retained. The prior version consumes the producer before refusing existing
targets; the current version does not open it.

The initial overwrite-success expectation failed on the original runtime and
again after narrowing the preflight. That experiment was reverted: runtime and
package sources remain byte-identical to the original accepted source, with only
the two integration-test files changed. A missing bounded-read option in the
first readback test was corrected. All six validation commands pass again, with
3,219 workspace tests and 4,232 native tests, including the three new tests and
the same 29 declared exclusions. A stale manual-test inventory in the wrapper
and an incorrect duplicate-field check in the packet verifier were corrected
against the actual schemas and complete original logs; both failed helpers or
receipts remain retained. The original 84 direct tests, 777 public cases and
Full43 pass retain their original source and executable identities. They are
not presented as new runs, and no new release build was needed.

## Hosted integration and production verification

[PR #1557](https://github.com/depsilon/shardloom/pull/1557) merges at
`e4814c5e1b58972924aa95f5de2e51024f6b24d8` after all 44 checks succeed on
`54f107d4add9d1d218f0f78e47c8d6f76785120a`. CI executes the synthetic merge
`4e9b27921c71c7176ad5b215704e531f2b20bf0e`; the PR head, synthetic merge and
actual merge have the same tree. The existing-target review comment is answered
with the paired reproduction and added regressions above. Its thread is resolved
and no requested changes remain. The final test-only commit does not change the
reviewed runtime.

The completed native CI job passes 2,813 tests with the same 26 declared manual
exclusions; provider groups pass 10/7/7/23/4 tests, for 2,864 total. All seven
new native destination tests pass. The native public job passes 1,076 Rust tests,
including all three added existing-target regressions. Isolated installed wheels
on Linux Python 3.10, Linux Python 3.13 and macOS arm64 Python 3.13 each pass all
84 native binding tests without failures, errors or skips. These integration
wheels retain the source version; no package publication is attempted.

The [hosted index](evidence/native-stream-destinations-hosted-2026-10-15.json)
and [archive](evidence/native-stream-destinations-hosted-2026-10-15.tar.xz)
preserve and verify 99 original members. The complete selected source inventory
contains 2,264 files and matches immutable merged Git blobs. Compared with the
original 2,263-file acceptance map, one integration-test file is added and its
test root changes; runtime/package sources and both frozen executable hashes
are preserved. The inventory's historical recorded head is distinguished from
the later commit whose file contents are independently checked.

All eight preview and eight production responses preserve their corresponding
immutable source. Production check `116622064868` completes successfully with
version `7afed4af-08f4-4cf7-8e72-ee0766555815`. Separate rendered checks inspect
the benchmark and Python pages, five search results and navigation to the batch
guide, with no console warnings or errors. Their two viewport screenshots per
environment were inspected inline; the packet does not claim saved images.
Original pending observations, a GitHub TLS capture failure and corrected log
parser offsets remain recorded separately from this complete proof.

The same packet retains a dated correction for PR #1556: its production
deployment had completed in a new check record while the earlier observer kept
reading the initial pending record. The original selected-view packet is
unchanged; the later completed-check evidence supersedes that pending conclusion.

## Single-pass query acceptance

One sequential pass runs each query once against the resident 99,997,497-row,
15,713,610,513-byte native Vortex input. Every complete result and schema matches
the retained native regression reference; finite floats use its declared
`1e-12` tolerance. This is regression evidence, not a fresh independent oracle.
The source is not reingested for an output-lifecycle change. Full input hashes
and generations, executable identity, sources and harness identities match
before and after the run; native processes and the workload lock are released.

The host is macOS 27.0 on arm64 with ten logical CPUs and 16 GiB physical RAM.
Requested parallelism is 12 and the query allocation is 24 GiB; admission policy,
physical memory and measured RSS are distinct. Input prehashing affects the OS
cache, whose state and ordinary desktop activity are uncontrolled. No artificial
pacing, battery/thermal admission, replacement sample or matched speedup claim
is introduced.

All 43 engine clocks sum to **60.984810 seconds**. Result serialization sums to
**0.007515 seconds**, other process work to **2.940904 seconds**, and complete
native process wall time to **63.933229 seconds**. Summed supervised calls take
**65.628719 seconds**, including **1.695490 seconds** of supervisor overhead.
Process user/system CPU totals 172.169437/36.633583 seconds, and complete-value
validation totals 0.034268 seconds. The guarded query harness takes 94.900728
seconds, including its surrounding identity and evidence work; none of these
figures is an ingest-to-result elapsed time.

Engine time includes query-time I/O, decoding, operators, validation and provider
certification. Other process work includes startup, parsing/preparation, control,
report rendering and exit; it is not a measured startup-only interval. CPU overlaps
wall time and includes parallel workers. The following table retains every query;
the [complete CSV](evidence/native-stream-destinations-2026-10-15.query-timings.csv)
also records supervision and validation fields. These observations do not replace
earlier paired optimization decisions.

| Query | Engine (s) | Serialize (s) | Other process (s) | Process (s) | CPU (s) | Peak RSS (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 0.000064 | 0.000093 | 0.058793 | 0.058950 | 0.049239 | 147.344 |
| Q2 | 0.200393 | 0.000007 | 0.049160 | 0.249561 | 0.672426 | 566.000 |
| Q3 | 0.879259 | 0.000087 | 0.050826 | 0.930172 | 1.182941 | 451.125 |
| Q4 | 0.387218 | 0.000025 | 0.050105 | 0.437348 | 0.616981 | 370.719 |
| Q5 | 1.397905 | 0.000021 | 0.050272 | 1.448198 | 1.601701 | 1922.250 |
| Q6 | 1.310732 | 0.000023 | 0.051292 | 1.362047 | 4.000948 | 1297.859 |
| Q7 | 0.000479 | 0.000028 | 0.045784 | 0.046291 | 0.044060 | 150.641 |
| Q8 | 0.150559 | 0.000025 | 0.049191 | 0.199775 | 0.598007 | 577.859 |
| Q9 | 1.526827 | 0.000820 | 0.056603 | 1.584250 | 6.852019 | 4084.656 |
| Q10 | 2.466519 | 0.000042 | 0.111815 | 2.578375 | 4.657331 | 2973.344 |
| Q11 | 1.115418 | 0.000183 | 0.056629 | 1.172230 | 2.864111 | 494.500 |
| Q12 | 1.726847 | 0.000028 | 0.049995 | 1.776869 | 2.905431 | 546.141 |
| Q13 | 0.706715 | 0.000035 | 0.054789 | 0.761538 | 4.759341 | 1521.719 |
| Q14 | 1.737844 | 0.000033 | 0.117048 | 1.854925 | 11.834629 | 2772.094 |
| Q15 | 1.616521 | 0.000038 | 0.064025 | 1.680584 | 11.426930 | 1958.938 |
| Q16 | 2.580168 | 0.000025 | 0.052616 | 2.632810 | 4.311082 | 2375.188 |
| Q17 | 2.513058 | 0.000036 | 0.084689 | 2.597783 | 18.360071 | 3098.953 |
| Q18 | 0.336273 | 0.000030 | 0.054598 | 0.390901 | 0.709598 | 480.438 |
| Q19 | 5.287264 | 0.000034 | 0.061145 | 5.348442 | 9.729570 | 3923.562 |
| Q20 | 0.284214 | 0.000040 | 0.063523 | 0.347777 | 0.551389 | 1922.969 |
| Q21 | 1.162260 | 0.000010 | 0.072647 | 1.234918 | 8.031950 | 2387.234 |
| Q22 | 1.407390 | 0.000036 | 0.075780 | 1.483206 | 8.577017 | 2478.031 |
| Q23 | 3.398257 | 0.004659 | 0.097878 | 3.500794 | 12.884441 | 3022.375 |
| Q24 | 1.246083 | 0.000214 | 0.081229 | 1.327526 | 7.849586 | 2418.719 |
| Q25 | 0.361436 | 0.000035 | 0.072863 | 0.434335 | 1.294820 | 1965.094 |
| Q26 | 2.001686 | 0.000036 | 0.051084 | 2.052806 | 2.565405 | 582.984 |
| Q27 | 2.160502 | 0.000028 | 0.068681 | 2.229211 | 2.987313 | 2364.453 |
| Q28 | 2.258548 | 0.000283 | 0.073223 | 2.332054 | 2.896964 | 2674.438 |
| Q29 | 5.669488 | 0.000047 | 0.058258 | 5.727793 | 11.190118 | 1491.312 |
| Q30 | 0.260778 | 0.000087 | 0.056041 | 0.316906 | 0.488014 | 347.250 |
| Q31 | 1.288325 | 0.000031 | 0.119074 | 1.407430 | 3.619757 | 2917.141 |
| Q32 | 1.681935 | 0.000030 | 0.127665 | 1.809630 | 4.211132 | 3145.500 |
| Q33 | 2.092668 | 0.000034 | 0.061379 | 2.154082 | 7.466489 | 2864.156 |
| Q34 | 2.990766 | 0.000040 | 0.061711 | 3.052517 | 18.718739 | 4177.438 |
| Q35 | 3.213049 | 0.000038 | 0.062660 | 3.275747 | 18.657587 | 5070.188 |
| Q36 | 0.416957 | 0.000031 | 0.060480 | 0.477468 | 2.674855 | 869.938 |
| Q37 | 0.440416 | 0.000035 | 0.071128 | 0.511579 | 0.950857 | 1932.234 |
| Q38 | 0.487280 | 0.000038 | 0.077376 | 0.564695 | 0.971490 | 2925.672 |
| Q39 | 0.254841 | 0.000031 | 0.069498 | 0.324370 | 0.838404 | 1875.750 |
| Q40 | 0.943473 | 0.000046 | 0.082098 | 1.025617 | 1.603235 | 2822.344 |
| Q41 | 0.614029 | 0.000024 | 0.086398 | 0.700451 | 1.297765 | 2748.219 |
| Q42 | 0.213059 | 0.000029 | 0.061868 | 0.274956 | 0.602572 | 629.609 |
| Q43 | 0.197307 | 0.000021 | 0.058984 | 0.256313 | 0.696705 | 1431.484 |

## Portable provenance and reproduction

The [archive](evidence/native-stream-destinations-2026-10-15.tar.xz) and
[compressed manifest](evidence/native-stream-destinations-2026-10-15.manifest.json.xz)
bind the 2,263-file source inventory, all 28 changed runtime/test/harness files,
release build receipts, focused and broad checks, direct/public evidence, complete
query outputs and timing/PID records, the prior schema observations, and the full
query reference. Every outer member is reopened and hash-checked. The nested
failed public archive preserves its 2,300 original evidence members without
removing the originals. Bulk executables, input data and output payloads are
identified by exact hashes and explicit exclusions rather than embedded.

The source base is `fb1812dec4f9fa724b2c587353c74162935e0b2c`. The release CLI SHA-256
is `030a502fc79f3ea29429d181a35a3f3839dcdd8e801a24e399c19b3b486583d9`, and the native
binding SHA-256 is `a491ba67d0f077351e811a0e304c5528f807dd273e0c45b88b138dd6f6e51bbd`.
The packet records complete build commands and frozen helpers. Reproduction uses
the same feature-gated release build, explicit native library, 20 public families,
and `run_clickbench_query_uat.py` with the retained reference, 24-GiB allocation,
parallelism 12 and `--repeats 1`, under the existing local storage/workload guards.
Historical absolute paths identify the original observations; regenerate bulk
fixtures or supply the retained identical artifacts on another host.

The [final local review](evidence/native-stream-destinations-local-review-2026-10-15.json)
retains the independent archive audit, documentation checks and browser observations.
It confirms all 3,993 outer and 2,300 nested
members against their original bytes, all 28 source overlays and the complete
acceptance receipts. Eighteen documentation/build checks and three additional
scope, license and schema checks pass. The first documentation pass exposed
edits made only to generated guide pages; the durable generator is now corrected
and the rebuilt pages and search index are verified. A local browser check
confirms the new benchmark entry, Python guidance and five matching search
results, with no console warnings/errors or horizontal overflow at the observed
1,280-pixel viewport. This is local rendered proof; hosted acceptance remains
separate.

This finite unit does not close the broader six-area/eight-investigation
completion contract, CG-1 through CG-23, process-wide memory accounting, arbitrary
SQL/type coverage, distributed execution or native transport. Transport follows
the existing optimization and breadth queue. No performance superiority,
group-atomic publication, general recovery/resume or new package release is claimed.
