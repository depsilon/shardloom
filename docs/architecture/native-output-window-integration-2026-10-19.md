<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native output-window integration

Status: finite two-batch output window retained after comparison and complete
current-host acceptance. This is the ninth finite decision in the conditional
pass; broader output-window and publication obligations remain open.

The frozen [two-batch design](native-two-batch-output-window-design-2026-10-19.md)
passes both comparison cohorts: the single predeclared target meets its benefit
gate, and all ten cells pass correctness, activation, memory and regression
controls. Both cohorts completed before inspection. The independently reopened
packet `output-credit-window-788.tar.xz`, SHA-256
`f4c936271e0470944c73c0bfa7d80000ca5e9b058d14446ac6c229f10bbe1316`, preserves
3,213 members, all 11 attribution/candidate source versions, 462 complete IPC
witness files, 63 guarded receipts and the exact compiled identities. It also
preserves failed attempts and the original protocols. The integrated source now
passes the acceptance recorded below. The target's engine medians increase from
34.243502 to 34.677749 ms in cohort A and from 34.852496 to 35.444584 ms in B;
the benefit is in the frozen complete-workflow objective, not engine speed.

## Integration boundary

The unchanged two-slot ledger and actual-plan admission are promoted into the
existing native Arrow-delivery feature boundary. The temporary
`output-window-candidate` build switch is removed. Observation remains behind
the separate opt-in `output-window-observation` feature, absent from normal
packages. There is no new dependency or provider. The Vortex-first classification
remains `use_vortex_native_provider`: Vortex 0.85.0 native memory sources, execution
and existing Arrow boundary leases; logical consumer demand belongs to ShardLoom.

The native Python plan and the public SQL/DataFrame `iter_batches` methods accept
`output_window_bytes=0` or `4194304`, defaulting to zero. Invalid types and sizes
fail before source demand. The public subprocess transport rejects a nonzero
request before starting work. This integration adds no CLI transport window.

The public adapter forwards the explicit request to the same native operation;
it adds no queue, materialization, source preparation or alternate execution.
Public `batch_rows` continues to slice native delivery batches and acknowledges
the preceding native batch only when it requests the next native batch. Aliases
and slices retain their existing native payload leases independently of those
logical acknowledgments.

The final report adds two string fields:

- `columnar_output_window_requested_bytes`: `0` or `4194304`.
- `columnar_output_window_selected_policy`: `ordinary_not_requested`,
  `ordinary_ineligible` or `two_batch_window`.

These fields are available only with the successful final report. They do not
turn an enqueue or provisional batch into completion. Other plan/source classes
retain ordinary demand when a window is requested and report their ineligibility.

## Unchanged algorithm and proof obligations

The exact frozen admission is preserved: actual immutable memory scan or one projection,
0–65,536 rows, 1–64 Int64 output fields, no filter/residual, nested operator,
file/preparation, producer declaration, dynamic/parameterized input or spill.
The original grant, 1,024-byte metadata reservation, one-event channel,
two unacknowledged batches, 2-MiB per-batch and 4-MiB total referenced-byte limits,
FIFO acknowledgments and final-callback barrier are unchanged. No workload threshold, queue
depth, algorithm or comparison criterion is changed after the measurements.

Integration changes are feature promotion, explicit public argument forwarding,
completed-policy evidence and durable tests/docs. The comparison describes the
frozen instrumented implementation, not a new measurement of the final package.
Do not add selected cohorts or reinterpret the result as an engine speed gain.

Diagnostic timing remains scoped to the relational batch producer, including a
primitive declaration that uses that producer. Other primitive providers retain
ordinary delivery; their opt-in observation is not a valid relational timing
sample. Their completed policy fields remain available. Normal packages contain
neither observation getters nor environment-controlled capture.

## Integrated acceptance

The [portable evidence index](../benchmarks/evidence/output-window-accepted-791.json)
binds accepted source to `f95490279ce06a91b27d0dc44a3b9ecf41b1d0d6` plus the
recorded tracked patch and ten added files; the commit alone is not the tested
source. It contains four integration source versions and all 2,890 accepted
source files. The normal release extension was built from integration version
`789-a3`; source continuity proves that `790-a4` changes only CI/documentation
inputs. The CLI and optimized native tests were freshly built from `790-a4`.
No rebuilt-extension claim is made for that documentation/CI update.

| Gate | Verified result |
| --- | --- |
| Formatting and strict workspace/native lint | Pass |
| Workspace tests | 3,235 pass; no ignored tests |
| Full native Vortex suite | 3,056 pass; 30 intentional opt-in/manual ignores |
| Native CLI tests | 1,087 pass; no ignored tests |
| Python source discovery | 729 tests run; 21 optional-class skips; no failures/errors |
| Normal release binding and explicit diagnostic binding | 68 and 13 pass respectively; no skips |
| Isolated installed wheel | 111 native binding tests pass without skips; CLI/extension identities and uninstall verified |
| Public39 | All 5,930 cases across 39 families; 8,151 no-fallback envelopes and 16,389 stored logs reopened |
| Full43 | All 43 queries once; all 381 result rows and schemas; 50.102590495 seconds engine time |
| Modular workflows | All 18 cohorts; 704 native workflows match 704 fresh independent oracle workflows |
| Public references, exports, CI matrix and API schema | Pass |

Suites overlap; these counts are not additive. Ledger and diagnostic tests cover
byte/slot denial, FIFO acknowledgment, final completion, close/cancel/failure,
actual-plan eligibility and payload leases that outlive logical acknowledgment.
Public tests cover integer-subclass validation, zero/default behavior, SQL and
DataFrame slicing, lazy producers, retained owners and subprocess refusal.
The Full43 input was the existing resident native artifact; full pre/post hashes
and unchanged source generation are verified. No replacement ingest or new
comparative benchmark was needed for this acceptance.

The archive retains an initial two-test DataFrame expectation failure, the
intentionally interrupted unoptimized native suite and the CLI build helper's
post-build metadata-location error. Corrected expectations pass the complete
normal release suite; the optimized native suite completes. The build itself
succeeded, and recovery verifies its existing fresh artifact with Cargo's
recorded environment before copying it. No failed or interrupted run is counted
as a pass.

The [independent packet verification](../benchmarks/evidence/output-window-accepted-packet-verification-791.json)
reopens all 20,059 outer members and the unchanged 3,213-member original comparison
packet. All four integration source versions are reconstructible without the
checkout; 83 frozen helper references and 39 outer guarded receipts are verified.
Large compiled binaries, wheel/environment payloads, resident inputs and generated
outputs are excluded with exact identities and reproduction records. The
[closeout source bridge](../benchmarks/evidence/output-window-closeout-source-792.json)
allows only the five final roadmap/decision documents to differ from accepted
source; runtime, Python, tests, harness and CI contents remain unchanged.

This is an opt-in native Python result-delivery contract for the admitted finite
shape. It establishes no CLI transport window, general producer overlap,
whole-process memory ceiling, engine speedup, package publication or other-platform
certification. Hosted checks and release authorization remain separate. The
broader PERF-07 owner, all 58 substeps, CG-1 through CG-23, published v0.6.0 and
the existing release hold are unchanged.
