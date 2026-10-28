<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared numeric values — local acceptance

This local candidate combines original-width numeric MIN/MAX updates with independently owned,
validated numeric output copying. Complete results, native/public/package checks, portable replay
and corruption controls pass. Hosted acceptance remains separate; Item 20 is still open.

## Frozen comparison

The frozen protocol specifies one isolated execution of the existing 347-case cohort. The accepted
baseline is reused, not rerun. Both observations validate all 12,565 output rows.

| Role | Engine seconds | Observation |
| --- | ---: | --- |
| Accepted baseline | 25.508976866 | Reused complete observation |
| Numeric candidate | 23.079365459 | One complete execution |

The candidate is 9.524535% lower in complete-cohort engine time, short of the frozen 10% cohort
threshold. It passes the alternative frozen gate through improvements in three declared families:

| Declared family | Baseline engine seconds | Candidate engine seconds | Reduction |
| --- | ---: | ---: | ---: |
| Global aggregation | 2.233468495 | 1.665257164 | 25.44% |
| Grouped aggregation | 11.508791828 | 9.969800537 | 13.37% |
| Joins | 1.919370334 | 1.719164168 | 10.43% |

No material family regressions were flagged. One case flag remains in the original complete-cohort
result: `aggregate_signed_value_group_key_sum_arithmetic_one_percent`, 0.022846667 to 0.036019708
engine seconds. The separately frozen bounded 18-execution AB, BA, AB diagnostic did not reproduce
it: case medians were 0.022359125 and 0.022218958 seconds. The cause remains unassigned; the
original flag and complete-cohort measurements are preserved.

The October 28 maintainer disposition also permits substantial net workload gains to outweigh a
small isolated timing regression. Here the complete observation saves 2.429611407 engine seconds,
including the flagged case's 0.013173041-second increase. Retain the implementation, the original
flag and its diagnostic disposition. This decision does not change the prospective protocol or
substitute later samples into the complete total.

The comparison clock is the sum of `engine_execution_seconds` for one execution of each of the 347
queries. The frozen protocol records the 2 GiB memory limit, parallelism 2, complete-output
validation and process cleanup. No new competitor or Full43 observation was made.

## Correctness and local validation

All 80 fresh held-out cases passed, covering 508,176 complete output rows and 133,216 input
read-back rows across two protocol seeds, shuffled and clustered layouts, every integer width,
float32/float64, signed zero, empty and all-null inputs, joins and reordered outputs. Native take
and cast behavior remains in place. Exact values, null and finite/temporal checks, count and error
ordering, cancellation, result ownership and retained-result reuse remain covered. The candidate
does not retain borrowed source arrays, and it does not add an external execution fallback.

Local checks passed: 3,471 native Vortex tests (32 existing ignored), 3,274 workspace tests and
1,032 CLI tests. The five public interfaces each passed three declarations, with two retained-result
reuses and producer-close coverage. The installed native wheel passed 202 integration tests with no
skips. These are local results; hosted acceptance remains pending.

Initial compile/import and temporal-constructor failures (r01/r02), and an r04 lint failure, were
fixed; their receipts remain in the local evidence. The frozen protocol is
[`protocol.json`](evidence/native-shared-numeric-values-2026-10-28/protocol.json). The candidate
source identity is `e13fe9d36b9ff3cca3a51733dc49b14bee356aec`, with its changed-source manifest;
the baseline source is `74c7cd652b2fccaeb4fa12292be40137b2dd6a58` (merged PR #1618).

## Replay, status and limits

The [portable packet](evidence/native-shared-numeric-values-2026-10-28/manifest.json) preserves
complete results, the accepted prior packet, frozen inputs, runtime/source identities, native
activation tests, package proof, original failures and the separate diagnostic. It replays
without query execution, PyArrow, the native runtime, archived-code execution or historical
absolute paths. An isolated source-only copy passes with Python assertions disabled.

Thirty corruption controls are verified: the initial full run passes 29, and the corrected control
passes separately. Its only source correction changes the expected error text for exact result
comparison; altered values were already rejected. An intervening invocation omitted the staging
packet path and ran zero tests; that error is also preserved. The
[final verification record](evidence/native-shared-numeric-values-2026-10-28/verification.json)
retains complete logs, source identities and this narrow correction. The CI workflow runs all 30
controls against the committed packet.

The [verified retirement](evidence/native-shared-numeric-values-2026-10-28/cleanup.json) removes
23,269 completed regular files and six environment symlinks after repeated identity and
closed-handle checks. Completed environments, staged packets, archived raw outputs and the
superseded key-lookup runtime/wheel are retired. The accepted numeric runtime, installed-tested
wheel, five current Parquet inputs, 347 independent references and complete compact evidence
remain. Replay passes after retirement; no payload backup or storage-limit change occurs.

Replay with:

```sh
python3 -B scripts/verify_shared_numeric_values_packet.py docs/benchmarks/evidence/native-shared-numeric-values-2026-10-28
python3 -B -m unittest discover -s scripts -p test_shared_numeric_values_packet.py -v
```

Hashes establish internal consistency, not signed provenance. This is one complete candidate
observation with an accepted baseline reused; it does not establish statistical confidence or
isolate each mechanism's contribution. The 32 native ignored tests remain opt-in measurements or
fixture tests. Large source inputs, binaries and the installed-tested wheel are recorded external
artifacts; copied numeric output remains independently owned.

Item 20 remains open. After this PR, its remaining checklist is one consolidated completion PR
by default, including expression work and the under-10-engine-second stretch objective for this
unchanged 347-case cohort. Item 18's default-demand, cross-surface/format timing, README, field
guide and site acceptance follows before 1.1.0. CG-1 through CG-23 remain unchanged. No new
competitor or Full43 pass, package publication or release-readiness claim follows from this record.
