<!-- SPDX-License-Identifier: Apache-2.0 -->

# Dynamic pivot workflow and Full43 acceptance

Frozen runtime `50cc1e22` passes 3,305 public workflow checks, including 846
dynamic-pivot checks, and all 258 paired Full43 retained-result comparisons.
Aggregate timing is effectively unchanged: the sum of fastest query runs changes
by −0.2107%, and the sum of query medians by +0.3828%. No predeclared timing or
memory investigation threshold is crossed. This is a local regression observation,
not a causal speedup or official benchmark claim.

## Runtime and comparison procedure

The [dynamic pivot unit](../architecture/native-dynamic-pivot-composition-2026-10-03.md)
connects the existing sparse pivot state to execution-time schema binding, native
relational operators and local sinks. Each execution discovers its own domains;
correlated inner rows receive separate state and schema. The direct pivot provider
and composed callers share completion, naming, aggregates, nulls and bounded output.

- Candidate: clean revision `50cc1e22c4df85883653ddba60783fa2e4108f3b`, optimized
  `release-user-surfaces`, Rust 1.99.0. Executable SHA-256:
  `a738248bcb8e8feaacc94854ff328f9d71cf47513fa92dfb9c6bfc2a694f0ed4`.
- Control: retained nested runtime `3b94ba2e1fdc7d1860398265856c44aa02a28f74`,
  executable SHA-256
  `ac4b6d0c49a0875953d057c45aaf1675905c727d8b4566bc59bbcc0a0717d2db`.
  Compiler, feature selection and Cargo lockfile match the candidate.
- Dataset: unchanged 99,997,497-row native ClickBench artifact, 15,682,956,489
  bytes, SHA-256
  `5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
  All 43 statements and retained references are unchanged. No full-size source
  was regenerated.
- Host: arm64, 10 logical CPUs, 16 GiB physical RAM, macOS 27.0. Both roles request
  a 24-GiB operation policy and maximum parallelism 12. This is distinct from
  physical RAM and observed RSS; it is not a total-process memory ceiling.
- Cohort: `paired43_20261003T064959262042Z`. Each query runs three times per role,
  alternating role order. The native clock includes process startup, complete
  output and exit. Hashing, reference checks, host snapshots and archive work
  remain outside that clock. Supervised cohort wall time is 386.643 seconds;
  the table's sums are not end-to-end elapsed time.
- Existing serial workload and storage guards remain enabled. No build or test
  workload overlaps the timed cohort. The source is prehashed; OS cache and
  ordinary host activity are uncontrolled. One-minute load averages span
  1.631–9.123. No answer cache or forced cache purge is used.
- Complete results match the retained ShardLoom references using exact structure
  and the existing `1e-12` finite-float comparison. Full43 is regression evidence;
  independent literal and formula public fixtures provide separate correctness
  evidence. Successful calls retain native certificates and no-fallback evidence.

## Paired observations

| Metric | Control | Candidate | Candidate change |
| --- | ---: | ---: | ---: |
| Sum of each query's fastest valid run | 50.255037 s | 50.149143 s | −0.2107% |
| Sum of each query's median | 50.876651 s | 51.071412 s | +0.3828% |
| Sum of all 129 process times per role | 153.227605 s | 153.256597 s | +0.0189% |
| Geometric mean of fastest query times | 0.539331 s | 0.537290 s | −0.3784% |
| Maximum observed process peak RSS | 4,945,166,336 bytes | 5,065,179,136 bytes | +120,012,800 bytes |
| Complete retained results | 129/129 | 129/129 | All pass |

Investigation requires both 10% and 100 ms for a query's median time, both 10%
and 32 MiB for its median RSS, or both 5% and one second for either aggregate
timing sum. No query or aggregate crosses these thresholds, so the predeclared
reversed-order repeat rule requests no additional run. All raw observations,
including smaller changes and CPU/memory measurements, remain in the packet.

The audit also finds an inherited Q1 reporting conflict in both binaries:
`local_primitive_no_query_answer_cache` appears as `true` and then `false`.
The retained-footer-count report supplies the former; a generic absent-report
default appends the latter. All six raw responses remain intact. That field is
excluded from acceptance proof and its repair remains a concrete follow-up.
Complete values, timing and the independent no-fallback fields still pass.

## Complete public workflows

The public matrix passes 3,305 checks and 7,687,525 complete row comparisons.
Its 846 pivot checks include 838 successful execution results, seven expected
denials and one inert missing-source inspection. These cover 624 complete writer
outputs, with 78 through each of Vortex, Parquet, Arrow IPC, Avro, ORC, JSON,
JSONL and CSV. Binary compatibility outputs normalize through their declared
reader and reopen through Vortex; JSON/JSONL and CSV receive complete value checks,
with explicit CSV text/null conversion. Native type/nullability tests are separate
from these text comparisons.

The matrix includes native and declared CSV inputs, SQL and DataFrame spelling,
17 core transformations, fresh repeated calls, null and quoted/Unicode domains,
name collisions, fill and margins, successive pivots, and correlated schemas.
SQL spellings of some boundary fixtures reuse the DataFrame-rendered statement;
all still compare with independently specified expected values.

The wide fixture admits 127 categories plus the index, and rejects a 129-field
result. Both public spellings write and reopen every row of a 65,541-row result
through all eight writers. Full collection remains denied above 65,536 rows;
a 97-row limited collection succeeds. The large native-output case reports
65,541 input rows, one held source and one discovery stage, followed by 33 batches
of at most 2,048 rows. Its observed reserved-buffer peak is 692,137,203 bytes
within the declared 1-GiB grant. The correlated case reports eight independent
discovery stages and 72 delivered source rows, including the outer scan.

The separate direct-unary matrix passes 202 checks and 131,734 row comparisons.
Thirteen native lifecycle tests cover exact dtypes/nullability, changed and empty
domains, execution/parameter ownership, cancellation, consumer failure, denied
state, source invalidation and result ownership after producer drop.

## Gates, evidence and remaining boundaries

Default workspace tests pass 3,446; native Vortex passes 2,240 with 23 existing
ignored tests; native CLI passes 1,581. Python passes 721 with 144 existing
environment skips. Configuration counts overlap. All 24 selected local gates pass,
including strict default/native Clippy, no-write/lean/MSRV checks, UAT helper tests,
documentation and generated website checks. The first website build encountered
missing local dependencies; installing the unchanged lockfile offline restored it.
The failed observation and passing rerun are retained.

The [portable acceptance packet](evidence/native-dynamic-pivot-composition-2026-10-03.json.xz)
contains build/source hashes, frozen oracles, complete result sets for every
successful pivot case, raw envelopes, certificate checks, denial/inspection
responses, all gate receipts, and the complete paired Full43 evidence. Its verifier
rehashes every saved public output and source generation, decompresses and verifies
all 7,518 public envelopes, checks all 43 paired archives and 258 complete results,
and rechecks full pivot readbacks. A fresh saved missing-source inspection
supplements the original matrix's summary-only record. Verification helpers are
embedded with machine paths replaced by named roots.

All runtime, SDK and UAT source hashes match the frozen build. The support-index
validator is the only later Python source change; it requires the new capability
record and receives its own passing current-source gate. Documentation follows the
freeze without changing the executable under test.

Hosted review remains pending. The inherited website advisory proposal is disabled
and still requires approval or remediation; its unit tests and website build do
not establish a clean dependency audit. No exception is activated by this unit.
Existing scalar admission, the 128-field boundary and memory denial remain in
force. Pivot-state spill, richer keys/types, broader adapters, reader/codec scratch
accounting and total-RSS enforcement retain their own owners. The execution
certificate records decoding and materialization; no zero-decode claim is made.
This completes local pivot acceptance, not the broader PERF or CG gates, and
does not publish a package or certify production readiness.
