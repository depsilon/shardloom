<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native pipeline consolidation: original 10,000-case observation

Status: implemented and validated locally; all 10,000 original cases pass in
one isolated pass. No PR, merge, package publication or release is part of this
observation. The [architecture contract](../architecture/native-pipeline-consolidation-2026-11-12.md)
and [compact evidence packet](evidence/native-pipeline-consolidation-2026-11-12/README.md)
record the implementation and proof boundaries.

## Result

| Outcome | Cases |
| --- | ---: |
| Complete, correct results | 10,000 |
| Unsupported / missing native support | 0 |
| Incorrect results | 0 |
| Engine, resource or harness failures | 0 |
| Incomplete cases | 0 |

The successful-case engine sum is **1,514.235435577 seconds**. The preceding
accepted observation took **1,540.711839539 seconds for exactly the same 10,000
IDs**. The observed reduction is **26.476403962 seconds (1.71845%)**. Because
all cases pass and every engine clock is valid, the successful-case sum is also
the complete 10,000-case total.

No cases became blocked or incomplete. The preceding observation's engine sum
for that empty set is **0 seconds**. There are no missing-capability failure
groups in this corpus. This is one observation per runtime; it establishes
neither repeatability nor the contribution of any individual code change.

## Execution contract

SQL, typed relations and embedded workflow declarations now compile into one
`PreparedVortexRelational` native operator plan. The former whole-query choice
between a file provider and the general executor is removed. File-specialized
kernels are composable operators in the same exhaustive dispatcher as joins,
windows, filters, aggregates and other implemented native operators. Missing
admission produces `native_pipeline_missing_operator`; kernel or consumer
failure is terminal and cannot retry the query through another executor.

The retained semantic declaration supports demand, ownership and pre-execution
policy analysis. It is not a failure-recovery execution path. Low-level typed
primitive APIs remain explicit individual kernel interfaces; they cannot execute
arbitrary unsupported relational plans. Existing native Vortex input/output,
source generations, memory, cancellation, spill and result ownership remain
within their original owners. No external query engine or dependency is added.

Persisted preparation remains optional. Original Parquet and Arrow IPC sources
feed native arrays into the same plan; native-memory and existing Vortex inputs
also execute directly. The source-demand reports for **6,122 direct-source
cases** record **zero prepared-input bytes written**. This count describes cases
with that direct-source report field, not all source routes. Regression tests
also cover direct original files and memory without preparation writes.

The audit of every saved raw response finds:

- `native_operator_pipeline` in all 10,000 cases;
- catch-all execution, fallback attempts and external-engine invocation false
  for every case;
- 66,280 native operator invocations;
- 2,974 file-kernel invocations within 2,379 cases, with at most three in a case.

File-kernel counts measure an optional optimization inside the pipeline. They
are not native execution coverage. The earlier 193-of-10,000 root-provider count
likewise did not mean that the remaining queries lacked native implementations.

## Remaining support and cost observations

At the measured runtime, two legacy SQL forms outside this 10,000-case corpus
were explicitly rejected by the shared frontend: `EXTRACT(minute FROM ...)` and
`DATE_TRUNC('minute', ...)`. Their route diagnostic is
`cg21.route.native_relational_not_admitted` with `UnsupportedSql`. Native
calendar kernels already existed; the gap was admission of those SQL spellings
into the common compiler. The [November 13 correction](../architecture/native-pipeline-consolidation-2026-11-12.md#november-13-sql-minute-correction)
adds both forms to that compiler with the existing epoch-second/text contracts
and separate correctness tests. It postdates this observation and does not
change its runtime, immutable packet or timing claims. The complete corpus
result does not claim universal SQL coverage.

File collections retain complete values, schema, order, streaming and output
semantics through shared scans and aggregation/sort operators. They no longer
claim the former whole-query partitioned row-reference sorting optimization.
The retained writer-coalescing report field is false under the shared writer
policy for native and compatibility sources.

The following engine sums identify where this corpus spends time. They are
optimization exposure, not missing-capability groups or approved new experiments.

| Family | Cases | Current engine seconds | Previous same-ID engine seconds |
| --- | ---: | ---: | ---: |
| Joins | 2,612 | 424.788129265 | 438.099118529 |
| Ordering | 206 | 187.323856216 | 187.702305599 |
| Windows | 776 | 163.841193145 | 165.706966164 |
| Multiway joins | 580 | 143.486871385 | 143.819306660 |
| Sets | 789 | 123.874390828 | 126.786741636 |
| Subqueries | 392 | 110.079233379 | 110.550027754 |
| Grouped aggregation | 1,756 | 95.763098481 | 99.547358723 |
| Strings | 399 | 64.319539757 | 64.597024815 |
| Temporal | 595 | 45.834605028 | 46.430015820 |
| Nullable | 205 | 39.425674416 | 39.553255413 |
| CTE composition | 392 | 30.413981625 | 31.592408387 |
| Numeric | 201 | 28.950510877 | 29.158723839 |
| Nested | 196 | 18.606345697 | 18.753125462 |
| Predicates | 410 | 15.498758314 | 15.775131038 |
| Distinct | 200 | 13.634350381 | 14.074740869 |
| Global aggregation | 264 | 7.162434283 | 7.352820414 |
| Projection | 26 | 1.021944584 | 1.006214250 |
| Composite | 1 | 0.210517916 | 0.206554167 |
| **Total** | **10,000** | **1,514.235435577** | **1,540.711839539** |

## Method and provenance

The tested code is local commit `3a861917ad6852e6a35ac8642d78c4d31451f29f`,
tree `b4410a94b6969a1da4806986e8aecf70b2538c24`, following implementation
commits `6ac5c1c81` and `9a092526c`. The parent is accepted main
`8b84361fe26afc816a9ad252ed95fffa1a7f99e8`. The later report/evidence commit
does not change the measured runtime.

The release build uses `release-user-surfaces`. The frozen CLI SHA-256 is
`5f82383b75665e7458083772cfc0e798fd7a332881c364a8c00ea1128190cfae`;
the native library SHA-256 is
`8e8fa8469ec2aa16229b1c9144fb1ecae80a286113ca8ffed61b2aacbd18185b`.
The installed-tested wheel SHA-256 is
`cc7b88109de86e0ff0d38c48cef133fa6628b487e6016fad67d6c75036b5781e`.
Build receipts bind the exact source inventory, compiler, features and environment.

The original 10,000 declarations, order, independent expectations, frozen Python
client, 65 harness files and capture loop are unchanged. Each case runs exactly
once with the original two-thread and 2 GiB native-memory policy. There are no
warmups, repeats, alternate attempts, profiling or cache manipulation. All other
task-owned work remains idle during query execution. Original storage limits,
workload guard, deadlines, complete-result checks and process cleanup remain
active; full hashing and recursive inventories stay outside query execution.

The supervisor passed, reaped its direct child and drained the process group.
The runtime/source identity and final input hashes passed. A separate offline
replay rechecked every complete result against its original independent
expectation and verified each saved engine clock. That replay executed no
query. Current and previous cases match exactly by ID, order, family, profile
and public surface.

Local raw evidence remains under
`/Users/dylan/LocalData/shardloom/native-pipeline-consolidation-2026-11-12/full10k-r1`.
The preceding matched observation remains under
`/Users/dylan/LocalData/shardloom/current-state-uat-2026-11-12/full10k-r1`.
The current raw result ledger SHA-256 is
`9ae6c214a607f5ceda189217ebf19fe6640a4265155f83eae9e526a5f3bdf4f4`.
The packet contains complete compact case observations and selected exact raw
certificate fields, not the full result/reference ledgers. Its portable verifier
recomputes hashes, matching, clocks and certificates; full correctness replay
is the separately recorded local proof.

## Validation and retained failures

Final exact-code checks pass:

- `cargo fmt --all -- --check`.
- `cargo clippy --offline --locked --workspace --all-targets -- -D warnings`.
- `cargo test --offline --locked --workspace --all-targets`: 3,323 passed.
- Native-feature workspace strict lint with
  `shardloom-python/python-native,shardloom-vortex/native-encoded-exchange-experiment`.
- CLI/Python Rust tests with `release-user-surfaces`: 1,561 passed and three
  existing ignored cases.
- Fresh release build and installed wheel: 441 integration tests passed,
  zero failures, errors or skips; uninstall verification passed.

The original full native-feature run had **7,101 passed, four failed and 39
ignored**. The four failures were stale expectations in two test-only Vortex
files. The repaired identity/provider subsets passed all four and 27 tests,
respectively. Subsequent changes were confined to five CLI files, two Python
test files and the architecture document; unchanged non-CLI production bytes
and complete CLI/Python plus installed refreshes are bound by the final
reconciliation. The full native suite was not repeated after those changes,
and the original failure is not relabeled as a later fresh green run.

Installed validation also exposed historical alias and declaration-reuse report
contracts. The final binder preserves those names using the source schema
without choosing another executor, and completed payload reporting is restored.
The original installed runs with 20 and two failure/error records remain intact.
All **51 guarded receipts (33 passed, 18 failed)** and original logs are retained.

Verified intermediate retirement removed 11 superseded task build/environment
roots: 32,146 regular files and 12 links among 36,459 entries. Repeated identity
and closed-handle checks passed. The tested runtime/wheel, both observations,
original failures, wheelhouse and all 220 current input generations remain
unchanged. No rollback payload archive or storage-limit change was made.

This closes the requested consolidation and one-pass coverage observation.
Item 19, the remaining item-20 investigations, the invalid B baseline and
CG-1 through CG-23 retain their independent status.
