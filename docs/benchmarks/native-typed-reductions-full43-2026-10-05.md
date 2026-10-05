<!-- SPDX-License-Identifier: Apache-2.0 -->

# Revised native engine and exact reductions: local acceptance

The revised engine passes 20,445 complete public workflow checks covering
14,282,070 row comparisons, 202 direct retained-workflow checks, and all 129
ClickBench executions. SQL, Python/DataFrame and CLI declarations enter the
shared Vortex-native plan family. The retired fixture-specific executor and
public benchmark-scenario dispatch are removed. This is correctness and
availability evidence; it makes no comparative performance claim.

The [implementation contract](../architecture/native-typed-reductions-2026-10-04.md)
defines exact aggregate expressions and Decimal128 aggregate, rolling and pivot
semantics, memory/source-free intake, typed result schemas and output fidelity.
Wider analytic frames, scalar-value subqueries, nested pivot state, broader
adapters and remaining resource/spill work are subsequent units. This record
does not close the complete universal-workflow queue or authorize publication.

## Frozen identities

| Artifact | Identity |
| --- | --- |
| Runtime source | `eb39c2ebd9315dcbbcc96056f3a46a2ea911f87b` |
| Native executable SHA-256 | `5088dc0c8742fda36ea77b44cde33f6b7a5803fb0a49a8fe1fc696124d6cb712` |
| Final evidence-checker source | `82766e8b8a8bafdeee0da9e9d70950b5b1623581` |
| Accepted source tree | `99279365495c2fbc9d6986a0eeda85552313b9e3` |
| Main integration | `ef6cd4c41472d66a45e47c0e8bde524ed87778f6` |
| Integrated main | `42eb2a033b9bc07859a58e1f8edb9c3f1a242302` |

The two later checker changes only align retained-plan evidence assertions and
one mocked report with the shared runtime. Every Rust/Cargo and public Python
runtime source remains byte-identical. The packet retains the exact before/after
texts, replacements, source hashes, failed checker observations and refreshed
affected tests. The main integration changes Git ancestry with exactly the same
source tree; it does not imply a rebuild at the integration commit.

## Complete public workflows

| Acceptance family | Complete checks |
| --- | ---: |
| Base composition, resources and aggregates | 797 |
| Primitive unary composition | 1,166 |
| Nested composition and keys/state | 5,214 |
| Dynamic pivot composition | 846 |
| Typed payloads, keys, expressions, unary state and reductions | 11,513 |
| Memory/source-free intake and conversion boundaries | 909 |
| **Disjoint union** | **20,445** |

Each family is independently supervised, and the suite verifies their exact
disjoint union against a frozen name-to-row-count manifest. The same final
runtime and immutable input/oracle identities apply to every family. Checks
include successful complete results and explicit denials; negative cases add
zero row comparisons. Collection, all eight local writers and all five requested
Python materialization boundaries are covered where their types are representable.
Unsupported format/type combinations must fail without a committed artifact.
The eight formats are Vortex, Parquet, Arrow IPC, Avro, ORC, JSON, JSONL and CSV.
The materialization boundaries are Python values, pandas, NumPy, Arrow and Arrow
IPC; these consume native results and do not execute ShardLoom operations.

The focused exact-reduction family passes 2,078 checks and 4,305 complete row
comparisons. Independent packet construction reopens 2,643 new complete-value
proofs, 1,734 reduction resource proofs, 344 expected reduction denials and all
42,722 public raw report envelopes. The separate direct retained-workflow matrix
passes 202 checks and 131,734 rows, including dynamic-schema binding and repeated
declaration reuse. It uses the shared native relational family.

## Modular workload harness

The redesigned traditional-analytics harness passes 1,408 complete records,
including 704 native candidate records, across parameterized raw/prepared inputs
and requested outputs. A separate 32-record probe, including 16 native records,
verifies the additional input-state boundary. Independent reference results are
frozen before candidate execution. The packet verifies workload declarations,
source bindings, complete values, binary/input hashes and no-fallback fields.

Those records were produced by an earlier harness commit whose complete harness
source inventory is byte-identical to the accepted one. Its frozen executable
has the same SHA-256 as the final executable. Both the original provenance and
the exact byte-identity proof are retained; the records are not relabeled as a
new run. Baseline processes are comparison oracles only. In Vortex cases they
read the equivalent original CSV, so these are correctness checks across logical
data, not same-format timing comparisons.

## ClickBench and source checks

All 43 ClickBench queries execute three times through public `shardloom run sql`.
All 129 complete typed results match their frozen references. Every raw report
asserts `public_workflow_native_vortex_plan_route_family=native_vortex_unified_plan`,
`fallback_attempted=false` and `external_engine_invoked=false`. The harness
supplies SQL and validates results; it does not select a separate benchmark
executor. The existing 99,997,497-row native artifact, all query text and the
complete result references are frozen before execution.

All 22 selected core source gates pass: required workspace formatting, strict
Clippy and tests; native feature Clippy and Vortex/CLI tests; Python tests;
native-without-write and lean feature checks; Rust 1.96 MSRV configurations;
UAT, consumer, archive, storage and family-controller tests; and affected
governance, API/schema and documentation validators. The admitted-semantics
matrix passes 144 stages and golden workflows pass nine stages. Test totals
overlap between configurations and must not be added into one unique-test count.

The required Rust commands include:

```sh
cargo fmt --all -- --check
cargo clippy --offline --locked --workspace --all-targets -- -D warnings
cargo test --offline --locked --workspace --all-targets --no-fail-fast -- --test-threads=2
cargo clippy --offline --locked --workspace --all-targets --features release-user-surfaces -- -D warnings
```

The packet includes exact commands, feature selections, toolchain versions,
source manifests, process receipts and hashed logs for every gate. Documentation
and website integration checks after this acceptance are recorded separately in
the [documentation receipt](evidence/native-typed-reductions-docs-2026-10-05.json).
The site build/check, dependency audit, static assets, readiness, public-status,
productization, CI matrix, user-surface reference and diff checks pass. The audit
reports zero vulnerabilities, and all 171 document contract tests pass. Hosted
review/checks remain pending and do not alter these immutable runtime observations.

The first hosted package smoke produced the expected complete rows but its
example checker required the retired preparation-ingest property. The checker
and release transcript now verify the shared native-plan family and declared
file/memory source-open counts. Missing or invalid evidence fails the example
and its printed status. The accepted executable passes the corrected example,
and all 157 release-script tests pass, including negative evidence checks.
The [package-checker receipt](evidence/native-typed-reductions-package-checker-2026-10-05.json)
retains the hosted failure, local checks and unchanged runtime-source hashes.

A subsequent hosted source-state gate exposed missing explicit false release
and publication declarations. Tracing its downstream consumers also found stale
route ownership, output-route counts, quickstart fields and golden-workflow
identities. The checkers now consume the current producer contracts and retain
strict rejection of missing, ambiguous or unsafe evidence. Golden replay records
its existing complete-row comparison, and each quickstart creates fresh source
and output paths so repeated runs preserve earlier files.

All 184 focused release-contract tests pass. Fresh resource-safety,
observability, golden-workflow and example-replay checks pass against the accepted
executable, including downstream correctness consumers. The
[release-report repair receipt](evidence/native-typed-reductions-release-report-repair-2026-10-05.json)
preserves the hosted failure, three failed local follow-ups, diagnostic evidence,
successful replay and exact repair sources. All 681 runtime source hashes and the
accepted binary hash are unchanged; this follow-up does not replace or relabel
the original runtime acceptance. Hosted rechecks remain pending.

The next hosted run found a metadata test that still required the shared golden
identities to be literal strings in the producer script. It now verifies their
shared definitions, producer uses and all three consumer imports. All 171
repository contract tests and strict contract-crate Clippy pass. The
[metadata-test receipt](evidence/native-typed-reductions-metadata-test-repair-2026-10-05.json)
retains the failure and proof: only that test differs within the preceding
681-file Rust/Cargo/Python source inventory; engine implementation and accepted
binary bytes remain unchanged.

The final hosted report job found three missing first-steps source declarations.
The guide now includes file, memory-row and generated-row entry points, builds
the admitted native feature set, and writes its example to a new temporary
directory each time. Two executions of the literal documented Python command
produce the complete expected row and preserve the earlier output.

The [first-steps repair receipt](evidence/native-typed-reductions-first-steps-repair-2026-10-05.json)
preserves the original documentation failure and an isolated replay against the
exact downloaded hosted evidence. All eight report commands required to exit
successfully in that job pass, including local finished-product readiness.
The ninth command, hard release readiness, remains blocked as the workflow
explicitly permits before publication; its blockers are retained. The downloaded
Linux executable is not run locally. This documentation-only correction leaves
runtime acceptance unchanged, and does not establish public release readiness.

## Resource and evidence boundaries

Large local work stays serial under the existing process/storage guards.
Public acceptance retains a 3,000-second deadline per family, 12-GiB free-space
headroom, 100-GiB workspace ceiling and combined 192-MiB log ceiling. Full43
retains the declared 24-GiB resource policy, 12-worker maximum and per-query
deadline. Policy grants and sampled memory observations are not a total-process
RSS guarantee. No large text/format performance campaign is resumed.

Failed and interrupted observations remain in the packet, including evidence
schema corrections, explicit storage/deadline stops and checker failures. A
failed run is not combined with another incomplete run to claim a complete
suite. New final runs use the same independently specified expected values;
assertion-only retention requires exact source and executable identity.

The portable [evidence packet](evidence/native-typed-reductions-2026-10-04.json.xz)
contains the runtime source inventory, oracles, original report envelopes,
complete results, source/build/gate receipts, failed history and verification
helpers. Its compressed size is 56,350,104 bytes; its SHA-256 is
`2a81e2248c9404d15ca8db337d30f23c9afe57462b41901b65ad120649097684`.
The uncompressed JSON is 3,337,616,300 bytes with SHA-256
`7386a3d5807921f7a95892841c119ed59371440801881b463d7c2124b6e95b84`.
Use a streaming parser rather than loading the whole packet into memory.
Independent streaming-parser inspection is recorded in the accompanying
[inspection receipt](evidence/native-typed-reductions-inspection-2026-10-05.json).

This accepted executable becomes the control for subsequent paired optimization
measurements. No speedup, engine superiority, general state-spill support,
production certification, package publication or whole PERF/CG completion is
inferred. Real Vortex output payload checks remain distinct from placeholder
artifact status; CG-1 through CG-23 retain their existing obligations.
