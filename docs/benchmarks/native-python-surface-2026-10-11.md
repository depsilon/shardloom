# Native Python ownership and complete-workflow acceptance

Status: source implementation, complete local source/runtime checks, isolated
wheel proof and paired Python workflow acceptance complete; hosted integration
remains pending. This implements the four priorities in the
[native Python contract](../architecture/native-python-surface-2026-10-10.md)
under PERF-02/03/07/11/12 and CG-19/20/21. Published v0.5.1 packages are unchanged.
The [six-area/eight-investigation completion contract](../architecture/native-local-completion-scope-2026-10-07.md)
remains active.

## Delivered behavior and shared providers

Python can retain native sessions, typed expression/relation declarations,
prepared operations, complete results and incremental streams. Results retain
their arrays and resource credits after the originating session closes. They
can feed another native operation, use the existing writers or export through
Arrow's same-process stream interface. Python row construction is an explicit
endpoint. Columnar import and complete result delivery avoid compulsory JSON
cells and Python row objects.

`ShardLoomClient(transport="native", ...)` uses the shared Rust command and
workflow interfaces. Its default remains the explicit subprocess transport.
`shardloom.native` exposes typed declarations; the existing high-level DataFrame
builders retain their SQL declarations. Both use the existing binder and native
providers. Actual SQL uses the same parser and file-provider selection as the
public command workflow. Provider errors never trigger a retry through another
executor. No external execution engine or Arrow expression evaluator is added.

| Boundary | Reused owner/provider and acceptance |
| --- | --- |
| Session and command admission | Existing configurable resident session, live memory pool, invocation resources and child cancellation. Per-call overrides do not become later defaults or ceilings. |
| Native input | Existing exact schema interpretation and native column builders; pinned Arrow stream import for columnar producers. Foreign buffers are copied into admitted ownership where safe borrowing is unavailable. |
| Typed plans and SQL | Existing expression and relational IR, shared binding/optimization and prepared Vortex providers. Declaration construction does not inspect data. |
| Complete results | Existing owned Vortex result batches, primitive count/unary/aggregate/sort producers, cross-part finalization and container credits. Complete native collection does not inherit the small JSON endpoint's limits. |
| Incremental output | Existing native batch consumer with one demanded output batch, child cancellation, worker draining and final source validation. Delivered batches remain provisional until completion. |
| Writing | Existing Vortex and compatibility writers, native spill and staged multi-output publication. Multi-output execution reports its existing replay behavior. |

The Vortex-first decision is to wrap these existing native owners and providers.
Vortex remains the execution and highest-fidelity persistence target. Arrow is
an optional interchange representation: encoded values can require conversion,
and importing caller buffers can require a charged copy. This is not a blanket
zero-copy or process-RSS-limit claim.

## Frozen build and package identity

The accepted build manifest is `native-release-artifacts-300.json`, SHA-256
`b26431edeeb81496488b4d561c3537c0ddf80d38c452234cf08365a2223f2fe2`.
It freezes all 1,318 selected Rust/Python/Cargo/provider/license source files from
the implementation based on `b0436a00ef886e97af0824898fc6f65b1d005405`, including
then-uncommitted native changes. The packet retains their exact bytes and hashes;
the parent commit alone is not the accepted implementation identity. Subsequent
documentation edits do not change those frozen runtime sources. The final CI
review separately updates two validation scripts to select the extracted CLI
library's tests; the supplement below records those exact non-runtime changes.

| Artifact | Exact identity |
| --- | --- |
| Release CLI | 74,983,808 bytes; SHA-256 `6c06e5852c95429f1fff5dba753bee2c3a52867b3d790f494fea0ed0a5baa2e8` |
| Release extension | 74,544,560 bytes; SHA-256 `68be32968d9f447740401dff058c9f7a3007a428947607cdc6b1142a040be49e` |
| Installed local wheel | 48,638,265 bytes; SHA-256 `80a3b8abf9156e0ca47a2df93eddfdb89f946a6fc94f6c83ea52c13dd230202b` |
| Wheel tag | `cp310-abi3-macosx_27_0_arm64`; actual local load on CPython 3.12.14 |

The final wheel proof is `native-wheel-proof-300b`. It verifies all RECORD
entries, exact native/CLI/Python contents, bundled notices and isolated imports,
then passes all 63 native integration tests without skips. The static integrity
record's `load_verified=false` predates installation; the separate successful
`load` step verifies the actual installed module, provider, CLI and hashes.
The conservative notice bundle covers 386 registry versions and two vendored
packages across 729 manifest records and 206 texts. It is not a linked-component
SBOM. PyO3 0.29.3 replaces the rejected vulnerable intake; the reviewed
[Arrow adapter patch](../../vendor/arrow-pyarrow/README.shardloom.md) preserves
the single Arrow 58.3.0 graph. The strict dependency audit passes for this graph.

The [source-install guide](../getting-started/source-checkout-install.md#optional-native-python-binding)
provides the build and isolated verification commands. Local proof is distinct
from the dedicated Linux/macOS hosted wheel lanes, Linux portability auditing,
Windows native extension support and public publication.

## Correctness, ownership and failure coverage

The installed suite checks complete SQL and typed-plan results; exact integer,
binary, decimal, temporal and nested values; empty/null outputs; producer and
source mutation; malformed or released capsules; slices and buffer alignment;
retained owners; deadlines, cancellation and interrupted iteration; admission
before factory demand; error propagation; native spill; and write/reopen fidelity.
It also exercises shared command ownership, primitive options, multi-file source
membership and provider reuse. Multi-output checks reopen all eight admitted
formats and cover refusal and failure without prefix publication.

Specific growth checks include 4,099 nonempty batches after 8,193 empty batches,
a 4,097-row columnar batch, two complete retained 4,097-field outputs and a
24,001-row spilled ordering. These are complete-value regression fixtures, not
new product limits or independent performance observations. The actual README
example exports all 90 expected values after session close.

| Final-source gate | Result |
| --- | --- |
| Formatting and default workspace Clippy | Pass; warnings denied |
| Default workspace, all targets | 3,213 passed, no failed or ignored tests |
| Reduced writer-profile CLI, all targets | 1,144 passed, no failed or ignored tests |
| Native workspace Clippy; native profile without writers; write-only, lean and all-feature checks; Rust 1.96 lean/native checks | All seven pass |
| Python suite against the actual final release extension | 733 passed, no failures, errors or skips |
| Native package-tool and dependency-notice tests | Seven plus seven passed, no skips |
| Version, license, channel source-contract and diff checks | Pass; source-contract checks do not claim a full publication dry run |
| Fresh installed-wheel native suite | 63 passed, no failures, errors or skips |
| Full native CLI/Vortex runtime profiles, all targets | 4,049 passed, no failures; 27 explicitly ignored manual/platform fixtures across 92 test summaries |

These suites overlap. Their counts must not be added as independent workloads.
The 27 ignored native tests comprise three non-Unix cases, one fixture-regeneration
helper and 23 explicitly invoked release/data-dependent observations. Their
presence is not runtime proof on this Mac. The complete native run takes 633.9
supervised seconds and verifies the 1,318 frozen source hashes before and after.
Required default checks are `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings` and
`cargo test --workspace --all-targets`; recorded runs also use `--offline --locked`
where applicable. The existing resource acceptance's public portfolio and Full43
results keep their original source identities; they were not rerun or relabeled
as this native Python measurement.

## Paired complete Python workflows

Accepted cohort: `native-surface-measurement-300c`. Each side uses the same
verified installed wheel and source, with either embedded execution or its
persistent subprocess client. Six workloads each have one untimed-for-scoring
preflight pair and five measured pairs, alternating execution order. All 72
samples are retained. Independent verification reads the saved Arrow files and
receipts without importing the driver, checks every value against separate
formulas, exact logical types and paired schema nullability/metadata, and verifies
all hashes, resource grants and explicit no-fallback evidence. All 36 paired
tables agree; there are no verification failures or errors.

| Complete Python workflow | Shape | Subprocess median | Native median | Native/subprocess ratio range across five pairs |
| --- | --- | ---: | ---: | ---: |
| Columnar input → filter/project → full Arrow output | 131,073 rows; two Int64 columns and nullable UTF8; 130,049 output rows | 504.848 ms | 10.367 ms | 0.0184–0.0209 |
| Generated range → full Arrow output | 131,073 Int64 values | 116.887 ms | 2.848 ms | 0.0158–0.0262 |
| Retain → filter → retain → project → Arrow | 32,769 input rows; 16,385 final values | 174.618 ms | 5.330 ms | 0.0296–0.0315 |
| Reuse one prepared SUM operation | 101 complete calls over a 32-row Vortex file | 98.262 ms | 20.126 ms | 0.2030–0.2219 |
| Build and execute a wide plan → Arrow | 257 output columns × 32 rows | 11.934 ms | 2.180 ms | 0.1780–0.1912 |
| Native-file SUM → one Arrow result | 1,000,017 Int64 input values; existing nullable Float64 SUM result | 11.097 ms | 10.286 ms | 0.7679–0.9901 |

These results support the native interface for the measured data-heavy Python
workflows and short operations. The file query changes modestly, with one pair
near parity. They establish no general query-kernel speedup, fresh-ingest result,
Full43 improvement or superiority over another engine.

The input comparison includes the subprocess interface's required columnar-to-row
conversion and JSON framing; the native side imports columnar buffers. The
retained chain explicitly supplies the same nonnullable schema on both sides.
The wide-plan comparison uses native typed declarations versus ordinary SQL
declaration construction, with the same semantics and complete output.

The workflow clock includes input conversion/admission, declaration construction,
execution, transport and complete Arrow return. Interpreter imports, initial
client/persistent-worker startup, fixture generation, prehashing, correctness
verification, evidence writes and final close are outside it. Per-operation work
inside the execution boundary remains timed. Streaming delivery and export share
one stage clock; a zero separate export field does not mean free export. Pure
kernel time is not separately observable through both interfaces and is recorded
as unavailable. The packet retains every stage and sample rather than inferring
kernel time by subtraction.

Host: `Mac17,3`, macOS 27 arm64, 16 GiB physical RAM, CPython 3.12.14 and PyArrow
25.0.1. The retained build guard selects the Rust 1.99 toolchain; Cargo records
release optimization level 3. A compiler-binary hash was not frozen separately.
Every operation declares 1 GiB memory and at most
two execution lanes. This grant differs from physical RAM and measured RSS.
Sources were hashed before use; OS cache state was uncontrolled. This is not a
cold-cache comparison. Native builds, tests and measurements were serialized
under the existing workload/storage guard.

The packet includes process-tree CPU/RSS samples every 0.5 seconds, host load,
fault counts and process-lifetime CPU/RSS counters. The following median RSS
counters include imports, setup, validation and cleanup, rather than only timed
work. Child peaks are separate maxima; adding them to the parent would not
produce simultaneous total RSS.

| Workflow | Subprocess Python / completed-child peak RSS | Embedded Python peak RSS |
| --- | ---: | ---: |
| Columnar input | 100.91 / 23.98 MiB | 114.61 MiB |
| Columnar output | 67.17 / 17.28 MiB | 74.75 MiB |
| Retained chain | 73.31 / 36.89 MiB | 69.77 MiB |
| Short repeated calls | 91.86 / 20.48 MiB | 82.52 MiB |
| Wide plan | 60.00 / 20.73 MiB | 60.08 MiB |
| File SUM | 58.11 / 30.44 MiB | 79.31 MiB |

## Preserved failures and corrections

- Cohort `299` stopped at an incorrect harness assumption that SUM returns Int64.
  Both existing providers declare nullable Float64. The original failed output,
  driver and schema inspection remain preserved.
- Cohort `300` exposed a real provider-selection regression: embedded file SQL
  always entered the general relational provider, while the command workflow
  selected its prepared primitive provider. The file SUM was slower in every
  pair. Shared declaration-based selection now supplies the same native provider;
  focused tests also verify general relational and memory-binding controls.
  That cohort also used an outdated report-field accessor, so its empty native
  evidence fields cannot accept the implementation.
- Cohort `300b` passed complete values but independent verification rejected all
  six retained-chain schema pairs: row re-import inferred nullable fields while
  retained native input preserved nonnullable fields. The final driver supplies
  the same explicit schema to both re-imports. Verification was not weakened.
- The first final-wheel attempt hit the unchanged free-space guard. Its partial
  output and logs remain retained. Verified compaction of completed older builds
  and exact redundant wheel copies recovered space before fresh proof `300b`.
  One archive attempt stopped on multiply linked files; a separate recovery
  receipt verifies the closed partial archive before retiring only its two
  verified originals. Failed evidence, live artifacts and storage ceilings were
  preserved.

Only cohort `300c` is used for the table above. Earlier successes, failures and
all samples remain separate rather than selecting faster observations across
builds or harness versions.

## Documentation and final review

All 18 documentation checks pass, including execution of the actual README and
guide examples against the isolated wheel, source/generated-content agreement,
reference/status checks, Astro validation/build, search policy and static assets.
Both examples return all 90 expected Int64 values after session close. Rendered
desktop and 390-pixel mobile checks cover dark/light themes and search navigation
to the new native section, without page overflow or browser warnings/errors.
Code blocks retain their own horizontal scrolling.

Final primary review rechecked retained-result and capsule ownership, demand and
cancel/drain behavior, shared SQL provider selection and the outstanding manifest
admission finding. The native manifest/directory binder now receives the admitted
pool before inspection and retains credited text, parser, path and listing
buffers. Existing tests cover tiny/competing grants, cancellation, complete paths,
Unicode, duplicate entries, cycles and credit release.

Review also corrected the hosted native test selection after the CLI entry point
was extracted into `shardloom_runtime`: the CI command, local release validator
and required-lane check now explicitly include `--lib`. The completed local
`--all-targets` run already exercised those tests. Only CI selection metadata and
documentation changed after that runtime acceptance; the final supplemental
checks verify the exact two-script delta and run the affected validation tests.
This primary review is not an independent security audit or hosted acceptance.
The [documentation/review index](evidence/native-python-documentation-2026-10-11.json)
and [supplemental packet](evidence/native-python-documentation-2026-10-11.tar.xz)
retain the checks, final source differences and rendered screenshots.

The initial hosted submission rejected the workflow before starting jobs because
the new wheel-download command contained a YAML-significant colon. The
[syntax correction](evidence/native-python-ci-syntax-2026-10-11.json) reproduces
that original parser failure and verifies all 16 jobs and three native wheel
lanes after changing only the command's YAML representation. The required-lane
validator passes; runtime/package sources remain unchanged. Actual hosted
execution must still pass on the corrected commit.

## Portable evidence and replay

The [portable packet](evidence/native-python-surface-2026-10-11.tar.xz) and
[acceptance index](evidence/native-python-surface-2026-10-11.json) retain complete accepted and diagnostic Arrow outputs,
fixtures, per-sample receipts and host observations, original drivers, independent
verifier, source snapshots, build/test/wheel logs, dependency audit and cleanup
receipts. Frozen executable and wheel identities are included; their large binary
copies remain local. All 2,995 archived members are independently reopened and
hash-verified; the extracted accepted cohort again passes all 72 complete-result
checks. The 19,691,952-byte archive has SHA-256
`a04539610f5dedfbc30b93ed9eaa14ea6f556d2d05d26fa0cdd820d3e899e31a`.

For result verification, extract the packet in local-only storage and run its
`observations/verify_native_measurement_300.py` with an interpreter containing
PyArrow 25.0.1, `--cohort` pointing to
`observations/native-surface-measurement-300c` and `--output` pointing to a new
JSON file. This reads preserved results; it does not execute the engine or
reproduce timings. Original absolute paths inside receipts are provenance.

To repeat timings, build and verify a fresh matching wheel, freeze its source and
release-artifact identities, and invoke the preserved cohort `driver.py` with
`--repo-root`, a new `--output-dir`, the new `--wheel-proof`, the new
`--build-evidence` and `--repetitions 5`. Rebind local paths in the preserved
supervision/build helpers and retain the same storage/workload guards. Never
reuse completed output paths or old build receipts for different binaries.

## Remaining work

Hosted integration must accept the final source and wheel matrix. Broader
streaming source/fanout gaps, resource-owner coverage, platform execution,
full-domain timestamp statistics/compression and the six-area/eight-investigation
queue keep their existing owners. Legacy/reduced-profile refusals are admission
evidence, not completion of useful workloads: connect useful paths to accounted
providers, and retire redundant fixtures only with equivalent shared-path proof.
No package publication or version change is part of this unit.
