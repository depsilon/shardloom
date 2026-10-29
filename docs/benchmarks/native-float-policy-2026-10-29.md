# Explicit floating-domain acceptance — October 29, 2026

Local public, default/native workspace and installed-package acceptance pass.
The accompanying source and portable-replay receipts record the final evidence
checks. Hosted PR acceptance remains a separate gate.

This item-20 unit adds an immutable caller-selected `float_policy="extended"`
for NaN, signed infinity and signed-zero semantics. The default remains `strict`.
The [implementation contract](../architecture/native-float-policy-2026-10-29.md)
separates carried bits, analytical identity, computed results and NULL validity.
The [public reference](../reference/native-analytical-aggregates.md) documents
Python and CLI selection and the admitted aggregate behavior.

Existing Vortex arrays, native memory owners, key comparison, aggregate/window
state and spill boundaries implement the policy. No dependency or external
execution provider is added. Vortex remains native input and output; Arrow IPC
and Parquet are explicit compatibility boundaries. This is capability and
ownership acceptance, with no benchmark or performance claim.

## Complete-value acceptance

This table records the initial accepted snapshot. The CI and review correction
below records the subsequent source/runtime delta without replacing these
observations.

| Evidence | Result |
| --- | --- |
| Public execution | 20 tests pass without skips. 82 equivalent-case cohorts execute 410 times across DataFrame, Python SQL, typed native relations, embedded workflows and CLI, comparing 3,315 complete rows. |
| Independent comparisons | 1,362 complete comparisons cover 108,081 rows including resource and lifecycle cases. Floating values use original F32/F64 bits; type, nullability and nested values are retained. Only the explicitly named finite correlation result permits a 1e-14 tolerance. |
| Native evidence | All 164 native certificates and 255 public envelopes identify the selected policy and report no external execution or fallback. |
| Semantics fixtures | Three complete SQL fixtures cover strict finite classification, extended classification and extended group identity; they do not substitute for the raw-bit fixtures. |
| Default workspace | 3,287 tests pass without ignores. Formatting, default and native strict lint, and the minimal native feature check pass. |
| Native workspace | The final full run passes 6,468 tests without failures; 35 existing opt-in tests remain ignored. |
| Installed package | The frozen CLI/library is packaged, verified and installed into an isolated environment. All 243 integration tests pass without skips, including all 20 floating-domain methods; uninstall verification passes. No publication occurs. |
| Source and documentation consumers | The final gate runs 15 commands covering generated references, declarations, function coverage and conformance consumers, with exact source/product identities and no skipped tests. Its complete results are retained in the packet. |

Fixtures distinguish positive/negative zero, both infinities, signed quiet and
signaling NaNs with different payloads, subnormals, finite extrema, NULL and
empty input. They exercise exact mixed-integer comparison boundaries,
classification, arithmetic, casts, keys, joins, sets, nested duplicates, pivot
keys, first representatives, reducers, paired statistics, quantiles, row windows
and recovery after nonfinite values leave a frame. Binary carriage preserves
bits; newly computed NaNs use the documented canonical pattern.

Vortex, Parquet and Arrow IPC readback includes source deletion, retained output,
re-registration and producer closure. Streamed collection, batches and native
write cover complete EOF, empty input, late errors and zero-limit late errors.
Strict intake refuses nonfinite values while respecting nullable parent masks.
Raw nonfinite text output fails before publication; a subsequent binary action
remains usable. Explicit UTF8 casts choose text representation.

## Resource and failure observations

Every pressure case compares complete values with an ample-memory execution and
independent expected values. Resident-only execution at the pressure grant is
denied. Explicit native spill completes under that grant and a 256 MiB disk
quota, validating every run and releasing owned files while preserving an
unrelated sentinel.

| Case | Complete rows | Grant | Runs / merge passes | Peak reservation | Peak spill bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Grouped SUM/AVG/MIN/MAX | 8,005 | 32 MiB | 202 / 100 | 30,119,954 | 112,273,588 |
| Moving SUM | 24,015 | 32 MiB | 102 / 49 | 30,152,046 | 112,293,480 |
| Pivot SUM with margins | 8,006 | 64 MiB | 593 / 296 | 67,090,181 | 115,146,708 |
| Last duplicate selection with F32/F64 payloads | 8,005 | 32 MiB | 158 / 78 | 10,132,682 | 96,208,564 |

The grouped case additionally verifies quota denial, cancellation, early consumer
closure, retained output and a successful retry. The retry writes and validates
202 runs with 100 merge passes, a 32,606,690-byte peak reservation and unchanged
complete results. Pivot's larger grant reflects its existing conservative output
buffer; no storage limit was raised to obtain a passing result.

## Regression evidence and replay

Initial adversarial review found three concrete gaps. Nested duplicate selection and scalar
pivot identity still distinguished NaN payloads and zero signs; attempt `f050`
failed 75 subcases across all five interfaces before those owners were corrected.
Private spill narrowed a carried signaling Float32 NaN through a numeric cast;
attempt `f056` changed raw little-endian bytes `420080ff` to `4200c0ff` in actual
duplicate-selection spill. The shared private writer now uses exact bit-preserving
narrowing. The focused corrected checks and the complete final public run pass.
Both failed attempts remain part of the evidence.

The [portable packet](evidence/native-float-policy-2026-10-29.json.xz) retains
numbered attempts and logs, complete comparisons, certificates, independent
reference functions, conformance results, source/product hashes and package
proof. Its verifier checks all carried bits, NULLs, policies, spill bounds and
cleanup, source linkage and installed-product identity. Seventeen corruption
controls cover those obligations. The companion
[replay receipt](evidence/native-float-policy-2026-10-29-replay.json) records
isolated Python verification using only the archived verifier and packet, without
importing the source checkout or loading the native engine. The
[cleanup receipt](evidence/native-float-policy-cleanup-2026-10-29.json) records
exact identities and closed-handle checks for retired temporary payloads while
retaining the tested runtime, installed-tested wheel and compact evidence.

## Hosted CI and review correction

The first PR head `8c9179fc707e5763774749e801f590c8f8679070` exposed two
additional metadata-consumer gaps. The context/client graduation inventory omitted
the inert `float_policy` property, and the hard-readiness consumer still required
47 semantic cases and 58 total stages after the conformance producer admitted
50 semantic cases and 61 total stages. The same three test failures reproduce
locally in `f071`; all 15 affected producer/consumer and graduation tests pass
after correction in `f072`.

The inventory now classifies the property with construction/configuration, and
readiness derives these exact counts from the canonical required case sets.
Negative controls still reject incomplete semantic, diagnostic, artifact and
no-fallback evidence. Generated surface indexes follow that inventory.

Hosted review also identified an unconditional strict-policy wrapper in scalar
assignment execution and non-composable Python classifier return types. Attempt
`f080` reproduces the arithmetic failure on the original runtime. The unary owner
now passes the execution context's policy to the shared arithmetic function;
its existing numeric test oracle also names that policy explicitly. Python
classifiers use the existing predicate owner and retain source/resource metadata.
Attempt `f085` additionally exposed Python `eval` refusing a zero divisor before
extended execution. That declaration now reaches native checks in extended mode;
strict behavior and the legacy primitive admission refusal remain intact.

The corrected public pass completes 22 tests with no skips: 106 equivalent-case
cohorts, 530 five-interface executions, 1,500 complete comparisons and 108,969
compared rows. It includes F32/F64 rewrites, finite overflow, zero division,
strict refusal, NULL/empty domains, repeated actions and Boolean classifier
composition across Vortex, Arrow IPC and Parquet. The original portable packet
and installed-wheel observations remain evidence for their recorded snapshot;
the [correction supplement](evidence/native-float-policy-ci-2026-10-29.json)
records the later delta and preserves failed attempts. Hosted checks and review
on the corrected PR head remain merge gates.

The corrected snapshot also passes default/native strict lint, formatting,
3,287 default workspace tests, 6,468 native workspace tests with the same 35
existing opt-in ignores, the minimal native feature check and three conformance
fixtures. Its isolated wheel passes all 245 integration tests without skips,
including the 22 floating-domain methods, and verified uninstall. The runtime
build predates a test-only oracle import correction; the later native test/lint
receipts prove that exact test source, while the production Rust inputs remain
identical to the built CLI/library. The supplement verifies that distinction.

The [isolated correction replay](evidence/native-float-policy-ci-2026-10-29-replay.json)
checks complete values, source/product linkage, policies, certificates, installed
package identity and 12 corruption controls using the archived verifier. The
[correction cleanup](evidence/native-float-policy-ci-2026-10-29-cleanup.json)
records exact retirement of the superseded runtime/wheel and completed temporary
package, fixture and raw-result payloads after verification. Current inputs,
the corrected tested runtime/wheel, compact results and the original acceptance
packet remain; no benchmark or package publication occurs.

## Remaining boundaries

The small Rust `MemoryColumnValues` and owned-Float64 convenience intakes remain
finite-only. Policy-aware columnar intake uses the existing Arrow-to-native owner.
Temporal linear interpolation, probabilities, weights and resource controls
remain finite under both policies. Exact integer/decimal casts reject nonfinite
values; finite accumulator overflow keeps its existing deterministic error.

Remaining composition and recursive execution, application batch functions,
diagnostics and combined item-20 acceptance stay open. This unit closes no
competitive gate and authorizes no release. The retained 347-query checkpoint
remains **15.304317834 engine seconds**; no new timing observation is supplied.
