<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared Sort validation and bounded payload construction

Status: locally accepted at `788a8925e587316ed6f6c3d9ca114fef29937bd5` under the
[optimization coverage contract](native-optimization-coverage-2026-11-13.md).
Complete source-bound validation, installed-package proof and the isolated
388-case comparison pass. This does not close coverage or authorize publication.

The correctness correction at `4d5fa4a46` restores payload validation that the
file Sort shortcut skipped. Its shared Sort still constructs all nested/decimal
output rows even when a following range retains a small prefix. Flat prefix
construction also reaches the scalar validator instead of the existing typed
selection validator.

The candidate connects `native_payload::validate_selection` to the existing
`Gather::column_prefix` owner for decimal, nested and admitted extension payloads.
Every index in the original sorted output batch is validated before the existing
native copier constructs only the retained prefix. Flat payloads retain their
complete original selection and cast before sharing the existing typed numeric,
boolean and variable-width validators. This preserves malformed-text coordinates,
nullability and cast failures outside the prefix. Unsupported typed validation
keeps the original scalar prefix path, including exact Float16-to-Float64
promotion. Unobserved-field and copy-policy barriers remain.

Full key validation and stable ordering still precede payload validation.
Original output batch boundaries and field visitation order remain, including
non-winning rows and fields hidden by later projections. A consumer failure
still precedes errors in later output batches. All temporary and retained
owners use the existing allocator, cancellation and cleanup contracts; no
source scan, provider retry, execution engine or pool is added.

The Vortex-first decision is `use_vortex_native_provider`: Cargo.lock pins
Vortex 0.93.0, and existing native arrays, `take`, `slice`, dtype and validity
handling remain inside shardloom-vortex's feature-gated boundary. ShardLoom's
existing recursive validator supplies its finite-value, precision, shape and
failure-order rules. The candidate introduces no dependency or representation.

Tests compare complete validation errors against unbounded Sort and require
N payload rows validated with at most K output rows constructed. Cases include
masked/unreferenced children, decimal precision, malformed text, Variant,
tensor/vector extensions, UUID, opaque binary descriptor movement, Float16
bits and promotion, empty/zero demand, cancellation, denied credit and output
ownership after producer disposal. Opaque descriptor movement does not resolve
the referenced binary object.

The completed comparison uses exactly the 388 declarations already measured
at `4d5fa4a46`, in their original order and with the same inputs, references,
grant and isolated supervisor. Their single current baseline totals
337.804658081 engine seconds. There is no combined historical baseline and no
new Full43 or full 10,000-case pass for this candidate. Acceptance requires
complete correctness, resource, ownership and no-fallback proof, followed by a
lower complete-cohort engine total. The accepted total is 327.589944482 engine
seconds, a 10.214713599-second reduction from the same current baseline.
Individual regressions and failed attempts remain evidence. All 388 complete
answers, schemas, certificates and clocks pass independent local replay; all
native pipeline/file-kernel counts and ten owned-source column controls remain
unchanged. No denied reservation or spill occurs. Retained reservations do not
increase; the largest peak-reservation increase is 25,264 bytes.

Default formatting, strict Clippy and tests pass (3,325 tests); complete native
validation passes 7,157 tests with 39 ignored, including 4,184 native-library
tests with 36 ignored. All 442 installed-package tests pass with zero skips.
The [portable evidence packet](../benchmarks/evidence/native-sort-prefix-validation-2026-11-13/README.md)
preserves 107 archived members, source/runtime identities, every observation,
the work review and initial verifier failure. Relocated verification and all
26 corruption/refusal controls pass. The verifier checks saved evidence without
executing queries or repeating the independent complete-answer oracle. One
observation per runtime does not establish repeatability, whole-engine speedup,
physical Vortex column-read avoidance or complete optimization coverage.
