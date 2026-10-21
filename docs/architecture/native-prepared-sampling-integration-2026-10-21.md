<!-- SPDX-License-Identifier: Apache-2.0 -->

# Prepared sampling: public identity-projection integration

Status: prospective integration correction, recorded before implementation and
its comparison. This belongs to the existing RFC 0044 sampling follow-up under
PERF-10/02/03/06, with PERF-12 acceptance. It adds no phase or roadmap owner.

The first candidate at `b3dd4b9d3c79da36ae8a705563d76c37f57bb2bb` passes both
complete frozen comparison cohorts and their controls. That result remains
valid for its measured public Rust preparation route. Public integration is
incomplete: SQL always adds a Project, including for SELECT *, so the admitted
physical root does not occur through ordinary SQL/Python sampling. Do not infer
public activation from the Rust comparison or retain the candidate on that
evidence alone. Preserve that complete comparison and its replay packet.

## Correction and semantic boundary

Normalize an exactly identical projection in the shared native binder, after
ordinary name, expression, type, payload and unique-field validation. Remove
the Project only when its complete ordered fields equal its input fields and
each bound expression is the corresponding plain column reference. This
preserves names, DTypes, nullability, cardinality, row order and field order.
Computed expressions, casts, renamed/reordered/subset/duplicated fields and
the existing untyped-NULL carrier coercion are not identical projections.
Invalid expressions must still fail before execution. Retain the binder's
existing conservative metadata admission and recursion checks.

SQL's backward pruning must preserve an existing all-column scan when its
required set is exactly the complete source schema. Sorting that set into a
new scan projection currently creates an unnecessary permutation even for
SELECT *. Actual column removal continues through the existing pruning path;
an explicit reorder stays a Project. Qualified names are resolved by the
existing SQL scope rules before native binding. This introduces no public API,
parser option or alternate execution route.

Sampling admission itself stays unchanged: after this exact normalization,
only a bound root unary sample directly over an immutable native memory scan
can retain the scalar model. Real intervening operators, child samples,
filters, source limits, file/batch/dynamic sources and spill remain excluded.
No model may escape its prepared object or outlive its credited ownership.

Vortex-first provider check: the pinned 0.85.0 DType, immutable array and native
projection providers remain in use. This is normalization of ShardLoom's bound
relational IR, not a new array representation or execution provider. The
existing binder in `local_primitive_relational_transform_bind.rs`, SQL demand
pass in `sql_native_relational_pruning.rs`, and report-only planner optimizer
have no identity-Project removal helper. Classification:
`implement_shardloom_kernel` for this small original native binding rule.
It introduces no decode, materialization, unsafe code or dependency.
Certificates retain `fallback_attempted=false` and no external executor.

## Acceptance fixed before implementation

Add focused tests that inspect the bound shape and complete values/schema:
identity and nested identity, nullable and empty inputs, qualified names,
rename/reorder/subset/duplicate/computed expressions, untyped NULL and invalid
names. Exercise sampling with identity projections above and below it, prove
one first calibration and later scalar reuse, and keep actual intervening
operators excluded. Release all owners and check zero final credits.

Exercise ordinary SQL and the installed Python plan lifecycle with complete
independently calculated seeded results, source/seed/projection changes,
explicit preparation release, and observed retained-credit differences. A
role label, elapsed time or correct result alone is not activation proof.

The [original frozen protocol](native-prepared-sampling-2026-10-21.md) remains
byte-identical. Preserve its workloads, ordinary baseline, mathematical
algorithm, reference, observer/harness, features, 24 cells, two cohort orders,
warmups, seven scored samples, all exactness/resource gates and all thresholds.
Bind a fresh integration candidate and execute the same comparison in a new
result namespace. Reuse the immutable original baseline binary and its full
compiler-input evidence; run new observed baseline and candidate preflights.
Do not overwrite or replace the first comparison, relax a control, or retry an
unchanged failure. Record namespace-only orchestration changes separately from
the frozen algorithm and oracle. An integration failure prevents retention.

Retention still requires formatting, strict Clippy, full workspace/native
tests, CLI/installed-package lifecycle proof, the applicable complete public
matrix, one Full43 pass and format/workflow acceptance. No public speed claim,
version change, package publication, broad cache capability or competitive-gate
completion follows from this correction.
