<!-- SPDX-License-Identifier: Apache-2.0 -->

# Private projection regeneration at a resident sort boundary

Status: finite design for `PERF-06.rematerialization`, before candidate code or
comparative measurements. The [conditional sequence](native-conditional-investigations-2026-10-19.md)
and RFC 0044 remain the active implementation authority. This is neither broad
memory-policy completion nor closure of any competitive gate. CG-1 through CG-23
retain their separate obligations.

## Opportunity and provider decision

The real `Project -> Sort` caller evaluates each projection into native arrays,
then retains those arrays in `native_relational_sort::Sort`'s `Batch`/`Table`
until ordered gathering. Immutable memory sources deliver 8,192-row scan ranges
which `LocalSelection` splits into the ordinary 2,048-row consumer batches.
Computed non-key payload therefore accumulates across batches. The original
prepared memory source remains owned throughout both ordinary and proposed
execution; its bytes cannot be counted as reclaimed.

The [earlier owner screen](../benchmarks/native-rematerialization-owners-2026-10-07.md)
observed release of private derived buffers, but did not establish complete
retain/spill/regenerate cost. This experiment connects that ownership mechanism
to a complete public prepared relational workflow. The expert comparator is an
execution-engine maintainer reviewing error order, retained dependencies,
last-owner release, bounded reconstruction and complete consumption together.

Vortex-first classification: `implement_shardloom_kernel`, using the existing
native expression and sort kernels over pinned Vortex 0.85.0 arrays. The pinned
`SharedArray` uses an `Arc<OnceLock<...>>` and a one-way transition to its cached
result; it is not a revocable derived-owner policy. `ScalarFnArray` retains
children for deferred scalar execution, but does not by itself preserve this
caller's eager checked-error order, original execution grant and final
certificate. Reuse existing `ArrayRef`, native slicing/gathering, `StructArray`,
typed expression evaluation and credited allocations. Do not add a provider,
dependency, cache, external engine or decode-to-Arrow route.

## Finite candidate

Only the bound root `Sort(Project(ScanSource::Memory))` is eligible. The scan
must have neither pushed nor residual filtering. Parameterized/dynamic roots,
file and one-shot batch sources, nested consumers, and explicit spill operations
keep their existing execution. The immutable source has 32,768 through 65,536
rows; there are at most 128 scan/project fields and one through four sort keys.
Identifiers are at most 256 bytes. These are optimization bounds, not new public
limits on ordinary execution.

Every output is nullable or nonnullable Int64. Direct outputs must retain the
input name. Between eight and 64 outputs must be genuinely derived checked
Int64 expressions. The grammar is columns, typed Int64 literals and binary
Add/Subtract/Multiply, with at most two binary levels and at least one column
in every derived expression. Literal-only outputs, renamed aliases, other
operators/types and derived sort keys are not admitted. Every sort key remains
a direct same-name source column with unchanged NULL ordering and stable ties.

Before allocating recipe metadata, find the complete union of input columns
referenced by all output expressions, including direct outputs and keys. Keep
the original bound expressions borrowed and the original source pinned. Charge
cloned dependency schema/key metadata before allocation. The recipe exists only
inside this operation and uses the original shared native memory grant.

For each input batch, evaluate the entire original projection in its original
expression order. This preserves eager checked-arithmetic errors before any
sorted output. Drop that private projected array and all its derived references,
then retain only the dependency projection in the existing Sort. Record actual
shared-pool credits immediately before projection, while its result is held and
after its final private owner is dropped. Logical array size is not release
evidence. No published result, alias or slice is revoked.

Reserve reconstruction planning headroom during retention. For output batch
bound `b`, output-field count `o`, dependency-field count `d`, sort-key count `k`
and maximum binary depth `h`, the checked byte reservation is:

`65,536 + 4,096 * (o + d + k) + b * (16 * (o + d) + 128 * (h + 1))`.

This deliberately conservative reservation covers planned canonical batch
output, dependency gathering, scalar temporaries and metadata. It is a planning
allowance, not a new allocator, an exact physical-byte estimate or a guarantee
against other owners consuming the grant. Release this allowance immediately
before existing Sort finalization; all real gather/expression allocations must
still reserve from the original pool and fail normally on denial. Record the
allowance separately from actual tracked peak. Do not reset the pool's lifetime
peak or present it as an isolated reconstruction peak.

The existing stable ordinal sort gathers the dependency fields in bounded
native batches. Evaluate the same original projection once per ordered batch
and synchronously deliver it. Check cancellation around each transition. Track
validated and regenerated rows, and reject any attempt to regenerate more than
the validated cardinality. There is no source replay, recursive retry, extra
grant, cross-operation state or effectful recipe.

## Frozen comparison

The machine-readable protocol is `rematerialization-protocol-778.json` in the
local evidence workspace; its SHA-256 freeze receipt precedes candidate edits.
One freshly compiled release executable contains identical harness/oracle code
and a test-only disable switch. Production has no experiment switch. The three
roles are ordinary retention, existing explicitly authorized native spill, and
the candidate with spill disabled. All use one source, one preparation, one
complete public `for_each_batch` call, 64-MiB native memory, one CPU lane and no
provider background workers. Spill uses the existing 1-MiB buffer, 128-MiB disk
quota and two-reader limit. Candidate admission does not override spill policy.

Four targets combine 32,768/65,536 rows and eight/64 derived columns. Nine
controls cover small inputs, one derived column, aliases, constants, derived
keys, an extra Boolean output, wide unused source columns, a wide dependency
closure and all-null arithmetic. Preflight also runs every declared cell at
zero and 17 rows. Full dtype, nullability, every output value and stable original
ordinal are checked by an independent formula-based oracle.

The consumer copies every complete native result into a bounded test witness
after checking its schema and row count. Witness/fixture storage belongs to the
test caller, outside the native grant; it cannot establish a process-RSS bound.
Actual and source witnesses are losslessly retained, content addressed and
independently reopened. Callback consumption is measured separately and
subtracted from the public-call elapsed time using the existing query timing
contract. Preparation, source intake, complete consumption, release and owner
destruction also form a separate complete-workflow control. Native clocks retain
their existing meanings; routine timing communication remains engine-only.

Both cohorts, three samples per cell/role, and all six warmups must finish before
comparative inspection. A runs retain/spill/regenerate; B reverses roles and
cell order. Each target must reduce median complete native-pool peak against
retention by at least 20% and 131,072 bytes in each cohort. Every cell must avoid
an engine or complete-workflow regression exceeding both 5% and 1 ms against
retention; median peak may not increase by more than 65,536 bytes. The four
targets must also avoid that throughput regression against actual native spill.
Spill must write real runs for every target. No refusal is counted as a timing
win, and controls cannot be dropped after results are seen.

## Acceptance and adversarial cases

Before comparative timing, prove exact activation/nonactivation, full outputs,
actual private credit release, full dependency retention, bounded regeneration,
one source construction, one completed operation, native certificates and final
zero reservations. Cover late overflow before any output, nulls and stable ties,
unsupported grammar/type/key/source shapes, recipe/headroom/allocation denial,
pre-cancellation and cancellation during reconstruction, failing consumers,
retained result clones/slices and last-owner cleanup. The earlier alias/key/
constant zero-reclaim controls remain binding.

The three principal review objections are changed eager-error semantics,
counting aliases as freed storage, and omitting source/rebuild overlap. Original
ordered validation, observed final-owner credit release and full shared-pool
peak/whole-dependency controls address them; tests must establish these claims.

Passing the finite comparison permits full integration acceptance: workspace
format/lint/tests, native suite, native CLI, installed Python, Public39, one
Full43 pass and modular workflows against exact final sources. A failed valid
gate rejects this mechanism. Preserve the original artifacts, remove candidate
and hooks, prove restoration to accepted production sources and record the
finite decision. Do not retune the candidate from measured controls. Broader
rematerialization, automatic pressure policy, source reconstruction, result
revocation and recovery remain separate obligations.

## Source anchors

- `local_primitive_relational_transform.rs`: original projection and sort caller.
- `local_primitive_relational_scan.rs` and `local_primitive_relational_selection.rs`:
  immutable-source ranges, local selection and bounded consumer batches.
- `local_primitive_prepared_relational.rs`: bound tree, public consumer and grant.
- `resident_memory_source.rs`: immutable prepared source and bound projection.
- `native_relational_expression.rs`: checked typed evaluation and dependencies.
- `native_relational_sort.rs` / `native_relational_batch.rs`: retained keys,
  stable ordinals and complete gathering.
- `native_relational_spill.rs`: existing authorized native spill comparison.
- `vendor/vortex-array/src/arrays/shared/array.rs` and
  `vendor/vortex-array/src/arrays/scalar_fn/array.rs`: provider alternatives.
