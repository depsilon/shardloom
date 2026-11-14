<!-- SPDX-License-Identifier: Apache-2.0 -->

# Constant-key regional aggregate investigation

Status: the fixed candidate is complete and **dropped**. Its exact experimental
source is preserved separately at `b6e9d5d5d6bbd00ad48890795b8fe95b27a1d088`.
The accepted [scalar regional implementation](native-regional-aggregates-2026-11-14.md)
at `e5f6728a2` is restored unchanged. The
[complete comparison](../benchmarks/native-constant-key-aggregates-2026-11-14.md)
and [portable packet](../benchmarks/evidence/native-constant-key-aggregates-2026-11-14/README.md)
preserve every result, regression, failure and frozen requirement.

All 5,014 declarations per runtime pass complete result, schema, resource and
native-execution checks. All ten requested-read witnesses improve, and 810 new
constant-group plus 350 inherited regional coverage/refusal expectations pass.
The separate 20-operation prepared lifecycle total decreases from 3.325826500
to 1.998268543 engine seconds. However, the complete fresh engine total increases
from 556.592642154 to 564.033758513 seconds, so the frozen rule rejects the
candidate. Its 3,211 individual regressions remain; no subgroup improvement
replaces the complete-cohort decision.

All required candidate workspace, native, feature-footprint, provider and installed
checks pass. Saved and relocated portable replay verify all 10,028 complete results
and 40 lifecycle operations with no query execution; all 47 saved-evidence and
36 packet-corruption controls pass. The preservation commit's 5,762 captured
source files match the actual build snapshot; its original build parent and
dirty-source identity remain explicit. Restoration was verified against the
accepted source, and neither a replacement build nor another timing run was added.

The subsequent [dictionary-domain N unit](native-dictionary-domain-2026-11-14.md)
is retained locally. Named source/domain facts and ALP/FastLanes predicate
admission are next. Broader constant-key support,
the parent metadata family, item 19/20, RFC 0044 and CG-1 through CG-23 remain open.
No merge, package publication, release or writer-default change is implied.

The following sections preserve the contract fixed before measurement. They
specify the rejected experiment, not behavior retained in the active runtime.

## Contract and existing owners

The expert comparator is an encoded-columnar engine maintainer checking group
equality, source-order semantics, exact coverage and allocation ownership. A
region may contribute one weighted group update only when every grouping key
is constant over those exact rows, the predicate includes every row, and every
requested measure has a compatible exact partial. Integer COUNT/MIN/MAX are
the initial domain; SUM/AVG, distinct, transformed groups/measures, float keys,
spill and unsupported sources retain their existing native execution.

Extend the current regional metadata reader and cross-column whole-zone proof.
Use the existing file-provider `GroupedAggregateStates`, native group-key
identity, `SimpleAggregateStates` partial merger and finalizer. The current
public provider binding remains authoritative: do not force a plan into a
different provider to manufacture activation. CLI SQL, embedded workflow,
Python SQL, DataFrame and typed-plan cases must prove their actual dispatch.
Relational compositions outside this existing provider's equivalence admission
retain their current native aggregate owner and become explicit controls.

For each integer key, exact null count zero plus exact equal extrema proves a
non-null constant. Exact null count equal to the positive region length proves
the null grouping key. Equal extrema with mixed or unknown null coverage do
not prove a single key. All keys and measure partials must cover the same
disjoint region; a whole-zone statistic cannot answer a subset. Keep signed,
unsigned, nullable dtype, alias, HAVING, limit and offset behavior unchanged.

## Vortex-first decision

Pinned Vortex 0.93.0 provides typed zoned `Min`, `Max` and `NullCount` partials,
native scalar/null identities, Constant and RunEnd representations, and native
scan ranges. Reuse those exact metadata and scan providers. A proven region is
semantically constant; do not expand it into repeated rows or fabricate a
grouped-list payload merely to invoke an upstream kernel. Existing Constant
and RunEnd arrays remain supported through their ordinary native consumers.

The pinned grouped COUNT kernel in
`vendor/vortex-array/src/aggregate_fn/fns/count/grouped.rs` consumes already
grouped arrays and ranges; it does not attach flat source key identities to
ShardLoom's groups or preserve its checked state/order policy. Classification:
`use_vortex_native_provider` for stored partials and native scans;
`implement_shardloom_kernel` for the small proof/merge connection into the
existing grouped owner. No dependency, new source family, Arrow execution
substrate or external query-engine fallback is introduced.

## Ordering, atomicity and ownership

Stage groups before publishing any target-state change. If all included rows
are answered by metadata, visit regions in source order and preserve the
existing first-seen group order, including limited group admission. If residual
rows remain, metadata may be merged ahead of scans only when ORDER BY includes
every grouping key and therefore fixes all output ties. Otherwise discard the
staged partials and scan all non-excluded regions in source order. Metadata
reads still appear in the certificate. Missing constancy proof is a scan
decision, never an invented key or a successful partial answer.

Reserve bounded staging, map capacity, cloned states, key storage and order
storage from the existing session pool before allocation. Transfer the group
reservation with the published groups so dropping a temporary proof cannot
refund live state. Preserve the existing scalar path's allocation and proof
contract. Optional refusal before publication leaves the native scan intact;
corruption, arithmetic overflow, cancellation and late source errors fail the
whole operation. Do not replay after partial publication. Existing worker or
special-state ownership that cannot safely accept partials declines this
connection before metadata work; do not silently combine two aggregate owners.

## Evidence and decision

Focused tests must cover all integer widths, extreme signed/unsigned values,
single/multiple keys, all-null versus mixed-null keys, repeated keys in
nonadjacent zones, independently zoned columns, fully covered and residual
regions, unsorted and tied-order refusal, empty sources, HAVING/limit/offset,
missing facts, wrong coverage, cancellation, held-generation changes, optional
reservation refusal and credits retained until the group state is destroyed.
Compare complete results, schemas and owned output after source disposal.

Before timing, freeze exact requests, all expected complete references,
source/input/runtime identities, observed work witnesses, ownership/resource
requirements and the full-cohort retain/drop rule. Reuse exact compatible
baseline packets; add versioned missing constant-key/nullable/ordering/layout
controls across every public surface. Preserve all slower controls. Require
actual fewer requested source bytes for the declared exposure witnesses and
strictly lower complete engine time, with any prepared lifecycle population
decided separately. Equal or higher totals drop the fixed candidate.

Run focused checks and the required default/native/provider/installed gates at
the coherent source boundary. Keep measured execution isolated and execute
each frozen declaration once. No new Full43, full10k, competitor pass, writer
default, merge, publication or release is authorized by this experiment.
Item 19, item 20, RFC 0044 and CG-1 through CG-23 remain open.
