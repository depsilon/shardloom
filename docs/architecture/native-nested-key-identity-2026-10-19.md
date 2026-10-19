<!-- SPDX-License-Identifier: Apache-2.0 -->

# Nested-key attribution and identity investigation — October 19, 2026

Status: **finite identity candidate rejected at the activation preflight**.
Recursive key work has a reproducible attribution signal, but the proposed
dictionary identity owner activates in only four of eight required target
cases. No candidate timing comparison ran. The candidate and observation hooks
are archived and removed; production behavior is unchanged.

This is the fourth finite decision in the
[conditional investigation sequence](native-conditional-investigations-2026-10-19.md),
within `PERF-05.nested_identity`. The broader conditional substep, other PERF-05
obligations and CG-1 through CG-23 remain open. The decision does not establish
that every possible nested identity mechanism is ineffective.

## Attribution before implementation

The [attribution packet](../benchmarks/evidence/nested-key-attribution-760.json)
preserves the frozen protocol, test-only disjoint observations of key owner
construction, comparison, hashing and exact serialization, and two complete
fresh-process cohorts. Both observation roles use the same frozen executable;
the disabled role follows the original algorithm. Observer overhead is charged
before a cell can establish material attribution.

The 128 cells cover 128/4,096 rows, nullable integer lists and structs containing
lists, eight/all-unique domains, plain/parent-dictionary physical layouts,
grouping, distinct counts, left joins and unary distinct, with one/four calls per
prepared workflow. Forty-eight complete native Vortex files have independently
specified value/schema witnesses and complete flat-layout inspections. Empty
and all-null controls are additional preflight cases. Dictionary codes are local
to each stored chunk; matching codes do not establish equality across domains.

Each measured cohort contains 770 complete workflows and 1,922 operations,
including two retained, excluded warmups. Both completed before comparative
analysis. The independent analyzer found 20 common repeated-key cells with
material attribution across the two logical shapes. Grouping, distinct count
and unary distinct qualified; joins did not. Seven cohort/cell observations
failed the observer-overhead eligibility rule and remain excluded. Attribution
permitted a candidate design; it did not establish an optimization benefit.

The packet retains the initial fixture/preflight failures and their corrected
source identities. The final preflight checks all 320 workflows and 704
operations. Its full-output oracle, six native key tests, 38 nested relational
tests, strict native lint and adversarial analyzer checks pass.

## Frozen candidate and provider boundary

The [candidate packet](../benchmarks/evidence/nested-key-identity-762.json)
contains the protocol frozen before implementation. Vortex-first classification
is `implement_shardloom_kernel`: pinned Vortex 0.85.0 supplies native dictionary
domains, codes and chunked arrays; ShardLoom binds exact unary serialization,
resource ownership and cancellation to those providers. No dependency, public
API, persistence format or external executor was introduced.

The candidate canonicalizes only referenced dictionary entries, resolves hash
collisions by full byte equality across domains, and retains exact serialized
representatives plus a row identity vector. Unary key writers replay those
representatives. Relational normalized equality/hashing and native output
selection use their existing paths. Pivot retains its original shared key owner.

Admission covers nested integer/Boolean/NULL types with 128–65,536 rows, bounded
dictionary domains and wrapper depth. It permits at most 64 representatives of
16 KiB each, reserves 4 MiB before construction and requires 8 MiB available.
The reservation shrinks to retained capacities after construction. Native
provider allocations use the same pool. Plain/mixed/all-unique and unsupported
leaf types retain the original key path; refusal releases tentative state, and
allocation errors/cancellation remain explicit.

Exactly eight original cells were targets: unary distinct over an eight-value
parent dictionary, both sizes, both shapes and both call counts. The other
120 cells were controls. The protocol required complete target activation and
semantic/resource preflight before either performance cohort. Its later benefit
gate required both aggregate and common-cell improvement, complete-workflow
controls and no material regression in any cell. No threshold or target scope
was relaxed when preflight failed.

## Preflight result and rejection

| Required preflight boundary | Result |
| --- | --- |
| Complete independent values, field order, dtypes, nullability and output order | All 320 workflows / 704 operations pass. |
| Native certificates, declared resource grant and final zero reservations | Pass for every workflow; supervised processes drained. |
| New collision, cross-domain, NULL, source-drop, low-grant, size-refusal, cancellation and failed-write tests | Six tests pass. |
| Existing key / nested relational / unary / membership tests | 6 / 38 / 56 / 15 tests pass; selectors can overlap. |
| Strict native lint and frozen release build | Pass. Initial compilation/lint failures remain in the packet. |
| Required candidate activation | Four 128-row cells pass; all four 4,096-row cells report zero activation. |
| Evidence validator | Rejects 23 corrupt records and six failing decision scenarios; positive and threshold-boundary checks pass. |

The complete independent oracle was applied to every preflight record after the
activation validator first stopped. This separately confirms semantic/resource
success and records all four activation failures. It does not convert the
failed preflight into acceptance. The comparison never started, and no candidate
speed or complete-workflow benefit is claimed.

The counters prove missing admission at the production consumer. They do not
identify which admission predicate refused the larger inputs. Source review
found native sliced views outside the direct Dict/Chunked tree admission, but
the exact rejected runtime wrapper tree was not recorded. That remains an
inference, not a measured root cause. A future mechanism must first establish
its actual provider/wrapper input and freeze a new admission design.

## Restoration and evidence limits

All experimental runtime changes, observation hooks and candidate-only tests
were removed after archival verification. Rust/Cargo inputs return exactly to
the pre-experiment production baseline. The
[restoration receipt](../benchmarks/evidence/nested-identity-restoration-762.json) binds the
removed paths and the complete final source inventory; the evidence indexes
retain the original source patches, added files, build commands and executable
hashes. Bulk binaries and native fixture files stay in local storage with exact
identities and reproduction references.

The two packets are separate: the attribution packet supports the opportunity
signal, while the candidate packet supports rejection before measurement.
Packaging-time inventories of rebuildable Cargo outputs are not historical
build identities. The copied frozen test executables have their own recorded
hashes. Replays must use new local output paths and retain the original guards.

There is no additional Full43, installed package, platform or whole-workspace
acceptance claim for this rejected candidate. Native Vortex input/output,
explicit unsupported behavior and the no-fallback boundary remain unchanged.
