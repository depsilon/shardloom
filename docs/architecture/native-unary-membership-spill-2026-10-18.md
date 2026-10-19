# Native unary membership under memory pressure

Status: implemented and locally accepted for the seven variants below. The
[acceptance report](../benchmarks/native-unary-membership-spill-2026-10-18.md)
retains complete values, source identities and failure history. Hosted, package,
other-platform and competitive acceptance are not claimed.
This finite RFC 0044 work unit belongs to PERF-06.membership/recovery/strategy,
with PERF-03 ownership and PERF-07/11/12 workflow/output obligations. Sampling
populations have a [separate finite contract](native-sampling-population-spill-2026-10-19.md);
subquery indexes and broader recovery remain open. CG-5/14/16/19/20/21
apply; CG-1 through CG-23 retain their independent evidence requirements. No
release or version change is included.

## Existing contract

At base `ca53145f`, relational unary execution uses the same `BoundUnary` selector
from native files, resident inputs and finite batch producers. The seven scoped
variants are distinct, deduplicate with first/last/all-duplicates policies, and
duplicate masks with those three policies. The existing pressure tests establish
resident key denial at 16 MiB and future-mask identity denial at 8 MiB even when
source replay spill is enabled. Their source is unchanged from the accepted
`7a55bc7f` native suite; unchanged baseline tests need not be rerun merely to
start this unit.

Unary keys use `NativeBatch::key`, including exact type tags, length framing,
nested values, null parents and floating bits. In particular, signed floating
zero is distinct here. Relational set equality is a different semantic contract
and must not replace unary key construction.

The predicate precedes key observation. Ordinals count passing rows. Distinct,
first deduplication and first duplicate masks stop evaluating keys once their
positive output limit is satisfied, while still draining the producer for
completion and validation. Last/all policies observe every passing key, including
the suffix beyond a mask output limit. Last deduplication evaluates and retains
each replacement payload; all-duplicates deduplication evaluates only the first
payload for a key and discards it on repetition. Those observation boundaries
must survive spill, including invalid values in unobserved suffixes/payloads.
Scalar floating payloads retained by the existing row owner can still be replaced,
removed by multiplicity, or excluded by the final limit. Private records preserve
their storage through compaction and ordering; the final selected public output
retains the existing finite-value check. Nested payloads keep the existing eager
selected-row validation, and skipped duplicate payloads are never selected.

Python DataFrame methods and their SQL table expressions lower to this shared
relational unary route. Ordinary SQL `SELECT DISTINCT` lowers to the existing
aggregate family. Legacy standalone `PreparedVortexUnary` retains its separate
resident-only contract; this change does not silently grant it spill permission.

The public index adds `unary_membership_spill` with the seven finite operations
and an explicit acceptance status. The existing broad `unary_state_spill` field
remains false and is identified as an all-family guarantee. Sampling populations
retained their separate false flag at this unit's acceptance; the later
[sampling acceptance](../benchmarks/native-sampling-population-spill-2026-10-19.md)
updates that scoped flag with explicit operation and row-bound restrictions.
Existing readers remain conservative, while readers of each scoped capability
can discover its implemented transition.

## Provider and state decision

Vortex-first classification: `implement_shardloom_kernel` for unary membership
semantics and observation order, using the already admitted pinned Vortex 0.85
array, dtype, validity, selection, serialization and file providers. RFC 0044,
the local provider inventory, existing unary selectors and the pivot replacement
store establish these boundaries. Vortex arrays/files do not supply this
query-local first/last/all policy, shared grant, observation contract or cleanup
certificate. No new format, dependency, external engine or fallback is needed.

Reuse the private chronological replacement-run provider currently serving
sparse pivots. Make its block-row size explicit while retaining the pivot's
1,024-row setting. Unary records use at most 256 rows, or a smaller requested
batch size, to bound simultaneous lookup, merge, identity and output owners.
The existing three-part private key remains exact: unary places its complete
encoded key in the index component, uses kind zero and an empty domain. This is
private operator state, not a general database, public key/value API or checkpoint.

One bounded ordered buffer holds the latest complete state per key: first/last
ordinal, exact count, and only the payload required by the resident policy.
Lookups check the buffer, then immutable runs newest first. Flush releases the
buffer before adjacent chronological compaction; newer complete state wins.
Keys, tree capacity, retained rows, bounds, lookup caches, merge input/output
and native codec/file work remain under the same execution memory owner.
The spill byte threshold controls batching, not a maximum supported record size:
one larger record must be admitted by the grant and flushed, or explicitly denied.

First-occurrence variants emit online, preserving prefix completion. Future
deduplication orders only final surviving payloads by their selected source
ordinal using the existing native ordering provider. Future masks record only
the requested prefix's exact keys and ordinals in bounded native identity runs,
then look up final state after all keys have been observed. A hot key therefore
does not require an unbounded identity vector or a retained full input payload.
Small state remains resident; permission selects a pressure-capable strategy,
while actual run counters separately establish file activity.
The legacy logical state-item counter reports peak buffered membership entries
plus pending mask identities for this strategy. It is not a count of every
temporary array, run, cache entry or allocation. The shared memory report remains
the reservation evidence; total observed keys, groups, mask identities, output
rows and lookup activity have separate strategy counters.

All producers run once per admitted execution. Every successful report requires
producer completion and final consumer completion. Early abandonment remains
provisional, and retained public arrays keep their own credits after temporary
state is released. The existing query store owns byte quotas, generation checks,
cancellation and exact cleanup. Crash handling recovers owned abandoned files
then starts a fresh execution; it does not resume an interrupted query.

## Acceptance and review

The expert comparator is a native encoded-columnar maintainer reviewing exact
observation semantics, bounded overlap, skew progress and failure ownership.
This is required capability work, with no performance superiority claim.

- Complete all seven variants beyond their original failing grants, comparing
  every value, type, null and defined source order. Preserve resident denial
  without permission. Sampling-population denial with permission was a control
  at this unit's original acceptance; current sampling pressure follows its
  separate contract above.
- Cover empty/all-null, unique/skewed/duplicate-heavy, nested/composite,
  decimal/binary/temporal and signed-zero keys; small resident controls, positive
  limits, predicates, ignored invalid suffixes/payloads and large single records.
- Exercise late producer/consumer errors, cancellation, zero-limit composition,
  corrupt/truncated/replaced runs, cached hits and gaps, merge generations,
  quota/admission failures, abandoned-process cleanup and fresh restart.
- Prove public SQL/DataFrame/direct-binding convergence, one producer execution,
  repeated-source replay, retained results, all eight write/reopen destinations
  and fanout, preserving existing targets and unknown files.
- Run focused unary and pivot regressions, native/provider/CLI and mandatory
  workspace formatting, strict lint and tests. At the cohesive acceptance
  boundary retain one complete Full43 engine observation and the format matrix,
  guarded in local-only storage with immutable source/build/result identities.

The main risks are changed observation semantics, an uncredited key/payload
copy, reader/output overlap under the original grant, and damage to pivot's
existing replacement behavior. Dedicated regression controls must cover each.
PulseWeave scheduling and existing Capillary admission remain shared; this unit
does not introduce metadata proofs, distributed execution or a process-RSS cap.

Finite local acceptance preserves the original 16-MiB key and 8-MiB future-mask
pressure fixtures. The wider direct-binding producer/publication and public
destination fixtures use 32 MiB; independent direct native-file readback uses
128 MiB. Those separate grants do not change the operator-pressure evidence.
All 5,126 public cases, 274 independently reopened unary value archives,
complete Full43 results and 704 native format workflows pass. The evidence binds
the compiled source to the two subsequent harness corrections and retains the
separate final-verifier correction without changing runtime results. Broader
PERF-06 owners and CG-1 through CG-23 remain open.
