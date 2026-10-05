<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native nested keys and retained state acceptance — October 4, 2026

Local acceptance passes on frozen source `d65907f6`. All public, direct-unary,
required local and paired regression checks pass; independent packet inspection
also passes. Hosted acceptance remains open.

This unit extends the existing native key and state owners to static lists,
fixed-size lists and structs. SQL, Python/DataFrame composition and direct native
calls share those owners and the existing local writers. The
[implementation contract](../architecture/native-nested-keys-state-2026-10-04.md)
defines the exact type, null, ownership and failure boundaries. Broader
aggregate/window, pivot, adapter and resource work remains open.

## Frozen identity and scope

| Evidence | Scope |
| --- | --- |
| Candidate source | `d65907f650a69aac544cdcfc1f0fe14305e11202` |
| Candidate executable SHA-256 | `6ef79dd3690fff820b428f7e5a80452d6db9ec209e3ed111874cedc34f68bdc8` |
| Paired control | `948551d476855682e7693973bcf2ef00260d9fd2`, the locally accepted typed-unary build |
| Control executable SHA-256 | `388aeac40e07fdd1cf9b29df4289897a26b6e26ba436bcfecd800bd920caea47` |
| Build | Rust 1.99.0, release profile, `release-user-surfaces`, identical Cargo lockfile |
| Source identity | Clean committed candidate; 837 source fingerprints; CLI and both fixture executables rebuilt and hashed |
| Declared complete public scope | 17,458 checks; 14,143,015 complete row comparisons |
| Added nested-key/state scope | 8,162 checks; 17,270 complete row comparisons; 406 independently declared workflows |
| Separate direct-unary scope | 202 checks; 131,734 complete row comparisons |

The new scope retains all 9,296 unaffected prior cases and their complete row
counts. Four previous blanket nested-key denials become positive SQL order, set,
group and COUNT DISTINCT checks. The scope also retains explicit structured SUM
and text-operation denials. No expected value is derived from the candidate.
Native composed, declared-Arrow composed and direct native routes use equivalent
SQL and DataFrame spellings, collection at parallelism one/two, and every
representable writer/readback path. Scope and complete oracles are frozen before
the public run.

## Semantics and ownership

Recursive logical equality, hashing and ordering now serve existing joins,
sets, groups, windows and membership. Lists compare lexicographically; struct
fields retain declared order. NULL parents hide their children. Child NULLs,
empty lists and NULL lists remain distinct as required by the bound operation.
Nested shape, field names, fixed widths and leaf metadata remain significant.
Key compatibility ignores recursive nullability; selected output and set
branches keep the stricter existing common-type contract, with exact child types
and only root-nullability promotion. Relational floating signed zero is normalized;
unary identity preserves exact floating bits.

COUNT uses parent validity. COUNT DISTINCT reuses the exact native row set.
MIN/MAX retain compact selected native values. CASE, COALESCE, NULLIF, comparison
and NULL tests preserve native nested results and lazy selection. Tail, sampling,
deduplication, selected rewrites, forward fill and melt use existing state owners
and native batches. Structured arithmetic, text kernels, incompatible melt
domains and unsupported schema shapes fail explicitly at binding, including on
empty sources. Dynamic nested pivot domains remain outside this contract.

Tests cover dictionaries, chunks, fixed-size lists, overlapping list coordinates,
null/empty inputs, selected-buffer lifetime, replacement overlap, cancellation,
consumer failure, source invalidation and native sort spill/merge. A two-MiB
unused child domain is released instead of being retained for a selected value.
Validity-only operations also pass a hidden million-child fixture under a
one-MiB grant. These tests prove the stated reservation and ownership boundaries;
they do not account for every provider allocation or enforce process RSS.

Nested results use native Vortex, Parquet, Arrow IPC, Avro, JSON and JSONL.
CSV and ORC nested output remain explicit denials before publication. Scalar
counts and masks exercise all eight local writers. Complete values and column
order are checked independently against the frozen oracles, and columnar output
also retains logical schema proof. Public resource evidence must identify the
correct route, source generation and peak reservation under the unchanged
one-GiB grant.

All 25 local gate categories pass on the frozen source. Default workspace
tests report 3,470 passed; native Vortex reports 2,358 passed with 23 existing
ignored tests; native CLI reports 1,599 passed. Python runs 875 tests with 144
skips. The UAT harness runs 19 tests; its consumer suite runs 42 tests with one
optional fixture skip. Counts overlap across build configurations. Formatting,
strict default/native lint, lean/no-write builds, Rust 1.96 compatibility and
the affected website/reference gates pass. Final documentation checks are
recorded separately.

## Paired Full43 regression

All 258 initial complete-result comparisons and six prescribed Q28 repeat
comparisons pass. Neither aggregate timing screen nor any RSS screen is crossed.

| Initial observation | Control | Candidate | Candidate change |
| --- | ---: | ---: | ---: |
| Sum of fastest valid query calls | 52.175276 s | 52.139135 s | -0.0693% |
| Sum of query medians | 53.790662 s | 53.186861 s | -1.1225% |
| Sum of all 129 native process calls per role | 161.548155 s | 160.761734 s | -0.4868% |
| Maximum observed process RSS | 5,623,808,000 B | 5,631,066,112 B | +7,258,112 B |

Q28 is the only initial timing flag: its median changes from 2.389302 to
2.101074 seconds (-12.0633%, -0.288228 seconds). The required reversed-order
cohort changes from 2.081453 to 2.092486 seconds (+0.5301%, +0.011033 seconds).
Repeat median RSS is 417,923,072 versus 417,873,920 bytes. Neither repeat screen
is crossed; the initial gain does not reproduce. Both cohorts remain retained.
Local availability is accepted within this unit's correctness and resource
contract; no general or causal speedup is claimed.

The supervised initial workflow takes 401.856823 seconds and the repeat takes
15.284389 seconds. These clocks include runner setup, result verification and
archiving, whereas the native process sums above include complete CLI startup,
execution, output and exit. The separately admitted preflight hashes the resident
source before those timed workflows. The public correctness workflow takes
2,262.221522 seconds and the direct-unary workflow takes 3.116991 seconds.
Observed one-minute host load ranges from 1.941895 to 6.788086 during the initial
cohort and from 3.328613 to 3.752441 during the repeat. These are context, not
proof of exclusive host use or a causal explanation for a timing difference.

Both roles read the same resident 99,997,497-row Vortex source and compare all
43 complete retained results, with three calls per query and role. These are
retained-result regression oracles, separate from the new independent nested
oracles. Source length is 15,682,956,489 bytes and SHA-256 is
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
No ingestion timing or external-engine comparison is included.

The macOS 27.0 arm64 host has 16 GiB physical RAM and 10 logical CPUs. Each
role uses parallelism 12 and a 24-GiB admission grant, which is not an enforced
RSS limit. Execution is sequential under the existing storage, deadline,
overlap and cleanup guards. Source hashing warms bytes; operating-system cache
and ordinary host activity remain uncontrolled.

Investigation thresholds are symmetric and frozen before the run: per-query
median timing changes of at least 10% and 0.1 seconds, median RSS changes of at
least 10% and 32 MiB, or aggregate fastest-call/median-sum changes of at least
5% and one second. Flagged queries repeat with reversed role order; an aggregate
flag requires all 43 queries to repeat. Availability acceptance does not require
or imply a speedup.

## Retained failures and evidence

Public attempt 3 stops after 4,523 passing checks and 3,756,979 complete row
comparisons at a direct nested group count. The metadata strategy selector
omits referenced nested types and chooses a flat result owner. The correction
selects the shared relational strategy before execution and reuses the admitted
source generation. Unreferenced nested fields and output aliases do not change
strategy. Tests pin source-open counts, retained execution and invalidation.

Attempt 4 stops after 4,543 passing checks and 3,757,043 row comparisons at
the empty direct group-count route. The source declarations match, but the
syntactic binding parser rejects zero limits and offsets. It now admits strict
nonnegative literals, and `LIMIT 0` selects the native relational limit operator.
This prevents metadata count or positive-limit primitives from omitting the
empty result. SQL/DataFrame regression tests cover counts, grouping, filtering,
sorting, zero offsets and worker reuse.

Attempt 5 stops after 5,403 passing checks and 3,758,631 row comparisons at
a direct nested group count over explicitly declared Arrow input. The query
refers to the prepared Vortex file while its source declaration still names
the Arrow file. The handoff now rebinds declarations to the prepared path and
authoritative schema, including native empty results. Retained reuse compares
the normalized request and preparation identities; each call still validates
the original and prepared generations, and failure releases the retained session.
Shared SQL writers carry original source identities through final commit and
alias checks. Regression tests cover repeated SQL/DataFrame calls, JSONL
readback, source mutation and explicit repreparation, direct native versus
original-source identity, and all six nested destinations rejecting the input.
The subsequent full CLI suite exposed forced general dispatch for prepared flat
writers. The correction retains preparation provenance on the existing native
source owner, preserving the optimized writer and its public route. Source-owned
and plan-owned preparations now pass source-change rejection during consumption
and just before commit across all eight local sinks. Metadata credits remain
held until the last clone; tests also prove alias rejection, attachment before
sharing, capacity/grant denial and direct persisted-Vortex independence.
Attempt 6 stops at the unchanged 192-MiB log ceiling after 13,717 passing checks
and 8,628,943 row comparisons, with all 8,162 new checks already passed. It has
a harness storage error rather than a query diagnostic. The archive helper now
compresses original JSON reports together, retaining raw hashes and the verified
source gzip identities/hashes. Every raw byte is verified before removal of the
temporary gzip wrapper. A guarded 384-report check reduces archive bytes from
2,265,960 to 203,656 without changing the original failed-run evidence or guards.
The three query-failure summaries and this storage-failure summary, their raw
diagnostics or final successful envelope, supervisor receipts and logs remain
intact and are excluded from completed acceptance. Earlier declaration, build
and focused regression attempts are retained separately.

The [portable evidence packet](evidence/native-nested-keys-state-2026-10-04.json.xz)
contains the frozen source/build identities, independent declarations/oracles,
complete public/direct results, original envelopes and persisted-output hashes,
all 264 paired/repeat results, retained failures, local gate logs and executable
verification tools. It retains 36,316 public envelopes, 3,915 supplemental
envelopes, and 6,842 new resource proofs with matching complete-value proofs.
Both finalization and independent inspection reopen the original archives,
verify their members and persisted outputs, compare every complete result, and
check 349,985 report field sets for unique names. The independent inspector
also verifies all 837 frozen source fingerprints.

The packet is 29,722,740 compressed bytes with SHA-256
`46256e87eebb0051b14125054e13927cc0ff5368863678cf940b06ecaedf187a`.
Its 2,318,293,529 uncompressed bytes have SHA-256
`3ec6a78864b2810768e32b8d7a3ccd6fe2da2b285bb5a54330395cf05de29c61`.
Local paths use placeholders. Replay must bind them to retained or regenerated
inputs and a new output identity; historical completed paths must not be reused.

Hosted gates remain pending the inherited website advisory decision. The npm
registry published `http-cache-semantics` 4.3.0 on October 4, but a fresh local
comparison of 4.2.0 and the integrity-verified 4.3.0 archive reproduces the same
security-zeroed cache reuse under `max-stale`; the benign public-cache control
also passes. This is not a verified remediation for
[GHSA-ch52-4w7c-c8xp](https://github.com/advisories/GHSA-ch52-4w7c-c8xp).
The dependency and disabled exception remain unchanged, and the packet preserves
the metadata, source identities and probe. The source
version is 0.4.0; this work does not publish a package, tag or release. Broader
pivot/aggregate/window semantics, adapters, shared scratch accounting and general
state spill/recovery remain open. No fallback execution or competitive
superiority is claimed.

## October 4 website dependency follow-up

The [dependency update](../dependencies/website-build-dependency-review.md#2026-10-04-registry-update)
selects `http-cache-semantics` 4.3.0 and passes the standard dependency audit.
The unused exception proposal is removed. Earlier website-blocker statements
in this report describe its original frozen revision; they no longer identify
the current dependency posture. Hosted runtime review and checks remain separate.
The recorded benchmark results, source identities and immutable packets are unchanged.
