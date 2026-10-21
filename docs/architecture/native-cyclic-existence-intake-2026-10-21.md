<!-- SPDX-License-Identifier: Apache-2.0 -->

# Cyclic existence joins: prospective intake and comparison contract

Status: prerequisite correctness and the ordinary-path opportunity pass; all
four targets meet the frozen materialized-work gate. A private candidate is now
implemented, with native/workspace validation and actual-work tests passing.
Fresh public build provenance and both scored comparisons are pending; the candidate is not
retained or released. This is the
authorized [follow-up](native-investigation-followups-2026-10-20.md#cyclic-existence-joins)
under `PERF-10.multiway_joins`, RFC 0044 and PERF-01/02/03/10/12. The current
implementation phase, CG-1 through CG-23 and 37 required breadth substeps retain
their separate obligations. No new roadmap ID or competitive claim is added.

## Contract and strongest existing comparator

An R(a,b) row qualifies if at least one S(b,c), T(a,c) pair satisfies all three
equalities. Use the existing public typed declaration
`R LEFT SEMI JOIN (S INNER JOIN T ON S.c=T.c)` with outer keys
`R.b=S.b AND R.a=T.a`. The inner projection contains only S.b and T.a.
Emit each qualifying R source ordinal once, preserving duplicate R rows and
source order. NULL equality never qualifies. Output aliases are
`outer_id,b,a`; a wide case adds eight R payload fields. This is an explicit
semi-join declaration, not a claim of newly supported SQL EXISTS lowering.

The ordinary binary semi-join already stops at its first matching right row.
The additional opportunity is avoiding construction of the complete S/T right
child. `local_primitive_prepared_relational.rs` currently builds that child
before `native_relational_join.rs` probes R. A comparison must use this
left-only semi-join with its narrow inner projection, rather than a full product
followed by a distinct/filter operation.

The new Rust regressions prove both outer-key orders, aliases, duplicate rows,
NULLs, empty inputs, 1/2/2048-row output boundaries, repeated collection,
cancellation, failed consumers, denied credits and recovery. Fresh native tests
pass. Public typed-plan regressions also pass against the retained accepted
library; that historical-library check is not a current-source package build.
The fresh current-source public library is required for observation and scoring.

## Provider and ownership decision

Vortex 0.85.0 supplies the array and gather boundary. The bounded inventory of
the pinned `vortex-array`, `vortex-layout`, `vortex-file`, `vortex-compute` and
`vortex-scan` source surfaces finds no relational cyclic-existence operator.
Its structural array hash is not a row-key equality index. This is a scoped
provider finding, not a claim about every upstream crate or version.

Conditional classification: `implement_shardloom_kernel`. Reuse native
`Batch`/`Table`, original-width `KeyColumn` equality/hash helpers, credited
`RowIndex`/`ReservedVec`, and native `Table::gather`/Vortex take. Do not introduce
an Arrow execution substrate, external engine, dependency or source API.
The ordinary binary path remains available for plans outside finite admission.
Unsupported execution still fails explicitly without external fallback.

If observation passes, the candidate may index T by (a,c), index S by b, and
probe S candidates for each R row until an exact T witness exists. Gathering
only R preserves semi-join multiplicity. Index construction occurs on every
execution and is charged. The prepared object may reuse its existing immutable
source/binding owner, but receives no new cross-call index cache.

Finite automatic admission is limited to three pure immutable memory scans,
one inner equality, two outer equalities that connect R to opposite inner
inputs, distinct cyclic key columns, nullable or nonnullable Int64 keys, and
left-only output. Each source must have at least 128 rows. Empty/tiny inputs,
ordinary two-input semi/anti joins, full-product joins, noninteger keys,
additional predicates, transformed/file/stream sources and explicit spill keep
ordinary native execution. Observation/debug switches must not bypass admission.

## Independent workload and opportunity gate

The exact generators, typed plan builders, schemas and complete ordered
references are in [the workload](../../scripts/native_cyclic_existence_workload.py).
[Independent tests](../../scripts/test_native_cyclic_existence_workload.py) check
the references against literal relational equalities, target formulas and
original full-product controls. Python supplies fixtures and correctness
references only; every observed and scored query runs in the public native API.

All sources have nullable Int64 x/y and nonnullable Int64 id (source ordinal).
R maps x/y to a/b, S to b/c, T to a/c. Division below means floor division.
Every normal key is present unless a case states NULL substitution.

| Case | Source formulas and size | Role |
| --- | --- | --- |
| `abundant` | R/S/T each 4096: `(n%64,n/64)` | many complete witnesses |
| `rare` | same; T.x gains 1024 except where T.y=63 | only the final c value witnesses |
| `absent` | same; every T.x gains 1024 | no complete witness |
| `skew_wide` | R 4096: `(n%64,7)`; S 8192: `(7,n%64)`; T as abundant | hot b; eight R payloads `id*17-3*i` |
| `null_duplicates` | exact earlier `cyclic_duplicates` source formulas and NULL moduli | semi-join NULL/duplicate control |
| `semi_small` | R/S/T each 16: `(n%4,n/4)` | tiny semi-join control |
| `empty_inner` | abundant R/T; empty S with explicit schema | empty result control |
| `binary_semi` | R 8192: `(17*n-3,n)`; S 8192: `(n,19*n-5)` | ordinary two-input semi-join |
| `binary_anti` | same R; S.x=`2*n` | ordinary two-input anti-join |
| `star_control` | exact earlier 4096/64/64 star sources and full nine-field output | original failed-control shape |
| `binary_control` | binary-semi sources, full six-field inner output | original binary control |
| `cyclic_dense` | R/S/T each 1024: `(n%32,n/32)`; full nine-field product | original dense control |
| `cyclic_small` | R/S/T each 16: `(n%4,n/4)`; full nine-field product | original tiny control |

The [original intake](native-multiway-join-intake-2026-10-20.md#independent-finite-workload)
fixes the inherited formulas. Preserve the original star warning: final
integration failed cohort A while B passed; its cause was not isolated.
Narrower admission alone does not resolve that warning.

Freeze the workload manifest before observing the current native library.
For each of all thirteen cases, validate complete ordered values and schema,
no-fallback reports, repeated prepared use and zero credits after teardown.
For each seven nested-semi cases, additionally execute the narrow S/T child
and validate every ordered row against its independent reference. At least two
of the four targets must produce at least 65,536 actual child rows and at least
16 child rows per outer result row (denominator `max(outer_rows,1)`). Otherwise
record no materialized-work signal and stop this mechanism before implementation.
Passing establishes an avoidable-row opportunity, not time attribution or benefit.

## Prospective candidate comparison

Freeze the workload, mechanism, complete schedule, numerical gates and baseline
source/library before observation and candidate implementation. If the
opportunity gate passes, bind the candidate source/build/library and final
driver hashes before scored timing without changing that fixed design. No
favorable subset, threshold relaxation or replacement cohort is allowed. A
corrected harness must preserve its failed attempts and start a new prospective
protocol before any new scored comparison.

- Use independently freshly compiled ordinary and candidate public libraries,
  normal automatic dispatch, one native CPU and a 128-MiB session grant.
  Require real recompilation of affected packages and source equality around
  each build; a cached library or role-selecting timing switch is insufficient.
- Use all thirteen cases, prepared call counts 1 and 4, five samples per
  case/call-count/role, two complete cohorts: 520 workflows and 1,300 engine
  calls. Cohort A follows manifest case order, call counts 1 then 4; B reverses
  both. For each case/call-count/sample pair alternate ordinary-first and
  candidate-first using the parity of manifest case index + original call-count
  index + sample index + cohort index (zero-based). Each workflow gets a fresh child process,
  session and source construction. Repeated calls share only that workflow's
  prepared object. No preliminary timed warm-up is discarded.
- Score each case/call count by the median of its five workflow engine totals,
  where a total is the sum of `engine_execution_seconds`, converted from the
  public report's `native_query_execution_nanos` by the existing Full43 timing
  helper with its clock/boundary checks. Each of the
  four targets must improve at least 2% in both call-count cells in both cohorts.
  Every other cell must have candidate/ordinary ratio at most 1.05 in both
  engine time and complete workflow time. Every target must also meet the
  complete-workflow ratio of at most 1.05. No absolute-noise allowance applies.
- Complete workflow cost starts before native session/source creation and ends
  after result release, prepared release, source release, session close and
  zero-credit verification. Include typed-plan construction, preparation,
  indexes, every collect, public report/schema access and full row export.
  Fixture generation/import, independent comparisons/hashing, process startup,
  final evidence serialization and supervision are recorded outside that clock.
  Keep those diagnostic clocks distinct; routine comparisons report engine time.
- Validate every complete ordered output and exact schema after timing, retain
  every sample and native report, require finite nonnegative clocks and exact
  zero final credits. Any incomplete or disturbed cohort receives no retention
  credit. Run builds, tests and queries sequentially under the unchanged local
  storage, process-group and workload guards; no artificial pacing.

Before scored timing, test-only observation must count actual S visits, exact T
probes, completed-witness exits and R payload rows gathered. Verify abundant,
rare, absent and skew behavior and admitted NULL/duplicate cases. Observe all
credited owners, cancellation, consumer failure, allocation denial and final
drain. Counters must be absent from scored builds. Existing batch-boundary
tests must exercise admitted candidate shapes as well as ordinary execution.

Passing microbenchmarks only permits final integration acceptance. A retained
candidate also needs native/workspace/public/package verification and applicable
PERF-12 evidence with the final ordinary selection path. A failed comparison
removes the candidate and rechecks the restored engine. These are finite local
mechanisms; no broad multiway, Full43, distributed, release or superiority claim
follows from intake or a focused gain.

## Preserved intake-harness correction

The first intake attempt stopped on the first outer collection when the probe
looked for `engine_execution_seconds` directly in the public result report.
The report exposes `native_query_execution_nanos`; the existing
`query_timing_report.py` converts that field into seconds and validates its
engine clock and boundary. No runtime code or clock changed. Preserve the
failed receipt, initial probe and initial protocol. The corrected probe uses
that shared helper, records its enclosing duration as a public collect call,
and starts under a new frozen protocol with unchanged workloads, schedule,
admission and numerical gates. No scored comparison had begun.

The second attempt reached the resource check but compared the live prepared
object against its source-only credit total. Preparation owns credited bound
projection metadata, so its retained state is expected until release. The
corrected observation records separate source and prepared totals: result
release returns to the prepared total, plan release returns to the source total,
and final source/session teardown reaches zero. Preserve this failed attempt
and its exact probe/protocol as well. This corrects the observation's ownership
boundary; it neither changes native ownership nor permits a residual credit leak.

## Completed ordinary-path observation

The [portable evidence](../benchmarks/evidence/cyclic-existence-2026-10-21/README.md)
replays against 1,969 frozen native inputs and 2,063 native/Python inputs.
The fresh current-source public library passes all thirteen cases: two prepared
outer calls per case and one additional narrow-child call for each nested semi
case, totaling 33 native calls. Every ordered value and exact schema matches,
no external engine or fallback is invoked, and every final credit total is zero.
The original full-product controls remain full products.

| Target | Actual S/T child rows | Actual outer rows | Opportunity gate |
| --- | ---: | ---: | --- |
| abundant | 262,144 | 4,096 | pass |
| rare | 262,144 | 4,096 | pass |
| absent | 262,144 | 0 | pass |
| skew_wide | 524,288 | 4,096 | pass |

All four targets exceed 65,536 actual child rows and the 16:1 ratio using the
frozen denominator floor. This proves avoidable intermediate-row work. It does
not assign exclusive engine cost to that work or establish candidate benefit.

The packet includes the initial native compile/clippy and public-schema test
corrections, both stopped intake probes above, and the successful checks. Its
offline verifier also rejects twelve corrupted value/schema/resource/clock or
prospective-protocol records. No failed attempt is scored or discarded.

Proceed with the finite candidate under the archived third protocol and its
unchanged 520-workflow comparison schedule. Retention still requires both full
cohorts, every control, actual candidate-work counters, failure/ownership tests,
and final public integration acceptance. The broader owner remains open.

## Private implementation and actual work

The experimental `local_primitive_relational_cyclic_exists.rs` module borrows
the already bound tree and keys. Admission allocates no new plan metadata.
It recognizes either outer-key order and either inner-projection order, checks
the distinct Int64 columns and pure memory sources, and runs only after explicit
spill has selected its existing strategy. The existing native path handles all
other shapes. Indexes and gathered payload remain credited execution-local owners;
the prepared plan does not retain a new physical cache.

Native `KeyColumn` hashes select candidate entries and exact original-width
equality proves each witness. Native Vortex gather preserves output aliases,
types and R ordinals. The implementation creates neither the S/T product nor
an Arrow execution representation. Test-only, thread-local counters and a
cancellation injection hook are compiled out of ordinary release builds.

All eight candidate tests and the three prerequisite tests pass, including two
complete calls for each of the four frozen targets. Each call rebuilds all S/T
index rows. Actual work is:

| Target | S rows visited | T membership probes | Witness exits | R rows gathered |
| --- | ---: | ---: | ---: | ---: |
| abundant | 4,096 | 4,096 | 4,096 | 4,096 |
| rare | 262,144 | 262,144 | 4,096 | 4,096 |
| absent | 262,144 | 262,144 | 0 | 0 |
| skew_wide | 4,096 | 4,096 | 4,096 | 4,096 |

The tests cover admitted NULL/duplicate inputs, both alias/key orders,
1/2/2048-row boundaries, 2051 output rows, repeated collection, failed consumers,
cancellation within a long NULL candidate band, partial allocation denial and
recovery. All-null inputs, full-width integer values, nonnullable keys, text and
Boolean payloads, every source's size threshold, noninteger keys, extra
predicates, transformed sources and explicit spill have direct boundary checks.
Retained native output also remains valid after preparation is dropped; its
credits drain with its final owner.

The full feature-enabled native all-target suite passes 3,079 tests with no
failures and 31 existing ignored tests, including the eleven cyclic prerequisite
and candidate tests. The required default workspace suite passes 3,235 tests
with no failures or ignored tests. Formatting, strict workspace lint and strict
feature-enabled native lint also pass. These counters and tests prove actual
work, exactness and ownership; they are not timing or retention evidence.

One earlier denial-test assertion failed because the public completion boundary
reserves report metadata before entering the candidate. The final test sweeps
remaining grants, requires both an observed denial after candidate index work
has begun and a successful execution, and checks exact baseline restoration at
every grant. Preserve the failed assertion's source and receipt. This was a test
correction; no runtime allocation policy was changed to make it pass.

The [comparison runner](../../scripts/native_cyclic_existence_compare.py) has
seven passing adversarial tests for the complete schedule, all five median
samples, full values/schema/source identity, ownership, native clock provenance
and both inclusive numerical bounds. A missing/reordered workflow, one failed
control in either cohort, changed gate or invalid clock cannot become a retained
decision. Before scoring, a separate binding must identify the fresh committed
candidate build, unchanged source inputs, the ordinary intake build, validation
receipts and exact public Python/driver files. No scored comparison has begun.
