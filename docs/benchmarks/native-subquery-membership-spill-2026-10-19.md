<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native subquery predicate pressure acceptance

Local source acceptance passed on October 19, 2026. Shared relational `IN`,
`EXISTS` and quantified `ANY`/`ALL` predicates now use explicit native query spill
for their required membership and correlation keys. Negation, indexed correlation,
parameterized execution and declared parameter-specific inner schemas preserve
their existing semantics. This is unreleased development capability on the
`vortex-write` Unix boundary. Hosted, installed-package and other-platform proof
remain separate; published v0.6.0 is unchanged.

The [implementation contract](../architecture/native-subquery-membership-spill-2026-10-19.md),
[evidence index](evidence/native-subquery-membership-spill-2026-10-19.json) and
[member manifest](evidence/native-subquery-membership-spill-2026-10-19.manifest.json.xz)
bind exact sources, compiled artifacts, complete values and failed observations.
The packet reopens every archived member against its size and SHA-256. Executables
and bulk generated outputs remain local; their identities, complete result values
and regeneration sources are retained.

Only this finite scope within `PERF-06.membership`, `PERF-06.recovery` and
`PERF-06.strategy` is accepted. Scalar subqueries retain their zero/one-row resident
state and cardinality error. Opaque undeclared inner sources and dynamic scalar
schema inference remain unsupported. Other sampling strategies, legacy standalone
unary state and broader recovery retain their existing boundaries. All 14 roadmap
owners and 58 substeps (37 required and 21 conditional) remain open, with CG-1
through CG-23 retaining their independent evidence requirements.

## Execution and ownership

The implementation uses pinned Vortex 0.85 arrays, DTypes, validity, native writers
and readers through the existing ShardLoom query-run store and typed ordering
owners. It adds no dependency, physical format or external execution provider.
Private records retain required native keys, correlation and complete-key hashes,
and full source ordinals. Hash lookup narrows candidates; full native keys establish
equality. Nullable row comparisons preserve the rule that a false component
dominates an unknown component. Empty `ALL` is true, other empty predicates are
false, and negation applies to the final nullable result.

Ordinary predicates build eagerly; guarded predicates build only when first
selected. Selected builds validate every inner key before skipping null correlation
prefixes or taking a predicate short circuit. Selected outer keys are validated
before empty-group handling. Existing scalar demand, unused-producer completion,
typed/nested validity and outer output order remain intact.

Directly bound indexed correlation builds one inner population. Public SQL/Python
outer references currently lower to parameterized execution, building a fresh inner
relation for each selected outer row. Completed per-parameter runs are released
before the next parameter. These strategies have distinct build/replay counts.

Predicate state, repeated-source replay and inner pivot/order state remain separate
owners. `relational_spillable_subquery_stages` records strategy selection;
`relational_spilled_subquery_stages` records completed predicate populations backed
by actual native runs. Small projected key sets may remain resident. In particular,
the public cardinality-only EXISTS fixture uses resident predicate state while its
source replay spills; that replay is not counted as predicate spill.

Explicit spill uses source-replay and predicate blocks of at most 64 rows, with
retained sorting thresholds capped by the smaller of the authorized buffer and one
eighth of the shared grant. Private key construction detaches required keys directly
without first duplicating the full source row. The shared merge cursor releases its
consumed block before the next refill when gathering a full output block. This
limits overlap among source data, retained keys, merge readers and the native push
writer's retained output. Original grants and record-size admission are unchanged;
actual reservations remain authoritative.

A single bounded lookup block validates the run generation on cache hits as well
as reads and finalization. Metadata, payloads, readers, ordering, merge overlap and
outputs share the admitted memory owner and disk quota. Managed reservations do
not establish a whole-process RSS ceiling or guarantee arbitrary row widths fit.

## Pressure, output and failure boundaries

The original 8,193-row native pressure fixtures complete at their unchanged 4-MiB
grant with explicit spill. The public wide-key mirror retains its 8-MiB grant.
No-spill controls still deny resident growth before eager outer-source demand.
Additional native wide-key fixtures force actual predicate runs and merges across
all six declared predicate/correlation forms, including a hot correlation group.

| Boundary | Grant |
| --- | --- |
| Original native predicate pressure and native wide-key controls | 4 MiB |
| Original public predicate pressure, direct binding and publication | 8 MiB |
| Deferred public pivot/replay compositions | 32 MiB |
| Independent direct native-file readback | 128 MiB |

The deferred composition grant is its established inner-owner requirement. Both
the prior sampling binary and this implementation reject that fixture at 8 MiB
and complete it at 32 MiB. This is separate from the predicate-pressure controls;
writer/readback grants do not replace those controls.

Native tests cover exact mixed numeric identity, dictionary/binary/decimal/temporal
and nested keys, null parents, deliberate hash collisions, empty groups, late invalid
keys, guarded demand, scalar cardinality and repeated execution. Failure coverage
includes quota/grant denial, producer/schema/consumer faults, cancellation,
corrupt/truncated/replaced runs, cached access and both merge inputs. Actual child
process abandonment preserves live, foreign and unknown owners, removes only proven
owned state and permits a fresh execution. This proves cleanup and restart, not
interrupted-query resume.

All eight local writers and fanout pass complete-value readback. The 24 public
failure/preservation cases comprise 18 incomplete-execution faults and six existing
destination refusals before producer demand. Each checks a fresh successful
execution afterward. Existing-target refusal is not proof of conditional replacement
or multi-destination atomicity. Early consumer closure stays provisional; successful
completion requires all input, output and owned cleanup checks.

## Verification

| Check | Result |
| --- | --- |
| Required native predicate-state selectors | 14 pass, including two rewritten pressure controls |
| Complete native subquery regression selectors | 60 pass |
| Native Vortex library, release features | 3,022 pass; 29 existing opt-in screens ignored |
| Default workspace tests | 3,235 pass; zero ignored |
| Selected native CLI suites | 1,087 pass; zero ignored |
| Formatting and strict workspace/native-feature lint | Pass |
| Required direct native binding tests | 105 pass; zero skipped |
| New direct-binding predicate matrix | 48 subcases pass within one test |
| Public context tests | 17 pass; one installed-package check skipped |
| Public reference tests | 6 pass |
| Public input/operator union | 5,930 cases across 39 families pass |
| Subquery complete-value archives | 483 archives; all 1,701 rows independently reopened |
| Full43 regression | 43 queries once each; all 381 result rows and schemas match |
| Modular workflows | 18 cohorts; 704 native and 704 fresh independent pandas oracle workflows match |

Suites overlap; their counts are not additive. The public union adds 438 cases to
the preceding 5,492-case union. Two prior pressure refusals are replaced by enabled
and disabled controls without changing that family's case count. The focused public
preflight passes 445 cases: the 438 new cases and seven retained pressure cases.
Independent verification checks all 16,389 compressed JSON members, 8,151 no-fallback
envelopes and frozen source snapshots. It also reopens the prior 406 sampling, 274
unary membership and 74 distinct-set complete-value archives.

The modular matrix covers collection from eight input formats and eight output
formats from CSV, in raw and prepared states, over all 22 declared workflows.
It is not a complete input/output Cartesian product. Pandas runs only as an
independent comparison oracle.

Full43 engine time is **51.811495794 seconds**, summed from
`engine_execution_seconds` for one execution of each query. This is one regression
observation against a retained native reference, not a fresh independent Full43
oracle or evidence of speed improvement or superiority. The unchanged input is
`fresh-roadmap-663-a1.vortex`, 15,713,610,513 bytes, SHA-256
`06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`.
It was not reingested. Full pre/post hashing and generation checks pass outside
query timing. Cache state is uncontrolled and preflight hashing may warm it.
Per-query engine clocks and supervision diagnostics remain in the packet.

The context skip is
`test_process_result_chaining_preserves_empty_nested_nullable_and_exact_types`.
It requires a separately selected clean installed package, which was not enabled.
Source acceptance does not replace that installed-package proof.

## Source identity and preserved corrections

Acceptance and both compiled artifacts use frozen source
`a61088426b31b8bf67c840d10f9c6f5905d42c43`, following base
`2da183da41c3ef43aeca9d6fc9fb101b93768e51`. The CLI SHA-256 is
`077541153ab445db522d6b286a00e43099644cbbc969be486d47cdc08668f01e`;
the native binding SHA-256 is
`c44c10f6811b67974627ff1dfc0977716ca254c7ca83f465401c95406c4512ff`.
All 2,208 frozen source entries, the plan and its five verification helpers are
checked through acceptance. The later documentation closeout changes only
`scripts/check_user_surface_reference.py` among those entries to validate the newly
accepted scoped claim. Its explicit bridge records old/new hashes; all 2,207 other
entries, including runtime, Python and harness sources, remain byte-identical.

Development pressure failures exposed overlap among replay, retained inner state,
merge prefetch and writer buffers. Their allocation traces and attempted repairs
remain preserved. The retained implementation fixes ownership and block geometry
and passes the original grant controls. Strict-lint/style failures are also retained.

The direct-binding preflight initially assumed indexed correlation for public outer
references. Corrected expectations follow the existing parameterized lowering, and
the distinct actual-predicate-spill counter prevents replay from being mistaken for
predicate spill. The deferred public fixture's incorrect 8-MiB test grant was
corrected only after the old/new 8/32-MiB comparison confirmed its established
32-MiB requirement. An existing-destination expectation was corrected to the actual
admission diagnostic. A preflight verifier's missing retained-family lookup was
corrected against the exact declared case set. Every failed observation remains
in the packet; none is represented as a successful pressure result.

The frozen build, binding, public, Full43, modular and final acceptance gates all
pass without an intervening source or verifier repair. Reproduction uses the
archived source/build manifests, frozen plan, guarded commands, independent oracles
and verifiers in a fresh local-only directory. Required broad gates are
`cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
and `cargo test --workspace --all-targets`. Native checks use the locked, offline
`release-user-surfaces` build; exact selectors and Python commands are retained.
Historical output paths are immutable and must be rebound for another run.
Storage and concurrency limits were not raised. No broader PERF owner, release,
publication or competitive gate is completed by this finite acceptance.
