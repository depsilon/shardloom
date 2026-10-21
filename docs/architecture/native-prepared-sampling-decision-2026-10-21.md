<!-- SPDX-License-Identifier: Apache-2.0 -->

# Prepared weighted sampling: finite retention decision

Status: finite implementation retained after both frozen comparisons, corrected
local acceptance and complete portable evidence replay on October 21, 2026.

The implementation retains scalar calibration metadata on one immutable prepared
weighted-replacement sample. Ordinary SQL, Python and DataFrame preparation use
the same native binding and execution path. This closes the additional finite
[sampling investigation](native-investigation-followups-2026-10-20.md#reusable-weighted-sampling-preparation)
under RFC 0044 and PERF-10/02/03/06, with PERF-12 acceptance. Broader index,
cache, resource, sampling and release owners remain open. No roadmap ID or
competitive gate is added or completed.

The [original protocol](native-prepared-sampling-2026-10-21.md) and
[public integration protocol](native-prepared-sampling-integration-2026-10-21.md)
remain unchanged historical records. The
[evidence packet](../benchmarks/evidence/prepared-sampling-2026-10-21/README.md)
retains both independent comparisons, corrected final acceptance, failures,
compiler inputs, full results and replay checks. Its source revisions identify
the measured implementations; the later documentation and test-fixture commit
does not relabel those measurements.

## Retained behavior and boundaries

The admitted operation is a bound root weighted-replacement sample directly
over immutable native memory, with Float64 weights, at least 4,096 population
rows and draws, and at least one draw per four source rows. It has no actual
intervening operator, source limit or residual filter. A complete ordered
identity projection is removed only after ordinary binding validates names,
expressions, types and uniqueness. Actual renames, reordering, subsets, computed
expressions and untyped-NULL coercions remain distinct operations.

One prepared object owns at most 128 additional logical metadata credits for
its shell and optional scalar model. Every query rebuilds its ordinary credited
population, performs the actual seeded draws and delivers complete native
output. No population, second CDF, source pointer or answer is cached. The
immutable bound source and plan establish identity; equal row counts or total
weights alone do not. Independently prepared objects cannot share the model.

The exact-correction search preserves the frozen arithmetic and ordinary
lower-bound semantics, including rounded duplicates and clamping. A successful
first execution can publish a calibrated radius or a calibration-refused
sentinel. Later calls reuse that scalar record. Source/plan changes, allocation
denial, cancellation, consumer errors, final collection or writer failure,
unwind and reentrant execution cannot publish invalid retained state. Local
pressure below 1 MiB of free shared credit evicts the model and bypasses it for
that call; later admitted work can recalibrate. Teardown releases the final
owner credits.

File, batch/streaming, dynamic, parameterized, child-sample, filtered, limited,
transformed and explicit-spill plans retain their existing behavior and
exclusions. This is not a pool-wide cache or new pressure/spill strategy. Vortex
0.85.0 remains the native provider; the bounded scalar helper is original
ShardLoom code. No dependency or external execution provider is added. Native
certificates preserve `fallback_attempted=false` and
`external_engine_invoked=false`; logical credits are not a process-RSS ceiling.

## Frozen comparison results

Each comparison retains 24 cells: eight distributions at one, four and sixteen
calls on a freshly prepared object. Each of cohorts A and B contains one warmup
and seven scored samples per cell and role, in the predeclared alternating
orders. Each comparison includes all 768 measured workflows and 5,376 engine
calls, plus 48 observed preflight workflows and 336 calls. The scored subset is
672 workflows and 4,704 calls per comparison. Complete values, schemas, source
identity, seeded order, activation counters and zero final credits all pass.

Both the original Rust-preparation comparison and the public integration
comparison pass every conjunctive target, control and ownership gate. In the
integration comparison, repeated targets use 8.35–13.71% less engine time than
their matched ordinary baseline:

| Distribution | Calls per prepared object | Cohort A engine ratio | Cohort B engine ratio |
| --- | ---: | ---: | ---: |
| Uniform | 4 | 0.862915 | 0.908478 |
| Uniform | 16 | 0.916504 | 0.882014 |
| Periodic | 4 | 0.887562 | 0.893249 |
| Periodic | 16 | 0.895903 | 0.909832 |

Ratios are candidate/baseline medians of complete workflow engine totals. All
eight target observations satisfy the 0.98 limit. The largest non-target
engine ratio is 1.042152, within the unchanged 1.05 control bound. Single-call,
few-draw, tiny, unweighted, refused-model and rounded-tie controls remain in
the evidence. These finite results do not establish a break-even count outside
the measured one/four/sixteen-call cases or a general engine-speed claim.

The earlier per-execution
[weighted-search predictor](native-learned-index-decision-2026-10-20.md) remains
not retained. Its failed controls are preserved. Prepared scalar reuse is the
separately declared and measured mechanism, with the same applicable control
shapes still binding.

## Corrections and final integration

The first prepared candidate passed its Rust comparison but did not activate
through ordinary SQL because identity projections remained in the bound plan.
The integration correction removes only validated identity projections and
preserves an existing all-column scan during SQL demand pruning. A separate
complete comparison proves the corrected candidate; neither comparison is
substituted for the other.

The integration suite then exposed an obsolete cyclic-projection exclusion
expectation. The recorded test correction uses a real transformed projection
as the exclusion control. A later feature-matrix correction restores the
required compile-time gate and Python test discovery imports. The source
bridges retain the original failing suite and prove the precise changed inputs.
Final source `4cd3a9e125cad4bba082b6e80c7c600f71239891` passes the corrected gates;
the historical scored revision and its failure remain in the comparison packet.

The original installed-wheel proof also failed two subcases of an old
output-window test: removing an identity projection makes its single computed
projection eligible for the existing two-batch window. The corrected fixture
checks that admitted path and adds a real nested rename/computed projection
that remains ineligible. Both batch sizes, complete values, retained Arrow and
iterator aliases, and final credit release remain checked. All 118 original
test methods execute with no failures, errors or skips. The new proof uses the
same native CLI/library and package source; its explicit test-directory overlay
changes only that fixture. The exact tested fixture is subsequently applied to
the repository. The original failed proof is retained.

All eighteen original format cohorts completed, but their post-run dependency
check detected changed bytes in 44 pandas and 19 NumPy native-library files.
That attempt is not accepted as an intact matrix. Its results and before/after
inventories remain recorded; the cause of the external file change is not
claimed. The complete unchanged matrix is repeated against a private snapshot
of the pinned reference packages, with source-copy checks, isolated import
paths and package file hashes before and after execution. The new matrix uses
the same ShardLoom executable, workloads, resources and reference roles.

Portable replay also exposed two verifier mistakes: comparing a member identity
map with its count, and looking for a receipt field that the runner does not
write. The corrected verifier checks every comparison member and its count,
uses the recorded runner field, and replays all results. Both failed packaging
attempts and their original helpers remain in the packet. No engine run or
measurement is repeated for these verifier corrections.

## Acceptance and remaining work

Final local acceptance includes formatting, strict workspace and native/CLI
Clippy, all workspace targets, feature-enabled native and CLI suites, minimal
feature coverage, optional provider features, Python discovery and reference
checks. The packet records 3,235 passing workspace tests across 107 targets,
3,107 native tests with 31 existing ignored tests, and 1,091 CLI tests. The
installed package executes all 118 binding tests, including the three prepared
sampling lifecycle tests, through isolated install/load/integration/uninstall.

The public matrix passes all 5,930 cases in 39 families. Full43 executes every
query once, verifies all 381 result rows and schemas, and records
**49.077810546 seconds of engine time**. This is regression acceptance, not a
sampling speed comparison. The input is unchanged and is not reingested. The
format matrix covers 704 native workflows and 704 independent pandas-oracle
workflows across eighteen cohorts: raw/prepared collection from eight inputs,
plus raw/prepared CSV input into eight output formats. It does not claim the
complete Cartesian product of input and output formats.

The portable packet verifies 28,698 archived members, both comparison source
closures, the 3,172-file final source closure and all complete results. Its
34 deliberate corruption cases are rejected: ten result/process cases, eight
required-gate cases, seven wheel cases, five reference-environment cases and
four comparison-inventory/count cases. These checks execute no native query.

Five release-scope inspections retain their existing blockers. No package is
published, version bumped or release authorized. Hosted/cross-platform,
upgrade/rollback, broader sampling and resource recovery, and CG-1 through CG-23
remain separate obligations. With the finite investigation closed, the next
authorized work is the 37 required breadth substeps, starting with the concrete
PERF-03 ownership and bounded scheduling gaps in the authoritative
[phase plan](phased-execution-plan.md).
