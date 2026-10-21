<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native structural provider acceptance

Status: six finite storage classes locally accepted at implementation commit
`89da798f949aaa83705491e1cf711e3e0d6ae9c5` on October 21, 2026.

Native Vortex layout and file construction now admits covered structural
storage before allocation and retains its credit with the actual shared owner.
The [design and compatibility mapping](native-structural-provider-ownership-2026-10-21.md)
define the six classes: child caches/classification, cumulative row offsets,
reader metadata vectors, segment specifications, file statistics tables, and
user metadata names/locators. Denial, partial construction, independent aliases
and final-owner release have dedicated regression coverage. The source also
corrects native segment slicing when a valid offset is less aligned than its
backing buffer.

This accepts the bounded implementation and its complete-result regressions.
It does not close all provider ownership, any PERF owner, CG-1 through CG-23,
or the separate release gates. No new execution dependency, fallback engine,
public format or whole-process memory ceiling is introduced.

## Source and package identity

The [portable evidence](../benchmarks/evidence/native-structural-provider-2026-10-21/README.md)
binds all 2,594 implementation, package and validation inputs to Git blobs at
the implementation commit. The tests ran against those files while the branch
was dirty. Their original base, working-tree state, input hashes, compiler
records, binaries and package identities remain in the records; committing the
same bytes does not relabel their build revisions.

The broad native/workspace gates precede two narrow acceptance corrections.
Their source bridges are checked independently:

- The derived-relational CLI cleanup test measures its legitimate retained
  metadata baseline in an independent successful session. A cold session still
  tests cancellation, consumer failure and changed generation; its retained
  credit, failed-execution counters and final teardown checks remain. Only that
  test and its helper/import changed after the broad native gates. The complete
  feature-enabled CLI suite and strict CLI lint then passed.
- Packaging initially rejected stale vendored modification descriptions. Only
  the `local_changes` strings for `vortex-file` and `vortex-layout` were aligned
  with reviewed provenance. Versions, dependencies, license texts and compiled
  code remained unchanged. The full isolated installed-package lifecycle and
  reference/provenance checks then passed.

Thirteen local compiler targets have recorded compiler dependency inputs.
The fresh CLI, shared library, isolated wheel and public-workflow package are
checked against the same implementation source. The installed suite uses the
repository tests directly, with no alternate fixture directory.

## Verification

| Boundary | Accepted evidence |
| --- | --- |
| Default workspace | `cargo fmt --all -- --check`, strict workspace Clippy and `cargo test --workspace --all-targets`; 3,235 tests across 107 targets, none ignored |
| Feature-enabled native | Strict native Clippy and release tests with `release-user-surfaces`; 3,176 pass across 13 targets, 31 existing ignored tests |
| Native CLI | Strict CLI Clippy and all 1,091 tests across six targets |
| Provider regressions | All 69 tests in the five new native modules are found individually in the passing native transcript |
| Vendored providers | 249 layout tests and 145 final file tests; applicable strict provider Clippy passes |
| Minimal feature surface | Minimal native compilation passes; default builds retain their existing feature boundary |
| Python | Core discovery finds 700 cases; unittest reports 559 run and 23 skip entries. All 168 release-script tests pass. Discovery, run and skip counters remain distinct |
| Installed package | All eight isolated lifecycle steps, including installation, clean installation and uninstall; all 118 binding tests pass with no failures, errors or skips |
| Public workflows | All 5,930 declared public cases pass on the fresh runtime and package |
| Full43 | All 43 complete result sets and schemas pass, including 381 result rows; no fallback markers |
| Format workflows | All 18 cohorts, containing 704 ShardLoom and 704 independent reference workflows, pass with complete native values and output readbacks |

Required source checks, guarded-process completion, result hashes, complete
values, schemas, native certificates and query timing fields are reopened by
the portable replay. External reference engines remain correctness/comparison
oracles; they do not evaluate ShardLoom residual execution.

The copied packet replays all 29,659 members successfully. Its 21 semantic
corruption cases rehash altered references and reject with the expected
diagnostics; the unchanged positive packet passes before and after them.

The packet retains the original fixture, lint, cleanup, notice and evidence
verification failures. It checks the corrected source boundaries rather than
erasing failures or weakening grants. New tests cover exact and one-byte-short
budgets, overflow before callbacks, malformed input, empty/wide/nested shapes,
competing owners, denial/retry, incremental metadata parsing and retained
aliases after outer file/session teardown.

## Full43 observations

The final isolated pass uses the unchanged candidate executable and immutable
100-million-row Vortex input, 24 GiB declared memory and twelve workers. All
43 queries execute exactly once and total **49.990920425 engine seconds**.
The maintainer accepted this result and directed continuation of the PR.

The earlier complete pass measured 112.675662535 engine seconds. Subsequent
diagnosis retained all twenty targeted calls and both complete Full43 passes:
58.538535004 engine seconds for the candidate and 99.840589744 for the control.
These observations remain separately identified. A targeted five-query group
is never presented as Full43, and no observation is dropped or substituted.

For the final pass, other task work was idle. Full input hashing and recursive
build/evidence inventories ran outside query execution. The standard runner's
storage checks, lightweight concurrency guard, deadlines, complete-result
validation and cleanup remained active. The
[isolated single-pass procedure](../skills/benchmarking.md#isolated-full43-procedure)
now records this standing practice for future routine runs.

The variation does not establish a code speed gain or prove which environmental
or supervision factor caused it. The final result resolves the immediate
slowdown concern for this accepted batch; it is not a new comparative
performance claim. The complete diagnostic records retain the other clocks
and resource observations without substituting them for engine time.

## Queue and remaining boundaries

The [phase plan](phased-execution-plan.md#planned) now derives required work from
concrete comparison workloads and existing declared guarantees. Its 58 child
IDs and fourteen canonical owners are preserved: fifteen scoped required
contracts, five scheduled prerequisite/investigation rows, and 38 optional
rows at the bottom. Those counts describe placement, not fifteen new projects
or completed gates. Existing valid evidence may satisfy required contracts.

After this PR, `PERF-12.comparisons` should identify actual workload gaps before
new implementation. The already requested shared prerequisites and four
investigations retain their gates. Optional breadth may never be implemented
and does not block acceptance of the selected required scope. The release
matrix and companion decisions use the same classification; historical
37-required/21-conditional records remain historical.

Schema strings/nested DTypes, read-context arrays, wider lookup/expression
caches, other codecs/I/O/writers and separately retained scalar payloads remain
outside the finite ownership claim. Hosted/cross-platform checks and package
publication are separate gates. No version change, release or publication is
performed by this acceptance.
