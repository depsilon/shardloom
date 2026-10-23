# Shared native providers across public surfaces

Local acceptance is complete at runtime commit
`7c65f339473e1f937d08da4b33fb538c00c171f7`, following the initial provider change
`c70f03af`. CLI SQL, embedded workflow, Python SQL, DataFrame and typed native
expressions each pass all 43 queries against the same independent reference.
The broader conformance work covers current runtime families beyond ClickBench.
This is source acceptance; published v0.7.0 artifacts are unchanged.

The [implementation contract](../architecture/shared-native-provider-coverage-2026-10-23.md),
[coverage map](evidence/native-shared-surfaces-2026-10-23/family-map.json) and
[acceptance manifest](evidence/native-shared-surfaces-2026-10-23/review.json)
define the scope and exact evidence identities. The current implementation is
the RFC 0044 native execution program, with RFC 0021/0022 expression and plan
contracts. No PERF owner, CG-1 through CG-23 gate or optional breadth item closes.

## What changed

Equivalent typed plans now select the existing native count, aggregate and
bounded sorting providers after complete binding and source/policy admission.
Column-only projections and infallible filters also reach the existing Vortex
scan through alias views. Computed expressions, ranges, residual predicates,
NULL comparison literals, nonfinite-capable float domains and narrowing integer
comparisons preserve their ordinary typed semantics. Lowering reserves its
metadata before allocation and retains source generations, resource ownership
and terminal error behavior.

SQL collection reuse now follows the selected provider's source-rebinding
contract. DataFrame offsets, temporal/domain scalar spellings and typed unary
declarations expose already implemented native behavior. SQL signed arithmetic
keeps the shared typed binder's result types. String comparisons preserve empty,
padded, Unicode, quoted and delimiter-bearing literals; compact SQL predicates
are selected only when their grammar preserves both literal value and type.
Grouped non-null reductions can reach the aggregate provider without changing
their public nullable result schema.

Selection uses operator/expression semantics, with no benchmark IDs, SQL-text
matching for dispatch or typed-plan serialization to SQL. Existing Vortex APIs
remain inside the native provider boundary. Runtime provider errors are not
retried through another strategy or external engine.

## Coverage beyond Full43

The [five-surface execution suite](../../python/tests/test_native_shared_provider_binding.py)
checks complete rows, schemas and no-fallback evidence for representative
current-family cases. It includes:

- Filters, aliases, scalar composition, nullable/wide/signed arithmetic,
  whitespace-sensitive strings, Unicode byte/character lengths and temporal units.
- Grouped reductions, joins, set operations, windows and `IN`, `EXISTS`,
  `ANY` and `ALL` subqueries.
- Thirteen unary parameter variants, dynamic pivot composition, repeated
  collection, limits and offsets before subsequent operators.
- Changing directory/manifest membership and declaration rejection before source
  access, with their applicable source-specific entry points.

The coverage map links 82 references to 66 distinct test symbols in 14 files.
Its 16 groups include a separately identified declaration-only group. It is a
source inventory, not a test result. Existing execution suites supply deeper
memory/typed intake, streaming, representable native/compatibility writers,
resource admission, spill, cancellation, deadlines and source-generation cases.
The map records exact entry points and assertions; it does not establish every
scalar/type/encoding/operator/adapter combination or all five entry points for
every source-lifecycle case. Unsupported combinations keep explicit diagnostics.

Final validation on unchanged Rust/Cargo and Python sources passed:

- Formatting and workspace Clippy with warnings denied, for the default build
  and the `release-user-surfaces` feature union.
- Both default and `release-user-surfaces` workspace/all-target test suites.
  Existing Windows-only cases and opt-in benchmark/regeneration helpers retain
  their ignored status; this run does not substitute for Windows CI.
- 121 Python construction tests and 132 native-binding tests using the matched
  release CLI/library. The binding run has no skipped tests.
- All 215 complete-value comparisons and matching cross-surface schemas on the
  independent 256-row fixture, five adapter-delivery checks, three adapter
  corruption controls and 172 nonexecuting preparation checks.
- The generated public-surface reference gate, covering 203 registered commands.

The manifest identifies all twelve final gates and their exact logs. Earlier
failed builds/tests, the literal-content reproductions and the stale-reference
failure remain in the packet. The generated reference was refreshed without
changing runtime sources or repeating the completed workspace gates.

## Isolated Full43

Each surface ran exactly one execution of each of the 43 queries, sequentially
in the order below. Other task work, agents, builds and profiling stayed idle.
The standard runner retained its storage/concurrency guards, deadlines, complete
result validation and cleanup. Input hashing and recursive identity checks ran
outside query execution. Cache state was uncontrolled; there were no cache
resets, artificial pacing or repeated trials.

Both timing columns are sums of the 43 `engine_execution_seconds` fields.

| Surface | Final engine seconds | Earlier complete cohort, engine seconds | Final complete results |
| --- | ---: | ---: | ---: |
| CLI SQL | 52.413672785 | 50.256160538 | 43/43 |
| Embedded workflow | 61.010366919 | 52.704550748 | 43/43 |
| Python SQL | 72.963240459 | 60.094810045 | 43/43 |
| DataFrame | 69.390462004 | 189.990777493 | 43/43 |
| Typed native expressions | 69.154491086 | 220.534652296 | 43/43 |

Every final surface returns all 381 expected rows. All 43 logical result schemas
match across the five surfaces. Required execution evidence reports
`fallback_attempted=false` and `external_engine_invoked=false` throughout.
Q28 and Q29 now report the shared primitive provider on every surface; the typed
Q20 scan delivers exactly its four result rows. These provider assertions are
acceptance checks in addition to complete-result equality.

The earlier complete cohort already had correct values but exposed missing
grouped-order admission and typed scan pushdown. Its observations are preserved.
The final SQL observations are higher, while the DataFrame and typed totals are
lower. These sequential single observations do not isolate the source change's
timing effect or establish a repeatable surface ranking. No general speedup or
competitive-superiority claim follows from this correction.

The preceding baseline also remains intact: CLI and embedded workflow completed
43 queries, Python SQL completed 35 selected queries, DataFrame passed 18 before
failing at query 19, and typed expressions passed 16 before failing at query 17.
Those partial roles have no Full43 total. Full-input Full43 was not run for the
intervening fixture-only correction builds.

The dataset is the existing 99,997,497-row ClickBench Vortex input
`fresh-roadmap-663-a1.vortex` (15,713,610,513 bytes). Its SHA-256 is
`06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`.
The [independent reference](native-independent-full43-2026-10-22.md) was reused
unchanged: DuckDB 2.0.0 over the original Parquet supplies testing-only expected
values, with no external engine in ShardLoom execution. Query, reference,
input, helper, source and executable identities are in the manifest.

The recorded host is Apple M5, arm64, macOS 27.0, 10 logical CPUs and 16 GiB RAM.
The matched release build uses Rust 1.99.0, LLVM 23.1.1, Python 3.12.14 and
`release-user-surfaces`. Requests use the existing 24-GiB/12-worker settings;
actual native resource grants remain in each report. The native deadline is
115 seconds, the per-query deadline 120 seconds and the per-role deadline
900 seconds. Frozen CLI/library hashes are retained in the build record.

## Portable evidence and retention

The [compressed packet](evidence/native-shared-surfaces-2026-10-23/records.json.xz)
contains 2,716 identified members, including complete final/historical outputs,
29 failed receipts, protocols, source patch and validation logs. Its 8,116,960
bytes exclude large inputs and generated binaries. Read-only
[replay](evidence/native-shared-surfaces-2026-10-23/replay.json) verifies 215 final
results, 215 fixture comparisons, 370 accepted historical results, cross-surface
schemas, engine clocks, provider evidence and all twelve final gates. Nine
corruption controls reject omissions and altered values, schemas, clocks,
execution flags and provider evidence.

From the repository root, with Python 3.12 or newer:

```bash
python3.13 docs/benchmarks/evidence/native-shared-surfaces-2026-10-23/verify.py \
  --review docs/benchmarks/evidence/native-shared-surfaces-2026-10-23/review.json \
  --packet docs/benchmarks/evidence/native-shared-surfaces-2026-10-23/records.json.xz \
  --repository . --check-current-source
python3.13 docs/benchmarks/evidence/native-shared-surfaces-2026-10-23/controls.py \
  --repository . --check-current-source
```

Replay uses the trusted repository parsers and does not execute the engine,
original dataset, external oracle or code embedded in the packet. Omit
`--check-current-source` when auditing these frozen observations from a later
revision; that mode does not certify the later runtime.

The [retirement receipt](evidence/native-shared-surfaces-2026-10-23/cleanup.json)
records identity and closed-handle checks before removing fourteen superseded
test binaries, totaling 3,585,342,064 logical bytes. The accepted CLI/library,
current source inputs, build identities and all compact successful/failed
evidence remain. No rollback payload archive was created.

This closes the shared-provider/surface prerequisite for returning to targeted
optimizations. The next candidate still needs its own finite correctness,
resource and benefit decision; prior rejected mechanisms and optional breadth
remain unchanged. Hosted integration, installed-package acceptance and release
publication are outside this local acceptance.
