<!-- SPDX-License-Identifier: Apache-2.0 -->

# Retained native unary workflows

Status: implementation, immutable release-build and local Full43 acceptance complete
under PERF-02/03/07/10/11/12. Hosted-check results are tracked on the PR. This follows
the accepted aggregate/ordered [result stream](native-workflow-streaming-2026-10-01.md)
and the [universal workflow plan](universal-workflow-completion-2026-10-01.md).
It does not close the broader PERF or competitive gate checklists.

## Contract and scope

Prepared unary operations and public worker collection retain a generation-bound
native source and their validated request across repeated calls, execute fresh operator state, and deliver
complete values through the same native array boundary used by local writers.
The operator result must be usable without a JSON intermediate or a second query.
Small collection remains separately limited to 65,536 rows and 8 MiB; file writes
use bounded synchronous batches and preserve typed empty output.

The inventory covers DISTINCT, deduplication and duplicate masks (first, last and
remove-all policies), tail, deterministic and weighted sampling, existing scalar
rewrites, melt, explode, rolling operations and pivot. Row selection, fixed-schema
computed output, and data-dependent pivot schemas have different state and schema
contracts; their implementations and tests remain separate within this work unit.
No new SQL syntax or arbitrary DataFrame chain is inferred from an existing
primitive. Nested/extension input and output must be admitted by actual native
dtype support, with remaining shapes recorded explicitly.

## Provider decision

Vortex-first provider check: `use_vortex_native_provider`. Reuse pinned Vortex
0.85 native scans, expressions, logical field and scalar/validity access, and
existing writers through the resident session. Upstream `ArrayRef::take`/slice
were also checked: Vortex `take` preserves a native selection and may retain its source buffers;
an index vector alone is not a payload-memory bound. Computed flat columns reuse
the existing reserved native result builder. Existing ShardLoom exact key,
sampling, rewrite, reshape and rolling semantics remain the operator providers;
no upstream query-engine integration or new execution dependency is introduced.

Preparation binds schema and pushdown without scanning rows. Execution uses one
admitted resident context; downstream consumers share that context rather than
reacquiring admission. Validate source generations before and after the operation,
including metadata-pruned output and final publication. Invalidation is terminal
for the call. Prepared handles retain no answers or mutable operator state.

## State and delivery

The producer reads scalar keys and selected values from native arrays, then builds
owned result columns in bounded batches. It does not retain whole source buffers
for sparse selections or claim zero-decode or zero-copy output. DISTINCT and
first-occurrence selection stop once their requested result limit is complete;
tail uses a native suffix range and sampling retains admitted candidates.
Last/remove-all deduplication retains one candidate per key,
its ordinal and occurrence state, rather than every duplicate payload. Final output
keeps source order. Duplicate masks with future-dependent policies retain the
necessary output identities and complete key counts/last positions.

Reserve container capacity, exact key bytes, retained values, output references and
native buffers before allocation. Keep leases attached until their owners drop.
Consume bounded batches synchronously, propagate cancellation and consumer errors,
and reject unsupported state pressure explicitly. Existing scalar/column helpers
must not silently allocate an unbounded result table. This unit does not claim a
total RSS ceiling or invent spill support for unary state; native spill is a separate
admitted operator contract under PERF-06.

Fixed-schema computed operations carry an explicit dtype through empty and all-null
results. Pivot discovers its output domain while executing once, then hands completed
state and its schema to the writer; it must not rerun the query to infer schema.
Public reports describe the same execution and may serialize values only at the
terminal collection boundary. Native writes retain atomic create-if-absent,
complete-consumption checks and owned staging cleanup.

## Acceptance

The expert comparator is an encoded-columnar engine maintainer checking exact keys,
selection order, source invalidation, buffer lifetimes and memory denial before
publication. Freeze independent expected values for each admitted family and policy.

- Repeated prepared and public worker calls reuse the source and execute fresh
  state. Changing request, resources or source invalidates the appropriate handle.
- Complete scalar values, nulls, UInt64 bounds, source order, deterministic sample
  seeds/weights, centered windows, empty inputs and typed empty results survive
  native output and reopen. Match existing semantics across chunk boundaries.
- Results above the collection row/byte limits complete through admitted writers;
  collection fails explicitly at its own limits. Verify every returned/written row.
- Exercise key/cardinality and wide-string pressure, slow/error consumers, parent
  cancellation, source replacement, existing destinations and no leaked leases or
  staging files. Unsupported spill must not become hidden full-table allocation.
- SQL where admitted, Python/DataFrame and CLI converge on the same native handlers.
  Use renamed schemas and public read/transform/write/reopen workflows, all eight
  local writers where their dtype contracts permit, and Full43 as regression proof.

Run focused tests followed by required workspace/native validation and hosted PR
checks. Timing claims require a separate frozen comparison; availability does not
depend on beating a benchmark. Paused large text/format performance runs, native
Python binding experiments and package publication remain outside this continuation.
Keep `fallback_attempted=false` and `external_engine_invoked=false` throughout.

## Public availability and remaining boundaries

The Python `collect()` report carries complete `result_rows` and `result_jsonl`.
`to_python_objects()` consumes that same execution, including empty results; it
does not replay the query through another frontend. The worker retains one unary
source/lowering until its request, policy or source generation changes. Every call
executes fresh state. Explicit Rust prepared handles also reuse their source across
writes; public write requests currently create their own admitted execution.

All ten families reach Vortex, Parquet, Arrow IPC, Avro, ORC, JSON, JSONL and CSV for
supported flat scalar schemas. Source filters precede DISTINCT, deduplication,
sampling, scalar rewrites, melt, explode, pivot and rolling computation. Public
normalization preserves the predicate for both collection and writing. Tail and
duplicate masks still reject a source predicate explicitly; their existing public
unfiltered forms are covered. Filtering after an operation, arbitrary chains,
general nested/extension results and unary state spill remain separate work.
Heterogeneous scalar Variant results retain their native/text admission and do not
gain binary compatibility admission from the flat-scalar matrix. Existing native
structured projections and nested explode providers keep their distinct contracts.

The reproducible `scripts/run_native_unary_uat.py` matrix uses renamed cargo fields,
three repeated collections and eight complete write/reopen comparisons for each
family, plus eight selective-filter variants. SQL DISTINCT, empty collection,
explicit collection row-limit denial, and complete 65,541-row Vortex/JSONL output
bring the positive complete-result checks to 202. It checks source generations,
source/binary/harness/SDK hashes and no-fallback public evidence under the local
storage and serial process guards. This is correctness/availability evidence,
not a performance comparison or total RSS bound.

## Local validation

Runtime, Python and harness revision `959f2eb0711f95ad9aa9fd161d94770ec5cb0a75`
was built with Rust 1.99 and `release-user-surfaces`. Frozen executable SHA-256:
`a29108099aa2792fde4b1eb097d1cccf692080124d8d1e5fa4014cb8c8e63a0a`.

| Check | Result |
| --- | --- |
| Default workspace all-target tests | 3,436 passed across 102 targets |
| CLI all-target tests with `release-user-surfaces` | 1,529 passed across 75 targets |
| Final native unary tests | 31 passed, including complete eight-format output and the final limited-deduplication evidence correction |
| Python suite | 680 passed; 144 existing retired/environment-dependent skips; 824 total |
| Formatting and default/native all-target Clippy | Passed, with warnings denied |
| Public surface/status/productization docs and contribution governance | Passed |

The broader native workspace and library runs also passed 4,816 and 2,103 tests,
respectively, with 23 existing ignored tests in each. Those overlapping runs
preceded the final public wiring and limited-deduplication evidence fixes; final
CLI, focused unary and Clippy checks cover the affected paths. The evidence packet
preserves this scope distinction and every retained validation log hash.

The release-build Python receipt is
`unary-workflows-20261001/logs/native_unary_20261001T231352940815Z/summary.json`,
SHA-256 `ffaad74bcefda57a440f36c85b8338885a7ecf3900c75fa5b52cf56721fcac83`.
It verifies all 202 complete results and the explicit collection row-bound denial.
The packet includes all 367 public request envelopes, source identities/hashes,
the unchanged binary/harness/SDK identities, and earlier failed-attempt receipts.

Full43 passes **129/129** complete-value comparisons across all 43 queries.
Receipt: `clickbench-100m-uat/logs/full43_20261001T231420055101Z/summary.json`,
SHA-256 `a62583e2b0b0a2c3375cbe07514adff2ac7a19409a2944c95fe12efe39446a44`.
Each query executes three times in a fresh process under a 24-GiB/12-worker policy.
The resident 15,682,956,489-byte source is fully hashed before and after acceptance;
all 43 retained reference identities and compressed/raw log hashes are checked.
These references provide native regression evidence, not an independent oracle.
OS page cache and ordinary host activity are uncontrolled. All guarded runs pass,
all owned locks are released, and the frozen source/executable remain unchanged.

The [portable acceptance packet](../benchmarks/evidence/native-unary-workflows-2026-10-01.json.xz)
contains the build, local tests, public envelopes, Full43 records, reference
identities, supervisors and verifier sources. Its SHA-256 is
`6e12f60a0ce075357fd3f995188eacb79cbc8f9e62c4edbb86afd3932b6ec3ec`.
Rebind path placeholders to resident local inputs when replaying the checked-in
runner. This packet records local acceptance before hosted PR checks; its scope
and timestamps remain immutable.
