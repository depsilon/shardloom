<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native strategy parity across format boundaries

Status: local acceptance passed; PR integration pending under PERF-10/11/07 and
CG-5/20/21. The October 21
maintainer explicitly requires one execution path regardless of input/output
format, using the accepted native Full43 path as the reference. Complete this
correction before the newly authorized release train, then resume the required
comparison-driven queue. Optional breadth remains optional.

## Contract and observed deviation

Equivalent queries over equivalent admitted data must reach the same native
strategy selector after input normalization. Input and output format names must
not independently select a less capable executor. Native metadata, field types,
query semantics, streaming requirements and resource policy remain valid physical
strategy inputs. A sink consumes the selected native producer with its existing
schema, backpressure, source-generation and publication guarantees.

The preceding accepted isolated Full43 observation is 49.990920425 engine seconds for one
execution of each query. Its existing Vortex source enters the public native
count, aggregate, filter/projection and ordering families. This observation
excludes original source conversion and result-file writing; it does not measure
raw-format query parity. The subsequent 129 file-writing queries all select
`native_vortex_primitive_row_export` after an existing or freshly prepared
Vortex input. Those writers do not report the engine clock, so their complete
workflow observations must not be relabeled as engine timings.

Review of the preceding source identified these deviations, now corrected:

- `public_resident_sql::route` sends every raw-format SQL request to relational
  execution before the ordinary primitive selector examines the query.
- `infer_native_vortex_route_payload` excludes compatibility inputs from inert
  SQL inference. Consequently embedded SQL can also miss the ordinary native
  workflow provider before preparation.
- `NativeWorkflowDeclaration::prepare` excludes normalized compatibility sources
  from its metadata-based extended-type admission. Selection must depend on
  authoritative native fields, with the original preparation owner retained.
- `Binder::source_columns` exposes private derived metadata while an all-column
  native scan hides it. This makes SQL wildcard binding disagree with execution
  when reopening some Parquet/ORC outputs. Public schema discovery must agree
  with the existing native all-column contract without removing physical fields.
- Normalization's quoted-source rebinding currently replaces matching string
  literals globally. Expanding its use requires source-position-only rebinding;
  a value equal to an input path must retain its original value.
- Scalar writes remap count to aggregate or force relational dispatch even when
  collection uses the metadata-only or filtered-count producer. Preserve the
  producer and hand its typed scalar to the existing result writers.
- Embedded SQL infers its primary source from a recognized filename before
  consulting declared bindings. A declared Arrow IPC source with an opaque
  extension must receive the same provider as the equivalent recognized name.
- The single-source attachment labels every compatibility request as having
  performed ingestion, even when its preparation report records reuse. Forward
  the observed preparation status.

## Reuse and provider decision

Reuse the current public SQL grammar, native workflow declaration, compatibility
preparation, `PreparedVortexSource`, `prepared_dispatch`, aggregate/count/order
providers and native result/sink owners. Keep metadata-only admission before
execution; an admitted provider error is terminal and cannot cause a second
execution attempt. Do not introduce another evaluator, source-format kernel,
persisted representation, dependency, public mode or benchmark shortcut.

Vortex-first decision: `use_vortex_native_provider` for the existing pinned Vortex
0.85 DType, retained file/array and scan providers. The corrective logic belongs
to ShardLoom's existing source binding and strategy selection. Preserve encoded
owners, physical derived metadata and late materialization; use the existing
private-field predicate for the visible schema. Native Vortex remains the
highest-fidelity persistence target. Compatibility writers remain boundary
adapters, with `fallback_attempted=false` and `external_engine_invoked=false`.

PulseWeave, capillary work and dynamic admission remain with the same caller-owned
CPU/memory envelope. Preparation identities must survive provider selection and
final publication. No additional compute pool, unaccounted retained source,
implicit spill, result cache or full-process memory guarantee is introduced.

## Maintainer feedback intake and existing observations

The October 21 review's shared-selection, scalar-result and preparation-reuse
findings are accepted into this correction. The existing mechanisms supply the
implementation: no extra prepared-source cache or per-format writer is needed.
First-use preparation optimization and a selective raw-Parquet adapter remain
separate workload-gated candidates. A codec or writer rewrite is not justified
by the three workflow totals.

The retained `full43-file-route-results-898.json`, route summaries and
`completion-897.json` records show one guarded preparation in each fresh
Parquet suite, followed by 43 native writer invocations against that suite's
same prepared path. All three suites contain 381 output rows. Independent
Parquet/ORC readback validates all output values; the original native wildcard
reopen failures remain recorded (16 Parquet and one ORC). These were real
readback defects, not query timing failures. The correction adds independent
small Parquet/ORC wildcard reproductions and complete readback checks.

Those old suites already used explicitly prepared native input for the query
pass. Their timings therefore do not measure the raw-format dispatch defect
corrected here. The writer invocations lack engine clocks. Keep that gap explicit;
do not subtract an ingestion from another run or relabel process durations as
engine execution. The accepted engine-only Full43 observation remains distinct.

Both fresh middles recorded 15,713,610,513 bytes, 99,997,497 rows, a 53,428,212-byte
footer inventory, 611,520 footer segments, persisted layout metadata and available
statistics. Both reported 6,097 text-dictionary preservation calls over
195,505,790 column values. These are observed counters and metadata contracts,
not a complete encoding inventory. Matching size alone does not establish byte
identity with the main benchmark artifact: the fresh middles' cleanup receipts
did not retain full-content hashes. Do not claim every physical layout matches.

Source review confirms that explicit and automatic preparation call the same
schema-aware preparation and ingest implementation. Automatic target identity
depends on source, declared format and schema; query text and output format do
not select another target. Existing source/artifact generation checks decide
reuse. Python `ctx.read(...)` regressions now change the query and destination
while checking the retained file generation, complete values and selected
provider against the prepared-Vortex control. Primitive reports expose the
actual reuse and ingestion fields. Composed relational plans retain and validate
the preparation identities internally but do not expose those single-source
attachments; their tests verify reuse through the persisted generation instead.

Resident extended-type selection also keeps the original preparation identity
in the existing prepared-query key. Moving that identity only into the retained
source made a repeated request look different and reopened its reader. The
resident regressions cover reader-open counts, repeated execution, session
release and rejection of a changed original generation.

## Implemented correction

Public SQL and embedded SQL apply the existing inert operation inference to
compatibility declarations, then normalize the source before metadata-based
provider admission. Explicit file formats take precedence over extension hints.
The complete SQL operation and original preparation owners survive extended-type
selection; complex expressions, nesting, memory bindings and unsupported shapes
remain with the common relational binder. There is no execute-and-retry path.

Count and filtered-count writes now share one typed scalar handoff in the Vortex
boundary, used by both the ordinary public writer dispatch and embedded native
plans. Destination preflight precedes producer execution. Original source
generations remain validated through publication. Producer scan/decode evidence
is kept distinct from the one-row sink's array conversion; metadata-only count
retains `data_read=false` and `upstream_vortex_scan_called=false`.

Source rebinding changes only relation-source tokens, preserving literals,
comments, quoted-path identity and nested SQL. The existing native single-input
contract permits an unquoted SQL table alias for the explicitly supplied primary
file. Bind that alias before strategy selection for compatibility inputs too;
the standard Full43 runner depends on this ordinary public contract. Quoted
paths retain exact identity, and explicit source-binding maps retain their
declaration checks. Multiple-source and composed declarations keep their existing
source resolver rather than using the single-source shorthand.
Public wildcard schema discovery
uses the same private-derived-field exclusion as the native all-column scan;
the physical metadata remains available to native operators. Whole-document JSON
schema hints use the existing scalar coercion rules, including explicit rejection
of negative unsigned values, fractional integers and boolean/integer mismatches.

## Acceptance

Prove query/strategy parity for all supported local file formats and writers,
including ordinary SQL, Python and DataFrame entrypoints. Compare selected
providers and complete results, not lifecycle labels alone. Include independent
nullable/empty, aggregate/group/order/filter, typed and nested controls; literals
matching source paths; source declarations; cancellation, source changes and
failed publication. Preserve existing composed and finite-stream guarantees.

Record the current wildcard failure before the correction and verify the original
small Parquet/ORC reproductions afterward. Keep every Full43 write observation and
its original readback defects. Run focused regressions before required workspace,
native-feature and public workflow checks. Perform any justified final Full43
acceptance once, isolated under the standard runner; do not rerun it to rename a
clock or manufacture a comparison. A shared selector alone does not establish a
new speedup, complete type/operator breadth, CG closure or release readiness.

## Accepted local results

Implementation `dd1e190e80372b62b6974dcb6d927aef4b96130f` passes the following
checks. The [acceptance record](../benchmarks/evidence/native-format-strategy-2026-10-21/acceptance.json)
and [portable archive index](../benchmarks/evidence/native-format-strategy-2026-10-21/index.json)
retain commands, logs, source identities, results and earlier failed attempts.

| Check | Result |
| --- | --- |
| Default workspace, all targets | 3,238 tests pass; no skips |
| Vortex library with release user surfaces | 3,161 tests pass; 31 existing manual benchmark tests remain ignored |
| CLI library and affected public/resident/preparation integrations | 1,099 tests pass; no skips |
| Formatting and default/native-feature lint | Pass |
| Python core shard | 703 tests run; 25 environment-dependent skips, no failures or errors |
| Clean installed native wheel | 121 tests pass; no skips; installed CLI/module hashes match the frozen build; uninstall passes |
| Public reference/status/governance/scope checks and golden workflows | Pass |
| Original minimized Parquet/ORC wildcard failures | Both reopen; all 12 rows match independent PyArrow readers |
| Isolated Full43 | 43 queries, one execution each; 381 complete rows pass retained-reference validation |

Full43 engine time is **48.754238077 seconds**. The original Vortex input, query
file and reference values are unchanged. Source and executable identities are
verified before and after the pass; all owned processes and the UAT lock are
released. Other task work is idle during query execution. This is one local
observation, not a paired performance claim or a measurement of raw-Parquet
first-use preparation. The retained Full43 reference is a regression reference;
the independent small fixtures establish the new format semantics separately.

The workspace/native/CLI tests ran in the working tree before the implementation
commits. Their receipts preserve the then-current HEAD and dirty-file list;
they are not mislabeled as clean-commit executions. The Vortex code did not change
after its broad suite. Subsequent CLI alias corrections passed the final CLI and
workspace suites. The optimized CLI, Python extension, installed-wheel proof,
original-fixture reopen and Full43 are bound to the clean implementation commit.

The archive preserves the three earlier 43-query writer observations and all
their values, missing writer engine clocks and 17 original native reopen
failures. The two retained minimized failure fixtures are the exact originals;
this acceptance does not claim a rerun of the 129 writer invocations or all 17
discarded output payloads. The new ordinary-source/writer matrix additionally
checks all eight supported sinks and metadata-only count work avoidance.

Two acceptance-helper errors are retained separately: an incorrect Rust library
target name after a successful build, and a copied supervisor path rejected
before any Full43 query. The build was reused only after checking its unchanged
source and fresh compiler records. The corrected preflight produced the single
accepted query pass. Neither failed setup is a runtime or performance sample.

To verify the packet without the original host, run:

```sh
python3 -B docs/benchmarks/evidence/native-format-strategy-2026-10-21/verify.py
```

The verifier reads the archive without extraction, checks every member hash,
replays complete Full43 values and clocks, verifies the installed-native totals
and original fixture readback, and checks all 141 prior-audit source digests.
Runtime reproduction requires rebuilding the recorded revision and binding the
retained driver to resident inputs and a new output directory. The archive does
not include large input data, executables, libraries, wheels or environments.
