<!-- SPDX-License-Identifier: Apache-2.0 -->

# Selected workload guarantee review — October 22, 2026

Status: the four follow-up reviews from the [22-task comparison acceptance](native-workload-comparisons-2026-10-22.md)
are accepted with the limits below. The source/test/run map is in
[review.json](evidence/perf12-guarantees-2026-10-22/review.json).
The review found no demonstrated selected-path defect requiring an engine change.
It completes this finite evidence review and advances the queue to the already
requested shared prerequisites. It does not close all fifteen required owner
contracts, CG-5/CG-6, or the independent Full43 reference gap.

## Decision and scope

The comparison already checks 704 complete normalized result pairs across the
22 declarations. Those outputs do not exercise every NULL/empty, exact floating,
resource-pressure or failure boundary. This review binds existing regressions
for those boundaries to their source and actual passing runs. It adds no engine
implementation, benchmark execution, operator/type/platform breadth or release.

| Follow-up action | Accepted disposition |
| --- | --- |
| Selected semantics and lifecycle | Retain the named exact-value, NULL/empty, key identity, ordering, overflow/floating-policy, writer and reader-lifetime evidence. Each guarantee has explicit test selectors and a limit. |
| Selected resource ownership | Retain the named native grants, competing owners, queue admission, spill failure, cancellation, generation validation and cleanup evidence. Native credits remain distinct from whole-process memory. |
| Independent Full43 reference | Accept the explicit missing-reference disposition. The current reference and six inspected historical candidate records establish native regression provenance, not an independent full-size oracle. |
| Selected installed support | Bind 20 existing public Python methods to their actual three development native-wheel lanes. Preserve the separate published 0.7.0 channel proofs. |

The [machine-readable acceptance](evidence/perf12-guarantees-2026-10-22/review.json)
retains all four action IDs and their owner IDs. It leaves `required_children_closed`,
`optional_rows_promoted` and `competitive_gates_closed` empty. Existing evidence
may satisfy these selected guarantees without requiring every possible provider
allocation or future workflow combination to be implemented.

## What the source assertions establish

The eleven guarantee groups retain the individual test names, complete source
files, recorded file identities, passing log lines and specific limits. The
principal distinctions from normalized comparison equality are:

- **Values and semantics:** empty scalar aggregates return zero counts and NULL
  sums/averages/extrema; grouped empty output retains its schema. Tests check
  complete integer and UTF-8 keys across dictionary domains, NULLs, ties,
  offsets, repeated execution and pressure transitions. Ordered floating sums
  retain their declared evaluation order, including cancellation-sensitive
  values and signed zero. Integer expression overflow stays observable even
  in a discarded group. This does not promise bitwise agreement between engines
  with different floating evaluation orders.
- **Owners and admission:** selected decoder workspaces pass at their exact
  grant and reject one byte short. A competing retained owner can cause denial;
  releasing it permits the checked retry. Queue overflow and cancellation
  preserve admission order and session reuse. Blocking work retains its buffer
  and I/O owners until it drains. Caller, Python, codec/library and OS allocations
  are not silently included in native reservation accounting.
- **Spill and recovery:** parent tests observe actual spill before failure or
  process death. They check credit release, live-owner protection, cleanup of
  dead owned runs, preservation of foreign files/replacement generations, and
  complete fresh-source results. This covers the selected set, unary, sampling,
  subquery, aggregate, join, window and replay families. Recovery starts a fresh
  execution; it does not resume a partially completed query. Aggregate recovery
  uses an instructed process exit; the other selected parent cases kill their
  child. Unknown-owner refusal is directly checked by the non-aggregate parents.
- **Publication and lifetimes:** selected native and compatibility writers check
  complete values, destination-specific schemas, empty output, source identity,
  staging cleanup and retained readers. Source mutation or cancellation cannot
  turn a failed write into a completed publication. Fanout executes once and
  validates input before publication, but a later publication collision can
  leave earlier complete outputs. It is not a group-atomic transaction. Two
  publication-ownership tests use synthetic payloads and are identified as such;
  the separate complete native readback tests carry the data-fidelity claim.
- **Public streaming:** installed tests preserve rich values and NULL masks,
  copied Arrow source ownership, global order, producer closure and retained
  results after cancellation. Completion follows final demand. A later error
  may follow provisional batches; retained prefix values remain valid without
  a completed report. Text export has its declared serialization contract and
  does not preserve every native logical type.

## Source and execution bindings

The accepted source is `1b0206636305c3c6490c3a4271dcf84b18367dc8`.
Its tree equals the tested PR #1587 revision `6286ce8e5f723ec0b8b56809496e375ce2a63a99`
and the recorded test checkout `cf104aeab069ba92674d3e06ceb92b9aafc46c91`.
Both selected Rust jobs belong to [run 39804670420](https://github.com/depsilon/shardloom/actions/runs/39804670420)
and completed successfully. The retained pre-merge receipt records all 44
required checks succeeding at that revision.

The packet binds **183 unique Rust selectors to 192 terminal `ok` observations**.
That inventory contains 180 ordinary regression selectors, two child-process
support entrypoints, and one diagnostic-capture test. The latter three are not
independent recovery guarantees. Counts describe selectors and observations,
not independent semantic guarantees or new executions. The eleven reviewed
groups identify the assertions used for acceptance; the wider selector inventory
provides supporting source/run context.

All 72 retained Rust test/helper files and two directly embedded fixture files
match the current, tested, hosted and published-runtime source commits. Eight
Python test/harness files match the current, tested and hosted sources. This is
a bounded inventory of **82 files**, not a transitive runtime-source or allocation
coverage claim. Full files are retained so readers can inspect setup, helpers,
failure injection and expected-value construction beyond a test name.

The hosted Python evidence comes from [run 39793398084](https://github.com/depsilon/shardloom/actions/runs/39793398084)
at `599cd299465398c0eb10031025ed382e79ed3eb1`:

| Development native-wheel lane | Retained normal integration | Selected methods |
| --- | --- | --- |
| Linux x86_64, Python 3.10 | 121 tests, zero failures/errors/skips | 20 exact source/log bindings |
| Linux x86_64, Python 3.13 | 121 tests, zero failures/errors/skips | 20 exact source/log bindings |
| macOS arm64, Python 3.13 | 121 tests, zero failures/errors/skips | 20 exact source/log bindings |

Each lane also retains a separate 13-test opt-in output-window summary. These
summaries have no retained per-test log and are not counted as the 60 selected
normal-integration observations. The Linux wheels declare the recorded
`manylinux_2_39_x86_64` tag; that does not prove compatibility with older glibc.
The 22-task comparison itself ran on macOS arm64. This review does not establish
Windows native-extension support or cross-platform execution of that full matrix.

The [published 0.7.0 channel proofs](../release/v0.7.0-publication-verification.md)
remain separate and unchanged. Their four original transcripts are retained in
this packet. The development wheels are not relabelled as published artifacts,
and no new installation or package publication was performed for this review.

## Full43 reference disposition

The exact reference consumed by the current accepted Full43 explicitly declares
`retained_native_regression` and `independent_correctness_oracle: false`.
Its original run, summary, reference values and verification record are retained
with their archive-member identities. The original 43-query, single-execution,
381-row observation remains accepted on that basis and is not rerun here.

Six additional candidate records also explicitly describe native references.
Their 258 indexed query outputs are an inventory, not inspected payload parity.
Container hashes and a current query-file hash cannot establish historical
oracle/version/input/semantics bindings. This finite audit is not an exhaustive
search of all repository history. An independently supported equivalent with
those exact bindings remains necessary before independent full-size parity;
performance claims additionally require their own compatible clocks, resources
and paired uncertainty.

## Reproduce the evidence checks

The [manifest](evidence/perf12-guarantees-2026-10-22/manifest.json) binds
142 stored members in the [portable packet](evidence/perf12-guarantees-2026-10-22/packet.json.xz).
Absolute paths inside the packet are lookup keys. Replay does not need those
original files, execute archived code, contact GitHub or run an engine.
The retained build program records how the packet was assembled; the primary
acceptance decision is the separate review document.

From the repository root, use Python 3.10 or later and new output paths:

```sh
evidence=docs/benchmarks/evidence/perf12-guarantees-2026-10-22
python3 "$evidence/verify_guarantees.py" --check-current-source . --output /tmp/perf12-guarantees.json
python3 "$evidence/test_guarantees.py" --output /tmp/perf12-guarantee-corruptions.json
```

Omit `--check-current-source` to replay the historical packet after a later
source change. The accepted replay and 52 rejected corruption cases are retained
alongside it. Corruption checks cover missing selectors/actions, altered source,
log and job identities, child-support promotion, installed skips, invented
platform/publication claims, erased limits and false Full43/owner/CG closure.
The comparison acceptance verifier also checks the current completion record
against the review's exact path, size, digest, status and action dispositions,
replays the guarantee proof, and compares the original action contracts with
their retained copy. Nineteen additional corruption cases reject changed
completion links, claims or contracts before accepting an empty action list.
Ordinary Python CI runs the same checks from an unrelated temporary directory.
Exported source archives omit historical evidence bundles; that specific test
is skipped there, while a missing packet in a Git checkout fails.

The next work is the existing shared prerequisite stage: justified independent
regions, the exact local reduction needed by their consumers, region/preparation
cost evidence and current instruction/dispatch attribution. The original
admission gates remain in force; optional breadth stays at the bottom of the queue.
