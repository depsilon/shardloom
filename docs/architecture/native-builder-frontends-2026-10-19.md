<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native builder frontend investigation — October 19, 2026

Status: finite substitution rejected. Direct typed construction did not meet the
frozen complete-workflow benefit gate in either cohort. Public SQL/DataFrame
routing and the existing typed API remain unchanged. This is a decision within
`PERF-02.builder`, not closure of that substep, another PERF owner or a competitive
gate. The [conditional investigation queue](native-conditional-investigations-2026-10-19.md)
retains all 21 rows.

The [portable evidence index](../benchmarks/evidence/native-builder-frontends-2026-10-19.json)
links the protocol, generator and independent oracle, analyzer and its tests,
both complete raw cohorts, preflight failures and exact source/build identities.
The native module is retained locally by size and hash; the archive contains its
source patch, added sources and build receipt. This is local macOS arm64 evidence,
not an installed-wheel or cross-platform acceptance result.

## Shared execution boundary

The existing Python native `Relation` and `Expression` APIs construct typed IR.
`Session.plan` passes that declaration through the shared native relational
preparation boundary. `Session.sql` parses and lowers SQL before using the same
native operators and retained result owners. Both roles use the same frozen
module, explicit memory sources, memory grant and CPU admission.

The high-level Python workflow currently retains SQL fragments. This experiment
compares six explicitly generated SQL/typed declarations; it does not implement
or measure a high-level AST bridge. No external engine executes any operation.
Python fixture/oracle code is an independent correctness boundary.

## Workload and preflight

The protocol was frozen before comparative execution. Its 72 matrix cells cover
0/64/4,096 rows, 4/12 input fields, one/eight calls per prepared declaration and
six families: computed filter/project, grouped aggregation, left join, stacked
limits, distinct union and projection/sort. Nullable integer and UTF-8 data
include negatives, duplicates, quotes, non-ASCII text and embedded NULs. The
right-side lookup includes a NULL key that must not equijoin.

Each cohort contains five samples per role/cell, twelve parameter-change controls
with eight fresh declarations per workflow, and one retained but excluded warmup
per role: 842 workflows and 4,202 operations. Both fresh-process cohorts completed
before comparative analysis, for 1,684 workflows and 8,404 operations. Roles and
case order are balanced and reversed across cohorts. Every workflow uses a
64-MiB grant and one admitted CPU lane.

Two preflight failures were corrected before measurements:

- The original oracle expected nullable integer SUM. The native contract is
  nullable `f64` for primitive numeric SUM; the oracle now uses that type and
  full-value hashes distinguish integer and floating JSON values.
- SQL rejects OFFSET inside a derived relation. Both declarations now use the
  same supported inner `LIMIT 25` and outer `LIMIT 9 OFFSET 5`. The independent
  oracle remains `rows[2:25][3:12]`, exactly equivalent to `rows[:25][5:14]`,
  including empty and short inputs. Nested OFFSET support was not added.

The archive retains both failed receipts, partial samples and historical harness
versions whose hashes match those samples. The corrected preflight passed all
170 workflows and 842 operations. Workload outputs, matrix coverage and decision
thresholds were unchanged. Nine analyzer tests cover missing/duplicate records,
semantic witnesses, identity mismatches, incomplete runs, aggregate/material
gates and the exact control-boundary rule.

## Frozen decision

Complete workflow cost includes session/resource/source construction, declaration
construction, preparation, every execution and complete row consumption, result
and source release, and session close. Fixture generation and independent full
verification are excluded from that clock. Source-value verification runs after
the timed calls, so it cannot warm the compared preparation or execution.

These are focused workflow diagnostics, not engine or Full43 timings:

| Frozen diagnostic gate | Cohort A | Cohort B | Decision |
| --- | --- | --- | --- |
| At least 3% reduction in the sum of all 72 cell medians | 1.19% improvement | 0.74% improvement | Fail in both |
| At least two common one-call cells from different families improve by 10% | Pass | Pass | Pass |
| No control regresses by both more than 5% and 1 ms | Pass | Pass | Pass |

The controls include every eight-call cell, every projection/sort cell and every
parameter-change workflow. Material improvements occur in common cells across
all six families, but they do not override the aggregate gate. There was no
selective sample removal, threshold change or additional comparison after the
result.

**Reject substituting the current typed builder under this frozen workload.**
There is no new runtime candidate to remove. The typed API remains available;
this result does not establish that a materially different high-level builder
or lowering mechanism cannot help. Such a proposal needs a separate design,
semantic/ownership proof and a new frozen comparison.

## Correctness and limits

Every source and output row is verified, with exact field order, dtype,
nullability, NULL semantics and output order. The analyzer independently
recomputes complete expected witnesses from the fixtures and oracle; it does not
accept success flags as semantic proof. Prepared reuse and the exact operation
count are checked. Each workflow returns to zero live reservations, stays within
its grant and reports no fallback or external-engine invocation. Every supervised
process group drained, and frozen source/artifact/harness identities were checked
before and after both cohorts. Host load and sampled process resources are
retained as context, not exclusive attribution or an RSS ceiling.

The rejected substitution changes no production routing, so no additional
Full43, package or whole-workspace run is attributed to this experiment. The
separate physical-cache candidate was subsequently rejected after its integration
control gate failed. No whole-engine speed or release claim follows.

Replay uses the archive's frozen protocol, source manifest, patch, added sources,
native build command, generator, analyzer and supervision helpers. Rebind
historical absolute paths into a new local evidence directory, preserve all
guards and thresholds, and never overwrite original run identities.
