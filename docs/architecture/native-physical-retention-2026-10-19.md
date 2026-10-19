<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native physical-array retention investigation — October 19, 2026

Status: finite candidate rejected and removed. The selected one-entry policy
failed its unchanged control gate after integration and ownership corrections.
The sixteen-entry policy remains rejected by the original comparison. All
experimental runtime, provider, feature, diagnostic and test changes were
preserved and removed together. This completes the finite investigation in
`PERF-02.physical_cache`; it closes no broader owner or competitive gate.

The [portable evidence index](../benchmarks/evidence/native-physical-retention-2026-10-19.json)
links the original attribution, frozen candidate protocol, both original
comparison cohorts, both integration cohorts, complete failure history,
source/build identities, analyzers and the exact removal receipt. The
[conditional queue](native-conditional-investigations-2026-10-19.md) retains all
21 conditional rows and their broader obligations.

## Observed work and provider boundary

The initial instrumented native workflows distinguished reader construction,
segment waiting and physical `ArrayRef` reconstruction. The same 39 cells met
the frozen reconstruction-attribution gate in both cohorts. This justified a
bounded retention experiment; references to an existing live owner were not
counted as reconstruction.

The candidate wrapped the pinned Vortex 0.85.0 flat-reader provider. It retained
validated host arrays only within one resident memory owner, using the exact
prepared source token and live immutable layout allocation. The admitted trees
contained VarBinView and only Primitive, Bool, Struct and VarBinView nodes.
Other encodings and shapes used ordinary native execution. No external engine,
query answer, reader, future, error, decoder context or canonicalization result
was retained.

Admission on a second successful reconstruction copied serialized bytes into
a known-capacity host allocation, repeated provider validation, and attached
payload and conservative metadata credits to every array node and buffer.
Complete costs included this duplicate work. Source validation remained
mandatory on lookup and insertion. Dropping a source removed its entries;
live children, slices and buffers retained their credits after eviction.
An optional weak pressure callback released idle entries and retried ordinary
reservation once without replaying a producer or waiting for live consumers.

## Frozen comparison and integration

The protocol compared ordinary execution, one retained entry and sixteen
retained entries over 96 fixture/consumer cells, each with one and eight calls.
The cells combine 4,096/32,768 rows, 3/12 fields, canonical/Zstd-dictionary
encoding, whole-struct/per-column flat layouts, full/one-integer projection,
and native or JSON consumers. Eight source-churn controls use fresh prepared
sources. Every workflow has a 256-MiB grant and one admitted CPU lane.

Each fresh-process cohort has three measured samples per role and scenario,
plus excluded warmups: 1,803 workflows and 8,355 operations. Role order is
balanced and reversed across cohorts. Both cohorts finish before comparative
analysis. Every fixture retains complete physical and logical identities.

The frozen gate requires at least 3% improvement in the sum of all 192 cell
medians in each cohort, at least two common activated repeated canonical cells
with 10% improvement, and no one-call or source-churn control regressing by
both more than 5% and 1 ms. These are complete-workflow diagnostic gates;
they are not engine or Full43 measurements.

The original comparison admitted the one-entry policy to integration. The
sixteen-entry policy failed a one-call Zstd whole-struct/JSON-64 control and
was rejected. Integration then exposed ordinary ownership assertions that
needed to distinguish optional idle retention from live consumer credits.
Corrections released empty table credits, preserved strict transient-owner
assertions, and covered native writer/reopen and retained-child lifetimes.
The archive preserves all failed and corrected checks.

The corrected candidate was frozen and rebuilt before repeating the original
comparison. Its workload, physical fixtures, gates and original analyzer were
unchanged. The recheck was required by those implementation corrections;
it was not an extra attempt to improve a failed performance result.

## Final decision

| One-entry gate after integration | Cohort A | Cohort B |
| --- | --- | --- |
| Exact values, types, NULLs, order, ownership and cleanup | Pass | Pass |
| Complete aggregate benefit | Pass | Pass |
| Common material repeated cells | Pass; 22 common cells | Pass; same 22 cells |
| One-call and source-churn regression controls | Pass | **Fail: two controls** |

The failing controls both use 32,768 rows, 12 fields, Zstd-dictionary encoding,
full projection and JSON-2048 consumption, with whole-struct and per-column
flat layouts. Neither recorded cache activation. The observation fails the
predeclared acceptance criterion; it does not establish the cause of the
variation. All samples and host/process observations remain in the evidence.
No samples were discarded, thresholds changed or further comparison run.

**Reject and remove the one-entry candidate.** The original sixteen-entry
rejection remains binding even though it passed the later diagnostic recheck.
Selecting it from that later observation would discard its earlier failure.
There is no accepted physical-array cache from this experiment.

## Verification, removal and limits

The final recheck validates all 3,606 workflows and 16,710 operations, including
full output verification, exact source/layout identity, actual activation or
bypass, grants, zero final reservations, source cleanup and drained processes.
The source and artifact hashes were verified before and after both cohorts.
Safety coverage includes source replacement/mutation/truncation and symlink
ABA, admission denial, pressure/shrink, concurrent access, cancellation,
consumer failure and surviving extracted children/buffers.

An integration revision passed 3,040 native tests, with 31 opt-in experiments
ignored. The final build passed all 15 focused retention safety tests, strict
native linting and 249 provider tests. The staged Python ownership test passed
on the identical native-module bytes. These checks establish their stated
correctness scope; they do not override a failed cost/control gate.

Before removal, all 42 changed tracked files and seven new candidate files
were copied and hash-verified. The tracked files were restored byte-for-byte
to the recorded baseline; only those seven archived candidate files were
removed. The provider notices, feature wiring and package license manifest
were restored with the implementation. Other investigation records and
unrelated work were preserved.

The restored tree passes `cargo fmt --all -- --check`,
`cargo clippy --workspace --all-targets -- -D warnings` and
`cargo test --workspace --all-targets`. The archive retains their exact
offline/locked invocations, complete logs and drained-process receipts.

The candidate did not advance to installed-wheel, public format/workflow or
one-call Full43 acceptance after the failed gate. No new Full43, package,
platform or whole-engine performance claim follows. Reproduction requires
the archived source patches, added sources, exact inputs and frozen protocols;
the removed experimental selectors are not present in the restored tree.
A materially different retention mechanism requires a new design and protocol.
