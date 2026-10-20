<!-- SPDX-License-Identifier: Apache-2.0 -->

# Exact predicate exclusion — finite candidate design

Status: historical candidate design, frozen before implementation and measurement.
The [completed comparison](native-predicate-exclusion-2026-10-19.md) rejected this
finite mechanism after two controls failed. All experimental runtime changes
were archived and removed; the original design is preserved in the evidence packet.
This is investigation 6 of the [conditional-first pass](native-conditional-investigations-2026-10-19.md),
under `PERF-10.predicate_exclusion`. It does not close the owner or establish
accepted performance. No public package or release change accompanies it.

The [frozen protocol](../benchmarks/evidence/predicate-exclusion-candidate-protocol-768.json)
has SHA-256 `e8653d467b11c9d5928b6be5bd2776ff840672b999d3dfaabac6ee3714c5ae5a`.
It controls admission, exact workloads, semantics, resource ownership, failure
tests, comparison and retain/drop gates. The preceding attribution verifies all
1,252 workflows and 2,916 operations; all three predefined repeated-cost targets
qualify in both cohorts. Attribution alone accepts no production mechanism.

## Native boundary

Public resident relational and aggregate collection already call the shared
`PreparedVortexRelational::collect_jsonl` boundary. Memory scans bind the original
typed predicate, then evaluate Vortex-native Boolean arrays and WHERE masks over
complete ordered ranges. The immutable source owner fixes dtype, nullability,
native payload, source extent and session ownership.

Vortex-first classification is `wrap_vortex_concept`: keep pinned Vortex 0.85.0
expression, array, mask and filter providers. Add only ShardLoom ownership,
complete-coverage and operation-completion evidence. There is no new provider,
dependency, external executor, persisted sidecar or decoded Arrow substrate.

## One exact fact per source

For eligible 8,192–65,536-row immutable memory sources, retain at most one fact
that an exactly identified predicate has no TRUE result over the entire source.
The first call still evaluates the native predicate. Later matching scans may
skip its ranges; count and grouped-sum consumers still compute their normal
empty-input results. This does not retain query answers or result rows.

The key is the full typed `PredicateExpr`, compared exactly, and the owning
source is its identity. Admission is limited to the protocol's small Int64
comparisons/membership, binary UTF8 contains and conjunctions. No inference from
a fact to negation, changed parameters, replacement sources, temporal semantics,
access/deletion changes, generated inputs, producer batches or residual filters
is allowed. Every other case keeps ordinary native execution.

Credit the key before cloning it. Each prepared key costs at most 16 KiB, shared
with any retained fact; the source has only one slot. Admission requires a
16-MiB session grant and 64 KiB remaining headroom after the key charge.
Preparation pressure below 96 KiB available evicts the slot. Retained prepared
keys may keep their credit until those owners are dropped. Contended optional
locks may miss; they cannot change results. The source/key graph has no cycle.

## Completion is the publication boundary

A scan stages a provisional fact only after every native range has completed
and every filtered array is empty. At most one fact is staged per operation.
It is published only after complete JSON/schema/certificate finalization and a
final cancellation check succeed. Errors discard it. A successful inner scan,
partial traversal, zero limit, cancelled query or failed late consumer cannot
publish a fact. Other native relational consumers may use a completed fact but
do not publish new ones in this finite mechanism.

Key preparation, lookup, construction, replacement, publication and destruction
remain in their respective compared clocks. Publication occurs after sink
finalization, so its measured tail is added to engine time. Native certificates
report exact excluded scans and rows alongside existing work counters.

## Decision boundary

The protocol freezes 114 cells, complete independent source/result/schema
witnesses, two reversed cohorts and explicit source/parameter replacement
controls. All three 65,536-row/four-call no-TRUE targets must save at least 20%
and 2 ms engine time in both cohorts. Separate complete-workflow benefit and
regression gates, bounded metadata overhead, exact activation and zero-reservation
cleanup are mandatory. A passing comparison still requires full integration
acceptance, including Public39, Full43 and all 18 modular cohorts.

Failure evidence and rejected source are preserved. A rejected finite candidate
is removed without undoing accepted compact-index production. Broader conditional
owners, remaining required work and CG-1 through CG-23 retain their existing gates.
