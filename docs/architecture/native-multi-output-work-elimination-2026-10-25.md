<!-- SPDX-License-Identifier: Apache-2.0 -->

# One computation for an explicit multi-output request

Status: dropped with the entire item-15 batch at the maintainer's request.
The implementation is removed; public/installed-package and finite multi-output
performance acceptance were not completed. The following is the historical
candidate contract, preserved with its tests and failures in the
[drop record](../benchmarks/native-workflow-drop-2026-10-25.md).
This extends package 1 in the
[workflow handoff](native-workflow-work-elimination-2026-10-25.md) after the
counted-join and computed-measure finite decisions. RFC 0044, RFC 0033 and
RFCs 0013–0017 retain execution, resource, correctness and publication ownership.

## Existing boundary and chosen extension

`PreparedVortexRelational::write_many_controlled` currently invokes the ordinary
writer for each destination. Retained sources and the budget are shared, but
computation repeats. The existing `write_many_with_input` path already computes
a batch-input plan once and stages all outputs from a credited native result.
Promote that owner to retained file and memory sources with an optional input
provider. One destination keeps direct streaming. Multiple destinations use
the existing native replay recorder and independent output adapters.

Pinned Vortex 0.90.0 supplies native arrays, allocation owners, file streams and
writers. ShardLoom's existing native result, replay and publication boundaries
already coordinate these providers. This is `use_vortex_native_provider` through
the existing local-primitives/write feature gates and execution/I/O certificates.
There is no new executor, output representation, dependency, global answer cache
or worker pool. Vortex remains native; compatibility writers retain their
existing fidelity/type limits and `fallback_attempted=false`.

The native primitive routes are part of the same completion unit. Their existing
count, unary, aggregate, sort, partitioned and relational providers must produce
once through native bounded delivery. Do not route them through the small owned
collector, which has independent row/byte limits and spill exclusions. Preserve
provider selection and complete source/generation validation. The existing batch
input path must continue to consume every declared producer exactly once.

## Ownership, failure and compatibility

Admit targets and adapters before demanding input. A complete native result is
retained under the original grant; without spill permission, exceeding that grant
is an explicit resource refusal. With explicit spill, replay uses native runs,
the declared disk quota and owned cleanup. No source replay or strategy retry may
follow a failed reservation. This retention requirement is now explicit for
repeatable sources too; single-output streaming remains available without it.
The relational policy owns one run store across producing operators and output
replay. Existing primitive aggregate/sort spill declarations admit only their
operator. Their output replay remains resident under the same grant; do not
duplicate an operator's disk quota into an independent replay allowance. A
shared primitive disk-quota extension requires separate owner work. This
distinction must appear in pressure tests and public limitations.

Every producer must finish and every adapter must complete privately before
publication. Release replay owners and finish native cleanup before certifying
success. Revalidate all source generations and targets before publishing. A late
publication failure preserves already published complete outputs and identifies
them for recovery; independent files do not become an atomic group. Preserve
foreign files and existing destinations on failure. Sampling preparation begins
once per request and remains provisional until publication succeeds.

Use the existing `WrittenVortexRelationalFanout` result: one execution certificate,
an ordered output list and optional replay observations. Coordinate the internal
Rust `write_many`/`write_many_controlled` return-type change and their callers;
the old vector of per-destination execution reports cannot describe this contract
without duplicating credited metadata. These workspace Rust crates are not
published APIs. Python/CLI method names, arguments, output order and per-target
publication fields remain stable. Existing execution-count and strategy fields
must report one computation and the actual replay mode. No second public method
is introduced for an alternative implementation.

## Required proof

Check complete ordered readback and exact schemas for all eight existing writers,
file and memory sources, typed empties, nulls, duplicates, composed joins and
aggregates, dynamic schemas and independent repeated requests. Prove exactly one
execution per successful multi-output request; a later independent request must
compute again. Cover single-output control, preflight refusal before demand,
late producer/schema/generation errors, later-adapter failure, cancellation,
publication failure, sampling-model invalidation, reservation denial and explicit
native spill. Returned reports and abandoned/failed calls must release credits.

The finite comparison must include an expensive composed computation with
multiple outputs, a cheap/count control, a single-output control and output that
exceeds the small collector's limits. Freeze its source/inputs, complete native
write boundary, separate query/output observations, paired schedule and target/
control gates before any comparison call. A lower execution counter alone is
not a whole-workflow timing claim. Preserve every result, failure and refusal.

Final acceptance also requires the public SQL/DataFrame and installed-package
routes, the required native/default checks, portable evidence, and the coherent
package's isolated Full43 before PR integration. Explicit shared branches and
guarded decorrelation follow under their own semantic admission contracts.

## Existing single-output resource limit

The retained-file pressure fixture has 65,537 strings of 256 bytes in separate
251-row native leaves. Relational single-output streaming completes under its
16 MiB grant, while both existing primitive direct Vortex and Parquet writers
refuse reservations under that grant. The primitive projection's singleton
dispatch, scan and output implementations are unchanged by this fanout work.
Its pressure test therefore proves refusal without reexecution, publication or
leaked owners; it does not claim that all singleton providers fit this grant.
Single-output behavior remains covered by the ordinary provider controls.
The original failed observations are retained. Allocation attribution and any
change to primitive scan/output buffering belong to the I/O performance owner;
this work does not increase production grants or alter those writer envelopes.
