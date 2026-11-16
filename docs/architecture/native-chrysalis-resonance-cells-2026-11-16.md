<!-- SPDX-License-Identifier: Apache-2.0 -->

# Partial native coverage and shared live computation

## Decision and scope

The November 16 maintainer request schedules **Chrysalis Cells and the Chrysalis
Map**, followed by **Resonance Cells and Resonance Leases**, after rich-data and
analytical acceptance and before portable serverless/Foundry integration. The
[phase plan](phased-execution-plan.md#planned) owns execution order. These are
proposed architectural components under the existing source, shared-execution
and resource owners, not implemented capabilities or new phase/CG identifiers.
Classification: `v1_candidate_pending_feasibility` for the new performance
mechanisms; the maintainer explicitly requests their bounded design and
implementation investigations. Retention still requires their prospective
correctness, resource and complete-workflow gates. No speedup is presumed.

This leaves the active direct join-to-aggregate work independent. RFC 0044 and
RFCs 0013/0014/0017 govern execution, ownership and recovery; RFCs 0031/0033
govern I/O and user workflows. CG-1 through CG-23 remain independent.

## Chrysalis: exact partial native coverage

A Chrysalis Cell identifies an immutable source generation, bounded row region,
field set, interpretation/validation policy and available physical
representations with their memory or artifact owners. Realization changes
representation without changing logical coordinates. The Chrysalis Map resolves
exact requested coverage across original-source and completed native cells.

Start with one immutable local Parquet source, natural row-group boundaries and
overlapping column demand. Both explicit preparation and eligible execution
use the same realization machinery. Capillary units bound the work; PulseWeave
governs its scheduling within caller ceilings; Vortex supplies native arrays,
encodings, layouts, scans and writers. Extend existing source/provider owners.

Before runtime implementation, write the targeted RFC and verify pinned Vortex
provider options. Specify interfaces for coverage lookup, realization admission,
validation, atomic publication, acquisition and release. Distinguish in-progress,
validated-resident and committed-fragment states. A completed filtered selection
cannot masquerade as whole-region coverage; cross-column coordinates and order
must agree. Regional capability evidence cannot establish global uniqueness,
ordering or complete aggregate statistics.

Durable publication requires an explicit storage policy. Ordinary reads must
not create an unbounded persistent cache. Cancelled or incomplete work cannot
publish reusable coverage. Source changes invalidate dependent coverage before
use. Bound map fragmentation, metadata, overlapping representations and retained
bytes. A partial artifact remains source-dependent; a self-contained export must
complete its dependencies. Preserve the existing single-artifact preference;
any fragment/map persistence design needs an explicit format and commit contract
before adding sidecars or a new artifact family.

Compare complete workflows on matched immutable inputs: ordinary direct
execution; full explicit preparation plus execution; partial realization plus
execution. Charge all creation, validation, publication and subsequent reuse
costs, with engine clocks identified separately and consistently. Freeze the
affected workload before measurement. Include single-use, disjoint-column,
full-width, empty/missing coverage, source-change, cancellation, publication
failure, reopen and memory-pressure controls. Prove work avoided with actual
reads/decodes/preparation evidence; timing alone does not prove the mechanism.

## Resonance: bounded shared live preparation

A Resonance Cell owns one bounded immutable native input region, charged backing
allocations, admitted consumers and compatible pure preparation/expression work.
A Resonance Lease defines a consumer's lifetime and resource rights. Selections,
mutable aggregate/join/ranking state, cursors, errors and results remain per
consumer. PulseWeave continues to own scheduling.

Start with two explicitly composed consumers over validated immutable native
input where attribution demonstrates costly repeated preparation. Share live
work while regions flow; do not require a complete-source intermediate before
consumers begin. This differs from completed immutable segment reuse and the
[rejected eager shared-producer screen](concurrent-source-reuse-screen-2026-09-26.md).
Do not rerun that rejected mechanism unchanged.

The targeted RFC must define compatible domains and policies, demand-scoped
validation, lease acquisition/release, cancellation isolation, bounded slow-reader
retention, admission refusal and drain. One consumer's cancellation or LIMIT
completion cannot terminate peers. Errors outside a consumer's demanded domain
must not automatically fail that consumer. Charge shared allocations through
their last owner. Share only pure work with proved equivalent observation and
numerical semantics. No automatic cross-request coalescing is implied.

Compare complete independent versus composed workflows, including a single
consumer, disjoint demands, mixed completion times, slow consumer, cancellation,
late failure and memory refusal. Preserve independent complete results and
ordered updates; report actual preparation counts, retained ownership and
per-consumer completion. Do not reinterpret the historical sequential benchmark
as a shared workload. Exact vector consumers are a later application of the
same contract, with independent exact accumulation/ranking and no new arithmetic
or approximate-search claim.

## Delivery and acceptance

For each component: ground the current owners/provider APIs, record the RFC and
bounded interfaces, freeze an impact-based cohort, implement through shared
owners, prove activation and complete semantics/resources, then apply the
complete-workflow retain/drop rule. Preserve every observation and regression.
Reuse the existing validation and benchmark harnesses; do not create another
full-corpus campaign or status framework. Targeted source/public-interface tests
precede required format, workspace lint/test, feature and installed-package checks.

Update truthful diagnostics, capabilities and examples only with implementation
evidence. Keep ordinary public APIs simple and equivalent interfaces on the same
native owners. Both components preserve Vortex-native I/O,
`fallback_attempted=false` and `external_engine_invoked=false`. No external query
engine, deployment, automatic persistence, package publication, distributed
execution or production/superiority claim is authorized by this queue addition.
