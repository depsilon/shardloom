<!-- SPDX-License-Identifier: Apache-2.0 -->

# Survivor batching admission

Status: the finite admission screen is complete. **Decline the unconditional
callback adapter before performance measurement.** This resolves the batch-contract
prerequisite under structural investigation I, item 20 and RFC 0044; broader I
attribution and a separately admitted narrow candidate remain open. No production
runtime change or performance result is accepted.
The accepted source at the start is `236dbccd590a301ede8f34cce09715be92f025e5`;
its production engine is the accepted source-domain implementation. The
[metadata/consumer queue](native-metadata-encoded-consumer-investigations-2026-11-13.md)
owns sequence. The earlier text-signature drop remains unchanged.

## Question and scope

Can the existing native compaction providers reduce actual downstream
preparation after selective filtering or joining while preserving the current
source, error, demand and allocation contracts? First run an executable
admission screen, before a performance candidate. A smaller emitted-batch count
does not answer this question by itself.

The expert comparator is a vectorized execution maintainer reviewing observable
failure order, encoded representation and allocation lifetime together. The
current push callback returns only `Result<()>`; the producer does not receive
the downstream expression domain, outstanding demand or application effects.
The following properties therefore need proof rather than assumptions:

- Combining rows must remove real key/expression/aggregate preparations.
- Source row order and multiplicity, nulls, strict floating observation and
  expression failure order must survive a change in batch boundaries.
- Delayed callbacks must not pull extra effectful source batches or retain their
  borrowed input owners. Completion obligations remain distinct from demand.
- Queue admission must charge whole retained backing allocations and overlap,
  not just the visible bytes of a slice. Optional compaction must release its
  owners after consumer failure, cancellation and denied growth.

## Vortex-first provider check

The pinned provider is Vortex 0.93.0. Its `ChunkedArray` can combine independently
encoded arrays without copying their values at construction. Multiple chunks
requested as canonical primitive or UTF-8 data go through a native builder;
logical concatenation alone does not establish zero-decode consumption.
`ArrayRef::nbytes` traverses visible buffer lengths. It is not a certificate of
the complete allocations retained by sliced views.

ShardLoom already owns `native_relational_output_buffer::OutputBuffer` for
completed, independently owned records, and replay `Capture` for physical
numeric/Boolean fragment copies. Reuse those concepts in the screen; do not
invent a second chunk container, compactor or allocator. The test-only decision
is `use_vortex_native_provider` through these existing native boundaries. There
is no new production provider admission. No Arrow execution, query-engine
integration, external residual evaluation or fallback is permitted.

## Frozen admission screen

Preserve the original test source, source identities, command and every failed
attempt. The screen has no timing or minimum-gain decision and performs no
Full43, full10k or competitor repeat.

1. Run complete native filter/project/aggregate and join-chain requests with
   sparse, dense, tiny, null and wide data. Count actual preparations using the
   existing debug-only owner observations. Keep original fixture geometry,
   logical layout zones, successful positional reads and consumer invocations
   separate; an unavailable boundary is not zero.
2. Compare direct consumer delivery, an array-list forwarding control, native
   logical concatenation and physical `OutputBuffer` compaction. Require exact
   ordered values and count actual key preparation. Identify canonicalization
   and copies where they occur; do not call wrapper construction a saving.
3. Execute competing-error cases across original input batches, callback/source
   failure and demand controls. Test both ordinary delayed flushing and flushing
   pending rows before propagating a producer error. A corrected final error
   does not undo an extra observable source callback.
4. Retain small slices of separately credited large primitive sources. Measure
   visible bytes and actual live credits independently; check release after
   flush, failure and cancellation, plus tiny/dense and low-grant behavior.

An unconditional callback adapter is declined if any semantic or ownership
obligation fails. That outcome does not reject all possible survivor batching.
A narrower candidate needs a source/consumer admission predicate demonstrated
against these controls and actual workload exposure before its own frozen
complete-operation comparison. A barrier that simply forwards every original
fragment is a correctness control, not a successful batching optimization.

## Compatibility and remaining evidence

This screen changes tests and evidence only. Existing callbacks, source demand,
batch sizes, writer defaults, public syntax and release artifacts retain their
current production implementation. The existing accepted batch contract remains
the basis for later FSST work unless a distinct candidate passes admission and
its complete performance comparison.

## Observed outcome

All eight complete native requests pass: seven filter/project/grouped-aggregate
cases (sparse, zoned sparse, dense, tiny, nullable, wide and empty) and one sparse
join chain. Their 40 complete output rows, ordered values, field names, widths
and nullability are independently checked from the saved observations. The
sparse filter processes 16 surviving text values but prepares its text expression
and aggregate values 16 times. The sparse join chain prepares 34 key columns and
emits 16 final batches. These are finite work witnesses, not a timing comparison
or a claim about every saved workload.

| One-column preparation control | Consumer calls | All observed key preparations |
| --- | ---: | ---: |
| Direct original fragments | 16 | 16 |
| Array list forwarded unchanged | 16 | 16 |
| Native logical concatenation | 1 | 1 |
| Existing physical output compaction | 2 | 2 |

Every control returns the same 16 ordered values. The accounting includes key
preparation inside compaction as well as the final consumer. Logical
concatenation requests canonical primitive data at the consumer; the physical
path copies through the existing native output buffer. Neither is a proved
zero-decode or timing improvement.

Two semantic counterexamples prevent unconditional admission. Combining two
original batches changes the first projection failure from signed arithmetic
overflow to division by zero because evaluation visits expressions within a
batch. Flushing pending records before propagating a later producer failure
recovers the original final error, but still invokes the source twice instead
of once. A flush-on-error repair therefore does not preserve the callback contract.
The callback signature does not supply a proof of source purity, consumer
infallibility, outstanding demand or owed completion. A narrower route must
establish those facts rather than inferring them from the absence of a failure
in these successful fixtures.

Eight one-row slices expose 64 bytes while keeping 2,099,328 bytes of source
credit live, including at least 2 MiB of original value payload. The screen
checks release after flush, consumer failure and cancellation. Existing native
output-buffer and source-completion tests also pass for low headroom, denied
compaction, bounded regrowth, retained output, late errors and demand limits.
Visible `nbytes` cannot serve as a generic backing-allocation admission bound.

Logical layout zones, segment requests, completed positional read requests and
bytes, root output batches and owner preparation counts remain separate fields.
The zoned fixture has 32 logical zones of 256 rows. Logical zone exclusions and
pinned backing bytes at the full-request consumer remain unavailable; the
separate slice experiment does not fill those gaps. Positional bytes do not
measure device traffic. No engine time is recorded or inferred.

## Validation and portable evidence

The [portable packet](../benchmarks/evidence/native-survivor-admission-2026-11-15/README.md)
preserves all source versions, original failed attempts, complete observations,
commands, source inventories and literal provider excerpts. Formatting and
strict default/native workspace lint pass. Default workspace tests pass 3,344;
native workspace tests pass 7,261 with 39 existing ignored tests. All seven
focused admission tests pass. These tests add no production observation hook.

Saved replay checks 13 observation records, eight existing boundary controls,
47 literal inventory excerpts and the test-only source diff. A relocated replay
denies original checkout/evidence reads, engines, subprocesses and networking;
all 22 altered-evidence controls pass. Two initial packaging checks and the
first relocation check failed in evidence tooling, and remain preserved. The
relocation correction loads Python's archive decoder before enabling the
external-read barrier; it changes no native observation or admission decision.

The existing accepted batch contract remains in force for FSST investigation.
This result closes the unconditional-adapter screen, not all survivor batching.
No completion of I's broader investigation, item 19, item 20, RFC 0044 or
CG-1 through CG-23 follows from it. No writer default, public API, package
version, release or fallback policy changes.
