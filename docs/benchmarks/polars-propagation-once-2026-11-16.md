<!-- SPDX-License-Identifier: Apache-2.0 -->

# One Polars observation on the saved propagation requests

The November 16 maintainer request authorizes one Polars pass over the exact
11,579 saved propagation requests. Every request executes once: **11,489 pass
and 90 produce incorrect results through the benchmark adapter**. No request
is retried, no native query is executed, and no prepared-lifecycle operation
is added. This is a complete observation with retained failures, not an
accepted complete-set competitive baseline.

The passing 11,489 requests total **101.07273444440216 seconds in Polars
`collect` calls**. The exact same saved native request IDs total
**888.133524476 engine seconds**. These clocks have different boundaries;
there is no speedup ratio or causal comparison. The native observation is the
November 15 runtime at `3cec5a410b24757137031833cdfe20dae1e9b54c`, not the
latest development runtime. Its original `not_admitted` performance/work
decision remains unchanged even though its complete fresh results pass.

[Propagation context](../architecture/native-optimization-propagation-2026-11-15.md)
· [Portable evidence](evidence/polars-propagation-once-2026-11-16/README.md)

## Frozen population and execution

The population contains 5,309 original declarations and 6,270 mechanism and
composition declarations: 1,380 dictionary, 3,510 text-signature, 285 direct
COUNT and 1,095 propagation cases. Their exact ordinals, IDs, independent
references, schemas, ordering and float policies are unchanged. Original
native interface and encoding labels retain every slot, including equivalent
logical requests. Polars runs through one lazy Python adapter; these labels
do not imply five different Polars interfaces or Vortex encoding execution.

The environment is Polars 2.1.0, PyArrow 25.0.1 and Python 3.13.13 on macOS 27
arm64, Apple M5, ten physical/logical CPU cores and 16 GiB physical RAM.
Polars uses two threads, `engine="auto"` and its default optimizer. Each slot
uses a fresh process and exactly one complete `collect`. There are no warmups,
timing repeats, cache manipulation, persistent result cache or artificial
pacing. Host and filesystem cache state are uncontrolled.

All other task-owned work remains idle during query execution. The standard
bounded process supervision, exclusive cohort lock, per-query deadlines,
storage limits, result transport and process cleanup remain enabled. Full
hashing and recursive evidence inventories run before/after execution; active
checks inspect bounded metadata and processes. The pass binds 1,075 frozen
files, 76 Polars input files and 132 native source counterparts.

Existing IPC/Parquet declarations use their original physical input or their
already-declared counterpart. Thirty-six additional IPC fixtures are generated
from the frozen literal populations; every row is checked independently and
bound to all 72 earlier complete Vortex readbacks. These substitutions are
explicit. They preserve logical inputs, not identical physical encodings.
ShardLoom query answers are never used as input fixtures.

## Correctness failure and retained boundaries

All 90 failures have one cause in the external benchmark adapter. A relational
constant projection must preserve source row count, but its Polars declaration
uses `source.select(literal)`, which produces one row. The saved optimized
plans replace the scan with a one-row constant frame. Subsequent grouped COUNT
therefore returns 1 instead of 257, 100,003, 1,000,000 or 5,000,000.

The affected population is exactly 30 signed/unsigned constant-count requests,
30 rename/reorder compositions and 30 duplicate-column compositions. A complete
structural inventory finds those same 90 constant-only projections and no
additional exposed requests. Saved plans, source cardinalities, complete
expected/actual values and unchanged reference hashes establish the cause.
This is an adapter declaration defect, not evidence of a Polars engine defect.

No output is repaired, expectation rewritten, tolerance loosened or failure
reclassified as passing. The frozen adapter remains part of the historical
record. A future corrected observation would require a row-preserving
declaration, explicit zero/one/many-row correctness checks and separately
authorized execution. This one-pass request does not authorize that rerun.

Every passing result is rechecked for complete values, row multiplicities,
ordering, names, field order and logical types. Polars does not expose the
same nonnullable-field contract; the original explicit nullability boundary
is retained, with actual NULL values still checked. The original independent
references are preserved for all requests, including failures.

## Clock and resource interpretation

Polars `collect_call_seconds` covers its single lazy collection, including
optimization and input I/O, and excludes declaration and Arrow/Python-row
delivery. `engine_execution_seconds` remains null for every Polars result.
No process, supervisor or workflow duration substitutes for an engine clock.
The archive retains all other clocks under their original diagnostic labels.

| Population | Passed / attempted | Polars collect seconds | Historical native engine seconds |
| --- | ---: | ---: | ---: |
| Complete declared set | 11,489 / 11,579 | Unavailable as a valid complete total | No paired complete total |
| Passing subset | 11,489 / 11,489 | 101.07273444440216 | 888.133524476 |
| Original declarations | 5,309 / 5,309 | 75.7736782906577 | 774.008899298 |
| Dictionary declarations | 1,380 / 1,380 | 2.0245632115984336 | 2.193434837 |
| Text-signature declarations | 3,510 / 3,510 | 4.363330613123253 | 4.140327729 |

These are different clocks and, for substituted formats, different physical
input representations. Rows in the table do not establish engine superiority,
repeatability or performance of the latest native build. Passing-subset timing
does not promote the full population into an accepted baseline.

The 1,773 periodic process-group samples stay within the frozen 2.5 GiB RSS
threshold, and storage/output guards do not raise. However, **69 per-child OS
high-water observations exceed that sampled threshold**, with a maximum of
5,302,255,616 bytes. They include the complete worker's harness, declaration,
query and delivery work; they are not an isolated Polars engine-memory metric.
All exceptions remain visible. This is not proof of a hard 2.5 GiB peak cap or
an identical memory boundary between engines.

## Verification and development use

Offline replay reopens all 11,579 Polars packets, frozen jobs and independent
references; reconstructs every relation/contract/source mapping; verifies all
raw member hashes, exactly one collection, clocks, supervision and source
identities; and reproduces the 90 failures. A separate extraction agrees on
all records. All 11,579 historical native packets are also reopened and their
complete values, schemas, no-fallback fields and saved clocks rechecked.

Thirty-five positive packets, selected by minimum request ID per declaration
kind, native interface and saved outcome, include empty results and retained
failures. All 1,120 deliberately corrupted variants are rejected. Engine
imports, subprocesses and network execution are forbidden in this verification.
Original preparation and offline-verifier failures remain preserved separately;
they never cause another scored query execution.

Original and relocated portable replay both pass, reproducing all 11,579
Polars outcomes, all 11,579 historical native results and all 1,120 corruption
rejections. The exact archive transport also rejects 24 malformed packets
and passes its byte-readback/path-alias control. The package contains complete
raw ledgers, independent references,
source bytes, input-equivalence/readback proofs, original failures and clocks.
Bulk inputs and installed engines are identity-only and are not reopened by
portable verification. Run `python3 -I -B packet/verify.py` with Python 3.13 or
later; it executes no workload queries.

One synthetic transport-test attempt used the wrong expected exception for
a missing archive part. Its log and receipt remain, but four failed/intermediate
test-source and report variants were overwritten before preservation. The
provenance record identifies this gap. Final controls bind their exact source
bytes; every scored query source, result, expectation and timing is retained.

Use these saved observations as evidence for the existing material-potential
screen, with their missing full-set baseline and clock boundaries explicit.
For example, all 2,697 join/multiway requests pass; their historical native
complete-query engine total remains 500.383376588 seconds. This is not exclusive
join cost. The separate resident diagnostics are needed to identify removable
work before choosing the next shared-owner change. No broad repeat, CG closure,
package publication or change to the Vortex-native/no-fallback contract follows.
