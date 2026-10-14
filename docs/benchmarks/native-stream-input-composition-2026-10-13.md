<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native streaming input composition acceptance

Status: local acceptance complete; hosted integration pending. Complete public
acceptance, workspace/native tests, Full43 regression and documentation pass. This unit covers
multiple finite producers, declared repeated-source replay, dynamic streaming
pivots and their shared preparation lifecycle. The
[architecture contract](../architecture/native-stream-input-composition-2026-10-13.md)
defines the implementation and limits. Whole PERF and CG gates remain open.

The [fresh main UAT](native-fresh-uat-2026-10-13.md) ran before implementation,
as requested. It records the fresh ingest, all 43 queries and the modular
format/workflow matrix. The acceptance below tests the subsequent composition
changes on the same 99,997,497-row data content without another ingest.

## Public and resource acceptance

The final release executable passes **757 cases across all 19 streaming-input
families** through the public Python/SQL transaction. The families are input,
growth, typed input, ordering, ordering pressure, aggregation, aggregate pressure,
joins, join pressure, windows, window pressure, protocol, preparation, multiple
producers, multiple-producer pressure, dynamic pivot, dynamic-pivot pressure,
replay and replay pressure. Their guarded stage takes 161.570107 seconds;
individual case clocks sum to 150.724183 seconds. These are acceptance timings,
not comparisons or operator-throughput claims.

Multiple producers have independent demand ordinals and explicit completion.
The matrix covers all seven join kinds, mixed resident/file inputs, unequal and
empty batches, failures in either producer, complete native writes and reopen,
and protocol errors after one source finishes. Repeated references consume a
producer once and preserve its complete required schema for later scans.
Self-joins, three scans, two repeated sources, windows and dynamic stages retain
exact values and source order. Replay ends with the execution.

Dynamic pivots discover their schemas and complete native state during execution.
The matrix checks empty domains, successive pivots, joined streamed pivots,
sum/count/mean/min/max/first-unique, downstream binding failures, and exact
integer, decimal, temporal, binary and nested native write/reopen values.
Compatibility preparation shares the transaction's cancellation owner and native
allocation, including cancellation before source access and after input creation.

Pressure acceptance pairs resident denial with native-spill completion under
the same grant. Dynamic pivot checks 24,580 input rows and all 6,145 output rows
at 16 MiB. Replay checks all 8,193 rows of 2,048-byte text, streamed and written
to Vortex then reopened, also at 16 MiB. Native tests exercise actual operator
process death, refusal to clean live or unknown owners, owned cleanup and a fresh
exact restart. This is cleanup/restart evidence, not execution resume.
Quota exhaustion, real run corruption, producer failure, early consumer close,
unchanged existing destinations and final-owner release remain part of acceptance.

Complete write/reopen testing exposed two memory regressions. Small result
fragments accumulated excessive writer footer entries; credited native batching
now bounds that growth and reports actual copy bytes and adapter scalar-leaf
constructions. Materialized readback now admits the requested row range before
payload work. A one-row delivery regression proves fixed split metadata under a
512-KiB grant for a 65,539-row file. Native array transfer retains its larger
source work units and original physical-copy limit. Grants and existing transfer
limits were not increased to make these checks pass. Failed and superseded
attempts remain in the evidence record.

## Final Full43 observation

| Observation | Result |
| --- | --- |
| Complete public query executions | 129/129; three new processes per query |
| Complete result rows independently reopened | 1,143; values and schemas checked |
| Sum of complete native process times | 214.675706 seconds |
| Supervised query-suite wall time | 268.283199 seconds |
| Sum of native child user/system CPU time | 627.266577 / 113.824937 seconds |
| Largest observed native process RSS | 5,751,406,592 bytes |

The dataset is the retained official ClickBench-derived native Vortex file,
15,713,610,513 bytes, SHA-256
`004f9639fa0aa527b70f326c366c37b1173dceeacb92b50b2d8fd02957155c18`.
The complete reference packet has SHA-256
`cb83c770674073f31ad0c4224f02faeb1ba557c512595a5c2d36f92bb2344b39`.
It is retained native regression evidence; it is not a fresh independent oracle.
Finite floating outputs use the existing `1e-12` comparison tolerance.

The host is Apple Silicon, macOS 27.0 arm64, with 10 reported CPUs and 16 GiB
physical RAM. Query admission is 24 GiB and 12 lanes; this is not a process-RSS
limit. The release build uses Rust 1.99.0, pinned Vortex 0.85 and
`release-user-surfaces`. Builds, tests and timed workloads run sequentially
under the local guards. Source hashing precedes timing; OS cache state and
ordinary desktop activity are uncontrolled. Native clocks include process
creation, complete public output and exit. The supervised clock additionally
includes the runner's per-call validation and log archival, and excludes initial
source hashing and subsequent independent evidence readback.
Host-wide load was not sampled for this final regression; per-process CPU and
RSS samples are retained.

The earlier baseline records 177.372562 seconds summed query time. These two
observations are not paired or cache-controlled, so they establish neither a
speedup nor the cause of the timing difference. Per-call wall/CPU/RSS, raw output,
timing/PID companions and every result remain available. Source and input
identities, no-fallback certificates, resource declarations and complete process
drain pass their final checks.

## Reproduction and evidence

Required formatting and default-workspace Clippy pass, along with write-feature
and read-feature Clippy. The default workspace passes 3,219 tests across 106
test groups. The final native-feature suite passes 4,145 tests across 92 groups
with no failures; 27 intentional exclusions comprise three non-Unix checks
assigned to Windows CI and 24 manual benchmark, fixture or workload-dependent
checks. The guarded native suite takes 925.842625 seconds and drains all children.
Its exact command is:

```sh
cargo test --offline --locked -p shardloom-vortex -p shardloom-cli \
  --all-targets --features vortex-local-primitives,vortex-write,universal-format-io \
  -- --test-threads=2
```

The accepted release executable is 75,265,616 bytes with SHA-256
`93e6976f618abd276b02b897e5be4c9fda0321b2f4d48b65bbc29252497b4b75`.
Its source manifest freezes 2,236 selected files on base
`ed253d1ec74b724215f71f0b410123656e596696`, including the composition changes.
The manifest has SHA-256
`d385601e233e879659f5bf15c252eb1e10a03e6557fc9c096171094be7799281`.
The base revision alone does not identify the changed source; the frozen file
identities and final PR do.

The repository runner is `scripts/run_native_streaming_input_uat.py`. Supply the
matched release executable, a new local-only UAT directory and
`--operation-timeout 600`; omitting `--family` runs all 19 families. Full43 uses
`scripts/run_clickbench_query_uat.py`, the retained native input and reference,
24 GiB, 12 lanes and three repetitions. The executed admission/driver records
carry the exact arguments, source identities and unchanged storage ceilings.
Do not rerun a historical driver against completed output paths.

All 15 documentation gates pass, including reference/status/governance checks,
Astro check/build, search policy and static readiness. The final text refresh
also passes the reference, public-status, backlink and whitespace checks.
Rendered Python, runtime/I/O and limitations guides pass desktop/mobile review,
light/dark reading, menu and internal-link checks. Pagefind returns five results
for `replay`, including all three changed guides; its Python result reaches the
batch-consumption section. Code and table overflow stay inside their containers.
The first documentation attempt's missing v1 matrix row and the rendered
preparation-wording correction remain in the evidence history.

The [portable evidence index](evidence/native-stream-input-composition-2026-10-13.json)
identifies the verified archive and compressed manifest. The packet retains
successful, failed and superseded receipts; complete public logs and frozen
source identities; all final Full43 outputs, references and timing companions;
and documentation logs, snapshots and screenshots. Bulk public output files and
the release executable are hash-identified rather than included. Every archived
member and the copied packet bytes are checked against their recorded identities.
The fresh baseline has its own linked packet. These records do not substitute
for hosted integration or a new independent correctness oracle.

Published version 0.5.1 is unchanged.
Broader memory accounting, spill-family recovery, remaining destinations/fanout
and conditional investigations retain their separate phase-plan obligations.
