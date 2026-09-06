# Numeric and Aggregation Performance Continuation

## Corrected control

PR #1433 merged as `f257395bbe5f09215d42e1a57b4e5e473ac9e981`. The measured
control is its second parent `c71a558e879cc24490eb370cc0f6182575bce943`, with
the identical Git tree `10bbaa0ad095e00a65a358c4106196cff8d60406`. This control
includes the ownership, cancellation, count-certificate and native publication
review corrections. It does not reuse C7 timing samples as current-head results.

| Complete public operation | Corrected control |
|---|---:|
| 99,997,497-row ingestion | 97.583357583 s |
| Ingestion peak RSS | 3,334,176,768 bytes |
| Native artifact | 18,643,482,956 bytes |
| Sum of 43 per-query bests, three runs each | 126.743874041 s |
| All 129 process executions | 386.648341662 s |
| Geometric mean of per-query bests | 1.063348363 s |
| Complete result comparisons | 129 / 129 pass |

The guarded ingest uses 24 GB and two requested workers. Queries use 24 GB and
twelve requested workers on the same ten-logical-CPU Apple Silicon machine,
macOS 26.5.1, with Rust 1.98.0. Runs use separate CLI processes and uncontrolled
OS page-cache state. No other large local build or benchmark overlaps these
measurements. The native process clock covers process creation through completed
output and exit; the ingest watchdog's 121-second observation span is not used
as the 97.58-second native operation time.

The new artifact's independently read SHA-256 is
`93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2`, matching
the retained numeric artifact byte for byte. Its path and generation are separate
and recorded. Numeric compression is retained; no new text zoning is promoted.

The 129 public comparisons use complete retained ShardLoom outputs. They are
regression evidence, not an independent correctness oracle. Candidate retention
also requires independent fixtures and adversarial ownership/concurrency tests.
No overall engine-superiority claim follows from this control.

## Observed work and hypothesis

Across 129 executions, the control's numeric-accessor counters report
66,700,699,008 typed payload bytes copied after native primitive execution.
This counter is derived from the accessor's actual processed row counts and
representation width; it is not a hardware memory-traffic measurement. Native
decode and typed-copy time are currently combined and cannot be assigned wholly
to the copy.

For Q7, the native U16 representation is about 200 MB, while the widened accessor
copy is about 800 MB. Q10 reports about 2.99 GB copied per execution; Q17 reports
about 1.18 GB, including its admitted passes. Eliminating this copy must preserve
typed kernels, native validity, exact integer identities, native ownership and
configured-context forwarding. Admitted-buffer credits and pinned provider
allocation gaps are separate evidence. Native decompression remains real work.

Native accessor call/row totals are not coverage-equivalent between snapshots.
The control's Filter-over-host-Primitive shortcut applied its mask into a typed
vector while returning default numeric work. The candidate routes eligible
filtered arrays through instrumented native Primitive execution, so the counter
includes newly observed filtering/canonicalization as well as compressed
decoding. Increased calls or call-rows alone do not establish additional source
scans, query passes or compressed-decode work. No per-array trace attributes all
observed query deltas to this instrumentation change.

Q34 and Q35 each reconcile 18,342,019 complete groups. The frozen corrected control
claims a shared entry counter for every new group and shares a comparison counter
between partitions. The unmeasured candidate reserves at most 1,024 entries per
block and publishes local comparison totals at reconciliation boundaries.
Reserved credits and actual
committed groups remain distinct; entry-credit changes do not replace byte
reservations for tables and string storage.

Claim/return counters count block bookkeeping operations; granted/refunded
counters count entry credits. After drain, granted minus refunded equals committed
groups and reserved credits must be zero. Wait counters count condition-variable
wait attempts, including timed or spurious wakes; they are not worker counts or
elapsed time. Comparison totals count actual matching-hash byte comparisons,
including rechecks after releasing the partition lock for credits, so concurrent
runs need not have identical totals. Comparison publication counts are nonzero
boundary flushes.

## Evidence locations and current status

- Frozen control binary: `clickbench-100m-uat/binaries/control-c71a558e` under
  `/Users/dylan/LocalData/shardloom/`; SHA-256
  `4f542e41d2da57cdc7f8807b15b75f25d7f703bb19ac6502ac24e833d66be6b8`.
- Ingest log: `clickbench-100m-uat/logs/ingest_cli_uat_gated_20260906T011503Z`.
- Query log: `clickbench-100m-uat/logs/full43_20260906T011906703636Z`.
- Packet: `/Users/dylan/LocalData/shardloom/perf-next-20260906/`, including binary
  and artifact manifests, extracted query work, and lossless archive receipts.

Two earlier 16 MB ingest stdout files were losslessly compressed with complete
round-trip SHA verification before the query control, preserving the existing
log-budget guard. Their archive paths and original hashes are retained in the
packet. Query result references and source files remain available.

Candidate implementation and retention measurements are in progress under
`../architecture/perf-numeric-aggregation-2026-09-06.md`. This control alone does
not establish a new optimization gain or close any whole PERF packet.
