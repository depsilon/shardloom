# Native operator investigations, November 3, 2026

Across 55 matched case-route requests, total ShardLoom engine time fell from
**82.719 s to 49.195 s**. All 110 results matched the independent expected
schemas, values and ordering. The complete-cohort gain rule retains the
candidate. A separate 5M-row prepared group recovered from its original RSS
failure. The release and Polars observations for the original 10,000 cases
remain the corpus baseline. [Original study](native-expanded-query-2026-11-03.md) ·
[Nine investigation dossiers](https://github.com/depsilon/shardloom/pull/1653#issuecomment-6615541800).

| Matched investigation | Requests | Parent engine s | Candidate engine s | Faster / slower |
|---|---:|---:|---:|---:|
| Membership demand | 33 | 17.932 | 6.272 | 26 / 7 |
| Group state and finalization | 9 | 1.263 | 0.979 | 3 / 6 |
| Window and final ordering | 13 | 63.523 | 41.945 | 9 / 4 |
| **Total** | **55** | **82.719** | **49.195** | **38 / 17** |

The selector bound 54 distinct case IDs, including two declared alternate
routes. It covered available primary and held-out membership shapes, prepared
group and window controls, and named 5M cases. The protocol records the
requested cells missing from the original design. Parent `0e8f5079` and
candidate `9a8dd89c` used the same release features, original worker source,
input generations, public interfaces, two threads and independent expectations.
There was one fresh process per request and role. The first role alternated
by case-route ordinal. Each execution had a 120-second deadline, a 2 GiB native
budget and a 2.5 GiB process-group RSS limit. Full input hashes and recursive
inventories ran outside scored execution. Each worker reported native execution
and its fallback fields. The comparison estimates the combined change on this
specific cohort. [Protocol, source and raw observations](evidence/native-expanded-query-2026-11-03/operator-investigations/index.json) ·
[Results by case and route](evidence/native-expanded-query-2026-11-03/operator-investigations/results.json).

## Source demand through bound membership

For `v2_subquery_membership_d377362103f6111c`, the empty inner relation and
grouped outer query returned the same empty result. Both builds delivered
5,000,000 scan rows. The binder now carries the outer consumer's columns,
probe keys and guard fields through statically bound membership. It carries
the build keys into the inner plan, whose filter keeps its own inputs.

| Measurement | Parent | Candidate |
|---|---:|---:|
| Engine s | 6.411 | 0.662 |
| Projected / available source columns | 39 / 42 | 4 / 42 |
| Reported data-read bytes | 539,320,433 | 37,186,908 |
| Copied columnar bytes | 1,042,256,238 | 30,848 |

The narrower path requires both inputs to admit source demand. Parameterized
and on-demand subqueries retain their existing evaluation boundaries.
Regression controls exercise nullable IN and NOT IN, hidden filter and guard
dependencies, and fallible evaluation. Two of the larger increases occurred
on null-aware anti-membership cases ending `ab5ece23c0cff055` and
`8b055ed9b49d549b`, up 0.028 and 0.024 engine seconds.

## Group state and bounded finalization

The prepared `COUNT(signed_value)` case `v2_single_key_reducer_a56dde8b9bd5d428`
formed 631,986 groups and returned 31 rows. Compact, validity-aware single-key
COUNT reduced group-update time from 246.8 to 143.2 ms. The finalizer borrows
group keys and reduces small ordered candidate sets as it visits groups.
It checks eligible groups, including HAVING and ordering values, before
discarding a candidate. Finalization fell from 232.8 to 52.4 ms. The new
peak counter recorded 62 buffered ordering candidates. Total engine time fell
from 0.484 to 0.200 s.

The corresponding MIN case `v2_single_key_reducer_75e1579c0df46929` used
general aggregate state on both builds. Finalization fell from 221.8 to
208.7 ms, with the same 62-candidate peak. Typed compact extrema remain open.
The 5M multi-key control ending `f30d4f98739cb9c9` formed only 100 groups and
rose from 0.241 to 0.257 s. Its prepared shared reservation peak was
516,436,046 bytes on both builds.

The separate candidate-only challenge `v2_single_key_reducer_d7da0557c5fb3f58`
returned 31 correct rows from 3,161,199 groups in 1.059 engine seconds,
again buffering at most 62 ordering candidates. Its peak shared reservation
was 1,627,243,967 bytes and peak process RSS was 2,260,516,864 bytes. The
archived attempt reached 3,433,742,336 RSS bytes and failed the guard.
Only the candidate has a completed engine observation for that prepared request.

## Simultaneous reservations on direct and prepared inputs

A separate diagnostic build tracked the origin and caller stage of each live
reservation. It saved the simultaneous balances at the first high-water mark
of the shared pool. Each route of the 5M multi-key group ran once and returned
the expected 31 rows. Both peaks matched the archived release observation
exactly. These diagnostics contribute no engine time to the comparison.

| Route | Peak reserved bytes | Reservation origins at peak | End reserved bytes |
|---|---:|---:|---:|
| Direct Parquet | 19,762,336 | 8 | 14,458,537 |
| Prepared Vortex | 516,436,046 | 16 | 6,951,321 |

At the prepared peak, **510,973,468 bytes (98.94%)** belonged to leases from
`ReservedHostAllocator::allocate` outside the marked aggregate caller scopes.
This allocator charges buffer capacity including alignment and attaches the
lease to the buffer. One 4,748,649-byte example from this site originated in
`ResidentFileReadAt::read_at` on the Vortex blocking pool. The direct peak
instead included 10,403,736 bytes at Parquet source admission and 7,831,784
bytes at a resident-session reservation site. All balances reconcile to each
route's observed peak.

The census identifies reservation origin. Its representative stack samples
one large growth per site. Identifying which consumer retained each read
buffer requires range identities and release events across the reader,
prefetch and cache boundaries. The 100-group control makes that a more
specific next experiment than changing the grouping map. The initial trace
build exposed a compile-time lease-size limit. The corrected trace stores
metadata out of line and checks the original inline lease size. Both earlier
build failures and the successful build are retained in the evidence.

## Bounded ordering after validated inputs

The prepared 5M rolling window `v2_window_rolling_sum_473addced11c72c6` fell
from 42.398 to 20.523 engine seconds. Its final ordering now validates five
million rows over 2,442 batches through the bounded prefix, with at most
500 retained prefix-buffer bytes. The complete operation's shared reservation
peak fell from 750,337,489 to 711,316,049 bytes. The direct route already
used the prefix and rose from 20.654 to 21.072 s. These observations separate
the final ordering route from the substantial window work still present in
both executions. Exclusive partition-sort and frame-evaluation clocks remain
unavailable.

Admission follows direct column projections back to a validated window
output or an eligible prepared file containing only integer ordering keys.
The existing prefix retains ties stably, honors offset and validates the
incoming values. A wide prepared file with hidden payloads still requires
the full validation path. Its historical Top-K deadline remains open.
Prepared ranked-position reads and selected-region payload retrieval require
a separate provider and validation proof.

## Other investigations

- **Join composition.** The 5M inner join and downstream grouping ending
  `a54e8e617685a05a` reported five of 43 source columns, 500,000 detached build
  rows and 245 build batches on the direct route. Its original engine clocks
  were 2.031 s direct and 2.173 s prepared. The packets expose no exclusive
  time for indexing, probing, gathering or the downstream group. Instrument
  those stages before selecting a join change.
- **Fresh interface declarations.** Across all 9,995 successful release
  packets the DataFrame and CLI median setup times were 97.004 and 97.322 ms.
  One fresh declaration on each interface isolated client export (36.774 and
  35.816 ms), client construction (2.429 and 2.333 ms) and plan construction
  including lazy exports (62.698 and 63.005 ms). Both constructed their plan
  without collecting or launching a worker. These are setup diagnostics.
- **String predicates.** The 5M `LENGTH(unicode_text) > 3` reduction ending
  `16a078078787ca2b` took 0.584 s direct and 0.524 s prepared in the original
  study. The current scalar path builds character counts. A fused mask could
  stop at the fourth character after value validation. Compare characters
  inspected and intermediate values with controls for Unicode, NULL and
  threshold boundaries.
- **Preparation.** The original 5M fact file took 11.691 s to prepare.
  The inclusive provider encode/write span was 11.599 s. The receipt reports
  4,096-row blocks and a 1 MiB target. The named text-Zstd and numeric
  compression scopes recorded zero calls. Attribution inside the upstream
  encoder, buffer assembly, writes and final metadata needs additional
  instrumentation. [Preparation and full readback](evidence/native-expanded-query-2026-11-03/preparation.json).

## Verification

The original and patched runtimes passed independent result checks on all
55 matched requests. Default workspace tests passed 3,319 cases. The native
release-feature crate passed 3,910 tests with 33 ignored. Default and native
strict Clippy checks passed. The portable verifier reconstructs results from
the frozen cases, raw worker output and both build records. A flipped archive
byte and a modified timing total with an updated file checksum each failed
verification. [Replay instructions](../../benchmarks/traditional_analytics/query_surface/README.md) ·
[Corruption controls](evidence/native-expanded-query-2026-11-03/operator-investigations/corruption-controls.json).

The next investigation batch used the same runtime to compare 33 pairs of
public plans, sample four composed requests and inspect the prepared file's
physical layout. [Broader workload findings](native-expanded-work-investigations-2026-11-03.md).
