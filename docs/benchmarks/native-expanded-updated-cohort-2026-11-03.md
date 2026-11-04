# Updated 10,000-case native observation, November 3, 2026

**All 10,000 original primary requests passed** their independent schema,
value, ordering and no-fallback checks. The updated engine total is
**1,825.631237664 seconds**. The five requests that reached their deadlines
on 1.1.0 all completed correctly.
[Complete result summary](evidence/native-expanded-query-2026-11-03/updated-native/results.json).

| Comparison | Cases | Original engine s | Updated engine s |
|---|---:|---:|---:|
| Complete updated cohort | 10,000 | Unavailable | 1,825.631238 |
| Correct with engine timings on both builds | 9,995 | 3,192.078921 | 1,822.597829 |
| Recovered original deadlines | 5 | Unavailable | 3.033408 |

The common-correct comparison improved by **42.9025%**. Of those 9,995
cases, 6,171 were faster and 3,824 were slower. The original release has
five missing engine clocks, so its complete-cohort total remains unavailable.
These are two single observations with uncontrolled cache context.
[Original release observation](native-expanded-query-2026-11-03.md) ·
[Bound capture and original comparison identities](evidence/native-expanded-query-2026-11-03/updated-native/index.json).

## Where the engine total changed

| Family | Common-correct cases | Original engine s | Updated engine s | Change s |
|---|---:|---:|---:|---:|
| Membership subqueries | 387 | 1,373.841091 | 118.872108 | -1,254.968984 |
| Windows | 776 | 514.128802 | 399.608238 | -114.520563 |
| Grouped aggregation | 1,756 | 124.426044 | 123.297266 | -1.128778 |
| Joins | 2,612 | 430.763232 | 429.425057 | -1.338176 |
| Nested values | 196 | 20.634833 | 21.831116 | +1.196283 |
| Ordering | 206 | 171.441679 | 172.498488 | +1.056808 |

The result file retains all 18 family totals. Membership and windows account
for nearly all of the observed reduction. This complete updated pass establishes
the native baseline for subsequent comparisons.
[Full family table](evidence/native-expanded-query-2026-11-03/updated-native/results.json) ·
[Implementation and focused controls](native-expanded-operator-investigations-2026-11-03.md).

The five recovered requests completed in 0.757636375, 0.205018416,
0.176942542, 0.215062708 and 1.678748417 engine seconds in original manifest
order. Their exact case IDs appear in the result file.
[Recovered cases](evidence/native-expanded-query-2026-11-03/updated-native/results.json).

## Source and execution contract

The maintainer explicitly requested this updated-state pass after the
investigation batch. The measured binary was compiled in release mode from `9a8dd89c`.
The requested PR head `dc54b60e` differs in evidence, replay tooling and a
test assertion message. The production Rust inputs and public Python client
match the measured build. The CLI and native extension hashes, fresh build
receipt, source objects and source comparison are preserved.
[Source and supervision packet](evidence/native-expanded-query-2026-11-03/updated-native/index.json).

Each of the 10,000 original declarations ran once through its original public
interface and physical source route. The pass used the original inputs,
prepared Vortex generations and independently frozen expectations. It covered
8,924 primary, 880 holdout, 132 smoke and 64 scale requests across all five
interfaces. Per-request limits remained two threads, a 2 GiB native budget,
a 2.5 GiB process-group RSS ceiling and a 120-second deadline. Full input hashes
and recursive inventories ran outside query execution. Other task-owned work
was idle throughout the isolated pass. Complete process cleanup and post-run
identity checks passed.
[Frozen protocol and capture](evidence/native-expanded-query-2026-11-03/updated-native/index.json).

The earlier Polars pass supplies the comparison. The twelve original
alternate-route observations retain their separate denominator. In
particular, the prepared wide Top-K deadline remains an open finding from
that alternate lane.
[Alternate-route evidence](native-expanded-query-2026-11-03.md).

## Saved-result verification

Independent local replay checked all 10,000 complete results, their request
identities, native attribution and runtime bindings. A second reducer agreed
on the engine totals. Portable replay reconstructs every updated request and
its original reference from 40 bounded evidence parts. The initial offline
reader import and alias errors remain in the proof archive. They affected
post-run analysis only.
[Replayable evidence](evidence/native-expanded-query-2026-11-03/updated-native/index.json).

Six corruption controls rejected coherently rehashed changes to result values,
request identity, native clocks, fallback claims, case order and the published
engine total. The original packet remained byte-identical.
[Corruption-control results](evidence/native-expanded-query-2026-11-03/updated-native/negative-controls.json).

Workspace formatting, warning-free Clippy and all 3,319 workspace tests passed.
Six focused Python tests cover the new denominator checks and related packet
contracts. The checked-in packet also passed complete offline replay.
[Validation receipts](evidence/native-expanded-query-2026-11-03/updated-native/validation.json).

From a checkout with Python 3.10 or later, verify the packet using the
standard library and saved observations.

```sh
PACKET=docs/benchmarks/evidence/native-expanded-query-2026-11-03
python -B -S benchmarks/traditional_analytics/query_surface/expanded_updated_replay.py \
  --baseline "$PACKET/packet" --archive "$PACKET/updated-native"
```
