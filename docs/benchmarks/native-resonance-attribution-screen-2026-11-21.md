<!-- SPDX-License-Identifier: Apache-2.0 -->

# Resonance Cells and Leases pre-implementation attribution screen

The bounded live shared-preparation mechanism proposed in [RFC 0046](../rfcs/0046-resonance-cells-and-leases.md)
is **not retained** after its frozen six-scenario matched release comparison failed the
prospective retain gate (`164,593,625 → 166,171,249` engine nanoseconds, **+1,577,624 ns /
+0.96%** regression overall; five of six scenarios regressed). Zero Resonance runtime code is
retained in the repository.

## Screen boundary and methodology

- **Build & host**: release profile (`--release`) over `ResidentVortexSession` (`256 MiB`,
  1 worker lane) with pinned Vortex `0.93.0` and Arrow/Parquet `59.3.2` on macOS `arm64`.
- **Dataset**: `131,072` rows across `8` chunks (`16,384` rows per chunk) containing
  `group_int` (`i64`, `1,024` distinct values), `group_utf8_hi` (nullable UTF-8, `8,192`
  key buckets), `group_utf8_med` (nullable UTF-8, `512` key buckets), `val_a`, and `val_b`.
- **Arms compared**:
  - `independent`: separate `ResidentVortexSession::with_native_execution_context` file scans
    preparing `KeyColumn` + `Values` per query (`source_segment_cache` active on session).
  - `composed`: shared `ResonanceCoordinator` / `ResonanceCell` sliding window
    (`MAX_LIVE_CELLS = 4`, window ceiling `min(16 MiB, grant / 8)`) sharing demand-scoped
    `SharedPreparedArtifact` (`KeyColumn(Hash)` and `KeyColumn(Values)`) across composed
    consumers with matching output tables verified on every scenario.
- **Invariant posture**: `fallback_attempted=false`, `external_engine_invoked=false`,
  `claim_gate_status=not_claim_grade`. No phase or CG-1 through CG-23 gate closes.

## Matched six-scenario comparison (`shardloom.resonance_comparison.v1`)

| Scenario | Independent ns | Composed ns | Delta ns | Delta % | Prep builds | Prep hits | Declined | Peak live cells | Peak window bytes | Backpressure pauses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `utf8_high_card_overlap` | 50,942,875 | 48,888,500 | -2,054,375 | -4.03% | 24 | 8 | 0 | 1 | 687,193 | 0 |
| `utf8_compound_overlap` | 20,846,917 | 22,010,750 | +1,163,833 | +5.58% | 32 | 16 | 0 | 1 | 560,481 | 0 |
| `numeric_overlap` | 13,676,041 | 14,247,458 | +571,417 | +4.18% | 24 | 8 | 0 | 1 | 556,121 | 0 |
| `mixed_limit_overlap` | 25,408,625 | 25,504,000 | +95,375 | +0.38% | 17 | 1 | 0 | 1 | 686,937 | 0 |
| `disjoint_control` | 29,591,375 | 30,757,375 | +1,166,000 | +3.94% | 32 | 0 | 0 | 1 | 687,449 | 0 |
| `single_consumer_control` | 24,127,792 | 24,763,166 | +635,374 | +2.63% | 16 | 0 | 0 | 1 | 686,937 | 0 |
| **Total** | **164,593,625** | **166,171,249** | **+1,577,624** | **+0.96%** | **145** | **33** | **0** | **1** | **687,449** | **0** |

## Attribution findings and disposition

1. **Correctness & lifecycle**: Prototype unit tests (`9/9`) verified output parity with
   independent execution, demand-scoped non-interference, `LIMIT` early exit without stopping
   peers, consumer cancellation isolation, strict float NaN validation isolation, bounded slow
   consumer backpressure (`<= 4` live cells), source-generation invalidation failures, and exact
   zero leaked `LiveMemoryPool` reservations after final drop (including releasing consumer hash
   tables retaining zero-copy `Cell::Utf8(ByteBuffer)` slices before checking session residual
   memory).
2. **Why net latency regressed (`+0.96%`)**:
   - The resident session's `source_segment_cache` already eliminates repeated Vortex file segment
     decoding/I/O between consecutive consumers in the independent arm.
   - `utf8_high_card_overlap` (`8,192` distinct strings across `8` chunks) saved `-2.05 ms`
     (`-4.03%`) from `8` `Preparation::Hash` hits, proving expensive UTF-8 string hashing is
     amortizable when shared.
   - However, per-chunk `scan.execute(Some(cursor..end))`, mutex-guarded `PreparedTable` lookups,
     window budget accounting, and per-tick source validation added fixed overhead that exceeded
     saved key preparation on numeric (`+4.18%`), compound (`+5.58%`), early-limit (`+0.38%`),
     disjoint (`+3.94%`), and single-consumer (`+2.63%`) workloads.
3. **Decision**: Following ShardLoom's R8 and PulseWeave finite-feedback precedent, RFC 0046 runtime
   code is **not retained** and all prototype code was reverted prior to closing the item in the
   [phased execution plan](../architecture/phased-execution-plan.md).
