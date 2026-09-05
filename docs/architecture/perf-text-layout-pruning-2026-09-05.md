# Native text layout pruning experiment

Status: isolated candidate after PR #1433, locally promoted to ordinary writer
dispatch pending 100M-row acceptance and a final retain/drop decision. Both ported
tests, nine file-observer tests and the first diagnostic I/O matrix pass. The
subsequent paired release matrices at `14e54146` pass all 448 complete query
results: 336 measured and 112 warmup records. Their control is `f607e4c8`, retaining
the C7 implementation recorded at `c5bada49`: numeric compression, typed consumers,
ownership and measured timing behavior. This note
continues PERF-08/09/12 under RFC 0044 and the existing CG-5/CG-6 evidence gates;
it does not complete those packets or change the public ingest admission threshold.

## Decision and provider boundary

Use the pinned Vortex 0.85 native `RepartitionStrategy` and `ZonedStrategy` around
the existing selected-field Zstd leaf. A table field override bypasses the default
numeric leaf's zone wrapper; selected text therefore needs an explicit wrapper
to retain native bounded-min/max and null-count pruning metadata. The candidate
uses the same row-block size, compression level/frame policy, source, values and
worker settings as its control. No text dictionary/helper redesign is included.

In this isolated branch, `large_source_text_vortex_write_strategy` now supplies
the zoned composition under `vortex-write`, with the same seven arguments,
including the actual writer session. `unzoned_source_text_vortex_write_strategy`
is retained only as the paired test control. The ordinary dispatch and its
existing admission policy select the new helper; no public knob is added. It
constructs the current C7 table strategy for non-overridden fields, preserving
numeric encoding admission and ownership.
The selected text branch is `Repartition(canonicalize=false) -> Zoned -> existing
Zstd text leaf`, with native Flat statistics storage. Existing applied-strategy
and stage-plan fields identify text zoning, and the regression guard keeps
lifecycle acceptance explicit. This local promotion is not yet a retained or
published production improvement. No predicate guard, dependency or query-engine
fallback is introduced.

Vortex-first classification: `use_vortex_native_provider`. Provider surfaces are
native layout writing, native file/scan, expressions and statistics. Tests remain
inside the `vortex-write`/native feature boundary. Native Zstd decode and scalar
materialization used to verify outputs are explicit; unsupported residual work
is not delegated to another engine. `fallback_attempted=false` remains required
for later public comparisons. Test-only metadata does not certify a public route.

## Evidence and limitations

The two tests ported from isolated `3d70119a` cover nullable/all-null/Unicode
roundtrip and filtered zone boundaries at one/three workers, plus disjoint and
null-only filter-text request pruning with conservative UTF8 bounds crossing the
provider's 64-byte extrema cutoff. Exact identifiers exceed binary64 precision.
These tests verify every returned scalar; they do not substitute for independent
physical-byte measurements. The first C7 port run exposed an invalid all-null
pruning assumption in the raw comparison test. Both corrected tests now pass;
the following stronger cases distinguish that provider limit.

Pinned `vortex-array` 0.85 `stats/rewrite/builtins.rs` derives a `Gte` falsifier as
`max(field) < min(literal)` (lines 145–150), without an all-null proof. Its
`StatsRewriteRule` contract (`stats/rewrite.rs`, lines 34–38) proves the predicate
is false, not null. An all-null bounded maximum yields null, which
`vortex-layout`'s `ZoneMap::prune` treats as inconclusive (`zoned/zone_map.rs`,
lines 155–163). This preserves three-valued expression semantics.

The test now retains complete raw-comparison output parity and explicitly checks
its null-only payload request. A separate positive WHERE-only variant uses native
`and(is_not_null(field), gte(field, nonnull_literal))` with the same independent
selected-row expectations and hard no-request assertions for both disjoint and
all-null filter-only zones. Native `IsNotNull` falsification uses null count equal
to zone row count (`stats/rewrite/builtins.rs`, lines 280–309;
`zoned/zone_map.rs`, lines 193–198). This is not a generic binary falsifier, a
rewrite under NOT, or a projected-Boolean equivalence: null is intentionally false
only at this WHERE boundary. No production expression rewrite is enabled.

The payload proof checks native chunk offsets/lengths and data/statistics segment
ID separation. The paired I/O tests match raw/guarded predicates across both
writers; comparing an unguarded control with a guarded candidate would confound
layout and expression changes.

Pinned scan construction registers projected payload requests before pruning
completes. The logical-request test consequently requires avoided requests for
unprojected filter-only text and retains full-row projection exactness checks.
Request registration and future polling are not physical I/O: native coalescing
may read adjacent ranges. The separate observer measures completed positional
file-read ranges, drains outstanding work and validates source generation before
reporting bytes avoided. Reads may be served by the OS page cache; no physical
storage-device or cold-cache claim follows from file-read accounting.

## Production null guard: dropped

Do not add the unconditional positive-comparison null guard to production WHERE
lowering. It preserves the tested selected rows and permits native all-null-zone
pruning, but the first real-file comparison exposes repeated text reads. On the
baseline filter-only scan, completed ranges are `[offset, length]` pairs:

```text
raw:     [[9495509, 65535], [8, 9558468]]
guarded: [[9495509, 65535], [8, 9558468], [8, 9558468]]
```

The guard repeats the entire 9,558,468-byte text range: total completed bytes rise
from 9,624,003 to 19,182,471, including the same footer read. The unpruned shuffled
zoned cases also approximately double, from 9,624,907 to 19,184,147 bytes. In the
clustered no-match case, adding the guard saves only the 164-byte all-null payload.
That saving does not justify the observed amplification. The guarded variant
remains a semantics/provider diagnostic, not the proposed production predicate.

The pinned provider source explains this result. `vortex-layout` 0.85.0
`scan/filter.rs:37–59` splits root `And` into conjuncts, and
`scan/tasks.rs:93–126` evaluates each conjunct sequentially. Each Flat
`filter_evaluation` constructs a fresh array future and segment request
(`layouts/flat/reader.rs:60–91,127–143`); the reader does not retain a decoded
array cache. `SharedSegmentSource` retains weak references to in-flight work
(`segments/shared.rs:24–26,43–60`), so a completed first conjunct does not ensure
the second can reuse its request. `vortex-file` 0.85.0 defaults to
`NoOpSegmentCache` (`open.rs:284–287`). Thus this composition has no guaranteed
completed-read reuse; the duplicate range above is observed evidence of the
consequence, not an inference from request registration alone. No unbounded
cache or generic three-valued-logic rewrite is introduced to conceal it.

## First diagnostic I/O matrix

The passing debug matrix uses 32,768 rows, four 8,192-row zones, two query workers
and two writer workers, with one sample and no warmup per writer/order. Baseline
and zoned writers receive the same independent values in clustered and bijective
shuffled orders. All 56 query results match complete scalar expectations,
including nullable UTF8 and integer identifiers above binary64 precision. This
geometry is diagnostic; it is not the configured large-source production case.

For raw comparisons with text used only as a filter, actual completed file-read
bytes, including open/footer reads, are:

| Physical order | Selected logical zones | Baseline bytes | Zoned bytes | Bytes avoided |
| --- | --- | ---: | ---: | ---: |
| Clustered | None | 9,624,003 | 65,699 | 9,558,304 |
| Clustered | One | 9,624,003 | 3,253,179 | 6,370,824 |
| Clustered | Half | 9,624,003 | 6,439,503 | 3,184,500 |
| Shuffled | None | 9,624,003 | 65,535 | 9,558,468 |
| Shuffled | One | 9,624,003 | 9,624,907 | -904 |
| Shuffled | Half | 9,624,003 | 9,624,907 | -904 |

The shuffled one/half selections span all physical zones, exposing a 904-byte
overhead rather than useful pruning. Full text projection and filter-only cases
are recorded separately; an all-row identifier-only projection does not prove
the cost of reading text. Observations include normal native coalescing and
repeated reads, with zero pending jobs after drain and final source validation.
The observer's nine tests cover its bounded accounting and lifecycle behavior.
The fixture uses a 128-MiB native query allocator; caller fixture/parser storage,
observation metadata and process RSS are outside that reservation scope.

Raw evidence is local-only at
`/Users/dylan/LocalData/shardloom/perf-text-io-tests.log`, under
`TEXT_LAYOUT_IO_EVIDENCE` schema `shardloom.text_layout_io_experiment.v1`.
This first debug run establishes exact results and scoped file-read savings and
costs. Its wall times support no speed claim. The release comparison below adds
configured-geometry evidence; no directory-publication durability claim follows
from fixture file synchronization and reopen checks.

## Matched release comparison before local promotion

The clean paired source is `14e5414605d4fd10deef18d50fb78bfaccb00d8a`, with
release test-binary SHA-256
`f6471f45d55190acd04a05d691cc9d6ad5a887132b1c3c72f5c813e74d2b267d`.
Both writer compositions run in that same binary; the unzoned control preserves
the writer at `f607e4c84d74b1ccc4423c4691b29f9d1c53eb7d`. The local evidence bundle
is `/Users/dylan/LocalData/shardloom/perf-text-layout-20260905/`, containing
`source-manifest.json`, `summary.json`, `summary.md` and the retained raw logs.
The build enables `release-user-surfaces`. The reusable report now distinguishes
that intended validation feature from its actual compile-time enabled boolean.

Each geometry has 224 exact query records, including 56 warmups excluded from
medians. The two geometries total 448 exact records, with 336 measured and 112
warmups. The `production_rows` fixture contains 1,048,576 rows delivered in four
262,144-row batches. Both writers use C7's bounded per-batch root, the configured
262,144-row zone geometry, identical values, two writer/query workers and matched
raw or guarded predicates. The paired test forces these writer compositions
below the ordinary 10M-row threshold; its name is not a public admission proof.

Median configured-geometry writer lifecycle and artifact sizes are:

| Text order | Baseline artifact bytes | Zoned artifact bytes | Baseline lifecycle ms | Zoned lifecycle ms | Change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Clustered | 25,488,884 | 25,491,252 | 47.583 | 49.089 | +3.2% |
| Shuffled | 25,714,188 | 25,716,980 | 47.940 | 50.112 | +4.5% |

Lifecycle includes native write/flush, file synchronization, full SHA-256
readback and native footer reopen. Source preparation, writer setup and geometry
inspection are retained separately and excluded. These numbers do not represent
complete public ingest or directory publication.

For raw predicates with text used only as a filter:

| Text order | Selected logical zones | Baseline read bytes | Zoned read bytes | Baseline native-array return ms | Zoned native-array return ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Clustered | One | 25,550,427 | 8,562,499 | 6.309 | 3.546 |
| Clustered | Half | 25,550,555 | 17,057,207 | 6.501 | 4.140 |
| Shuffled | One | 25,775,519 | 25,777,095 | 6.181 | 6.100 |
| Shuffled | Half | 25,775,519 | 25,777,095 | 6.119 | 6.192 |

Clustered one/half selections avoid 66.5%/33.2% of completed read bytes; shuffled
selections instead add 1,576 bytes. The selective clustered timings improve, while
the shuffled timings are mixed. Native-array timing ends at array return;
complete scalar canonicalization and verification are separately timed. These
are not full scalar/JSON query latencies. Read accounting includes repeated and
coalesced `read_exact_at` ranges through final drain, with no physical-device or
cold-cache claim. The guard's read amplification persists in the release matrices,
so the production null-guard decision remains dropped.

This evidence supports evaluating the promoted helper through the ordinary
100M-row route. It does not establish that route's correctness or lifecycle cost.
The production threshold, typed numeric path and predicates remain unchanged;
the 100M-row comparison and final retention decision are still open.

## Acceptance and retain/drop checklist

- [x] Both ported tests pass against C7 with complete raw/guarded output parity,
  explicit raw null-zone limitation and hard skipped-request assertions for the
  guarded disjoint/all-null zones.
- [x] The diagnostic paired baseline/zoned fixtures use identical independently
  specified values and complete outputs for selective, all-match, no-match, null
  and UTF8 cases (56 exact results).
- [x] A fresh, bounded real-file observer measures bytes/ranges for every query,
  including footer/statistics/prefetch; drain/cancellation/failure counters and
  source identity prove the interval is complete. No cache-only request proof.
- [x] Measure diagnostic projected and filter-only text separately, including
  eager projection and read-coalescing limitations; do not generalize one to the
  other. Drop the production null guard on measured repeated-read evidence.
- [x] Compare release-mode native writer lifecycle, artifact bytes, native-array
  query timings and complete independently verified values at matched configured
  geometry, preserving raw samples and excluding warmups from medians. Keep the
  narrower timing boundaries explicit; overlapping work is not subtracted.
- [ ] Validate the locally promoted ordinary writer on the 100M-row public
  source, including complete query acceptance and ingest/query lifecycle costs,
  with unchanged safety guards. Small advisor-forced tests verify ordinary
  dispatch and stored text zoning only; they do not prove public admission.
- [ ] Retain only supported scoped benefit after lifecycle evidence, or record
  a reasoned drop. The local promotion remains a pending candidate until that
  decision; it has not been retained or published.

The root agent owns serial formatting, builds and tests. Initial focused command:
`CARGO_TARGET_DIR=/Users/dylan/.cache/shardloom/cargo-target cargo test -p shardloom-vortex --features release-user-surfaces text_zone_tests -- --nocapture`.
The ported, observer and paired release I/O tests have passed. Promoted runtime
gates and the guarded 100M-row lifecycle comparison at a frozen source/binary
remain the root agent's validation responsibility. The
[PR #1433 packet](../benchmarks/perf-drop-ship-2026-09-05.md) remains the historical
control; its earlier text candidate is not retroactively marked accepted.
