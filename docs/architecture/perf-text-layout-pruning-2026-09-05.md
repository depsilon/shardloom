# Native text layout pruning experiment

Status: isolated candidate after PR #1433. Both ported tests, nine file-observer
tests and the first diagnostic I/O matrix (56 complete exact query results) pass.
Configured-geometry release and lifecycle comparison remain pending. The control
is `c5bada49`, with C7's retained numeric
compression, typed consumers, ownership and measured timing behavior. This note
continues PERF-08/09/12 under RFC 0044 and the existing CG-5/CG-6 evidence gates;
it does not complete those packets or change ordinary public ingest admission.

## Decision and provider boundary

Use the pinned Vortex 0.85 native `RepartitionStrategy` and `ZonedStrategy` around
the existing selected-field Zstd leaf. A table field override bypasses the default
numeric leaf's zone wrapper; selected text therefore needs an explicit wrapper
to retain native bounded-min/max and null-count pruning metadata. The candidate
uses the same row-block size, compression level/frame policy, source, values and
worker settings as its control. No text dictionary/helper redesign is included.

`zoned_source_text_vortex_write_strategy` is test-only and has the same seven
arguments as the unchanged production `large_source_text_vortex_write_strategy`,
including the actual writer session. It constructs the current C7 table strategy
for non-overridden fields, preserving numeric encoding admission and ownership.
The selected text branch is `Repartition(canonicalize=false) -> Zoned -> existing
Zstd text leaf`, with native Flat statistics storage. No public runtime option,
large-source default, dependency, query-engine fallback or certificate claim is
added before a retain/drop decision.

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
costs. Its wall times support no speed claim. Release-mode comparisons at the
configured geometry must measure the complete write/validation/query lifecycle
before retaining the zoned production layout; no directory-publication durability
claim follows from fixture file synchronization and reopen checks.

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
- [ ] Compare release-mode ingest/write/validation/publication, artifact/statistics
  bytes, first/repeated query costs and complete results on the same source at
  the configured geometry, with matched raw predicates and unchanged safety guards.
  Preserve current inclusive writer spans; overlapping work is not subtracted.
- [ ] Retain only supported scoped benefit after lifecycle evidence, or record
  a reasoned drop. Larger public ingest remains unchanged until that decision.

The root agent owns serial formatting, builds and tests. Initial focused command:
`CARGO_TARGET_DIR=/Users/dylan/.cache/shardloom/cargo-target cargo test -p shardloom-vortex --features release-user-surfaces text_zone_tests -- --nocapture`.
The ported, observer and first paired I/O tests have passed. Native feature clippy,
required broad repo gates and guarded lifecycle runs at a frozen source/binary
remain the root agent's validation responsibility. The
[PR #1433 packet](../benchmarks/perf-drop-ship-2026-09-05.md) remains the historical
control; its earlier text candidate is not retroactively marked accepted.
