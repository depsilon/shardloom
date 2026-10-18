# Native ingest text size: bounded admission

This PERF-08/09 experiment follows the maintainer's October 17 request to focus
on smaller ingest output. PR #1571 remains a separate shared-execution change.
No new writer profile is admitted by this experiment alone.

## Source and cost signal

The complete physical inventory of the currently accepted artifact identifies
15,713,610,513 file bytes, 15,658,321,452 unique encoded-segment bytes and
55,289,061 remaining bytes. The original Parquet and accepted artifact remain
unchanged. The artifact SHA-256 is
`06f70eb6f118d11e4a5226eb641225d708fdb4ac1b3f45942872d5ef5df4fc22`.
All 611,520 flat references are distinct; column/auxiliary byte attribution is
additive for this artifact. The 28 source Zstd text columns occupy 7,602,132,264
bytes (48.38% of the complete file); URL, Referer, Title and OriginalURL occupy
7,013,740,680 bytes (44.63%). Halving the whole file also requires gains outside
these text columns.

The accepted input has 3,120 source batches of at most 32,768 rows. Each column
has 3,120 flat references. The retained source-text writer uses Zstd level -3,
8,192 values per frame and no compression dictionary. Vortex 0.85.0 trains a
dictionary only with at least eight frames. Consequently, just changing the
constructor at the retained batch/frame sizes would not train dictionaries.

## Frozen screen

Before measurement, select six profiles: levels -3 and 3, each with 8,192 values
per frame without a dictionary, 4,096 without a dictionary, and 4,096 with a
trained dictionary. The 4,096 no-dictionary arms distinguish frame-size effects
from training. Level 3 is the pinned provider's standard compression level;
level -3 is the retained fast writer. No candidate is selected from its name.

Read the first four 32,768-row batches from Parquet row groups 0, 113 and 225.
Project URL, Referer, Title, OriginalURL and SearchPhrase. These are benchmark
fixtures, never runtime field-name rules. Rotate and reverse profile order over
the twelve batches. Each column/batch is persisted separately through the same
native table writer and bounded layout, preserving the real compression batch
shape. Record both complete file bytes and physical column bytes; single-column
file overhead must not masquerade as a whole-file estimate.

The retained arm calls the existing production leaf factory. Experimental arms
use the same compact UTF-8 input, native Zstd provider, chunked/flat persistence,
single compression worker and empty temporary-statistics request. Reopen every
artifact, compare all values, validity, order, dtype and footer statistics,
inventory physical encodings, and witness dictionary/frame metadata on the
persisted result. Run three complete native reads and LIKE/NOT LIKE count checks
against decoded input oracles. Those are provider-screen costs, not Full43
engine times or proof of public query dispatch.

Use the existing 512-MiB admitted memory pool, 64-MiB per-column artifact cap,
bounded output, local-only source/storage checks, exclusive workload guard and
supervised process cleanup. Report allocations outside the native pool honestly.
Do not change storage ceilings or overlap heavy local work. Preserve failed
attempts and the frozen source, binary and input identities.

An arm advances to production-path investigation only if all exact checks pass,
aggregate physical column bytes fall by at least 30%, no sampled region grows,
aggregate native write cost is at most 1.25 times retained, and median repeated
native read/filter cost is at most 1.05 times retained. For each consumer family
(complete read, positive count and negative count), sum the per-sample medians
of three repetitions and apply that limit independently. This is a bounded screen,
not retention permission. Report every arm even when none qualifies; do not
relax the thresholds after seeing results. Full retention still requires the
existing complete-artifact, guarded ingest and first/repeated-query lifecycle
gates, including full correctness and performance comparisons. The broader
PERF-08/09 and CG-1 through CG-23 obligations remain open.

## Provider and reuse boundary

Vortex-first decision: `use_vortex_native_provider`, test-only until admitted.
Use pinned Vortex 0.85.0 `Zstd::from_var_bin_view` and
`Zstd::from_var_bin_view_without_dict`, existing native memory admission,
`CompressingStrategy`, `ChunkedLayoutStrategy`, `FlatLayoutStrategy` and the
existing ShardLoom table/bounded writer. Add no format, execution engine or
dependency. Canonicalization is explicit at ingest; query verification uses
native file scans. `fallback_attempted=false` and
`external_engine_invoked=false` throughout.

ShardLoom technique review: preserve native allocation admission and bounded
writer ownership; use metadata-first byte attribution and evidence-tier gates.
PulseWeave/query topology changes are outside this isolated codec screen.
Previously rejected FSST, source-dictionary preservation and larger-frame-only
profiles remain rejected; this screen measures compression dictionaries and
their controls at the actual source batch size.

## Measured outcome

All five alternatives are **dropped under the frozen screen**. The retained
production writer is unchanged. Across 60 column/batch cases per profile:

| Level | Values/frame | Trained dictionary | Physical column bytes | Change from retained | Decision |
| --- | ---: | --- | ---: | ---: | --- |
| -3 | 8,192 | No | 44,413,684 | Baseline | Retained control |
| -3 | 4,096 | No | 45,140,064 | +1.64% | Insufficient size reduction |
| -3 | 4,096 | Yes | 50,814,072 | +14.41% | Size, write and consumer gates fail |
| 3 | 8,192 | No | 27,657,172 | -37.73% | Repeated consumer gates fail |
| 3 | 4,096 | No | 29,568,640 | -33.42% | Repeated consumer gates fail |
| 3 | 4,096 | Yes | 29,867,088 | -32.75% | Write and repeated consumer gates fail |

The three level-3 profiles meet the sample size threshold and shrink each sampled
region. None meets all three repeated-consumer limits. Compression dictionaries
also fail the write-cost gate. Complete observations, including all rejected
profiles, remain in the evidence packet; no threshold was relaxed after measuring.

The screen verifies 360 separately persisted native files, 1,080 complete exact
reads and 2,160 predicate counts. Each real-size artifact has one persisted Zstd
leaf, the selected frame bounds and the expected dictionary presence. Values,
validity, order and dtype match the source exactly. Footer statistics match
across profiles for each field/batch; this is a comparison with the retained
writer, not an independent source-statistics oracle. Native memory reservations
return to zero after every batch. Two automatic tests cover balanced profile
order and nullable UTF-8, including Unicode, embedded NUL and all-null inputs.

Required workspace tests pass 3,225 executions with no failures or exclusions.
Formatting and strict default/native Clippy pass. The optional native tests and
manual screen retain their exact measured-source bridges, and all sixteen
documentation/site checks pass. No production engine behavior is changed.

The original measured sources, plan, binary identity, source identity, failed
preflight and all successful observations are preserved. Formatting happened
after measurement, with all other 3,264 selected sources unchanged at that
bridge. A second exact source comparison accounts for three native lint fixes:
two method-reference substitutions and a non-narrowing metadata bounds check.
All three are in the untimed structural inspection; codec and timed consumer
code is unchanged, and all 2,400 measured frame lengths satisfy both checks.
This does not establish a complete-file size reduction, replacement-ingest
performance or public-query improvement. The full-file inventory identifies the
opportunity; the bounded screen rejects these five implementations of it.

## Reproduction and evidence

The [portable evidence index](evidence/native-ingest-text-size-2026-10-18.json)
binds the measured plan and source exceptions to immutable base commit
`77d5b5da9561b04b4c5cb81e94cce07529528cd8`, every selected source hash, the complete
case observations and the unmodified decision. It also preserves the physical
inventory's two unsuccessful attempts: a wrongly scoped log-budget argument
refused before reading data, and a flat-reference bound that proved too small.
The successful inventory uses the exact footer-derived reference count. The
screen's initial source preflight likewise refused before reading data because
macOS `UF_TRACKED` was mistaken for a residency problem; allowing that documented
flag did not relax the full-allocation check or storage limits.

The manual test is
`vortex_ingest::dictionary_preservation::source_dictionary_bench::zstd_size_screen::clickbench_zstd_size_admission_screen`.
Build its release test binary with `vortex-write,universal-format-io,vortex-local-primitives`,
an explicitly unsynced `CARGO_TARGET_DIR`, and the repository's local storage and
workload guards. Resolve the output through Cargo metadata and the compiler's
reported executable. Set `SHARDLOOM_R1B_SOURCE` to the resident official
`hits.parquet`; invoke that binary with `--exact`, the test name, `--ignored` and
`--nocapture` through `scripts/run_local_readonly_proof.py`, with a unique run name,
the same source, explicit additional source inputs and the existing UAT limits.
Preserve all six profiles, the original source batch size and the frozen gates.
No full replacement ingest is needed to reproduce this bounded rejection.
