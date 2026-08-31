# ClickBench Ingest Optimization Ledger

This ledger tracks local 100M ClickBench ingest experiments for ShardLoom. It is an engineering
iteration log, not an official benchmark claim.

Constraints:

- Final product artifact must be one `.vortex` file.
- No query-answer sidecars, materialized views, precomputed aggregate summaries, or hidden external
  execution engines.
- Temporary files are allowed only as workspace-safe atomic staging and must be removed or renamed
  into the final `.vortex` artifact.
- Load time must include official-source read, normalization, embedded metadata/layout creation,
  Vortex write, digest, and required public evidence.
- Public/local CLI execution counts as public runtime. No smoke caps or direct compatibility routes
  should appear in the public path.

## Best-Known Retained Local Evidence

- Profile: single `.vortex` artifact, Parquet official source, source-text fast Zstd writer profile,
  lean source-native embedded runtime metadata, embedded OLAP layout/statistics in the artifact,
  max parallelism `12`.
- Artifact: `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/vortex/hits-parquet-100m.vortex`.
- Source: `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/sources/hits.parquet`
  (`14.78 GB` logical file). macOS file-provider behavior can report `0` allocated source bytes
  before and after a run; the `2026-08-31T15:41:42Z` replacement ingest proved the source can
  hydrate transiently during the run, but wall time includes that local source-availability delay.
- Current artifact size: `38,148,327,444` bytes.
- Latest full replacement evidence:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260831T154142Z`.
- Current retained result:
  - `elapsed_seconds=601` local wall time, including the delayed source-file hydration phase
  - `prepare_once_millis=560724`
  - `vortex_write_millis=256228`
  - `vortex_segment_write_millis=256220`
  - `vortex_compression_millis=112451`
  - `vortex_encode_write_millis=143769`
  - `universal_ingest_decode_millis=3688`
  - `universal_ingest_derived_metadata_build_millis=29980`
  - `footer_segment_count=34371`
  - `vortex_writer_runtime_applied_parallelism=12`
  - `vortex_writer_compression_field_count=28`
- Current branch adjustment: source-text compression fields are now derived from the admitted
  source schema instead of a static ClickBench field list. Hidden derived metadata and numeric
  fields cannot activate the text-compression strategy; a text-heavy writer profile requires at
  least one real source UTF-8 or dictionary-UTF8 candidate field.
- Current branch full UAT reference:
  `docs/benchmarks/clickbench-100m-current-branch-uat.json`.
- Current branch full query UAT after replacement ingest:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_after_replacement_ingest_current_branch_20260831T155208Z/summary.json`
  completed `43/43` with `fallback_attempted=false`, `external_engine_invoked=false`, query total
  `200.904s`, geomean `1.382s`, and local end-to-end `801.904s` using the `601s` replacement-ingest
  wall time.
- Current known pain: prepare/load remains writer/encode/segment dominated. Earlier evidence recorded
  prepare around `515s`, with Vortex write/segment write around `455s`; the latest retained lean
  metadata run reduces artifact size but remains dominated by Vortex compression plus encode/write
  work.

## Current Branch Experiments

### `2026-08-30` Resource-Envelope Writer And Compact Artifact Shape

- Change under validation: public prepare/load now wires the layout-advisor writer parallelism into
  Vortex's `CurrentThreadRuntime` worker pool, reuses the shared local resource-envelope row-block
  and coalescing defaults, applies explicit fast-Zstd compression only to source-schema-admitted
  UTF-8 and dictionary-UTF8 payload/text fields while excluding numeric and generated derived metadata columns,
  adds a lean source-native runtime metadata profile for large columnar sources, and records source decode,
  derived metadata build, Arrow-to-Vortex, compression, encode/write, segment write, and final
  commit timing fields.
- Artifact-shape rule: large columnar sources now prefer a lean source-native/dictionary-derived
  runtime metadata profile that keeps the high-value URL, Referer, SearchPhrase, and EventTime
  helpers while omitting broader candidate columns such as OriginalURL, ClientEventTime,
  LocalEventTime, and Title length unless a cheaper adapter path is available. Text-row adapters may
  still emit admitted hidden derived columns when they can build them during the existing typed-row
  path without adding a separate preprocessing pass. Writer compression candidates come from the
  actual source schema and Arrow dtypes, not a benchmark-specific field list.
- Expected gain: reduce avoidable single-thread writer bottlenecks and expose enough stage timing
  evidence to make ship/drop decisions without weakening the single `.vortex` artifact contract.
- Evidence fields to check: `vortex_writer_runtime_kind`,
  `vortex_writer_runtime_requested_parallelism`, `vortex_writer_runtime_applied_parallelism`,
  `vortex_writer_runtime_background_workers`, `vortex_writer_compression_field_count`,
  `vortex_writer_compression_field_names`, `universal_ingest_decode_millis`,
  `universal_ingest_derived_metadata_build_millis`, `universal_ingest_arrow_to_vortex_convert_millis`,
  `vortex_compression_millis`, `vortex_encode_write_millis`, `vortex_segment_write_millis`, and
  `vortex_final_commit_millis`.
- Focused validation result: Rust Vortex and CLI contract tests pass locally for the new evidence
  fields, source-native compact metadata preference, lean large-columnar metadata profile, selected
  compression field list, and writer runtime worker-pool evidence.
- Desktop UAT result:
  - Dropped: first retest of a six-field URL/search/payload-only compression profile exceeded the
    `80 GB` artifact guard after `452s` with no completed JSON report.
  - Retained: rebuilt release CLI with broad source UTF-8 text compression completed replacement
    ingest in `273s` wall time and reported `prepare_once_millis=204074`,
    `vortex_segment_write_millis=203821`, `vortex_compression_millis=134621`,
    `vortex_encode_write_millis=69200`, `universal_ingest_decode_millis=2953`,
    `universal_ingest_derived_metadata_build_millis=44104`,
    `vortex_writer_runtime_applied_parallelism=12`, `fallback_attempted=false`, and
    `external_engine_invoked=false`.
  - Query sanity: targeted post-ingest run
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_ingest_shape_query_uat_20260831T020821Z`
    completed Q01/Q21/Q23/Q24/Q29/Q33/Q34/Q35 with zero failures; selected-lane total was
    `93.089s` and geomean was `4.702s`.
  - Full UAT after current branch replacement ingest:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_after_replacement_ingest_current_branch_20260831T155208Z/summary.json`
    completed all `43` query lanes with query total `200.904s`, geomean `1.382s`, zero fallback,
    and zero external engine invocation.
- Decision: retain the writer worker-pool, timing split, broad source UTF-8 compression, and compact
  source-native metadata preference as the current best local evidence. Do not ship the six-field
  compression profile.

### `2026-08-31` Replacement-Ingest Harness Safety And Query UAT

- Change retained: `scripts/run_clickbench_ingest_uat.sh --replace-existing` now runs source
  residency preflight before deleting the current target or temporary candidates. This prevents a
  sparse/nonresident official-source placeholder from removing the only usable single `.vortex`
  artifact before ingest can start.
- Evidence:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260831T145517Z/prepare_summary.json`
  stopped at `source_residency_preflight_failed`, reported source logical bytes `14779976446`,
  source allocated bytes `0`, `target_exists=true`, and target bytes `38148327444`.
- Query evidence:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_current_branch_query_uat_20260831T145526Z/summary.json`
  completed one run of all `43` ClickBench query lanes with zero failures, zero fallback, and zero
  external-engine invocation. Query total was `205.642s`; query geomean was `1.507s`.
- Decision: ship the harness safety fix. The query path is clean on the existing artifact. A true
  replacement-ingest run for the newest writer compression/admission changes completed later in the
  same session at
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260831T154142Z`;
  keep the earlier preflight failure as proof that the harness no longer deletes the current target
  before source checks.

### `2026-08-30` Metadata-First Public Source Identity

- Change under validation: public `prepare dataframe` defaults to metadata-only source identity and
  makes full source-content fingerprinting an explicit `content_digest` proof opt-in.
- Expected gain: remove the up-front full-source read before Universal Ingest starts streaming and
  writing the `.vortex` artifact. The replacement-ingest UAT stall observed on the 14.78 GB
  ClickBench Parquet source was in `fingerprint_local_source_file_with_budget_report`, not in the
  writer.
- Evidence fields to check: `source_fingerprint_policy`, `source_fingerprint_kind`,
  `source_fingerprint_identity_source`, `source_content_fingerprint_requested`,
  `source_content_fingerprint_performed`, `source_read_byte_acquisition_millis`, and
  `source_read_scout_timing_split_status`.
- Focused validation result: ship the metadata-first source identity path. Default public prepare no
  longer opens a whole-source content fingerprint stream; content fingerprinting is explicit proof
  work only.
- Desktop UAT result: gated replacement-ingest UAT on `2026-08-30T16:38:57Z` stopped before running
  ShardLoom because `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/sources/hits.parquet`
  reported `14779976446` logical bytes and `0` allocated bytes. The full start-to-finish ingest
  timing remains pending until the official source is physically materialized locally.

### `2026-06-28` Deferred Large Layout Inventory

- Change: large public prepares use upstream Vortex writer row-count summary plus streaming artifact
  digest at prepare time, and defer the expensive Vortex layout inventory open until query/open time.
- Expected gain: remove the long post-write idle/reopen tail after the final artifact already exists.
- Evidence:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_deferred_inventory_20260628T011807Z`.
- Result: the ad hoc Python harness was flawed because it captured stdout without draining it. The
  CLI process became idle after the final artifact was stable, so the run was terminated and not used
  as a load-time claim.
- Decision: keep the code path because it is still structurally correct and removes a plausible
  large-artifact post-write reopen hazard, but do not claim load-time improvement from that run.

### `2026-06-28` CLI-Only Gated Runner

- Change: added `scripts/run_clickbench_ingest_uat.sh`.
- Purpose: run `target/release/shardloom prepare dataframe ...` directly, file-back stdout/stderr,
  track workspace-safe hidden temp files, and enforce runtime/artifact/idle gates.
- Status: runner validates with `bash -n` and is now the primary local ingest UAT harness.
- Current gate shape: runtime cap, artifact-size cap, stable-idle cap, and minimum-progress cap.
  The default minimum-progress gate requires more than `1 GB` of candidate artifact bytes by
  `360s`, which allows the current retained profile but drops profiles that burn several minutes of
  CPU without entering the write ramp.

### `2026-06-28` Source-Native Derived Metadata Preference

- Change: large columnar sources now prefer source-native dictionary/typed-time derived metadata and
  avoid defaulting to full per-row hidden UTF-8 length/domain synthesis when source-native metadata
  is available.
- Expected gain: reduce per-row string work and artifact bloat for ClickBench-style Parquet input.
- Evidence:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260628T014257Z`.
- Result: the CLI prepare completed cleanly in `602s`, produced the expected single
  `34.93 GB` `.vortex` artifact, and reported:
  - `prepare_once_millis=541867`
  - `vortex_write_millis=520784`
  - `vortex_segment_write_millis=519910`
  - `workspace_stage_millis=875`
  - `reopen_verify_millis=0`
  - `footer_segment_count=36660`
  - `derived_columns=14`
- Decision: not shipped as a performance improvement. The run is functionally clean and preserves
  the single-artifact contract, but it did not reduce the writer-dominant long pole. Keep the
  structural source-native path for correctness and future dictionary preservation, but treat the
  next material lever as writer policy and derived metadata representation.

### `2026-06-28` Restored Broad Source-Text Fast-Zstd Profile

- Change retained: after dropping the selective payload-only profile, restored the broad source-text
  fast-Zstd profile for ClickBench text fields and kept public evidence for the compression policy,
  compression field count, and field names.
- Evidence:
  - `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260628T031727Z`
    completed in `421s`.
  - `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260628T040813Z`
    restored the canonical artifact after the rejected ultra-row-block experiment and completed in
    `390s`.
- Result: the latest retained CLI prepare completed cleanly, produced the expected single
  `34.93 GB` `.vortex` artifact, and reported:
  - `prepare_once_millis=351003`
  - `vortex_write_millis=336171`
  - `vortex_segment_write_millis=336159`
  - `workspace_stage_millis=12`
  - `reopen_verify_millis=0`
  - `footer_segment_count=36660`
  - `writer_compression_policy=vortex_large_source_text_fast_zstd_no_dict_layout_statistics`
  - `writer_compression_field_count=28`
  - `writer_layout_strategy_applied=vortex_write_strategy_row_block_262144_target_8mb_source_text_fast_zstd_no_dict_embedded_olap_layout_statistics`
  - `fallback_attempted=false`
  - `external_engine_invoked=false`
- Decision: retained as the current local UAT baseline. This is a material load-time improvement
  over the earlier `602s` CLI run and restores the canonical single-artifact UAT file, but it is not
  the final ingest architecture because write/segment work is still the dominant cost.

### `2026-06-28` Vortex-To-Vortex Probe

- Change tested: added a temporary local probe that opened the retained
  `hits-parquet-100m.vortex` artifact with upstream Vortex, streamed
  `VortexFile::scan().into_array_stream()` directly into
  `VortexSession::write_options().write(...)`, and wrote one candidate `.vortex` target. This was a
  true Vortex-source probe, not Parquet-to-Vortex import.
- Evidence:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/vortex_to_vortex_repack_20260628T172833Z`.
- Result: small fixture V2V worked in `~3.9ms` internal time, but the 100M artifact probe was
  stopped after about `94s` with only about `112MB` staged. This shows the scan-stream writer route
  re-encodes/resegments arrays and is not an encoded-layout-preserving copy path.
- Decision: do not ship scan-stream re-encode as the default V2V Universal Ingest path. Promote
  `UNIVERSAL-INGEST-VORTEX-SOURCE-LANE-1` in the phase plan: existing `.vortex` sources should be
  admitted as prepared/native Vortex through pass-through or workspace-safe copy, while any explicit
  layout rewrite must fail closed until an encoded segment-preserving rewrite provider exists.

### `2026-06-28` Universal Ingest Native Vortex Source Lane

- Change retained: public `prepare dataframe --input-format vortex` now admits existing `.vortex`
  artifacts through `native_vortex_artifact_prepare` before compatibility-source adapter selection.
  Same-artifact targets use metadata/footer pass-through; distinct targets use a workspace-safe byte
  copy. Neither path calls `VortexFile::scan().into_array_stream()` or the upstream Vortex writer,
  so encoded layout is preserved and the slow scan-stream re-encode probe remains dropped.
- Evidence:
  - Focused Rust tests:
    `CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target-codex-v2v cargo +1.91.1 test -p shardloom-vortex --features vortex-write --lib native_vortex_artifact_prepare -- --nocapture`.
  - Focused CLI check:
    `CARGO_BUILD_JOBS=2 CARGO_TARGET_DIR=target-codex-v2v cargo +1.91.1 check -p shardloom-cli --features vortex-write,universal-format-io,vortex-local-primitives --bin shardloom`.
  - 100M same-artifact public UAT with the rebuilt ShardLoom CLI:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/retained-evidence/v2v_public_prepare_20260628T185919Z.summary.json`.
  - Small copy public UAT with the rebuilt ShardLoom CLI:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/retained-evidence/v2v_public_prepare_small_copy_20260628T190453Z.summary.json`.
- Result:
  - 100M same-artifact public UAT completed in `0.86s` wall time with
    `prepare_once_millis=16.142`, `vortex_write_millis=0`, `input_row_count=99997497`,
    `vortex_to_vortex_policy=vortex_native_prepared_state_pass_through`,
    `vortex_to_vortex_encoded_layout_preserved=true`,
    `vortex_to_vortex_reencode_performed=false`,
    `vortex_to_vortex_workspace_copy_performed=false`,
    `vortex_to_vortex_upstream_vortex_scan_called=false`,
    `vortex_to_vortex_upstream_vortex_write_called=false`,
    `fallback_attempted=false`, and `external_engine_invoked=false`.
  - Small copy public UAT completed through the same rebuilt release CLI route with
    `vortex_to_vortex_policy=vortex_native_workspace_safe_byte_copy`,
    byte-identical source/target SHA-256 digests, `input_row_count=5`,
    `prepare_once_millis=1.228`, `vortex_write_millis=0.353`,
    `vortex_to_vortex_reencode_performed=false`,
    `vortex_to_vortex_upstream_vortex_scan_called=false`, and
    `vortex_to_vortex_upstream_vortex_write_called=false`.
- Decision: retain as the default Universal Ingest Vortex-source behavior. Vortex-to-Vortex
  lifecycle now means prepared-artifact pass-through or byte-preserving copy unless a future
  encoded segment-preserving layout rewrite provider is implemented and separately admitted.

### `2026-06-28` Ultra Row-Block / Segment-Economy Profile

- Change tested: increase large text/high-cardinality row blocks to reduce footer/segment overhead
  and artifact metadata pressure.
- Evidence:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260628T035008Z`
  and
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_ultra_segment_profile_20260628T035812Z/summary.json`.
- Result: load looked materially better in isolation (`360s`, `34.86 GB`, `25038` segments,
  `prepare_once_millis=314240`, `vortex_write_millis=299419`), but downstream order-by/top-K
  locality regressed. The saved `CB-Q25`
  `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10` guard moved
  from the retained sub-second row-ref path to `13.734s`.
- Decision: dropped and reverted. Fewer/larger segments are not sufficient by themselves; future
  writer/layout iterations must balance segment economy with row-position/order-key locality and
  must run the Q24-Q27 guard set before shipping.

### `2026-06-28` Selective Source-Text Compression Profile

- Change tested: retain fast Zstd only for high-value free-text/URL payload columns and let
  categorical/short string fields use the default dictionary/layout path.
- Evidence addition: public preparation output now includes
  `vortex_writer_compression_field_count` and `vortex_writer_compression_field_names` so a run can
  prove which source-text compression profile was active from the JSON artifact.
- Expected gain: materially reduce Vortex writer CPU by avoiding Zstd work on many low-value
  categorical text columns, while keeping the columns that dominate string scans and storage
  compressed inside the same `.vortex` artifact.
- Evidence:
  - `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260628T024452Z`
    failed with `No space left on device` because a stale duplicate artifact was still present.
  - `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260628T024931Z`
    failed with return code `137` after `215s`, wrote only a `168 MB` hidden temp artifact, and did
    not produce the canonical `.vortex` target.
- Decision: dropped. The selective payload-only compression profile is not a shippable performance
  improvement. The active source-text profile returns to the prior broad fast-Zstd text field set
  that has full replacement UAT evidence. Keep the compression field-count/name evidence because it
  is useful for future ship/drop comparisons.

## Dropped Or Not-Yet-Shipped Profiles

- Dense indexed 4-ary heap for proofbound heavy-hitter sketches:
  - Reason dropped: correct and slightly faster, but below the material-gain threshold.
  - Evidence:
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/heavy_hitter_indexed_heap_20260831T124753Z/summary.json`.
  - Result: targeted original Q23/Q34/Q35 UAT completed `9/9` runs with
    `fallback_attempted=false` and `external_engine_invoked=false`. Best retained timings were
    Q23 `10.912s`, Q34 `28.043s`, and Q35 `28.085s` versus the restored-lean retained baseline
    Q23 `11.152s`, Q34 `30.821s`, and Q35 `29.525s`.
  - Decision: dropped and reverted because Q34+Q35 improved by about `7%` combined, below the
    `10%` ship threshold. Keep optimizing these lanes through dictionary/code summaries,
    candidate-free pruning, and aggregate-state/layout changes rather than an isolated heap swap.
- Large-source uncompressed fast load:
  - Reason dropped: previous branch evidence showed artifact-size regression.
  - Relevant ledger area:
    `docs/architecture/phased-execution-completed-ledger.md` around the June 22 ingest writer
    experiments.
- All-column balanced BtrBlocks:
  - Reason dropped: previous branch evidence rejected it versus source-text profile.
- Ad hoc Python replacement-ingest harness:
  - Reason dropped: it can measure Python pipe blocking instead of ShardLoom ingest runtime.
- Exact per-segment string-frequency summaries:
  - Reason not active: under the current single `.vortex` artifact rule, Vortex 0.75 exposes
    standard file statistics (`is_constant`, sortedness, min, max, sum, null count,
    uncompressed-size, and NaN count) through the public writer, but not a stable arbitrary
    in-file frequency-summary page for ShardLoom-owned exact string maps.
  - Decision: do not implement this as a sidecar, materialized view, query-answer cache, or broad
    hidden physical-column expansion. Re-open only if upstream Vortex adds a stable in-file
    metadata/custom-stat provider or ShardLoom has an approved single-file extension-column design
    that improves load, artifact size, and Q23/Q34/Q35-style runtime together.
- Release PGO/native allocator matrix:
  - Reason not active: `GAR-PERF-2H` already shipped the optimized build-profile evidence lane,
    including `release-lto`, `release-pgo`, `release-native-benchmark`, and
    `scripts/build_shardloom_pgo.py`.
  - Decision: keep allocator experiments out of the current runtime optimization queue until a
    fresh local profile shows material runtime benefit without portability, memory, packaging, or
    reproducibility regressions.
- Generic cross-thread grouped-aggregate state fork:
  - Reason not active: the shared grouped aggregate runtime already ships capillary partial paths
    for dictionary counts, transformed dictionary code-pair measures, materialized string partials
    after direct-provider misses, compact count/sum/avg state, and top-K retained windows. A second
    worker-local aggregate engine would duplicate that path and risks reintroducing the prior
    Q33-like near-input-cardinality regression.
  - Decision: keep aggregate optimization inside the existing shared runtime unless a targeted
    ship/drop pass proves a narrower worker-state fork materially improves eligible lanes without
    slowing Q33/Q34/Q35-style guards.

## Open Material Hypotheses

1. Upstream writer buffering is too opaque for fast load. ShardLoom may need a true capillary
   writer pipeline that emits bounded source units earlier while still committing one final
   `.vortex` artifact.
2. Text compression is still likely too expensive in the current source-text fast Zstd profile, but
   the first selective payload-only profile was killed during replacement UAT. Future attempts need
   a larger architectural change, such as dictionary-derived metadata or source-native dictionary
   preservation, rather than simply reducing the field override list.
3. Hidden derived metadata should be dictionary/code-map metadata wherever possible, not full
   per-row columns, especially for URL domain and UTF-8 length families.
4. Parquet source-native dictionaries should be preserved further into Vortex write; avoid
   decode/re-encode loops for dictionary-heavy string columns.
5. The writer needs better progress/timing evidence: source read, Arrow batch production,
   dictionary/derived metadata, Arrow-to-Vortex conversion, compression, layout buffering, segment
   write, workspace stage, and final evidence should be separated enough for ship/drop decisions.
6. URL/string predicate lanes need embedded dictionary or segment-membership metadata inside the
   single `.vortex` artifact. Current official Q21-Q24-style evidence still does broad URL/string
   predicate scans before aggregation or row-ref top-K; reducing that cost is materially more
   valuable than further small writer knobs.

## Required Ship/Drop Cadence

- Use `scripts/run_clickbench_ingest_uat.sh` for ingest experiments.
- Run short gated passes first. A profile should show meaningful byte progress within the configured
  window before a full replacement run is allowed.
- Do not edit the runner while a run is active.
- A change ships only if it improves one of:
  - load time,
  - artifact size,
  - downstream query time,
  - evidence clarity needed to isolate the next material bottleneck,
  without materially worsening the others.
- A change drops if it adds complexity, increases artifact size, or slows load/query runtime without
  enabling a clearly superior next step.
