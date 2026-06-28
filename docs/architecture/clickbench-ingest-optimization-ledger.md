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

## Best-Known Baseline To Beat

- Profile: single `.vortex` artifact, Parquet official source, source-text fast Zstd writer profile,
  embedded OLAP layout/statistics in the artifact, max parallelism `2`.
- Artifact: `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/vortex/hits-parquet-100m.vortex`.
- Source: `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/sources/hits.parquet`
  (`14.78 GB` local file).
- Typical artifact size: about `34.93 GB`.
- Best retained full replacement evidence:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260628T040813Z`.
- Current known pain: prepare/load remains writer/encode/segment dominated. Earlier evidence recorded
  prepare around `515s`, with Vortex write/segment write around `455s`; the latest retained run is
  faster but still dominated by Vortex write/segment work.

## Current Branch Experiments

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

- Large-source uncompressed fast load:
  - Reason dropped: previous branch evidence showed artifact-size regression.
  - Relevant ledger area:
    `docs/architecture/phased-execution-completed-ledger.md` around the June 22 ingest writer
    experiments.
- All-column balanced BtrBlocks:
  - Reason dropped: previous branch evidence rejected it versus source-text profile.
- Ad hoc Python replacement-ingest harness:
  - Reason dropped: it can measure Python pipe blocking instead of ShardLoom ingest runtime.

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
