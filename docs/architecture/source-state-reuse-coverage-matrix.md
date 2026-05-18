# Source-State Reuse Coverage Matrix

Status: completed for `GAR-PERF-1B`

This matrix classifies source-state reuse for the traditional analytics prepared/native batch lane.
It is a runtime-plumbing coverage contract for `traditional-analytics-vortex-batch-run`, not a
performance, encoded-native, SQL/DataFrame, object-store/lakehouse, production, or
Spark-displacement claim.

## Contract

The prepared/native batch path remains:

```text
prepared Vortex artifacts -> one batch process -> optional scoped SourceState families -> scenario rows -> evidence
```

Every scenario family is classified with one of these statuses:

- `source-state-reused`: a scoped in-process source-state family is shared by multiple requested
  child scenarios.
- `source-state-not-needed`: the scenario has no reusable derived source-state family in the
  current batch lane, or there is only one consumer for the relevant family.
- `blocked-with-reason`: the scenario family is intentionally outside the scoped prepared/native
  source-state reuse lane.
- `unsupported-with-reason`: the source-state coverage contract does not support that scenario
  family.

The batch evidence emits:

- `source_state_coverage_schema_version=shardloom.traditional_analytics.source_state_coverage.v1`
- `source_state_coverage_matrix_ref=docs/architecture/source-state-reuse-coverage-matrix.md`
- `source_state_coverage_status_vocabulary`
- `source_state_coverage_all_requested_scenarios_classified=true`
- `source_state_coverage_matrix`
- `scenario_<slug>_source_state_coverage_status`
- `scenario_<slug>_source_state_coverage_family`
- `scenario_<slug>_source_state_coverage_reason`
- `source_state_digest_status=not_emitted_scoped_in_memory_source_state`
- `source_state_digest_reason`
- `source_state_fallback_attempted=false`
- `source_state_external_engine_invoked=false`

The current scoped source-state families are in-memory derived runtime state. They intentionally
keep `source_state_digest_status=not_emitted_scoped_in_memory_source_state`. `GAR-IOREUSE-1A` adds
a separate universal, format-neutral SourceState benchmark row contract with local source IDs,
digests, source-format/location/fingerprint/schema fields, parse/decode plan digest, reuse
hit/reason, no-fallback fields, and claim boundaries. Invalidation and cross-format prepared/output
reuse remain follow-up work.

## Coverage Matrix

| Scenario family | Scenario rows | Coverage status | Source-state family | Evidence fields | Reason |
| --- | --- | --- | --- | --- | --- |
| Basic ingest/sum | `csv/file ingest` | `source-state-not-needed` | `source_scan` | `source_state_coverage_*`, `source_metadata_snapshot_*` | The row reads the prepared artifact directly and has no reusable derived source-state family in the current batch lane. |
| Selective filter and filter/project/limit | `selective filter`, `filter + projection + limit` | `source-state-reused` when both rows are requested; otherwise `source-state-not-needed` | `selective_filter` | `source_state_selective_filter_*`, `batch_source_state_metric_aggregation_used` | The shared filtered `id,value,metric` state is reused across the pair; single-consumer runs keep scenario-local scan evidence explicit. |
| Projection-only | `wide projection` | `source-state-not-needed` | `projection_only` | `source_state_coverage_*`, `source_backed_scan_*` | Projection-only execution does not currently build a reusable derived source-state family. |
| Grouped aggregation | `group by aggregation`, `multi-key group by` | `source-state-reused` when both rows are requested; otherwise `source-state-not-needed` | `group_category_metric` | `source_state_group_category_metric_*` | The shared `group_key,category,metric` grouped state is reused across the pair. |
| Distinct and high-cardinality grouping | `distinct count`, `high-cardinality string group/distinct` | `source-state-reused` when both rows are requested; otherwise `source-state-not-needed` | `category_metric` | `source_state_category_metric_*` | The shared `category,metric` grouped state is reused across the pair. |
| Joins | `hash join`, `join + aggregate` | `source-state-reused` when both rows are requested; otherwise `source-state-not-needed` | `dimension_label` | `source_state_dimension_label_*` | The shared dimension-label lookup state is reused across the pair. |
| Ranking/window | `sort and top-k`, `top-N per group`, `row number window` | `source-state-reused` when at least two ranked rows are requested; otherwise `source-state-not-needed` | `ranked_metric` | `source_state_ranked_metric_*` | The shared ranked `group_key,id,metric` state is reused across ranked consumers. |
| Date/null metrics | `partition pruning`, `null-heavy aggregate` | `source-state-reused` when both rows are requested; otherwise `source-state-not-needed` | `date_null_metric` | `source_state_date_null_metric_*` | The shared `event_date,metric,nullable_metric_00` state is reused across the pair. |
| Many-file fixture | `many-small-files scan` | `source-state-not-needed` | `split_fixture` | `source_state_coverage_*`, `source_metadata_snapshot_*`, universal `source_state_*` row fields | The current prepared/native lane starts from prepared Vortex artifacts; GAR-IOREUSE-1A records local SourceState posture, while reusable split-discovery execution remains follow-up work. |
| Dirty input cleanup | `clean/cast/filter/write`, `malformed timestamp / dirty CSV` | `source-state-reused` when both rows are requested; otherwise `source-state-not-needed` | `dirty_input` | `source_state_dirty_input_*` | The shared dirty-input cleanup state is reused across the pair. |
| CDC overlay | `small change over large base` | `source-state-not-needed` | `cdc_overlay` | `source_state_coverage_*`, CDC preparation fields | The CDC overlay row is a single incremental-state workflow in the current batch lane. |
| Nested JSON | `nested JSON field scan` | `source-state-not-needed` | `nested_json` | `source_state_coverage_*`, `source_backed_scan_*` | The nested field scan is a single messy-data workflow in the current batch lane. |
| Stress workloads | `scale stress skewed join aggregation`, `scale stress multi-stage etl` | `blocked-with-reason` | `stress` | `source_state_coverage_*` | Stress rows are outside the scoped prepared/native source-state reuse smoke lane and require separate correctness/resource gates. |
| Object-store/table/lakehouse/generated-source scenarios | not part of `benchmarks/common/scenario_catalog.json` traditional analytics rows | `unsupported-with-reason` | not applicable | future `GAR-IOREUSE-*`, `GAR-GEN-*`, and object-store/table evidence | These families require separate admission and evidence before any source-state reuse support can be claimed. |

## Claim Boundary

Source-state reuse is scoped to one explicit prepared/native batch command invocation. It is not a
daemon, service, global cache, hidden fast mode, performance claim, encoded-native claim,
SQL/DataFrame runtime claim, object-store/lakehouse runtime claim, Foundry claim, or package-release
claim.

External engines are never invoked for source-state reuse:

```text
source_state_fallback_attempted=false
source_state_external_engine_invoked=false
fallback_attempted=false
external_engine_invoked=false
```

## Next Runtime Work

This matrix leaves the next meaningful runtime work explicit:

- `GAR-PERF-1C`: fused filter/project/limit and selection-vector execution path.
- `GAR-PERF-2C`: Vortex Scan API pushdown completion.
- `GAR-PERF-2E`: fused operator pipeline.
- `GAR-IOREUSE-1A`: completed universal local SourceState row contract with stable IDs and digests.
- `GAR-IOREUSE-1B` and later: prepared-state/output-plan invalidation and cross-format reuse.
