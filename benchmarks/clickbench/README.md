<!-- SPDX-License-Identifier: Apache-2.0 -->

# ClickBench OLAP Runtime Coverage

`queries.sql` is the repo-managed ClickBench OLAP query-family fixture. It mirrors the 43
canonical `hits` queries from the upstream ClickBench `clickhouse/queries.sql` surface as of
2026-06-18.

This fixture is not a benchmark result and does not authorize a performance claim. It exists so
ShardLoom runtime work can be checked against the complete query family instead of adding
scenario-only shims.

Run the coverage gate with:

```bash
python3 scripts/check_clickbench_olap_runtime_coverage.py
```

The gate writes `target/clickbench-olap-runtime-coverage.json` with:

- `CB-Q01` through `CB-Q43` query ids;
- required operator tags and input-column inventory;
- current route status for admitted primitive SQL shapes;
- stable route-family ids and next actions;
- capillary work units and PulseWeave pressure signals for stateful aggregate, distinct, top-K,
  offset, and string-scan shapes;
- fail-closed memory/spill posture for stateful scale shapes where native spill is not yet
  certified;
- fixture-tier strategy fields for small local correctness, medium sequential UAT, and optional
  full-scale artifact production;
- `fallback_attempted=false`, `external_engine_invoked=false`, and
  `performance_claim_allowed=false` for every row.

The current coverage map validates 43 admitted rows and 0 implementation-required rows. Admitted
rows include primitive count/filter-project rows, no-group scalar aggregate projections over
`count`, `sum`, `avg`, `min`, and `max`, count-distinct state, filtered grouped aggregates, grouped
order/top-K/offset, multi-key group-by, raw-row sorted top-K, UTF-8 `LIKE`/`NOT LIKE`, `IN`, date/time
extract/trunc, `length`/`HAVING`, regex-domain group keys, group ordinals/constants, arithmetic
group keys, `CASE` group keys, and repeated `SUM(column +/- constant)` measures.

This is local route-readiness evidence only. It must not be treated as a ClickBench performance
result, public superiority claim, or permission to route through DuckDB, Polars, pandas, Spark,
DataFusion, or any other fallback engine.

## Scale Fixture Strategy

The validator records three fixture tiers so runtime coverage and performance publication do not
collapse into the same check:

| Tier | Purpose | Required for PR fast lane |
| --- | --- | --- |
| `small_deterministic_local` | Local deterministic correctness and route-readiness coverage for all 43 query families. | Yes |
| `medium_sequential_uat` | Optional local stress/UAT over larger generated `hits`-like data, run sequentially for device safety. | No |
| `full_100m_artifact_runner` | Manual/offline full-scale artifact production after maintainer approval. | No |

All tiers are intended to run sequentially at the query/harness level for laptop safety. Public
runtime evidence records requested and effective max parallelism; current local runtime may apply a
safe effective floor of `2` for eligible scan/aggregate/prepare work. The coverage artifact is not a
timing result. Full-scale performance claims require a promoted benchmark artifact and the normal
claim gates.

## Local 100M UAT Evidence

The current local Desktop 100M full-query UAT artifact is:

`/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_post_merge_1356_replaced_20260623T151523Z/summary.json`

The checked-in burndown intake is:

`docs/benchmarks/clickbench-100m-uat-burndown.json`

This is implementation/UAT evidence only, not an official ClickBench submission. The run replaced
the local 100M `.vortex` artifact from official Parquet input, used
`execution_policy=native_vortex`, observed `max_parallelism=2`, and executed queries sequentially.
It attempted all 43 rows: 43 completed successfully, 0 hit the 180-second UAT cap, and every
successful row reported no fallback or external-engine execution. Successful query wall time totaled
563.581 seconds with a 3.063971-second geomean.

Remaining above-1s rows are optimization candidates, not fallback permission. Current work focuses
on deeper consumption of embedded Vortex layout/statistics, string/domain metadata, encoded
dictionaries, capillary aggregate state, and row-reference top-K materialization while preserving the
shared native Vortex runtime family and single-artifact `.vortex` contract.

The first burndown batch implements direct UTF-8 contains counting for count-only predicates, mixed
predicate splitting so safe conjuncts still push into Vortex scans, typed exact distinct/group keys,
functional-dependency pruning for deterministic offset-derived group keys, source-order no-sort
group output, a single-key grouped aggregate fast path for identity/length/URL-domain keys,
capillary ordered-candidate selection for grouped top-K/offset finalization, direct non-null
encoded-layout admission guards, bounded top-K retention evidence, row-reference final-K
materialization for large bounded payload projections, direct scalar aggregate updates for
`count`/`sum`/`avg`/`min`/`max` and exact `count_distinct`, repeated numeric SUM/AVG expression
fusion, transformed chunk-dictionary grouping for URL-domain/length expressions, and compact exact
`length(...)` grouped measures. The post-merge local UAT retained functional coverage and no-fallback
evidence; its timing remains local implementation evidence rather than a public performance claim.

## State And Spill Fields

Rows that use aggregate, grouped aggregate, count-distinct, top-K, offset, rolling/window, or
string-scan pressure carry:

- `state_budget_schema_version`
- `state_budget_required`
- `state_family`
- `capillary_work_units`
- `pulseweave_pressure_signals`
- `spill_policy`
- `spill_required`
- `spill_supported`
- `fail_closed_if_spill_required`

Current stateful rows report bounded in-memory route budgets and `spill_supported=false`. If a
future scale fixture needs spill before native spill is certified, the route must fail closed with a
stable diagnostic instead of silently continuing or delegating to another engine.
