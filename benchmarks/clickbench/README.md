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
