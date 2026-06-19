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
- stable implementation ids and next actions for remaining OLAP kernel families;
- `fallback_attempted=false`, `external_engine_invoked=false`, and
  `performance_claim_allowed=false` for every row.

The current coverage map validates 28 admitted rows and 15 implementation-required rows. Admitted
rows include primitive count/filter-project rows, no-group scalar aggregate projections over
`count`, `sum`, `avg`, `min`, and `max`, count-distinct state, filtered grouped aggregates, grouped
order/top-K/offset, multi-key group-by, and repeated `SUM(column +/- constant)` measures.

The remaining rows are implementation tracks for raw-row sort/top-K, UTF-8 `LIKE`/`NOT LIKE`,
`IN`, date/time extract/trunc, `length`/`HAVING`, regex replace, group ordinals/constants,
arithmetic group keys, and `CASE` group keys. They must not be treated as completed blockers or
routed through DuckDB, Polars, pandas, Spark, DataFusion, or any other fallback engine.
