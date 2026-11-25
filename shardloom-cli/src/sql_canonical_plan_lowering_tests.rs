use super::*;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use serde_json::{Value, json};
use shardloom_core::{ExecutionResourceOrigin, ExecutionResources, ScalarValue};
use std::fs::File;
use std::sync::Arc;

fn write_parquet_rows(path: &Path, batch: &RecordBatch, row_group_rows: usize) {
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(row_group_rows))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), Some(props)).unwrap();
    for offset in (0..batch.num_rows()).step_by(row_group_rows) {
        let len = row_group_rows.min(batch.num_rows() - offset);
        writer.write(&batch.slice(offset, len)).unwrap();
    }
    writer.close().unwrap();
}

fn try_prepare_file_sql(
    statement: &str,
) -> NativeResult<(ResidentVortexSession, PreparedVortexRelational)> {
    use crate::native::{SqlSource, SqlSourceBinding, prepare_sql};

    let resources =
        ExecutionResources::from_bytes(64 << 20, 2, ExecutionResourceOrigin::Context).unwrap();
    let session = ResidentVortexSession::new(64 << 20, 2).unwrap();
    let bindings: Vec<SqlSourceBinding> = source_leaves(statement)?
        .into_iter()
        .map(|leaf| {
            let path_str = leaf.path.to_string_lossy().into_owned();
            let format = if path_str.ends_with(".parquet") {
                "parquet"
            } else {
                "vortex"
            };
            SqlSourceBinding {
                reference: path_str,
                source: SqlSource::File {
                    format: format.to_string(),
                    schema: None,
                },
            }
        })
        .collect();
    let prepared = prepare_sql(
        statement,
        resources,
        &session,
        &bindings,
        &CancellationToken::default(),
    )?;
    Ok((session, prepared))
}

fn prepare_file_sql(statement: &str) -> (ResidentVortexSession, PreparedVortexRelational) {
    try_prepare_file_sql(statement).unwrap_or_else(|error| panic!("{statement}: {error}"))
}

fn collect_rows((_, prepared): &(ResidentVortexSession, PreparedVortexRelational)) -> Vec<Value> {
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    result
        .result_jsonl
        .value()
        .lines()
        .map(|row| serde_json::from_str::<Value>(row).unwrap())
        .collect()
}

fn batch_i64(cols: &[(&str, Vec<Option<i64>>)]) -> RecordBatch {
    let fields: Vec<Field> = cols
        .iter()
        .map(|(name, vals)| {
            let nullable = vals.iter().any(Option::is_none);
            Field::new(*name, DataType::Int64, nullable)
        })
        .collect();
    let arrays: Vec<Arc<dyn arrow_array::Array>> = cols
        .iter()
        .map(|(_, vals)| Arc::new(Int64Array::from(vals.clone())) as Arc<dyn arrow_array::Array>)
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn batch_i64_str(
    i_cols: &[(&str, Vec<Option<i64>>)],
    s_cols: &[(&str, Vec<Option<&str>>)],
) -> RecordBatch {
    let mut fields = Vec::new();
    let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
    for (name, vals) in i_cols {
        fields.push(Field::new(
            *name,
            DataType::Int64,
            vals.iter().any(Option::is_none),
        ));
        arrays.push(Arc::new(Int64Array::from(vals.clone())));
    }
    for (name, vals) in s_cols {
        fields.push(Field::new(
            *name,
            DataType::Utf8,
            vals.iter().any(Option::is_none),
        ));
        arrays.push(Arc::new(StringArray::from(vals.clone())));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

#[test]
fn canonical_three_way_mixed_vortex_parquet_star_join_and_runtime_filter_pruning() {
    let dir = TestDirectory::new();
    let vx = fixture(); // value: 1..=5, metric: 10..=50
    let fact_pq = dir.0.join("fact.parquet");
    let dim2_pq = dir.0.join("dim2.parquet");

    // 32 rows across 8 row groups (4 rows each): k1=0..32, k2=100..132.
    write_parquet_rows(
        &fact_pq,
        &batch_i64(&[
            ("k1", (0..32).map(Some).collect()),
            ("k2", (100..132).map(Some).collect()),
            ("payload", (1000..1032).map(Some).collect()),
        ]),
        4,
    );
    write_parquet_rows(
        &dim2_pq,
        &batch_i64_str(
            &[("id", vec![Some(102), Some(104), Some(999)])],
            &[("tag", vec![Some("hit_two"), Some("hit_four"), Some("miss")])],
        ),
        4,
    );

    let sql = format!(
        "SELECT f.k1 AS k1, d1.metric AS metric, d2.tag AS tag \
         FROM '{}' AS f \
         INNER JOIN '{}' AS d1 ON d1.value = f.k1 \
         INNER JOIN '{}' AS d2 ON f.k2 = d2.id \
         WHERE d1.metric >= 20 \
         ORDER BY k1",
        fact_pq.display(),
        vx,
        dim2_pq.display()
    );
    assert!(is_relational(&sql).unwrap());
    assert!(!is_plain_select(&sql).unwrap());
    assert_eq!(source_leaves(&sql).unwrap().len(), 3);

    let (_session, prepared) = prepare_file_sql(&sql);
    let exec = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let rows: Vec<Value> = exec
        .result_jsonl
        .value()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![
            json!({"k1": 2, "metric": 20, "tag": "hit_two"}),
            json!({"k1": 4, "metric": 40, "tag": "hit_four"}),
        ]
    );
    // Probe runtime filters prune fact.parquet's 8 row groups using d1 & d2 keys.
    let input_report = exec.execution.input.as_ref().unwrap();
    assert!(input_report.columnar_row_groups_read < input_report.columnar_row_groups_total);
}

#[test]
#[allow(clippy::too_many_lines)]
fn canonical_four_way_chain_residual_on_and_join_variants() {
    let dir = TestDirectory::new();
    let a = dir.0.join("a.parquet");
    let b = dir.0.join("b.parquet");
    let c = dir.0.join("c.parquet");
    let d = dir.0.join("d.parquet");

    write_parquet_rows(
        &a,
        &batch_i64(&[
            ("id", vec![Some(1), Some(2), Some(3), Some(4)]),
            ("b_id", vec![Some(10), Some(20), Some(10), Some(99)]),
            ("threshold", vec![Some(5), Some(50), Some(15), Some(1)]),
        ]),
        4,
    );
    write_parquet_rows(
        &b,
        &batch_i64(&[
            ("id", vec![Some(10), Some(20)]),
            ("c_id", vec![Some(100), Some(200)]),
            ("score", vec![Some(20), Some(10)]),
        ]),
        4,
    );
    write_parquet_rows(
        &c,
        &batch_i64(&[
            ("id", vec![Some(100), Some(200)]),
            ("d_id", vec![Some(1000), Some(2000)]),
        ]),
        4,
    );
    write_parquet_rows(
        &d,
        &batch_i64_str(
            &[("id", vec![Some(1000), Some(3000)])],
            &[("label", vec![Some("matched_d"), Some("unused_d")])],
        ),
        4,
    );

    // 4-way chain with reversed ON and non-equi residual ON condition.
    let chain_sql = format!(
        "SELECT a.id AS aid, b.score AS score, d.label AS label \
         FROM '{}' AS a \
         INNER JOIN '{}' AS b ON b.id = a.b_id AND b.score > a.threshold \
         INNER JOIN '{}' AS c ON c.id = b.c_id \
         LEFT OUTER JOIN '{}' AS d ON c.d_id = d.id \
         ORDER BY aid",
        a.display(),
        b.display(),
        c.display(),
        d.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&chain_sql)),
        vec![
            json!({"aid": 1, "score": 20, "label": "matched_d"}),
            json!({"aid": 3, "score": 20, "label": "matched_d"}),
        ]
    );

    // Multi-join mix exercising RIGHT OUTER, FULL OUTER, LEFT SEMI, LEFT ANTI, and CROSS JOIN.
    let multi_kind_sql = format!(
        "SELECT a.id AS aid, d.label AS dlabel \
         FROM '{}' AS a \
         RIGHT OUTER JOIN '{}' AS b ON a.b_id = b.id \
         FULL OUTER JOIN '{}' AS d ON b.id = d.id - 990 \
         ORDER BY aid NULLS LAST, dlabel NULLS LAST",
        a.display(),
        b.display(),
        d.display()
    );
    let multi_kind_rows = collect_rows(&prepare_file_sql(&multi_kind_sql));
    assert!(multi_kind_rows.contains(&json!({"aid": 1, "dlabel": "matched_d"})));
    assert!(multi_kind_rows.contains(&json!({"aid": null, "dlabel": "unused_d"})));

    let semi_anti_cross = format!(
        "SELECT a.id AS qid, d.label AS dlabel \
         FROM '{}' AS a \
         LEFT SEMI JOIN '{}' AS b ON a.b_id = b.id \
         CROSS JOIN '{}' AS d \
         WHERE a.id IN (1, 2) AND d.id = 1000 \
         ORDER BY qid",
        a.display(),
        b.display(),
        d.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&semi_anti_cross)),
        vec![
            json!({"qid": 1, "dlabel": "matched_d"}),
            json!({"qid": 2, "dlabel": "matched_d"}),
        ]
    );

    let derived_multi_sql = format!(
        "SELECT sub.aid AS aid, d.label AS dlabel \
         FROM ( \
             SELECT a.id AS aid, c.d_id AS did \
             FROM '{}' AS a \
             INNER JOIN '{}' AS b ON a.b_id = b.id \
             INNER JOIN '{}' AS c ON b.c_id = c.id \
         ) AS sub \
         INNER JOIN '{}' AS d ON sub.did = d.id \
         ORDER BY aid",
        a.display(),
        b.display(),
        c.display(),
        d.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&derived_multi_sql)),
        vec![
            json!({"aid": 1, "dlabel": "matched_d"}),
            json!({"aid": 3, "dlabel": "matched_d"}),
        ]
    );

    let anti_sql = format!(
        "SELECT a.id AS aid \
         FROM '{}' AS a \
         LEFT ANTI JOIN '{}' AS b ON a.b_id = b.id \
         LEFT JOIN '{}' AS d ON a.id = d.id",
        a.display(),
        b.display(),
        d.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&anti_sql)),
        vec![json!({"aid": 4})]
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn canonical_grouping_having_only_agg_distinct_unprojected_sort_cte_and_expressions() {
    let dir = TestDirectory::new();
    let vx = fixture(); // value 1..=5, metric 10..=50
    let groups_pq = dir.0.join("groups.parquet");
    let mult_pq = dir.0.join("mult.parquet");

    write_parquet_rows(
        &groups_pq,
        &batch_i64_str(
            &[("id", vec![Some(1), Some(2), Some(3), Some(4), Some(5)])],
            &[(
                "dept",
                vec![
                    Some("eng"),
                    Some("eng"),
                    Some("sales"),
                    Some("sales"),
                    Some("ops"),
                ],
            )],
        ),
        3,
    );
    write_parquet_rows(
        &mult_pq,
        &batch_i64(&[
            ("id", vec![Some(1), Some(2), Some(3), Some(4), Some(5)]),
            (
                "tag",
                vec![Some(10), Some(10), Some(20), Some(21), Some(30)],
            ),
        ]),
        3,
    );

    // GROUP BY + aggregates + aggregate ONLY in HAVING + non-recursive WITH CTE.
    let cte_agg_sql = format!(
        "WITH enriched AS ( \
             SELECT g.dept AS dept, v.metric AS metric, m.tag AS tag \
             FROM '{vx}' AS v \
             INNER JOIN '{}' AS g ON v.value = g.id \
             INNER JOIN '{}' AS m ON g.id = m.id \
         ) \
         SELECT e.dept AS dept, SUM(e.metric) AS sum_m, COUNT(DISTINCT e.tag) AS tags, \
                MIN(e.metric) AS min_m, MAX(e.metric) AS max_m \
         FROM enriched AS e \
         INNER JOIN '{}' AS m2 ON e.tag = m2.tag \
         GROUP BY e.dept \
         HAVING COUNT(*) >= 2 AND AVG(e.metric) >= 15.0 \
         ORDER BY sum_m DESC \
         LIMIT 1 OFFSET 0",
        groups_pq.display(),
        mult_pq.display(),
        mult_pq.display(),
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&cte_agg_sql)),
        vec![json!({"dept": "sales", "sum_m": 70.0, "tags": 2, "min_m": 30, "max_m": 40})]
    );

    // SELECT DISTINCT + unprojected ORDER BY rejection vs non-distinct unprojected ORDER BY.
    let distinct_sql = format!(
        "SELECT DISTINCT g.dept AS dept \
         FROM '{}' AS g \
         JOIN '{vx}' AS v ON g.id = v.value \
         JOIN '{}' AS m ON v.value = m.id \
         ORDER BY dept",
        groups_pq.display(),
        mult_pq.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&distinct_sql)),
        vec![
            json!({"dept": "eng"}),
            json!({"dept": "ops"}),
            json!({"dept": "sales"}),
        ]
    );

    let unprojected_distinct = format!(
        "SELECT DISTINCT g.dept AS dept \
         FROM '{}' AS g \
         JOIN '{vx}' AS v ON g.id = v.value \
         JOIN '{}' AS m ON v.value = m.id \
         ORDER BY v.value",
        groups_pq.display(),
        mult_pq.display()
    );
    assert!(try_prepare_file_sql(&unprojected_distinct).is_err());

    let unprojected_sort_sql = format!(
        "SELECT g.dept AS dept, \
                CASE WHEN m.tag >= 20 THEN 'hi' ELSE 'lo' END AS tier, \
                COALESCE(CAST(v.value AS utf8), 'none') AS v_txt \
         FROM '{}' AS g \
         JOIN '{vx}' AS v ON g.id = v.value \
         JOIN '{}' AS m ON v.value = m.id \
         WHERE v.value BETWEEN 1 AND 4 \
           AND g.dept LIKE 'eng%' \
           AND g.id IS DISTINCT FROM 99 \
         ORDER BY v.metric DESC \
         LIMIT 1 OFFSET 1",
        groups_pq.display(),
        mult_pq.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&unprojected_sort_sql)),
        vec![json!({"dept": "eng", "tier": "lo", "v_txt": "1"})]
    );

    // Ambiguous unqualified column across joined tables fails explicitly.
    let ambiguous_sql = format!(
        "SELECT id FROM '{}' AS g INNER JOIN '{}' AS m ON g.id = m.id INNER JOIN '{vx}' AS v ON m.id = v.value",
        groups_pq.display(),
        mult_pq.display()
    );
    let err = try_prepare_file_sql(&ambiguous_sql).err().unwrap();
    assert!(err.to_string().contains("ambiguous"), "{err}");
}

#[test]
fn canonical_bound_parameters_and_inputs_execute_through_declaration() {
    use crate::native::NativeRelationDeclaration;

    let left = Arc::new(
        NativeRelationDeclaration::sql(
            "SELECT 1 AS k, 10 AS v UNION ALL SELECT 2 AS k, 20 AS v",
            vec![],
        )
        .unwrap(),
    );
    let mid = Arc::new(
        NativeRelationDeclaration::sql(
            "SELECT 1 AS k, 100 AS m UNION ALL SELECT 2 AS k, 200 AS m",
            vec![],
        )
        .unwrap(),
    );
    let right = Arc::new(
        NativeRelationDeclaration::sql(
            "SELECT 1 AS k, 5 AS w UNION ALL SELECT 3 AS k, 9 AS w",
            vec![],
        )
        .unwrap(),
    );
    let sql = "SELECT l.k AS k, l.v + m.m + r.w AS total \
               FROM left_in AS l \
               INNER JOIN mid_in AS m ON l.k = m.k \
               INNER JOIN right_in AS r ON r.k = m.k \
               WHERE l.v >= :min_v";
    let decl = NativeRelationDeclaration::sql_with_parameters(
        sql,
        vec![
            ("left_in".into(), left),
            ("mid_in".into(), mid),
            ("right_in".into(), right),
        ],
        BTreeMap::from([("min_v".into(), ScalarValue::Int64(5))]),
    )
    .unwrap();

    let resources =
        ExecutionResources::from_bytes(64 << 20, 2, ExecutionResourceOrigin::Context).unwrap();
    let session = ResidentVortexSession::new(64 << 20, 2).unwrap();
    let token = CancellationToken::default();
    let (prepared, _) = decl
        .prepare(resources, &session, &[], None, &token)
        .unwrap();
    let rows: Vec<Value> = prepared
        .collect_jsonl(&token)
        .unwrap()
        .result_jsonl
        .value()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(rows, vec![json!({"k": 1, "total": 115})]);
}

struct CanonicalTempDir(PathBuf);

impl CanonicalTempDir {
    fn new(prefix: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "shardloom-canon-{prefix}-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for CanonicalTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn canonical_ast_bound_windows_frames_named_exclude_and_over_group_by() {
    let dir = CanonicalTempDir::new("win");
    let path = dir.path().join("events.parquet");
    let batch = batch_i64_str(
        &[
            ("id", vec![Some(1), Some(2), Some(3), Some(4)]),
            ("val", vec![Some(10), Some(20), Some(15), Some(30)]),
        ],
        &[("grp", vec![Some("a"), Some("a"), Some("b"), Some("b")])],
    );
    write_parquet_rows(&path, &batch, 4);

    let win_sql = format!(
        "SELECT id, grp, \
         ROW_NUMBER() OVER w AS rn, \
         RANK() OVER w AS rnk, \
         DENSE_RANK() OVER w AS drnk, \
         NTILE(2) OVER w AS nt, \
         LAG(val, 1) OVER w AS prev_val, \
         LEAD(val, 1) OVER w AS next_val, \
         FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv, \
         LAST_VALUE(val) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS lv, \
         NTH_VALUE(val, 2) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nv, \
         SUM(val) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) AS excl_sum, \
         COUNT(*) OVER (PARTITION BY grp ORDER BY val RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rng_cnt, \
         SUM(val) OVER (PARTITION BY grp ORDER BY id GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS grp_sum \
         FROM '{}' \
         WINDOW w AS (PARTITION BY grp ORDER BY id) \
         ORDER BY id",
        path.display()
    );
    assert!(canonical::parse_canonical_plan(&win_sql).is_some());
    let rows = collect_rows(&prepare_file_sql(&win_sql));
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0]["rn"], json!(1));
    assert_eq!(rows[0]["nt"], json!(1));
    assert_eq!(rows[0]["prev_val"], Value::Null);
    assert_eq!(rows[0]["next_val"], json!(20));
    assert_eq!(rows[0]["excl_sum"], json!(20.0));
    assert_eq!(rows[1]["rn"], json!(2));
    assert_eq!(rows[1]["prev_val"], json!(10));
    assert_eq!(rows[1]["next_val"], Value::Null);
    assert_eq!(rows[1]["excl_sum"], json!(10.0));
    assert_eq!(rows[1]["nv"], json!(20));
    assert_eq!(rows[1]["grp_sum"], json!(30.0));

    let group_win_sql = format!(
        "SELECT grp, SUM(val) AS total, RANK() OVER (ORDER BY SUM(val) DESC) AS total_rank \
         FROM '{}' GROUP BY grp ORDER BY total_rank, grp",
        path.display()
    );
    assert!(canonical::parse_canonical_plan(&group_win_sql).is_some());
    let group_rows = collect_rows(&prepare_file_sql(&group_win_sql));
    assert_eq!(
        group_rows,
        vec![
            json!({"grp": "b", "total": 45.0, "total_rank": 1}),
            json!({"grp": "a", "total": 30.0, "total_rank": 2}),
        ]
    );
}

#[test]
fn canonical_uncapped_set_operations_above_10k_rows_and_distinct_ops() {
    let dir = CanonicalTempDir::new("setops");
    let a_path = dir.path().join("set_a.parquet");
    let b_path = dir.path().join("set_b.parquet");
    let a_ids: Vec<Option<i64>> = (0..6000).map(Some).collect();
    let b_ids: Vec<Option<i64>> = (5000..11000).map(Some).collect();
    write_parquet_rows(&a_path, &batch_i64(&[("id", a_ids)]), 2048);
    write_parquet_rows(&b_path, &batch_i64(&[("id", b_ids)]), 2048);

    let union_all_sql = format!(
        "SELECT id FROM '{}' UNION ALL SELECT id FROM '{}'",
        a_path.display(),
        b_path.display()
    );
    assert!(canonical::parse_canonical_plan(&union_all_sql).is_some());
    let all_rows = collect_rows(&prepare_file_sql(&union_all_sql));
    assert_eq!(all_rows.len(), 12_000);

    let s1_path = dir.path().join("s1.parquet");
    let s2_path = dir.path().join("s2.parquet");
    write_parquet_rows(
        &s1_path,
        &batch_i64(&[("id", vec![Some(1), Some(2), Some(2), Some(3)])]),
        4,
    );
    write_parquet_rows(
        &s2_path,
        &batch_i64(&[("id", vec![Some(2), Some(3), Some(3), Some(4)])]),
        4,
    );
    for (op, expected) in [
        ("UNION", vec![json!(1), json!(2), json!(3), json!(4)]),
        ("INTERSECT", vec![json!(2), json!(3)]),
        ("EXCEPT", vec![json!(1)]),
    ] {
        let sql = format!(
            "SELECT id FROM '{}' {op} SELECT id FROM '{}'",
            s1_path.display(),
            s2_path.display()
        );
        assert!(canonical::parse_canonical_plan(&sql).is_some());
        let mut vals: Vec<i64> = collect_rows(&prepare_file_sql(&sql))
            .into_iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        vals.sort_unstable();
        let expected_ints: Vec<i64> = expected.iter().map(|v| v.as_i64().unwrap()).collect();
        assert_eq!(vals, expected_ints, "failed for {op}");
    }
}

#[test]
fn canonical_multi_ref_cte_materializes_shared_plan_and_respects_not_materialized() {
    let dir = CanonicalTempDir::new("cte");
    let path = dir.path().join("cte_src.parquet");
    write_parquet_rows(
        &path,
        &batch_i64(&[
            ("k", vec![Some(1), Some(2), Some(3), Some(4)]),
            ("v", vec![Some(10), Some(20), Some(30), Some(40)]),
        ]),
        4,
    );
    let token = CancellationToken::default();

    let mat_sql = format!(
        "WITH c AS (SELECT k, v FROM '{}' WHERE v >= 20) \
         SELECT a.k AS k, a.v + b.v AS paired \
         FROM c AS a INNER JOIN c AS b ON a.k = b.k ORDER BY a.k",
        path.display()
    );
    assert!(canonical::prefers_canonical_lowering(
        canonical::parse_canonical_plan(&mat_sql)
            .as_deref()
            .unwrap(),
        &[]
    ));
    let (_session_m, prep_m) = prepare_file_sql(&mat_sql);
    let res_m = prep_m.collect_jsonl(&token).unwrap();
    let rows_m: Vec<Value> = res_m
        .result_jsonl
        .value()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        rows_m,
        vec![
            json!({"k": 2, "paired": 40}),
            json!({"k": 3, "paired": 60}),
            json!({"k": 4, "paired": 80}),
        ]
    );
    assert_eq!(res_m.execution.composition.materialized_relations, 1);
    assert_eq!(res_m.execution.composition.materialized_reuses, 1);
    assert!(res_m.execution.scan_rows_delivered > 0);

    let unmat_sql = format!(
        "WITH c AS NOT MATERIALIZED (SELECT k, v FROM '{}' WHERE v >= 20) \
         SELECT a.k, a.v + b.v AS paired \
         FROM c AS a INNER JOIN c AS b ON a.k = b.k ORDER BY a.k",
        path.display()
    );
    let (_session_u, prep_u) = prepare_file_sql(&unmat_sql);
    let res_u = prep_u.collect_jsonl(&token).unwrap();
    assert_eq!(res_u.execution.composition.materialized_relations, 0);
    assert_eq!(
        res_m.execution.scan_rows_delivered * 2,
        res_u.execution.scan_rows_delivered
    );
}

fn write_part_lineitem_subquery_fixtures(dir: &CanonicalTempDir) -> (PathBuf, PathBuf) {
    let parts_path = dir.path().join("part.parquet");
    let lines_path = dir.path().join("lineitem.parquet");
    write_parquet_rows(
        &parts_path,
        &batch_i64(&[
            ("p_partkey", vec![Some(1), Some(2), Some(3), None]),
            ("p_tag", vec![Some(10), Some(20), Some(30), Some(40)]),
        ]),
        4,
    );
    write_parquet_rows(
        &lines_path,
        &batch_i64(&[
            (
                "l_partkey",
                vec![Some(1), Some(1), Some(1), Some(2), Some(2), None],
            ),
            (
                "l_quantity",
                vec![Some(2), Some(10), Some(18), Some(5), Some(35), Some(100)],
            ),
            (
                "l_extendedprice",
                vec![
                    Some(20),
                    Some(100),
                    Some(180),
                    Some(50),
                    Some(350),
                    Some(999),
                ],
            ),
        ]),
        6,
    );
    (parts_path, lines_path)
}

#[test]
fn canonical_decorrelated_subqueries_exists_in_q17_and_correlated_scalar() {
    let dir = CanonicalTempDir::new("subquery");
    let (parts_path, lines_path) = write_part_lineitem_subquery_fixtures(&dir);

    // 1. Decorrelated TPC-H Q17-style correlated scalar aggregate comparison.
    let q17_sql = format!(
        "SELECT p_partkey, SUM(l_extendedprice) AS total_price \
         FROM '{}' AS p INNER JOIN '{}' AS l ON p.p_partkey = l.l_partkey \
         WHERE l.l_quantity < (SELECT 1 * AVG(l2.l_quantity) FROM '{}' AS l2 WHERE l2.l_partkey = p.p_partkey) \
         GROUP BY p_partkey ORDER BY p_partkey",
        parts_path.display(),
        lines_path.display(),
        lines_path.display()
    );
    assert!(canonical::parse_canonical_plan(&q17_sql).is_some());
    assert_eq!(
        collect_rows(&prepare_file_sql(&q17_sql)),
        vec![
            json!({"p_partkey": 1, "total_price": 20.0}),
            json!({"p_partkey": 2, "total_price": 50.0}),
        ]
    );

    // 2. Correlated EXISTS and NOT EXISTS (including inequality correlation like Q21).
    let exists_sql = format!(
        "SELECT p_partkey FROM '{}' AS p \
         WHERE EXISTS (SELECT 1 FROM '{}' AS l WHERE l.l_partkey = p.p_partkey AND l.l_quantity > 10) \
         ORDER BY p_partkey",
        parts_path.display(),
        lines_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&exists_sql)),
        vec![json!({"p_partkey": 1}), json!({"p_partkey": 2})]
    );

    let not_exists_sql = format!(
        "SELECT p_partkey FROM '{}' AS p \
         WHERE NOT EXISTS (SELECT 1 FROM '{}' AS l WHERE l.l_partkey = p.p_partkey) \
         ORDER BY p_partkey NULLS LAST",
        parts_path.display(),
        lines_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&not_exists_sql)),
        vec![json!({"p_partkey": 3}), json!({"p_partkey": null})]
    );

    // 3. IN and NOT IN with 3-valued NULL semantics (NULL in subquery blocks NOT IN true result).
    let in_sql = format!(
        "SELECT p_partkey FROM '{}' WHERE p_partkey IN (SELECT l_partkey FROM '{}') ORDER BY p_partkey",
        parts_path.display(),
        lines_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&in_sql)),
        vec![json!({"p_partkey": 1}), json!({"p_partkey": 2})]
    );
    let not_in_with_null_sql = format!(
        "SELECT p_partkey FROM '{}' WHERE p_partkey NOT IN (SELECT l_partkey FROM '{}')",
        parts_path.display(),
        lines_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&not_in_with_null_sql)),
        Vec::<Value>::new()
    );

    // 4. Correlated scalar subquery inside SELECT (covering zero-match -> NULL).
    let select_scalar_sql = format!(
        "SELECT p.p_partkey, \
         (SELECT SUM(l.l_extendedprice) FROM '{}' AS l WHERE l.l_partkey = p.p_partkey) AS sum_price \
         FROM '{}' AS p ORDER BY p.p_partkey NULLS LAST",
        lines_path.display(),
        parts_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&select_scalar_sql)),
        vec![
            json!({"p_partkey": 1, "sum_price": 300.0}),
            json!({"p_partkey": 2, "sum_price": 400.0}),
            json!({"p_partkey": 3, "sum_price": null}),
            json!({"p_partkey": null, "sum_price": null}),
        ]
    );
}

#[test]
fn canonical_fetch_qualify_is_true_and_having_in_subquery() {
    let dir = CanonicalTempDir::new("truth_having");
    let flags_path = dir.path().join("flags.parquet");
    let allowed_path = dir.path().join("allowed.parquet");
    let bool_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("grp", DataType::Utf8, false),
        Field::new("flag", DataType::Boolean, true),
    ]));
    let bool_batch = RecordBatch::try_new(
        bool_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "a", "b"])),
            Arc::new(arrow_array::BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
            ])),
        ],
    )
    .unwrap();
    write_parquet_rows(&flags_path, &bool_batch, 3);
    write_parquet_rows(
        &allowed_path,
        &batch_i64(&[("cnt", vec![Some(2), Some(99)])]),
        2,
    );

    let truth_sql = format!(
        "SELECT id, flag IS TRUE AS is_t, flag IS FALSE AS is_f, NOT (flag IS TRUE) AS not_t \
         FROM '{}' ORDER BY id FETCH FIRST 3 ROWS ONLY",
        flags_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&truth_sql)),
        vec![
            json!({"id": 1, "is_t": true, "is_f": false, "not_t": false}),
            json!({"id": 2, "is_t": false, "is_f": true, "not_t": true}),
            json!({"id": 3, "is_t": false, "is_f": false, "not_t": true}),
        ]
    );

    let qualify_sql = format!(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM '{}' QUALIFY rn = 1",
        flags_path.display()
    );
    assert!(try_prepare_file_sql(&qualify_sql).is_err());

    let having_in_sql = format!(
        "SELECT grp, COUNT(*) AS rows FROM '{}' GROUP BY grp \
         HAVING rows IN (SELECT cnt FROM '{}') ORDER BY grp",
        flags_path.display(),
        allowed_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&having_in_sql)),
        vec![json!({"grp": "a", "rows": 2})]
    );

    let having_not_in_sql = format!(
        "SELECT grp, COUNT(*) AS rows FROM '{}' GROUP BY grp \
         HAVING rows NOT IN (SELECT cnt FROM '{}') ORDER BY grp",
        flags_path.display(),
        allowed_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&having_not_in_sql)),
        vec![json!({"grp": "b", "rows": 1})]
    );
}

#[test]
fn canonical_window_wildcards_and_rejected_sql_modifiers() {
    let dir = CanonicalTempDir::new("win_wildcards");
    let flags_path = dir.path().join("flags.parquet");
    let bool_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("grp", DataType::Utf8, false),
        Field::new("flag", DataType::Boolean, true),
    ]));
    let bool_batch = RecordBatch::try_new(
        bool_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "a", "b"])),
            Arc::new(arrow_array::BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
            ])),
        ],
    )
    .unwrap();
    write_parquet_rows(&flags_path, &bool_batch, 3);

    let wildcard_window_sql = format!(
        "SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM '{}' ORDER BY id",
        flags_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&wildcard_window_sql)),
        vec![
            json!({"id": 1, "grp": "a", "flag": true, "rn": 1}),
            json!({"id": 2, "grp": "a", "flag": false, "rn": 2}),
            json!({"id": 3, "grp": "b", "flag": null, "rn": 3}),
        ]
    );

    let wildcard_order_window_sql = format!(
        "SELECT * FROM '{}' ORDER BY ROW_NUMBER() OVER (ORDER BY id DESC)",
        flags_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&wildcard_order_window_sql)),
        vec![
            json!({"id": 3, "grp": "b", "flag": null}),
            json!({"id": 2, "grp": "a", "flag": false}),
            json!({"id": 1, "grp": "a", "flag": true}),
        ]
    );

    for bad_sql in [
        format!(
            "SELECT * EXCLUDE (flag), ROW_NUMBER() OVER (ORDER BY id) AS rn FROM '{}'",
            flags_path.display()
        ),
        format!(
            "SELECT id, grp FROM '{}' UNION ALL BY NAME SELECT grp, id FROM '{}'",
            flags_path.display(),
            flags_path.display()
        ),
        format!(
            "SELECT id, FIRST_VALUE(flag) IGNORE NULLS OVER (ORDER BY id DESC) AS fv FROM '{}'",
            flags_path.display()
        ),
    ] {
        assert!(try_prepare_file_sql(&bad_sql).is_err(), "{bad_sql}");
    }
}

#[test]
fn canonical_order_by_ordinals_and_guarded_case_subqueries() {
    let dir = CanonicalTempDir::new("ord_case");
    let flags_path = dir.path().join("flags.parquet");
    let two_row_path = dir.path().join("two_rows.parquet");
    let flags_batch = batch_i64_str(
        &[("id", vec![Some(1), Some(2), Some(3)])],
        &[("grp", vec![Some("a"), Some("a"), Some("b")])],
    );
    write_parquet_rows(&flags_path, &flags_batch, 3);
    write_parquet_rows(
        &two_row_path,
        &batch_i64(&[("cnt", vec![Some(2), Some(99)])]),
        2,
    );

    let ord_win_sql = format!(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM '{}' ORDER BY 1 DESC LIMIT 1",
        flags_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&ord_win_sql)),
        vec![json!({"id": 3, "rn": 3})]
    );

    let set_ord_sql = format!(
        "SELECT id, grp FROM '{}' WHERE id = 1 UNION ALL SELECT id, grp FROM '{}' WHERE id = 3 ORDER BY 1 DESC",
        flags_path.display(),
        flags_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&set_ord_sql)),
        vec![json!({"id": 3, "grp": "b"}), json!({"id": 1, "grp": "a"})]
    );

    for bad_ord_sql in [
        format!(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM '{}' ORDER BY 0",
            flags_path.display()
        ),
        format!(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM '{}' ORDER BY 3",
            flags_path.display()
        ),
        format!(
            "SELECT id, grp FROM '{}' UNION ALL SELECT id, grp FROM '{}' ORDER BY 3",
            flags_path.display(),
            flags_path.display()
        ),
    ] {
        assert!(try_prepare_file_sql(&bad_ord_sql).is_err(), "{bad_ord_sql}");
    }

    let guarded_case_sql = format!(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn, CASE WHEN TRUE THEN 7 ELSE (SELECT cnt FROM '{}') END AS v FROM '{}' ORDER BY id",
        two_row_path.display(),
        flags_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&guarded_case_sql)),
        vec![
            json!({"id": 1, "rn": 1, "v": 7}),
            json!({"id": 2, "rn": 2, "v": 7}),
            json!({"id": 3, "rn": 3, "v": 7}),
        ]
    );

    let guarded_coalesce_sql = format!(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn, COALESCE(7, (SELECT cnt FROM '{}')) AS v FROM '{}' ORDER BY id",
        two_row_path.display(),
        flags_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&guarded_coalesce_sql)),
        vec![
            json!({"id": 1, "rn": 1, "v": 7}),
            json!({"id": 2, "rn": 2, "v": 7}),
            json!({"id": 3, "rn": 3, "v": 7}),
        ]
    );

    let active_bad_case_sql = format!(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn, CASE WHEN FALSE THEN 7 ELSE (SELECT cnt FROM '{}') END AS v FROM '{}'",
        two_row_path.display(),
        flags_path.display()
    );
    let (_, prepared) = prepare_file_sql(&active_bad_case_sql);
    let Err(err) = prepared.collect_jsonl(&CancellationToken::default()) else {
        panic!("selected branch multi-row scalar subquery must fail");
    };
    assert!(
        err.to_string().contains("scalar subquery cardinality"),
        "{err}"
    );
}

#[test]
fn canonical_order_by_named_windows_and_substring_for() {
    let dir = CanonicalTempDir::new("win_substr");
    let flags_path = dir.path().join("flags.parquet");
    let flags_batch = batch_i64_str(
        &[("id", vec![Some(1), Some(2), Some(3)])],
        &[("grp", vec![Some("a"), Some("a"), Some("b")])],
    );
    write_parquet_rows(&flags_path, &flags_batch, 3);

    let order_named_win_sql = format!(
        "SELECT id FROM '{}' WINDOW w AS (ORDER BY id DESC) ORDER BY ROW_NUMBER() OVER w LIMIT 1",
        flags_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&order_named_win_sql)),
        vec![json!({"id": 3})]
    );

    let unknown_order_named_win_sql = format!(
        "SELECT id FROM '{}' ORDER BY ROW_NUMBER() OVER missing_w LIMIT 1",
        flags_path.display()
    );
    assert!(try_prepare_file_sql(&unknown_order_named_win_sql).is_err());

    let substring_sql = format!(
        "SELECT id, SUBSTRING('abcdef' FOR 2) AS s_for, SUBSTRING('abcdef' FROM 2) AS s_from, SUBSTRING('abcdef' FROM 2 FOR 3) AS s_both, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM '{}' ORDER BY id LIMIT 1",
        flags_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&substring_sql)),
        vec![json!({
            "id": 1,
            "s_for": "ab",
            "s_from": "bcdef",
            "s_both": "bcd",
            "rn": 1
        })]
    );

    let bad_ts_prec_sql = format!(
        "SELECT CAST(TIMESTAMP '2020-01-01T00:00:00.123456Z' AS TIMESTAMP(0)) AS t, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM '{}'",
        flags_path.display()
    );
    assert!(try_prepare_file_sql(&bad_ts_prec_sql).is_err());

    let ok_ts_prec_sql = format!(
        "SELECT id, CAST(TIMESTAMP '2020-01-01T00:00:00.123456Z' AS TIMESTAMP(6)) AS t, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM '{}' ORDER BY id LIMIT 1",
        flags_path.display()
    );
    assert_eq!(collect_rows(&prepare_file_sql(&ok_ts_prec_sql)).len(), 1);

    let filter_path = dir.path().join("agg_filter.parquet");
    let filter_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
        Field::new("keep", DataType::Boolean, true),
    ]));
    let filter_batch = RecordBatch::try_new(
        filter_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["10", "bad", "20"])),
            Arc::new(arrow_array::BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
            ])),
        ],
    )
    .unwrap();
    write_parquet_rows(&filter_path, &filter_batch, 3);
    let filtered_agg_sql = format!(
        "SELECT SUM(CAST(v AS BIGINT)) FILTER (WHERE keep) AS s, COUNT(CAST(v AS BIGINT)) FILTER (WHERE keep) AS c, COUNT(*) OVER () AS win_cnt FROM '{}'",
        filter_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&filtered_agg_sql)),
        vec![json!({"s": 10.0, "c": 1, "win_cnt": 1})]
    );
}

#[test]
fn canonical_projected_correlated_subqueries_and_quoted_wildcards() {
    let dir = CanonicalTempDir::new("corr_proj_quote");
    let outer_path = dir.path().join("outer.parquet");
    let inner_path = dir.path().join("inner.parquet");
    write_parquet_rows(&outer_path, &batch_i64(&[("id", vec![Some(1)])]), 1);
    write_parquet_rows(&inner_path, &batch_i64(&[("id", vec![Some(1)])]), 1);

    let corr_sql = format!(
        "SELECT \
         EXISTS (SELECT -id AS id FROM '{}' WHERE id = o.id) AS e_shadow, \
         EXISTS (SELECT 1 FROM '{}' AS i WHERE i.id = o.id) AS e_const, \
         o.id IN (SELECT id FROM '{}' AS i WHERE i.id = o.id) AS in_corr, \
         (-o.id) IN (SELECT -id AS id FROM '{}' WHERE id = o.id) AS in_shadow \
         FROM '{}' AS o",
        inner_path.display(),
        inner_path.display(),
        inner_path.display(),
        inner_path.display(),
        outer_path.display(),
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&corr_sql)),
        vec![json!({
            "e_shadow": true,
            "e_const": true,
            "in_corr": true,
            "in_shadow": true
        })]
    );

    let quoted_wildcard_sql = format!(
        "SELECT \"t\".*, COUNT(*) OVER () AS cnt FROM '{}' AS \"t\"",
        outer_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&quoted_wildcard_sql)),
        vec![json!({"id": 1, "cnt": 1})]
    );
}

#[test]
fn canonical_window_exclude_scopes_multi_def_and_subqueries() {
    let dir = CanonicalTempDir::new("win_excl_scopes");
    let data_path = dir.path().join("win.parquet");
    write_parquet_rows(
        &data_path,
        &batch_i64(&[
            ("id", vec![Some(1), Some(2), Some(3), Some(4)]),
            ("k", vec![Some(1), Some(1), Some(2), Some(3)]),
            ("v", vec![Some(10), Some(20), Some(30), Some(40)]),
        ]),
        4,
    );

    // 1. Derived subquery named window EXCLUDE + multi-definition WINDOW w1, w2 + outer same name `w`.
    let sql = format!(
        "WITH c AS ( \
            SELECT id, \
                   SUM(v) OVER w AS cte_excl, \
                   SUM(v) OVER w2 AS cte_ties \
            FROM '{}' \
            WINDOW \
              w AS (ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW), \
              w2 AS (ORDER BY k ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE TIES) \
        ) \
        SELECT d.id, d.sub_excl, d.cte_excl, d.cte_ties, SUM(d.id) OVER w AS outer_no_excl \
        FROM ( \
            SELECT id, cte_excl, cte_ties, SUM(cte_excl) OVER w AS sub_excl \
            FROM c \
            WINDOW w AS (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING EXCLUDE CURRENT ROW) \
        ) AS d \
        WINDOW w AS (ORDER BY d.id ROWS CURRENT ROW) \
        ORDER BY d.id",
        data_path.display()
    );
    // Rows sorted by k: (1,v=10), (2,v=20), (3,v=30), (4,v=40):
    // For k: all rows except id=1/2 don't tie on k=1 -> for w2 (ORDER BY k):
    //   id=1 (k=1): frame rows [1,2], ties to EXCLUDE={id=1,2}, so EXCLUDE TIES excludes peer 2 (keeps self 1) => 10.
    //   id=2 (k=1): frame rows [1,2,3], peer 1 excluded by EXCLUDE TIES, keeps 2 & 3 => 20+30 = 50.
    //   id=3 (k=2): frame [2,3,4], no ties => 20+30+40 = 90.
    //   id=4 (k=3): frame [3,4], no ties => 30+40 = 70.
    // For w (EXCLUDE CURRENT ROW):
    //   cte_excl = [20, 40, 60, 30].
    //   sub_excl (id=1..4, CURRENT ROW..1 FOLLOWING EXCLUDE CURRENT ROW of cte_excl) = [40, 60, 30, null].
    assert_eq!(
        collect_rows(&prepare_file_sql(&sql)),
        vec![
            json!({"id": 1, "sub_excl": 40.0, "cte_excl": 20.0, "cte_ties": 10.0, "outer_no_excl": 1.0}),
            json!({"id": 2, "sub_excl": 60.0, "cte_excl": 40.0, "cte_ties": 50.0, "outer_no_excl": 2.0}),
            json!({"id": 3, "sub_excl": 30.0, "cte_excl": 60.0, "cte_ties": 90.0, "outer_no_excl": 3.0}),
            json!({"id": 4, "sub_excl": null, "cte_excl": 30.0, "cte_ties": 70.0, "outer_no_excl": 4.0}),
        ]
    );

    // 2. Scalar subquery with named window EXCLUDE GROUP alongside outer named window of same name.
    let scalar_sub_sql = format!(
        "SELECT o.id, \
         (SELECT SUM(i.v) OVER w FROM '{}' AS i \
          WINDOW w AS (ORDER BY i.k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) \
          ORDER BY i.id DESC LIMIT 1) AS sub_g, \
         SUM(o.v) OVER w AS out_sum \
         FROM '{}' AS o \
         WINDOW w AS (ORDER BY o.id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
         ORDER BY o.id",
        data_path.display(),
        data_path.display()
    );
    let scalar_rows = collect_rows(&prepare_file_sql(&scalar_sub_sql));
    assert_eq!(
        scalar_rows[0],
        json!({"id": 1, "sub_g": 60.0, "out_sum": 10.0})
    );
    assert_eq!(
        scalar_rows[3],
        json!({"id": 4, "sub_g": 60.0, "out_sum": 100.0})
    );

    // 3. Frameless EXCLUDE fails cleanly via both prepare and NativeSqlDeclaration.
    for bad in [
        format!(
            "SELECT SUM(v) OVER (ORDER BY id EXCLUDE CURRENT ROW) FROM '{}'",
            data_path.display()
        ),
        format!(
            "SELECT SUM(v) OVER w FROM '{}' WINDOW w AS (ORDER BY id EXCLUDE TIES)",
            data_path.display()
        ),
    ] {
        assert!(try_prepare_file_sql(&bad).is_err(), "{bad}");
        assert!(
            super::declaration::NativeSqlDeclaration::new(&bad, &[], BTreeMap::new()).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn canonical_aggregate_only_order_by_and_authoritative_rejections_compose_cleanly() {
    let dir = CanonicalTempDir::new("agg_order_auth");
    let data_path = dir.path().join("t.parquet");
    write_parquet_rows(
        &data_path,
        &batch_i64(&[
            ("k", vec![Some(2), Some(1), Some(2)]),
            ("v", vec![Some(5), Some(100), Some(15)]),
        ]),
        3,
    );

    // 1. Aggregate-only ORDER BY inside materialized CTE & grouped query.
    let cte_agg_order = format!(
        "WITH c AS MATERIALIZED (SELECT 1 AS one FROM '{}' ORDER BY SUM(v)) SELECT one FROM c",
        data_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&cte_agg_order)),
        vec![json!({"one": 1})]
    );

    let grouped_agg_order = format!(
        "SELECT k FROM '{}' GROUP BY k ORDER BY SUM(v) ASC",
        data_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&grouped_agg_order)),
        vec![json!({"k": 2}), json!({"k": 1})]
    );

    // Ungrouped column combined with ORDER BY aggregate must fail explicitly.
    let bad_ungrouped = format!("SELECT k FROM '{}' ORDER BY SUM(v)", data_path.display());
    assert!(try_prepare_file_sql(&bad_ungrouped).is_err());

    // 2. Authoritative rejections must preserve canonical diagnostics without legacy parser fallback.
    let rejections = [
        (
            format!(
                "SELECT k, v FROM '{}' UNION ALL BY NAME SELECT v, k FROM '{}'",
                data_path.display(),
                data_path.display()
            ),
            "BY NAME",
        ),
        (
            format!(
                "SELECT k, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM '{}' QUALIFY rn = 1",
                data_path.display()
            ),
            "QUALIFY",
        ),
        (
            format!(
                "SELECT FIRST_VALUE(v) IGNORE NULLS OVER (ORDER BY k) FROM '{}'",
                data_path.display()
            ),
            "IGNORE NULLS",
        ),
        (
            format!("SELECT * EXCLUDE (v) FROM '{}'", data_path.display()),
            "wildcard modifiers",
        ),
        (
            format!(
                "SELECT * FROM '{}', LATERAL (SELECT 1) AS sub",
                data_path.display()
            ),
            "LATERAL",
        ),
    ];
    for (sql, expected_fragment) in rejections {
        for err in [
            try_prepare_file_sql(&sql).err().unwrap(),
            super::declaration::NativeSqlDeclaration::new(&sql, &[], BTreeMap::new())
                .err()
                .unwrap(),
            is_relational(&sql).err().unwrap(),
        ] {
            assert!(
                err.to_string().contains(expected_fragment),
                "expected `{expected_fragment}` in error `{err}` for `{sql}`"
            );
        }
    }
}

#[test]
fn canonical_quantile_windows_and_cte_grouped_aggregates() {
    let dir = CanonicalTempDir::new("canon_quantiles");
    let data_path = dir.path().join("q.parquet");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("fund", DataType::Utf8, false),
            Field::new("number", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["A", "A", "A", "B"])),
            Arc::new(Int64Array::from(vec![10, 20, 40, 100])),
        ],
    )
    .unwrap();
    write_parquet_rows(&data_path, &batch, 4);

    let win_sql = format!(
        "SELECT id, \
         QUANTILE_CONT(number, 0.5) OVER (PARTITION BY fund ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS w_cont, \
         QUANTILE_DISC(number, 0.5) OVER (PARTITION BY fund ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS w_disc \
         FROM '{}' ORDER BY id",
        data_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&win_sql)),
        vec![
            json!({"id": 1, "w_cont": 15.0, "w_disc": 10}),
            json!({"id": 2, "w_cont": 20.0, "w_disc": 20}),
            json!({"id": 3, "w_cont": 30.0, "w_disc": 20}),
            json!({"id": 4, "w_cont": 100.0, "w_disc": 100}),
        ]
    );

    let cte_sql = format!(
        "WITH base AS (SELECT fund, number FROM '{}') \
         SELECT fund, QUANTILE_CONT(number, 0.25) AS c25, QUANTILE_DISC(number, 1.0) AS d100 \
         FROM base GROUP BY fund ORDER BY fund",
        data_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&cte_sql)),
        vec![
            json!({"fund": "A", "c25": 15.0, "d100": 40}),
            json!({"fund": "B", "c25": 100.0, "d100": 100}),
        ]
    );

    // Out-of-range or single-arg quantiles fail admission cleanly.
    let bad_level = format!(
        "SELECT QUANTILE_CONT(number, 1.5) FROM '{}'",
        data_path.display()
    );
    assert!(super::canonical::parse_canonical_plan(&bad_level).is_none());
    let missing_level = format!(
        "SELECT QUANTILE_DISC(number) FROM '{}'",
        data_path.display()
    );
    assert!(super::canonical::parse_canonical_plan(&missing_level).is_none());
}

#[test]
fn canonical_having_and_order_by_agg_preserve_input_column_shadowed_by_select_alias() {
    let dir = CanonicalTempDir::new("agg_shadow_alias");
    let data_path = dir.path().join("shadow.parquet");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("x", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![-1, 0, 2, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
        ],
    )
    .unwrap();
    write_parquet_rows(&data_path, &batch, 4);

    // Output alias `v` (`v + 1`) shadows input `v`; `SUM(v)` in HAVING and ORDER BY
    // must sum input `v` (group v=-1 sums to -1 and is excluded; v=0 sums to 0; v=2 sums to 4),
    // while top-level output alias `s` in OVER (ORDER BY s) still resolves cleanly.
    let shadowed_window_sql = format!(
        "SELECT v + 1 AS v, SUM(x) AS s, ROW_NUMBER() OVER (ORDER BY SUM(x) DESC) AS rn \
         FROM '{}' GROUP BY v HAVING SUM(v) >= 0 AND s >= 20 \
         ORDER BY SUM(v) ASC, ROW_NUMBER() OVER (ORDER BY s DESC) DESC",
        data_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&shadowed_window_sql)),
        vec![
            json!({"v": 1, "s": 20.0, "rn": 2}),
            json!({"v": 3, "s": 70.0, "rn": 1}),
        ]
    );

    let filter_shadow_sql = format!(
        "SELECT v * 10 AS v, SUM(x) AS total FROM '{}' GROUP BY v \
         HAVING SUM(v) FILTER (WHERE v >= 0) >= 0 ORDER BY SUM(v) DESC",
        data_path.display()
    );
    assert_eq!(
        collect_rows(&prepare_file_sql(&filter_shadow_sql)),
        vec![
            json!({"v": 20, "total": 70.0}),
            json!({"v": 0, "total": 20.0}),
        ]
    );
}
