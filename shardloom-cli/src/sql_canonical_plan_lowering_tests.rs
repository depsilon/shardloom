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
           AND g.dept ILIKE 'ENG%' \
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
