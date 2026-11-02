//! Whole table visibility, evolution and lifecycle over real encoded files.

use super::*;
use arrow_array::{Array, Int32Array, Int64Array, StringArray};
use arrow_schema::Field;
use serde_json::{Value, json};
use shardloom_core::input::LocalTableSelection;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{fs, io::Write as _, path::Path};

#[path = "table_fixture.rs"]
mod fixture;
use fixture::*;
#[path = "table_checkpoint_tests.rs"]
mod checkpoint;
#[path = "table_hudi_tests.rs"]
mod hudi;
#[path = "table_iceberg_tests.rs"]
mod iceberg;

fn pool() -> LiveMemoryPool {
    LiveMemoryPool::new(64 << 20).unwrap()
}

fn admit(
    path: &Path,
    format: &str,
    selection: &LocalTableSelection,
    pool: &LiveMemoryPool,
) -> Arc<LocalColumnarSource> {
    LocalColumnarSource::admit_table(
        path,
        format,
        selection,
        false,
        pool,
        &CancellationToken::default(),
    )
    .unwrap()
}

fn scan(
    source: &Arc<LocalColumnarSource>,
    pool: &LiveMemoryPool,
    names: &[&str],
    limit: Option<u64>,
) -> (Vec<Value>, ColumnarReadReport) {
    let token = CancellationToken::default();
    let schema = source.schema();
    let projection = names
        .iter()
        .map(|name| schema.index_of(name).unwrap())
        .collect();
    let prepared = PreparedColumnarSource {
        source: Arc::clone(source),
        projection,
        row_groups: source.prepare_row_groups(limit, &[], &token).unwrap(),
        max_rows: limit,
        metadata: pool.reserve(8192).unwrap(),
        region_selection: None,
    };
    let metrics = Arc::new(LocalReadMetrics::default());
    let mut reader = generic::Reader::new();
    let mut rows = Vec::new();
    while let Some((batch, first)) = reader
        .next_preparation(&prepared, &metrics, pool, &token)
        .unwrap()
    {
        assert_eq!(first, rows.len() as u64);
        for row in 0..batch.num_rows() {
            let values = batch
                .schema()
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(field, column)| {
                    let value = if column.is_null(row) {
                        Value::Null
                    } else if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
                        json!(values.value(row))
                    } else if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
                        json!(values.value(row))
                    } else if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
                        json!(values.value(row))
                    } else {
                        panic!("unhandled fixture scalar {:?}", field.data_type());
                    };
                    (field.name().clone(), value)
                })
                .collect::<serde_json::Map<_, _>>();
            rows.push(Value::Object(values));
        }
    }
    (rows, reader.report(&prepared, &metrics).unwrap())
}

fn ints(
    fixture: &Fixture,
    name: &str,
    column: &str,
    values: &[i64],
    id: Option<i32>,
) -> std::path::PathBuf {
    let field = id.map_or_else(
        || Field::new(column, DataType::Int64, false),
        |id| id_field(column, DataType::Int64, false, id),
    );
    fixture.parquet(
        name,
        vec![field],
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
}

#[test]
fn delta_reconciles_files_and_freezes_latest_or_historical_state() {
    let fixture = Fixture::new();
    let a = ints(&fixture, "a.parquet", "id", &[1, 2], None);
    let b = ints(&fixture, "b.parquet", "id", &[3, 4], None);
    let c = ints(&fixture, "c.parquet", "id", &[7, 8], None);
    let ghost = ints(&fixture, "orphan.parquet", "id", &[999], None);
    let fields = json!([
        delta_field("id", "long", false, None),
        delta_field("region", "string", true, None)
    ]);
    fixture.commit(
        0,
        &[
            json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
            delta_metadata(&fields, &["region"], "none"),
            delta_add(&a, "a.parquet", &json!({"region":"west"})),
            delta_add(&b, "b.parquet", &json!({"region":"east"})),
        ],
    );
    let latest = fixture.commit(
        1,
        &[
            json!({"remove":{"path":"a.parquet","dataChange":true}}),
            delta_metadata(
                &json!([
                    fields[0],
                    fields[1],
                    delta_field("added", "string", true, None)
                ]),
                &["region"],
                "none",
            ),
            delta_add(&c, "c.parquet", &json!({"region":null})),
        ],
    );
    let memory = pool();
    let source = admit(
        &fixture.0,
        "delta",
        &LocalTableSelection::default(),
        &memory,
    );
    let (rows, report) = scan(&source, &memory, &["id", "region", "added"], None);
    assert_eq!(
        rows,
        json!([{"id":3,"region":"east","added":null},{"id":4,"region":"east","added":null},
        {"id":7,"region":null,"added":null},{"id":8,"region":null,"added":null}])
        .as_array()
        .unwrap()
        .clone()
    );
    assert_eq!(report.files, 2);
    assert!(!report.parquet_selected_chunks);
    assert_eq!(report.table_snapshot.unwrap().snapshot, "1");
    let old = admit(
        &fixture.0,
        "delta",
        &LocalTableSelection {
            version: Some(0),
            ..Default::default()
        },
        &memory,
    );
    assert_eq!(
        scan(&old, &memory, &["id"], None).0,
        json!([{"id":1},{"id":2},{"id":3},{"id":4}])
            .as_array()
            .unwrap()
            .clone()
    );
    fixture.commit(
        2,
        &[delta_add(
            &ghost,
            "orphan.parquet",
            &json!({"region":"later"}),
        )],
    );
    assert_eq!(
        scan(&source, &memory, &["id"], Some(1)).0,
        vec![json!({"id":3})]
    );
    fs::OpenOptions::new()
        .append(true)
        .open(latest)
        .unwrap()
        .write_all(b"\n")
        .unwrap();
    assert!(source.validate_generation().is_err());
    drop((old, source));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn delta_mapping_preserves_renames_and_does_not_reuse_names() {
    for mode in ["name", "id"] {
        let fixture = Fixture::new();
        let physical = if mode == "name" { "col-1" } else { "old_name" };
        let data = ints(&fixture, "data.parquet", physical, &[41, 42], Some(1));
        fixture.commit(
            0,
            &[
                json!({"protocol":{"minReaderVersion":2,"minWriterVersion":5}}),
                delta_metadata(
                    &json!([
                        delta_field("renamed", "long", false, Some(1)),
                        delta_field("old_name", "long", true, Some(2))
                    ]),
                    &[],
                    mode,
                ),
                delta_add(&data, "data.parquet", &json!({})),
            ],
        );
        let memory = pool();
        let source = admit(
            &fixture.0,
            "delta",
            &LocalTableSelection::default(),
            &memory,
        );
        let (rows, report) = scan(&source, &memory, &["renamed", "old_name"], None);
        assert_eq!(
            rows,
            vec![
                json!({"renamed":41,"old_name":null}),
                json!({"renamed":42,"old_name":null})
            ]
        );
        assert_eq!(report.physical_leaf_columns, 1);
        drop(source);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn delta_rejects_incomplete_history_features_missing_files_and_foreign_selectors() {
    for case in ["gap", "feature", "vector", "missing", "selector"] {
        let fixture = Fixture::new();
        let data = ints(&fixture, "data.parquet", "id", &[1], None);
        let mut protocol = json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}});
        if case == "feature" {
            protocol = json!({"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}});
        }
        let mut add = delta_add(&data, "data.parquet", &json!({}));
        if case == "vector" {
            add["add"]["deletionVector"] = json!({"storageType":"i","pathOrInlineDv":"bad"});
        }
        fixture.commit(
            u64::from(case == "gap"),
            &[
                protocol,
                delta_metadata(
                    &json!([delta_field("id", "long", false, None)]),
                    &[],
                    "none",
                ),
                add,
            ],
        );
        if case == "missing" {
            fs::remove_file(data).unwrap();
        }
        let selection = LocalTableSelection {
            snapshot_id: (case == "selector").then_some(1),
            ..Default::default()
        };
        let memory = pool();
        let result = LocalColumnarSource::admit_table(
            &fixture.0,
            "delta",
            &selection,
            false,
            &memory,
            &CancellationToken::default(),
        );
        assert!(result.is_err(), "{case}");
        assert_eq!(memory.snapshot().reserved_bytes, 0, "{case}");
    }
}

#[test]
fn empty_delta_snapshot_retains_schema_and_memory_lifecycle() {
    let fixture = Fixture::new();
    fixture.commit(
        0,
        &[
            json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
            delta_metadata(&json!([delta_field("id", "long", true, None)]), &[], "none"),
        ],
    );
    let memory = pool();
    let source = admit(
        &fixture.0,
        "delta",
        &LocalTableSelection::default(),
        &memory,
    );
    assert_eq!(source.schema().field(0).name(), "id");
    assert_eq!(scan(&source, &memory, &["id"], None).0, Vec::<Value>::new());
    assert!(
        source
            .validate_output_destination(&fixture.0.join("new.parquet"))
            .is_err()
    );
    assert!(
        source
            .validate_output_destination(&fixture.0.join("_delta_log/00000000000000000000.json"))
            .is_err()
    );
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let small = LiveMemoryPool::new(8192).unwrap();
    assert!(
        LocalColumnarSource::admit_table(
            &fixture.0,
            "delta",
            &LocalTableSelection::default(),
            false,
            &small,
            &CancellationToken::default()
        )
        .is_err()
    );
    assert_eq!(small.snapshot().reserved_bytes, 0);
    let token = CancellationToken::default();
    token.cancel();
    assert!(
        LocalColumnarSource::admit_table(
            &fixture.0,
            "delta",
            &LocalTableSelection::default(),
            false,
            &memory,
            &token
        )
        .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
#[allow(clippy::too_many_lines)]
fn iceberg_applies_deletes_by_field_identity_before_projected_prefix() {
    let fixture = Fixture::new();
    let logical = iceberg_schema(
        &json!([
        {"id":1,"name":"id","required":false,"type":"long"},
        {"id":2,"name":"renamed","required":false,"type":"string"},
        {"id":3,"name":"old_name","required":false,"type":"string"}]),
        0,
    );
    let fields = || {
        vec![
            id_field("physical_id", DataType::Int32, true, 1),
            id_field("old_name", DataType::Utf8, true, 2),
        ]
    };
    let a = fixture.parquet(
        "a.parquet",
        fields(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(3)])),
            Arc::new(StringArray::from(vec!["one", "two", "null", "three"])),
        ],
    );
    let b = fixture.parquet(
        "b.parquet",
        fields(),
        vec![
            Arc::new(Int32Array::from(vec![Some(2), Some(4)])),
            Arc::new(StringArray::from(vec!["new-two", "four"])),
        ],
    );
    let equality = fixture.parquet(
        "equality.parquet",
        vec![id_field("delete_key", DataType::Int64, true, 1)],
        vec![Arc::new(Int64Array::from(vec![Some(2), None]))],
    );
    let position = fixture.parquet(
        "position.parquet",
        vec![
            id_field("file_path", DataType::Utf8, false, 2_147_483_546),
            id_field("pos", DataType::Int64, false, 2_147_483_545),
        ],
        vec![
            Arc::new(StringArray::from(vec![uri(&a), uri(&b)])),
            Arc::new(Int64Array::from(vec![0, 1])),
        ],
    );
    let data1 = iceberg_manifest(
        &fixture,
        "data1.avro",
        &logical,
        "data",
        &[iceberg_entry(&a, 4, 0, None, &Value::Null)],
    );
    let data2 = iceberg_manifest(
        &fixture,
        "data2.avro",
        &logical,
        "data",
        &[iceberg_entry(&b, 2, 0, None, &Value::Null)],
    );
    let deletes = iceberg_manifest(
        &fixture,
        "deletes.avro",
        &logical,
        "deletes",
        &[
            iceberg_entry(&equality, 2, 2, None, &json!([1])),
            iceberg_entry(&position, 2, 1, None, &Value::Null),
        ],
    );
    let list1 = iceberg_list(&fixture, "list1.avro", &[(&data1, 0, 1)]);
    let list2 = iceberg_list(
        &fixture,
        "list2.avro",
        &[(&data1, 0, 1), (&data2, 0, 2), (&deletes, 1, 2)],
    );
    let metadata = fixture.json("metadata.json",&json!({"format-version":2,"location":uri(&fixture.0),
        "current-snapshot-id":2,"current-schema-id":0,"schemas":[logical],"partition-specs":[{"spec-id":0,"fields":[]}],
        "snapshots":[{"snapshot-id":1,"schema-id":0,"sequence-number":1,"manifest-list":uri(&list1)},
        {"snapshot-id":2,"schema-id":0,"sequence-number":2,"manifest-list":uri(&list2)}]}));
    let memory = pool();
    let source = admit(
        &metadata,
        "iceberg",
        &LocalTableSelection::default(),
        &memory,
    );
    let (rows, report) = scan(&source, &memory, &["renamed", "old_name"], None);
    assert_eq!(
        rows,
        vec![
            json!({"renamed":"three","old_name":null}),
            json!({"renamed":"new-two","old_name":null})
        ]
    );
    assert_eq!(
        report.physical_leaf_columns, 2,
        "unprojected equality key is physically read"
    );
    assert_eq!(report.rows_decoded, 6);
    assert_eq!(report.table_snapshot.unwrap().snapshot, "2");
    assert_eq!(
        scan(&source, &memory, &["renamed"], Some(1)).0,
        vec![json!({"renamed":"three"})]
    );
    assert_eq!(
        scan(&source, &memory, &["renamed"], Some(0)).0,
        Vec::<Value>::new()
    );
    let old = admit(
        &metadata,
        "iceberg",
        &LocalTableSelection {
            snapshot_id: Some(1),
            ..Default::default()
        },
        &memory,
    );
    assert_eq!(
        scan(&old, &memory, &["id"], None).0,
        vec![
            json!({"id":1}),
            json!({"id":2}),
            json!({"id":null}),
            json!({"id":3})
        ]
    );
    assert!(source.validate_output_destination(&equality).is_err());
    fs::write(&equality, b"changed").unwrap();
    assert!(source.validate_generation().is_err());
    drop((source, old));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
