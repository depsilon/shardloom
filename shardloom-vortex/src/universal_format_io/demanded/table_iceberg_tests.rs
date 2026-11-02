use super::*;

fn fields() -> Value {
    json!([{"id":1,"name":"id","required":false,"type":"long"}])
}

fn metadata(fixture: &Fixture, logical: &Value, specs: &Value, list: &Path) -> std::path::PathBuf {
    fixture.json("metadata.json", &json!({"format-version":2,"location":uri(&fixture.0),
        "current-snapshot-id":2,"current-schema-id":0,"schemas":[logical],"partition-specs":specs,
        "snapshots":[{"snapshot-id":2,"schema-id":0,"sequence-number":2,"manifest-list":uri(list)}]}))
}

#[test]
fn iceberg_empty_v1_and_v2_snapshots_retain_schema_and_release_ownership() {
    for version in [1, 2] {
        let fixture = Fixture::new();
        let logical = iceberg_schema(&fields(), 0);
        let path = fixture.json(
            "metadata.json",
            &json!({"format-version":version,
            "location":uri(&fixture.0),"current-snapshot-id":-1,"schema":logical,
            "current-schema-id":0,"schemas":[logical],"partition-spec":[],
            "partition-specs":[{"spec-id":0,"fields":[]}],"snapshots":[]}),
        );
        let memory = pool();
        let source = admit(&path, "iceberg", &LocalTableSelection::default(), &memory);
        assert_eq!(source.schema().field(0).data_type(), &DataType::Int64);
        let (rows, report) = scan(&source, &memory, &["id"], None);
        assert_eq!(rows, Vec::<Value>::new());
        assert_eq!(report.table_snapshot.unwrap().snapshot, "empty");
        drop(source);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn iceberg_v1_manifest_without_v2_fields_reads_and_ignores_deleted_entries() {
    let fixture = Fixture::new();
    let logical = iceberg_schema(&fields(), 0);
    let data = fixture.parquet(
        "data.parquet",
        vec![id_field("old_id", DataType::Int32, false, 1)],
        vec![Arc::new(Int32Array::from(vec![7, 8]))],
    );
    let wire = json!({"type":"record","name":"manifest_entry","fields":[
        {"name":"status","type":"int","field-id":0},
        {"name":"snapshot_id","type":"long","field-id":1},
        {"name":"data_file","field-id":2,"type":{"type":"record","name":"data_file","fields":[
            {"name":"file_path","type":"string","field-id":100},
            {"name":"file_format","type":"string","field-id":101},
            {"name":"partition","field-id":102,"type":{"type":"record","name":"r102","fields":[]}},
            {"name":"record_count","type":"long","field-id":103},
            {"name":"file_size_in_bytes","type":"long","field-id":104}
        ]}}
    ]});
    let mut added = iceberg_entry(&data, 2, 0, None, &Value::Null);
    added["snapshot_id"] = json!(17);
    let mut deleted = added.clone();
    deleted["status"] = json!(2);
    deleted["data_file"]["file_path"] = json!(uri(&fixture.0.join("absent.parquet")));
    let manifest = fixture.avro(
        "v1.avro",
        &wire,
        &[added, deleted],
        &[
            ("schema", logical.to_string()),
            ("partition-spec", "[]".into()),
        ],
    );
    let path = fixture.json(
        "metadata.json",
        &json!({"format-version":1,"location":uri(&fixture.0),
        "current-snapshot-id":17,"schema":logical,"partition-spec":[],
        "snapshots":[{"snapshot-id":17,"manifests":[uri(&manifest)]}]}),
    );
    let memory = pool();
    let source = admit(&path, "iceberg", &LocalTableSelection::default(), &memory);
    assert_eq!(
        scan(&source, &memory, &["id"], None).0,
        vec![json!({"id":7}), json!({"id":8})]
    );
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
#[allow(clippy::too_many_lines)]
fn iceberg_delete_scope_preserves_spec_partition_and_sequence_boundaries() {
    let fixture = Fixture::new();
    let logical = iceberg_schema(&fields(), 0);
    let spec = json!([{"name":"bucket","transform":"bucket[16]","source-id":1,"field-id":1000}]);
    let shape = json!({"type":"record","name":"r102","fields":[
        {"name":"bucket","type":["null","int"],"field-id":1000,"default":null}]});
    let mut paths = Vec::new();
    let mut manifests = Vec::new();
    for (name, spec_id, partition, sequence) in [
        ("a", 1, 7, 1),
        ("b", 1, 8, 1),
        ("c", 2, 7, 1),
        ("d", 1, 7, 2),
    ] {
        let path = ints(
            &fixture,
            &format!("{name}.parquet"),
            "id",
            &[1, 2, 3, 4],
            Some(1),
        );
        let mut entry = iceberg_entry(&path, 4, 0, Some(sequence), &Value::Null);
        entry["data_file"]["partition"] = json!({"bucket":partition});
        manifests.push((
            iceberg_manifest_partition(
                &fixture,
                &format!("{name}.avro"),
                &logical,
                "data",
                &[entry],
                spec_id,
                spec.as_array().unwrap(),
                &shape,
            ),
            0,
            sequence,
            spec_id,
        ));
        paths.push(path);
    }
    let equality = ints(&fixture, "local-eq.parquet", "id", &[1], Some(1));
    let mut equality_entry = iceberg_entry(&equality, 1, 2, None, &json!([1]));
    equality_entry["data_file"]["partition"] = json!({"bucket":7});
    let position = fixture.parquet(
        "position.parquet",
        vec![
            id_field("file_path", DataType::Utf8, false, 2_147_483_546),
            id_field("pos", DataType::Int64, false, 2_147_483_545),
        ],
        vec![
            Arc::new(StringArray::from(
                paths.iter().map(|p| uri(p)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(vec![2; 4])),
        ],
    );
    let mut position_entry = iceberg_entry(&position, 4, 1, None, &Value::Null);
    position_entry["data_file"]["partition"] = json!({"bucket":7});
    manifests.push((
        iceberg_manifest_partition(
            &fixture,
            "local-deletes.avro",
            &logical,
            "deletes",
            &[equality_entry, position_entry],
            1,
            spec.as_array().unwrap(),
            &shape,
        ),
        1,
        2,
        1,
    ));
    let global = ints(&fixture, "global-eq.parquet", "id", &[2], Some(1));
    manifests.push((
        iceberg_manifest(
            &fixture,
            "global.avro",
            &logical,
            "deletes",
            &[iceberg_entry(&global, 1, 2, None, &json!([1]))],
        ),
        1,
        2,
        0,
    ));
    let refs = manifests
        .iter()
        .map(|(p, c, s, i)| (p.as_path(), *c, *s, *i))
        .collect::<Vec<_>>();
    let list = iceberg_list_specs(&fixture, "list.avro", &refs);
    let path = metadata(
        &fixture,
        &logical,
        &json!([
        {"spec-id":0,"fields":[]},{"spec-id":1,"fields":spec},{"spec-id":2,"fields":spec}]),
        &list,
    );
    let memory = pool();
    let source = admit(&path, "iceberg", &LocalTableSelection::default(), &memory);
    let expected = [4, 1, 3, 4, 1, 3, 4, 1, 2, 4]
        .into_iter()
        .map(|id| json!({"id":id}))
        .collect::<Vec<_>>();
    assert_eq!(scan(&source, &memory, &["id"], None).0, expected);
    assert_eq!(scan(&source, &memory, &["id"], Some(2)).0, expected[..2]);
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn iceberg_dropped_equality_field_and_absent_field_obey_null_equality() {
    let fixture = Fixture::new();
    let selected = iceberg_schema(&fields(), 1);
    let old = iceberg_schema(
        &json!([
        {"id":1,"name":"id","required":false,"type":"long"},
        {"id":2,"name":"dropped","required":false,"type":"string"}]),
        0,
    );
    let a = fixture.parquet(
        "a.parquet",
        vec![
            id_field("id", DataType::Int64, true, 1),
            id_field("dropped", DataType::Utf8, true, 2),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("x"), Some("keep"), None])),
        ],
    );
    let b = ints(&fixture, "b.parquet", "id", &[4, 5], Some(1));
    let delete = fixture.parquet(
        "eq.parquet",
        vec![id_field("dropped", DataType::Utf8, true, 2)],
        vec![Arc::new(StringArray::from(vec![Some("x"), None]))],
    );
    let data_manifest = iceberg_manifest(
        &fixture,
        "data.avro",
        &old,
        "data",
        &[
            iceberg_entry(&a, 3, 0, None, &Value::Null),
            iceberg_entry(&b, 2, 0, None, &Value::Null),
        ],
    );
    let delete_manifest = iceberg_manifest(
        &fixture,
        "delete.avro",
        &old,
        "deletes",
        &[iceberg_entry(&delete, 2, 2, None, &json!([2]))],
    );
    let list = iceberg_list(
        &fixture,
        "list.avro",
        &[(&data_manifest, 0, 1), (&delete_manifest, 1, 2)],
    );
    let path = fixture.json("metadata.json",&json!({"format-version":2,"location":uri(&fixture.0),
        "current-snapshot-id":2,"current-schema-id":1,"schemas":[old,selected],
        "partition-specs":[{"spec-id":0,"fields":[]}],
        "snapshots":[{"snapshot-id":2,"schema-id":1,"sequence-number":2,"manifest-list":uri(&list)}]}));
    let memory = pool();
    let source = admit(&path, "iceberg", &LocalTableSelection::default(), &memory);
    let (rows, report) = scan(&source, &memory, &["id"], None);
    assert_eq!(rows, vec![json!({"id":2})]);
    assert_eq!(report.physical_leaf_columns, 2);
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn iceberg_rejects_ambiguous_identity_and_invalid_sequence_before_rows() {
    for case in [
        "existing_without_sequence",
        "future_sequence",
        "future_manifest_sequence",
        "unknown_spec",
        "duplicate_field_ids",
        "duplicate_field_names",
        "unknown_snapshot",
    ] {
        let fixture = Fixture::new();
        let logical = iceberg_schema(&fields(), 0);
        let data = if matches!(case, "duplicate_field_ids" | "duplicate_field_names") {
            fixture.parquet(
                "data.parquet",
                vec![
                    id_field("id", DataType::Int64, true, 1),
                    id_field(
                        if case == "duplicate_field_names" {
                            "id"
                        } else {
                            "duplicate"
                        },
                        DataType::Int64,
                        true,
                        if case == "duplicate_field_names" {
                            2
                        } else {
                            1
                        },
                    ),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![2])),
                ],
            )
        } else {
            ints(&fixture, "data.parquet", "id", &[1], Some(1))
        };
        let mut entry = iceberg_entry(&data, 1, 0, None, &Value::Null);
        if case == "existing_without_sequence" {
            entry["status"] = json!(0);
        }
        if case == "future_sequence" {
            entry["sequence_number"] = json!(3);
        }
        if case == "future_manifest_sequence" {
            entry["sequence_number"] = json!(2);
        }
        let manifest = iceberg_manifest(&fixture, "data.avro", &logical, "data", &[entry]);
        let list = iceberg_list_specs(
            &fixture,
            "list.avro",
            &[(&manifest, 0, 1, i64::from(case == "unknown_spec"))],
        );
        let path = metadata(
            &fixture,
            &logical,
            &json!([{"spec-id":0,"fields":[]}]),
            &list,
        );
        let memory = pool();
        let selection = LocalTableSelection {
            snapshot_id: (case == "unknown_snapshot").then_some(99),
            ..Default::default()
        };
        assert!(
            LocalColumnarSource::admit_table(
                &path,
                "iceberg",
                &selection,
                false,
                &memory,
                &CancellationToken::default()
            )
            .is_err(),
            "{case}"
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0, "{case}");
    }
}
