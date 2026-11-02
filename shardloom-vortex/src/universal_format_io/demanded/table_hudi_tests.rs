use super::*;

fn schema() -> Value {
    json!({"type":"record","name":"Row","fields":[{"name":"id","type":"long"}]})
}

fn configure(fixture: &Fixture, version: u32, table_type: &str) {
    fs::create_dir_all(fixture.0.join(".hoodie/timeline")).unwrap();
    fs::write(fixture.0.join(".hoodie/hoodie.properties"),format!(
        "hoodie.table.name=fixture\nhoodie.table.type={table_type}\nhoodie.table.version={version}\nhoodie.timeline.layout.version=2\nhoodie.table.create.schema={}\n",schema())).unwrap();
}

fn commit(
    fixture: &Fixture,
    begin: &str,
    completed: &str,
    action: &str,
    writes: &Value,
    replaced: &Value,
) -> std::path::PathBuf {
    commit_with_schema(
        fixture,
        begin,
        completed,
        action,
        writes,
        replaced,
        &schema(),
    )
}

fn commit_with_schema(
    fixture: &Fixture,
    begin: &str,
    completed: &str,
    action: &str,
    writes: &Value,
    replaced: &Value,
    schema: &Value,
) -> std::path::PathBuf {
    // Literal wire names from Hudi's pinned HoodieCommitMetadata and
    // HoodieReplaceCommitMetadata Avro schemas; unused nullable fields omitted.
    let wire = json!({"type":"record","name":"HoodieReplaceCommitMetadata","fields":[
        {"name":"partitionToWriteStats","type":["null",{"type":"map","values":{"type":"array","items":{
            "type":"record","name":"HoodieWriteStat","fields":[
                {"name":"fileId","type":["null","string"],"default":null},
                {"name":"path","type":["null","string"],"default":null}]}}}],"default":null},
        {"name":"compacted","type":"boolean","default":false},
        {"name":"extraMetadata","type":["null",{"type":"map","values":["null","string"]}],"default":null},
        {"name":"version","type":"int","default":1},
        {"name":"operationType","type":["null","string"],"default":null},
        {"name":"partitionToReplaceFileIds","type":["null",{"type":"map","values":{"type":"array","items":"string"}}],"default":null}
    ]});
    fixture.avro(&format!(".hoodie/timeline/{begin}_{completed}.{action}"), &wire,
        &[json!({"partitionToWriteStats":writes,"compacted":false,"extraMetadata":{"schema":schema.to_string()},
            "version":1,"operationType":if action == "replacecommit" {"CLUSTER"} else {"UPSERT"},
            "partitionToReplaceFileIds":replaced})], &[])
}

#[test]
fn hudi_schema_uses_latest_requested_schema_write_within_completed_membership() {
    let fixture = Fixture::new();
    configure(&fixture, 9, "COPY_ON_WRITE");
    let evolved = json!({"type":"record","name":"Row","fields":[
        {"name":"id","type":"long"},
        {"name":"added","type":["null","long"],"default":null}
    ]});
    let old = "a_0-0-0_20261101000100.parquet";
    ints(&fixture, old, "id", &[1], None);
    commit(
        &fixture,
        "20261101000100",
        "20261101000500",
        "commit",
        &json!({"":[{"fileId":"a","path":old}]}),
        &json!({}),
    );
    for (begin, completed, group, value, action, logical, replaced) in [
        (
            "20261101000200",
            "20261101000300",
            "a",
            2,
            "commit",
            evolved,
            json!({}),
        ),
        (
            "20261101000400",
            "20261101000600",
            "b",
            4,
            "replacecommit",
            schema(),
            json!({"":["a"]}),
        ),
    ] {
        let name = format!("{group}_0-0-0_{begin}.parquet");
        fixture.parquet(
            &name,
            vec![
                Field::new("id", arrow_schema::DataType::Int64, false),
                Field::new("added", arrow_schema::DataType::Int64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![value])),
                Arc::new(Int64Array::from(vec![value * 11])),
            ],
        );
        commit_with_schema(
            &fixture,
            begin,
            completed,
            action,
            &json!({"":[{"fileId":group,"path":name}]}),
            &replaced,
            &logical,
        );
    }
    let memory = pool();
    for (instant, value) in [
        (Some("20261101000200"), 2),
        (Some("20261101000100"), 2),
        (None, 4),
    ] {
        let source = admit(
            &fixture.0,
            "hudi",
            &LocalTableSelection {
                instant: instant.map(str::to_owned),
                ..Default::default()
            },
            &memory,
        );
        assert_eq!(source.schema().fields().len(), 2);
        assert_eq!(
            scan(&source, &memory, &["id", "added"], None).0,
            vec![json!({"id":value,"added":value * 11})]
        );
        drop(source);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn hudi_cow_selects_completed_versions_and_replaced_groups() {
    for version in [8, 9] {
        let fixture = Fixture::new();
        configure(&fixture, version, "COPY_ON_WRITE");
        let a = "p/a_0-0-0_20261101000100.parquet";
        let b = "p/b_0-0-0_20261101000100.parquet";
        let next = "p/a_0-0-0_20261101000200.parquet";
        let c = "p/c_0-0-0_20261101000500.parquet";
        ints(&fixture, a, "id", &[1, 2], None);
        ints(&fixture, b, "id", &[10], None);
        ints(&fixture, next, "id", &[3, 4], None);
        ints(&fixture, c, "id", &[30], None);
        ints(
            &fixture,
            "p/orphan_0-0-0_20261101000900.parquet",
            "id",
            &[999],
            None,
        );
        commit(
            &fixture,
            "20261101000100",
            "20261101000300",
            "commit",
            &json!({"p":[{"fileId":"a","path":a},{"fileId":"b","path":b}]}),
            &json!({}),
        );
        commit(
            &fixture,
            "20261101000200",
            "20261101000400",
            "commit",
            &json!({"p":[{"fileId":"a","path":next}]}),
            &json!({}),
        );
        let replace = commit(
            &fixture,
            "20261101000500",
            "20261101000600",
            "replacecommit",
            &json!({"p":[{"fileId":"c","path":c}]}),
            &json!({"p":["a","b"]}),
        );
        fs::write(
            fixture
                .0
                .join(".hoodie/timeline/20261101000900.commit.inflight"),
            b"pending, not a completed metadata record",
        )
        .unwrap();
        let memory = pool();
        let latest = admit(&fixture.0, "hudi", &LocalTableSelection::default(), &memory);
        let (rows, report) = scan(&latest, &memory, &["id"], None);
        assert_eq!(rows, vec![json!({"id":30})]);
        assert_eq!(
            report.table_snapshot.unwrap().snapshot,
            "completion:20261101000600"
        );
        let old = admit(
            &fixture.0,
            "hudi",
            &LocalTableSelection {
                instant: Some("20261101000200".into()),
                ..Default::default()
            },
            &memory,
        );
        assert_eq!(
            scan(&old, &memory, &["id"], None).0,
            vec![json!({"id":3}), json!({"id":4}), json!({"id":10})]
        );
        assert!(latest.validate_output_destination(&replace).is_err());
        fs::write(replace, b"replaced").unwrap();
        assert!(latest.validate_generation().is_err());
        drop((latest, old));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn hudi_rejects_mor_archives_legacy_and_unsupported_completed_actions() {
    for case in ["mor", "archive", "legacy", "rollback", "missing", "version"] {
        let fixture = Fixture::new();
        configure(
            &fixture,
            if case == "version" { 6 } else { 9 },
            if case == "mor" {
                "MERGE_ON_READ"
            } else {
                "COPY_ON_WRITE"
            },
        );
        let name = "a_0-0-0_20261101000100.parquet";
        ints(&fixture, name, "id", &[1], None);
        commit(
            &fixture,
            "20261101000100",
            "20261101000200",
            "commit",
            &json!({"":[{"fileId":"a","path":name}]}),
            &json!({}),
        );
        if case == "archive" {
            fs::create_dir_all(fixture.0.join(".hoodie/timeline/history")).unwrap();
            fs::write(fixture.0.join(".hoodie/timeline/history/_version_"), b"1").unwrap();
        } else if case == "legacy" {
            fs::write(
                fixture.0.join(".hoodie/timeline/20261101000000.commit"),
                b"{}",
            )
            .unwrap();
        } else if case == "rollback" {
            fs::write(
                fixture
                    .0
                    .join(".hoodie/timeline/20261101000000_20261101000100.rollback"),
                b"{}",
            )
            .unwrap();
        } else if case == "missing" {
            fs::remove_file(fixture.0.join(name)).unwrap();
        }
        let memory = pool();
        assert!(
            LocalColumnarSource::admit_table(
                &fixture.0,
                "hudi",
                &LocalTableSelection::default(),
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

#[test]
fn hudi_completion_selection_does_not_resurrect_older_or_replaced_file_groups() {
    let fixture = Fixture::new();
    configure(&fixture, 9, "COPY_ON_WRITE");
    for (begin, completed, id, value, action, replaced) in [
        (
            "20261101000100",
            "20261101000500",
            "a",
            1,
            "commit",
            json!({}),
        ),
        (
            "20261101000200",
            "20261101000300",
            "a",
            2,
            "commit",
            json!({}),
        ),
        (
            "20261101000400",
            "20261101000600",
            "c",
            4,
            "replacecommit",
            json!({"":["a"]}),
        ),
        (
            "20261101000300",
            "20261101000700",
            "a",
            3,
            "commit",
            json!({}),
        ),
    ] {
        let path = format!("{id}_0-0-0_{begin}.parquet");
        ints(&fixture, &path, "id", &[value], None);
        commit(
            &fixture,
            begin,
            completed,
            action,
            &json!({"":[{"fileId":id,"path":path}]}),
            &replaced,
        );
    }
    let memory = pool();
    for (instant, expected) in [
        (Some("20261101000200"), 2),
        (Some("20261101000100"), 2),
        (None, 4),
    ] {
        let selection = LocalTableSelection {
            instant: instant.map(str::to_owned),
            ..Default::default()
        };
        let source = admit(&fixture.0, "hudi", &selection, &memory);
        assert_eq!(
            scan(&source, &memory, &["id"], None).0,
            vec![json!({"id":expected})]
        );
        drop(source);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn hudi_empty_timeline_uses_authoritative_schema_and_excludes_pending_files() {
    let fixture = Fixture::new();
    configure(&fixture, 8, "COPY_ON_WRITE");
    ints(
        &fixture,
        "a_0-0-0_20261101000100.parquet",
        "id",
        &[999],
        None,
    );
    fs::write(
        fixture
            .0
            .join(".hoodie/timeline/20261101000100.commit.requested"),
        b"pending",
    )
    .unwrap();
    let memory = pool();
    let source = admit(&fixture.0, "hudi", &LocalTableSelection::default(), &memory);
    assert_eq!(source.schema().field(0).name(), "id");
    let (rows, report) = scan(&source, &memory, &["id"], None);
    assert_eq!(rows, Vec::<Value>::new());
    assert_eq!(report.table_snapshot.unwrap().snapshot, "empty");
    drop(source);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
