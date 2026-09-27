use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-public-io-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn public_io_declared_schema_is_bound_to_cold_warm_and_held_generations() {
    let fixture = Fixture::new();
    let source = fixture.0.join("labels.csv");
    let target = fixture.0.join("prepared.vortex");
    fs::write(&source, "label\n001\n010\n").unwrap();
    for policy in ["metadata_only", "content_digest"] {
        let prepare = |schema, overwrite| {
            prepare_local_source_as_vortex_for_public_workflow_with_schema(
                &source,
                &target,
                Some("csv"),
                overwrite,
                1,
                Some(1),
                Some(policy),
                Some(schema),
            )
        };
        let cold = prepare("label:utf8", true).unwrap();
        let original = fs::read(&target).unwrap();
        let warm = prepare("label:utf8", false).unwrap();
        assert_reused_identity(&cold, &warm);
        cold.validate_generation().unwrap();
        warm.validate_generation().unwrap();
        assert!(prepare("label:int64", false).is_err());
        assert_eq!(fs::read(&target).unwrap(), original);
        let replacement = fixture.0.join("replacement.vortex");
        fs::write(&replacement, &original).unwrap();
        fs::rename(&replacement, &target).unwrap();
        assert!(cold.validate_generation().is_err());
        assert!(warm.validate_generation().is_err());
    }
    let absent = fixture.0.join("absent").join("artifact.vortex");
    assert!(
        prepare_local_source_as_vortex_for_public_workflow_with_schema(
            &source,
            &absent,
            Some("csv"),
            false,
            1,
            Some(1),
            None,
            Some("invalid"),
        )
        .is_err()
    );
    assert!(!absent.parent().unwrap().exists());
}

#[test]
fn public_io_repeated_preparation_reuses_embedded_binding_across_inputs() {
    let fixture = Fixture::new();
    let columns = vec!["id".to_string(), "text".to_string()];
    let rows = vec![vec![
        ("id".into(), ScalarValue::Int64(7)),
        ("text".into(), ScalarValue::Utf8("hello".into())),
    ]];
    let inputs = [
        ("csv", b"id,text\r\n7,\"hello\r\nworld\"\r\n".to_vec()),
        ("json", br#"[{"id":7,"text":"hello"}]"#.to_vec()),
        ("jsonl", b"{\"id\":7,\"text\":\"hello\"}\n".to_vec()),
        (
            "parquet",
            shardloom_vortex::encode_flat_parquet_rows(&columns, &rows).unwrap(),
        ),
        (
            "arrow_ipc",
            shardloom_vortex::encode_flat_arrow_ipc_rows(&columns, &rows).unwrap(),
        ),
        (
            "avro",
            shardloom_vortex::encode_flat_avro_rows(&columns, &rows).unwrap(),
        ),
        (
            "orc",
            shardloom_vortex::encode_flat_orc_rows(&columns, &rows).unwrap(),
        ),
    ];
    for (format, bytes) in inputs {
        let source = fixture.0.join(format!("source.{format}"));
        fs::write(&source, bytes).unwrap();
        let target = fixture.0.join(format!("prepared-{format}.vortex"));
        for policy in ["metadata_only", "content_digest"] {
            let first = prepare_local_source_as_vortex_for_public_workflow(
                &source,
                &target,
                Some(format),
                true,
                1,
                Some(1),
                Some(policy),
            )
            .unwrap();
            assert_eq!(first.target_path, target);
            let original = fs::read(&target).unwrap();
            let second = prepare_local_source_as_vortex_for_public_workflow(
                &source,
                &target,
                Some(format),
                false,
                1,
                Some(1),
                Some(policy),
            )
            .unwrap();
            assert!(
                second.fields.iter().any(|(key, value)| key
                    == "public_workflow_preparation_prepared_state_reused"
                    && value == "true"),
                "{format}/{policy}: unchanged prepared state was not reused"
            );
            assert_reused_identity(&first, &second);
            assert_eq!(fs::read(&target).unwrap(), original);
        }
        let original = fs::read(&target).unwrap();
        fs::write(&source, b"changed").unwrap();
        assert!(
            prepare_local_source_as_vortex_for_public_workflow(
                &source,
                &target,
                Some(format),
                false,
                1,
                Some(1),
                Some("metadata_only")
            )
            .is_err()
        );
        assert_eq!(fs::read(&target).unwrap(), original);
    }
}

fn assert_reused_identity(
    first: &PublicWorkflowVortexPreparation,
    second: &PublicWorkflowVortexPreparation,
) {
    for field in [
        "source_state_id",
        "source_state_digest",
        "prepared_state_id",
        "prepared_state_digest",
        "prepared_state_identity_policy",
        "prepared_state_reuse_manifest_path",
        "prepared_state_reuse_manifest_digest",
    ] {
        let key = format!("public_workflow_preparation_{field}");
        let first_value = first.fields.iter().find(|(name, _)| name == &key);
        let second_value = second.fields.iter().find(|(name, _)| name == &key);
        assert!(first_value.is_some(), "cold {field} missing");
        assert!(first_value == second_value, "{field} changed on reuse");
    }
    for field in [
        "prepared_state_reuse_reason",
        "prepared_state_invalidation_reason",
    ] {
        let key = format!("public_workflow_preparation_{field}");
        assert!(
            second
                .fields
                .iter()
                .any(|(name, value)| name == &key && !value.is_empty()),
            "warm {field} missing"
        );
    }
}

#[test]
fn public_io_preparation_generation_is_held_through_execution() {
    let fixture = Fixture::new();
    let stable_source = fixture.0.join("stable.csv");
    fs::write(&stable_source, "id,text\n7,hello\n").unwrap();
    let stable = prepare_local_source_as_vortex_for_public_workflow(
        &stable_source,
        stable_source.with_extension("vortex"),
        Some("csv"),
        false,
        1,
        Some(1),
        Some("metadata_only"),
    )
    .unwrap();
    for (warm, change_source, right_input) in [
        (false, false, false),
        (false, true, false),
        (true, false, false),
        (true, true, false),
        (false, false, true),
        (false, true, true),
        (true, false, true),
        (true, true, true),
    ] {
        let source = fixture
            .0
            .join(format!("source-{warm}-{change_source}-{right_input}.csv"));
        let target = source.with_extension("vortex");
        fs::write(&source, "id,text\n7,hello\n").unwrap();
        let cold = prepare_local_source_as_vortex_for_public_workflow(
            &source,
            &target,
            Some("csv"),
            false,
            1,
            Some(1),
            Some("metadata_only"),
        )
        .unwrap();
        let preparation = if warm {
            prepare_local_source_as_vortex_for_public_workflow(
                &source,
                &target,
                Some("csv"),
                false,
                1,
                Some(1),
                Some("metadata_only"),
            )
            .unwrap()
        } else {
            cold
        };
        preparation.validate_generation().unwrap();
        let (left, right) = if right_input {
            (&stable, Some(&preparation))
        } else {
            (&preparation, None)
        };
        let result =
            crate::public_workflow_route::with_prepared_source_generations(left, right, || {
                // Replace atomically with equal bytes: identity must track
                // the admitted generation, not just a valid footer/binding.
                let changed_path = if change_source { &source } else { &target };
                let replacement = changed_path.with_extension("replacement");
                fs::write(&replacement, fs::read(changed_path).unwrap()).unwrap();
                fs::rename(&replacement, changed_path).unwrap();
                crate::cli_output::emit(
                    "run",
                    OutputFormat::Json,
                    CommandStatus::Success,
                    "must not escape".into(),
                    String::new(),
                    Vec::new(),
                    preparation.fields.clone(),
                );
            });
        assert!(result.is_err(), "warm={warm}, source={change_source}");
        assert!(preparation.validate_generation().is_err());
        let ran = std::cell::Cell::new(false);
        assert!(
            crate::public_workflow_route::with_prepared_source_generations(left, right, || ran
                .set(true),)
            .is_err()
        );
        assert!(!ran.get());
    }
}

#[test]
fn public_io_empty_binary_preparation_preserves_schema_and_reuses_binding() {
    let fixture = Fixture::new();
    let columns = vec!["id".to_string(), "text".to_string()];
    let dtypes = vec![Some(LogicalDType::Int64), Some(LogicalDType::Utf8)];
    let inputs = [
        (
            "parquet",
            shardloom_vortex::encode_flat_parquet_rows_with_dtypes(&columns, &dtypes, &[]).unwrap(),
        ),
        (
            "arrow_ipc",
            shardloom_vortex::encode_flat_arrow_ipc_rows_with_dtypes(&columns, &dtypes, &[])
                .unwrap(),
        ),
        (
            "avro",
            shardloom_vortex::encode_flat_avro_rows_with_dtypes(&columns, &dtypes, &[]).unwrap(),
        ),
        (
            "orc",
            shardloom_vortex::encode_flat_orc_rows_with_dtypes(&columns, &dtypes, &[]).unwrap(),
        ),
    ];
    for (format, bytes) in inputs {
        let source = fixture.0.join(format!("empty.{format}"));
        fs::write(&source, bytes).unwrap();
        let target = fixture.0.join(format!("prepared-{format}.vortex"));
        prepare_local_source_as_vortex_for_public_workflow(
            &source,
            &target,
            Some(format),
            false,
            1,
            Some(1),
            None,
        )
        .unwrap();
        let original = fs::read(&target).unwrap();
        let reused = prepare_local_source_as_vortex_for_public_workflow(
            &source,
            &target,
            Some(format),
            false,
            1,
            Some(1),
            None,
        )
        .unwrap();
        assert!(
            reused.fields.iter().any(|(key, value)| key
                == "public_workflow_preparation_prepared_state_reused"
                && value == "true"),
            "{format}"
        );
        assert_eq!(fs::read(&target).unwrap(), original);
    }
}

#[test]
fn public_io_preparation_rejects_unbound_files_and_changed_partition_membership() {
    let fixture = Fixture::new();
    let source = fixture.0.join("source.csv");
    let target = fixture.0.join("existing.vortex");
    fs::write(&source, b"id\n1\n").unwrap();
    fs::write(&target, b"unrelated user file").unwrap();
    assert!(
        prepare_local_source_as_vortex_for_public_workflow(
            &source,
            &target,
            Some("csv"),
            false,
            1,
            Some(1),
            None
        )
        .is_err()
    );
    assert_eq!(fs::read(&target).unwrap(), b"unrelated user file");
    let partitions = fixture.0.join("parts");
    fs::create_dir(&partitions).unwrap();
    fs::write(partitions.join("first.csv"), b"id\n1\n").unwrap();
    let target = fixture.0.join("parts.vortex");
    prepare_local_source_as_vortex_for_public_workflow(
        &partitions,
        &target,
        Some("csv"),
        false,
        1,
        Some(1),
        None,
    )
    .unwrap();
    prepare_local_source_as_vortex_for_public_workflow(
        &partitions,
        &target,
        Some("csv"),
        false,
        1,
        Some(1),
        None,
    )
    .unwrap();
    let original = fs::read(&target).unwrap();
    fs::write(partitions.join("second.csv"), b"id\n2\n").unwrap();
    assert!(
        prepare_local_source_as_vortex_for_public_workflow(
            &partitions,
            &target,
            Some("csv"),
            false,
            1,
            Some(1),
            None
        )
        .is_err()
    );
    assert_eq!(fs::read(&target).unwrap(), original);
}

#[test]
fn public_io_json_array_preserves_exact_integer_null_and_escaped_text() {
    let columns = vec!["id".to_string(), "text".to_string()];
    let rows = vec![
        vec![
            ("id".into(), ScalarValue::UInt64(u64::MAX)),
            ("text".into(), ScalarValue::Utf8("a,\nb\"".into())),
        ],
        vec![
            ("id".into(), ScalarValue::Null),
            ("text".into(), ScalarValue::Null),
        ],
    ];
    let output = SqlLocalSourceOutputFormat::Json
        .render_normalized_rows(&columns, &[None, None], &[None, None], &rows)
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(
        result,
        serde_json::json!([{"id":u64::MAX,"text":"a,\nb\""},{"id":null,"text":null}])
    );
    assert_eq!(
        SqlLocalSourceOutputFormat::Json
            .render_normalized_rows(&columns, &[None, None], &[None, None], &[])
            .unwrap(),
        b"[]\n"
    );
}

#[test]
fn public_io_csv_records_preserve_embedded_newlines_and_reject_unclosed_quotes() {
    let input = "id,text\r\n7,\"line\r\n\"\"quoted\"\"\"\r\n8,plain\n";
    let mut reader = std::io::Cursor::new(input);
    let mut record = String::new();
    assert!(read_csv_record(&mut reader, &mut record).unwrap() > 0);
    assert_eq!(record, "id,text\r\n");
    assert!(read_csv_record(&mut reader, &mut record).unwrap() > 0);
    assert_eq!(
        split_csv_record(record.trim_end_matches(['\r', '\n'])).unwrap(),
        ["7", "line\r\n\"quoted\""]
    );
    assert!(read_csv_record(&mut reader, &mut record).unwrap() > 0);
    assert_eq!(record, "8,plain\n");
    assert_eq!(read_csv_record(&mut reader, &mut record).unwrap(), 0);
    assert!(
        read_csv_record(&mut std::io::Cursor::new("7,\"open\n"), &mut record)
            .unwrap_err()
            .to_string()
            .contains("not closed")
    );
}
