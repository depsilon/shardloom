use super::*;
use arrow_array::{
    ArrayRef, StructArray,
    builder::{ListBuilder, MapBuilder, StringBuilder},
};
use arrow_buffer::NullBuffer;
use std::sync::Arc;

fn strings(values: &[Value], path: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(
        values
            .iter()
            .map(|row| path.iter().fold(row, |value, key| &value[*key]).as_str())
            .collect::<Vec<_>>(),
    ))
}

fn longs(values: &[Value], path: &[&str]) -> ArrayRef {
    Arc::new(Int64Array::from(
        values
            .iter()
            .map(|row| path.iter().fold(row, |value, key| &value[*key]).as_i64())
            .collect::<Vec<_>>(),
    ))
}

fn object(columns: Vec<(&str, ArrayRef)>, valid: Vec<bool>) -> ArrayRef {
    let fields = columns
        .iter()
        .map(|(name, array)| Arc::new(Field::new(*name, array.data_type().clone(), true)))
        .collect::<Vec<_>>();
    Arc::new(StructArray::new(
        fields.into(),
        columns.into_iter().map(|(_, array)| array).collect(),
        Some(NullBuffer::from(valid)),
    ))
}

fn checkpoint(fixture: &Fixture, name: &str, actions: &[Value]) -> std::path::PathBuf {
    let valid = |kind: &str| {
        actions
            .iter()
            .map(|value| value.get(kind).is_some())
            .collect::<Vec<_>>()
    };
    let mut partitions = ListBuilder::new(StringBuilder::new());
    let mut values = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for action in actions {
        if let Some(names) = action["metaData"]["partitionColumns"].as_array() {
            for name in names {
                partitions.values().append_value(name.as_str().unwrap());
            }
        }
        partitions.append(action.get("metaData").is_some());
        if let Some(partitions) = action["add"]["partitionValues"].as_object() {
            for (name, value) in partitions {
                values.keys().append_value(name);
                values.values().append_option(value.as_str());
            }
        }
        values.append(action.get("add").is_some()).unwrap();
    }
    let protocol = object(
        vec![
            (
                "minReaderVersion",
                longs(actions, &["protocol", "minReaderVersion"]),
            ),
            (
                "minWriterVersion",
                longs(actions, &["protocol", "minWriterVersion"]),
            ),
        ],
        valid("protocol"),
    );
    let metadata = object(
        vec![
            ("id", strings(actions, &["metaData", "id"])),
            (
                "format",
                object(
                    vec![(
                        "provider",
                        strings(actions, &["metaData", "format", "provider"]),
                    )],
                    valid("metaData"),
                ),
            ),
            (
                "schemaString",
                strings(actions, &["metaData", "schemaString"]),
            ),
            ("partitionColumns", Arc::new(partitions.finish())),
        ],
        valid("metaData"),
    );
    let add = object(
        vec![
            ("path", strings(actions, &["add", "path"])),
            ("size", longs(actions, &["add", "size"])),
            ("partitionValues", Arc::new(values.finish())),
        ],
        valid("add"),
    );
    let remove = object(
        vec![("path", strings(actions, &["remove", "path"]))],
        valid("remove"),
    );
    let columns = vec![protocol, metadata, add, remove];
    let fields = ["protocol", "metaData", "add", "remove"]
        .into_iter()
        .zip(&columns)
        .map(|(name, array)| Field::new(name, array.data_type().clone(), true))
        .collect();
    fixture.parquet(name, fields, columns)
}

#[test]
fn delta_classic_and_multipart_checkpoints_reconstruct_without_expired_json() {
    for multipart in [false, true] {
        let fixture = Fixture::new();
        let a = ints(&fixture, "a.parquet", "id", &[2, 4], None);
        let b = ints(&fixture, "b.parquet", "id", &[8], None);
        let actions = vec![
            json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
            delta_metadata(
                &json!([
                    delta_field("id", "long", false, None),
                    delta_field("region", "string", true, None)
                ]),
                &["region"],
                "none",
            ),
            delta_add(&a, "a.parquet", &json!({"region":"checkpoint"})),
            json!({"remove":{"path":"expired.parquet"}}),
        ];
        if multipart {
            checkpoint(
                &fixture,
                "_delta_log/00000000000000000005.checkpoint.0000000001.0000000002.parquet",
                &actions[..2],
            );
            checkpoint(
                &fixture,
                "_delta_log/00000000000000000005.checkpoint.0000000002.0000000002.parquet",
                &actions[2..],
            );
        } else {
            checkpoint(
                &fixture,
                "_delta_log/00000000000000000005.checkpoint.parquet",
                &actions,
            );
        }
        fixture.commit(
            6,
            &[
                json!({"remove":{"path":"a.parquet"}}),
                delta_add(&b, "b.parquet", &json!({"region":"replay"})),
            ],
        );
        let memory = pool();
        let source = admit(
            &fixture.0,
            "delta",
            &LocalTableSelection::default(),
            &memory,
        );
        assert_eq!(
            scan(&source, &memory, &["id", "region"], None).0,
            vec![json!({"id":8,"region":"replay"})]
        );
        let old = admit(
            &fixture.0,
            "delta",
            &LocalTableSelection {
                version: Some(5),
                ..Default::default()
            },
            &memory,
        );
        assert_eq!(
            scan(&old, &memory, &["id"], None).0,
            vec![json!({"id":2}), json!({"id":4})]
        );
        drop((source, old));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn delta_does_not_select_an_older_version_when_newer_checkpoint_is_unsupported() {
    let fixture = Fixture::new();
    fixture.commit(
        0,
        &[
            json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
            delta_metadata(&json!([delta_field("id", "long", true, None)]), &[], "none"),
        ],
    );
    fixture.json(
        "_delta_log/00000000000000000005.checkpoint.00000000-0000-0000-0000-000000000000.json",
        &json!({}),
    );
    let memory = pool();
    let error = LocalColumnarSource::admit_table(
        &fixture.0,
        "delta",
        &LocalTableSelection::default(),
        false,
        &memory,
        &CancellationToken::default(),
    )
    .err()
    .unwrap()
    .to_string();
    assert!(error.contains("unsupported checkpoint"), "{error}");
    let old = admit(
        &fixture.0,
        "delta",
        &LocalTableSelection {
            version: Some(0),
            ..Default::default()
        },
        &memory,
    );
    assert_eq!(scan(&old, &memory, &["id"], None).0, Vec::<Value>::new());
    drop(old);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn delta_requires_every_checkpoint_part_before_using_it() {
    let fixture = Fixture::new();
    let actions = vec![
        json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        delta_metadata(&json!([delta_field("id", "long", true, None)]), &[], "none"),
    ];
    checkpoint(
        &fixture,
        "_delta_log/00000000000000000005.checkpoint.0000000001.0000000002.parquet",
        &actions,
    );
    fixture.commit(0, &actions);
    let memory = pool();
    let error = LocalColumnarSource::admit_table(
        &fixture.0,
        "delta",
        &LocalTableSelection::default(),
        false,
        &memory,
        &CancellationToken::default(),
    )
    .err()
    .unwrap()
    .to_string();
    assert!(error.contains("incomplete at version 1"), "{error}");
    let old = admit(
        &fixture.0,
        "delta",
        &LocalTableSelection {
            version: Some(0),
            ..Default::default()
        },
        &memory,
    );
    assert_eq!(scan(&old, &memory, &["id"], None).0, Vec::<Value>::new());
    drop(old);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    fixture.commit(6, &[json!({"commitInfo":{}})]);
    assert!(
        LocalColumnarSource::admit_table(
            &fixture.0,
            "delta",
            &LocalTableSelection::default(),
            false,
            &memory,
            &CancellationToken::default()
        )
        .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
