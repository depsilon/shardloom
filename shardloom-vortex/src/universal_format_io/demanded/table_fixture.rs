//! Small wire-format fixtures. Avro framing is authored independently of the
//! production metadata decoder; data files use the existing Parquet writer.

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub(super) struct Fixture(pub(super) PathBuf);

impl Fixture {
    pub(super) fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-table-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub(super) fn json(&self, name: &str, value: &Value) -> PathBuf {
        let path = self.0.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        path
    }

    pub(super) fn commit(&self, version: u64, actions: &[Value]) -> PathBuf {
        let path = self.0.join(format!("_delta_log/{version:020}.json"));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            actions
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )
        .unwrap();
        path
    }

    pub(super) fn parquet(
        &self,
        name: &str,
        fields: Vec<Field>,
        columns: Vec<ArrayRef>,
    ) -> PathBuf {
        let path = self.0.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let properties = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            fs::File::create(&path).unwrap(),
            batch.schema(),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    pub(super) fn avro(
        &self,
        name: &str,
        schema: &Value,
        rows: &[Value],
        properties: &[(&str, String)],
    ) -> PathBuf {
        let mut output = b"Obj\x01".to_vec();
        long(i64::try_from(properties.len() + 2).unwrap(), &mut output);
        for (key, value) in [
            ("avro.schema", schema.to_string()),
            ("avro.codec", "null".into()),
        ]
        .iter()
        .chain(properties)
        {
            bytes(key.as_bytes(), &mut output);
            bytes(value.as_bytes(), &mut output);
        }
        long(0, &mut output);
        let sync = [0x5a; 16];
        output.extend(sync);
        if !rows.is_empty() {
            let mut block = Vec::new();
            for row in rows {
                datum(schema, row, &mut block);
            }
            long(i64::try_from(rows.len()).unwrap(), &mut output);
            bytes(&block, &mut output);
            output.extend(sync);
        }
        let path = self.0.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, output).unwrap();
        path
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

pub(super) fn id_field(
    name: &str,
    dtype: arrow_schema::DataType,
    nullable: bool,
    id: i32,
) -> Field {
    Field::new(name, dtype, nullable).with_metadata(HashMap::from([(
        parquet::arrow::PARQUET_FIELD_ID_META_KEY.into(),
        id.to_string(),
    )]))
}

pub(super) fn uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

pub(super) fn delta_metadata(fields: &Value, partitions: &[&str], mode: &str) -> Value {
    json!({"metaData": {"id": "fixture-table", "format": {"provider":"parquet", "options":{}},
        "schemaString": json!({"type":"struct", "fields":fields}).to_string(),
        "partitionColumns": partitions, "configuration":{"delta.columnMapping.mode":mode}}})
}

pub(super) fn delta_field(name: &str, kind: &str, nullable: bool, id: Option<i32>) -> Value {
    json!({"name":name, "type":kind, "nullable":nullable, "metadata":id.map_or_else(
        || json!({}), |id| json!({"delta.columnMapping.id":id, "delta.columnMapping.physicalName":format!("col-{id}")}))})
}

pub(super) fn delta_add(path: &Path, name: &str, partitions: &Value) -> Value {
    json!({"add":{"path":name, "partitionValues":partitions, "size":fs::metadata(path).unwrap().len(),
        "modificationTime":0, "dataChange":true}})
}

pub(super) fn iceberg_schema(fields: &Value, id: i32) -> Value {
    json!({"type":"struct", "schema-id":id, "fields":fields})
}

pub(super) fn iceberg_entry(
    path: &Path,
    rows: u64,
    content: u8,
    sequence: Option<u64>,
    equality: &Value,
) -> Value {
    json!({"status":1, "snapshot_id":null, "sequence_number":sequence, "file_sequence_number":null,
        "data_file":{"content":content, "file_path":uri(path), "file_format":"PARQUET", "partition":{},
        "record_count":rows, "file_size_in_bytes":fs::metadata(path).unwrap().len(), "equality_ids":equality}})
}

pub(super) fn iceberg_manifest(
    fixture: &Fixture,
    name: &str,
    logical: &Value,
    content: &str,
    rows: &[Value],
) -> PathBuf {
    iceberg_manifest_partition(
        fixture,
        name,
        logical,
        content,
        rows,
        0,
        &[],
        &json!({"type":"record","name":"r102","fields":[]}),
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn iceberg_manifest_partition(
    fixture: &Fixture,
    name: &str,
    logical: &Value,
    content: &str,
    rows: &[Value],
    spec_id: i64,
    spec: &[Value],
    partition: &Value,
) -> PathBuf {
    let schema = json!({"type":"record", "name":"manifest_entry", "fields":[
        {"name":"status", "type":"int", "field-id":0},
        {"name":"snapshot_id", "type":["null","long"], "field-id":1, "default":null},
        {"name":"sequence_number", "type":["null","long"], "field-id":3, "default":null},
        {"name":"file_sequence_number", "type":["null","long"], "field-id":4, "default":null},
        {"name":"data_file", "field-id":2, "type":{"type":"record", "name":"data_file", "fields":[
            {"name":"content", "type":"int", "field-id":134},
            {"name":"file_path", "type":"string", "field-id":100},
            {"name":"file_format", "type":"string", "field-id":101},
            {"name":"partition", "field-id":102, "type":partition},
            {"name":"record_count", "type":"long", "field-id":103},
            {"name":"file_size_in_bytes", "type":"long", "field-id":104},
            {"name":"equality_ids", "field-id":135, "type":["null",{"type":"array","items":"int","element-id":136}], "default":null}
        ]}}
    ]});
    fixture.avro(
        name,
        &schema,
        rows,
        &[
            ("schema", logical.to_string()),
            ("schema-id", "0".into()),
            ("partition-spec", serde_json::to_string(spec).unwrap()),
            ("partition-spec-id", spec_id.to_string()),
            ("format-version", "2".into()),
            ("content", content.into()),
        ],
    )
}

pub(super) fn iceberg_list(
    fixture: &Fixture,
    name: &str,
    manifests: &[(&Path, u8, u64)],
) -> PathBuf {
    iceberg_list_specs(
        fixture,
        name,
        &manifests
            .iter()
            .map(|&(path, content, sequence)| (path, content, sequence, 0))
            .collect::<Vec<_>>(),
    )
}

pub(super) fn iceberg_list_specs(
    fixture: &Fixture,
    name: &str,
    manifests: &[(&Path, u8, u64, i64)],
) -> PathBuf {
    let schema = json!({"type":"record","name":"manifest_file","fields":[
        {"name":"manifest_path","type":"string","field-id":500},
        {"name":"manifest_length","type":"long","field-id":501},
        {"name":"partition_spec_id","type":"int","field-id":502},
        {"name":"content","type":"int","field-id":517},
        {"name":"sequence_number","type":"long","field-id":515},
        {"name":"min_sequence_number","type":"long","field-id":516},
        {"name":"added_snapshot_id","type":"long","field-id":503},
        {"name":"added_files_count","type":"int","field-id":504},
        {"name":"existing_files_count","type":"int","field-id":505},
        {"name":"deleted_files_count","type":"int","field-id":506},
        {"name":"added_rows_count","type":"long","field-id":512},
        {"name":"existing_rows_count","type":"long","field-id":513},
        {"name":"deleted_rows_count","type":"long","field-id":514}
    ]});
    let rows = manifests.iter().map(|&(path, content, sequence, spec_id)| json!({"manifest_path":uri(path),
        "manifest_length":fs::metadata(path).unwrap().len(), "partition_spec_id":spec_id, "content":content,
        "sequence_number":sequence, "min_sequence_number":sequence, "added_snapshot_id":sequence,
        "added_files_count":1, "existing_files_count":0, "deleted_files_count":0,
        "added_rows_count":0, "existing_rows_count":0, "deleted_rows_count":0})).collect::<Vec<_>>();
    fixture.avro(name, &schema, &rows, &[])
}

// Avro 1.12 binary specification: signed zigzag variable integers, length-prefixed
// bytes, schema-order records, explicit union branch, and terminated collections.
fn long(value: i64, output: &mut Vec<u8>) {
    let mut encoded = u64::from_ne_bytes(((value << 1) ^ (value >> 63)).to_ne_bytes());
    while encoded >= 128 {
        output.push(u8::try_from(encoded & 0x7f).unwrap() | 0x80);
        encoded >>= 7;
    }
    output.push(u8::try_from(encoded).unwrap());
}

fn bytes(value: &[u8], output: &mut Vec<u8>) {
    long(i64::try_from(value.len()).unwrap(), output);
    output.extend(value);
}

fn datum(schema: &Value, value: &Value, output: &mut Vec<u8>) {
    if let Some(branches) = schema.as_array() {
        let index = branches
            .iter()
            .position(|branch| (branch.as_str() == Some("null")) == value.is_null())
            .unwrap();
        long(i64::try_from(index).unwrap(), output);
        return datum(&branches[index], value, output);
    }
    match schema.as_str().or_else(|| schema["type"].as_str()).unwrap() {
        "null" => assert!(value.is_null()),
        "boolean" => output.push(u8::from(value.as_bool().unwrap())),
        "int" | "long" => long(value.as_i64().unwrap(), output),
        "string" => bytes(value.as_str().unwrap().as_bytes(), output),
        "record" => {
            for field in schema["fields"].as_array().unwrap() {
                datum(
                    &field["type"],
                    &value[field["name"].as_str().unwrap()],
                    output,
                );
            }
        }
        "array" => {
            let values = value.as_array().unwrap();
            if !values.is_empty() {
                long(i64::try_from(values.len()).unwrap(), output);
            }
            for value in values {
                datum(&schema["items"], value, output);
            }
            long(0, output);
        }
        "map" => {
            let values = value.as_object().unwrap();
            if !values.is_empty() {
                long(i64::try_from(values.len()).unwrap(), output);
            }
            for (key, value) in values {
                bytes(key.as_bytes(), output);
                datum(&schema["values"], value, output);
            }
            long(0, output);
        }
        kind => panic!("test fixture Avro type not supported: {kind}"),
    }
}
