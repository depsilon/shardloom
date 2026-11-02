//! Iceberg v1/v2 metadata, manifest inheritance and live-file selection.

use super::{
    ColumnMapping, DataFile, Metadata, Result, Snapshot, deletes, failed, metadata, schema,
};
use serde_json::Value;
use shardloom_core::input::LocalTableSelection;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{collections::BTreeMap, path::Path};

// Resolve manifests, schema evolution and deletes under one snapshot owner.
#[allow(clippy::too_many_lines)]
pub(super) fn read(
    path: &Path,
    selection: &LocalTableSelection,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Snapshot> {
    let mut held = Metadata::new(memory, cancellation)?;
    let document = held.json(path)?;
    let table = document.value();
    let version = u32::try_from(metadata::unsigned(table, "format-version")?).map_err(failed)?;
    if !matches!(version, 1 | 2) {
        return Err(failed(
            "Iceberg local reader admits table versions 1 and 2 only",
        ));
    }
    if table
        .get("encryption-keys")
        .is_some_and(|v| v.as_array().is_none_or(|v| !v.is_empty()))
    {
        return Err(failed("encrypted Iceberg table metadata is not admitted"));
    }
    let root = metadata::local_path(
        path.parent()
            .ok_or_else(|| failed("metadata file has no parent"))?,
        metadata::string(table, "location")?,
    )?;
    if !root.is_dir() {
        return Err(failed("Iceberg table location must be a local directory"));
    }
    let current = table
        .get("current-snapshot-id")
        .filter(|v| !v.is_null())
        .map(|v| {
            v.as_i64()
                .ok_or_else(|| failed("invalid Iceberg current-snapshot-id"))
        })
        .transpose()?
        .filter(|id| *id != -1);
    let selected_id = selection.snapshot_id.or(current);
    let snapshots = table
        .get("snapshots")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let selected = selected_id
        .map(|id| {
            let matching = snapshots
                .iter()
                .filter(|v| v.get("snapshot-id").and_then(Value::as_i64) == Some(id))
                .collect::<Vec<_>>();
            if matching.len() != 1 {
                return Err(failed("Iceberg selected snapshot is missing or duplicated"));
            }
            Ok(matching[0])
        })
        .transpose()?;
    let schema_id = if selection.snapshot_id.is_some() {
        selected
            .and_then(|v| v.get("schema-id"))
            .and_then(Value::as_i64)
            .or_else(|| table.get("current-schema-id").and_then(Value::as_i64))
    } else {
        table.get("current-schema-id").and_then(Value::as_i64)
    };
    let schemas = table
        .get("schemas")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let mut schema_ids = std::collections::BTreeSet::new();
    for schema in schemas {
        if !schema_ids.insert(metadata::integer(schema, "schema-id")?) {
            return Err(failed("duplicate Iceberg schema ID"));
        }
    }
    let selected_schema = schema_id
        .and_then(|id| {
            schemas
                .iter()
                .find(|v| v.get("schema-id").and_then(Value::as_i64) == Some(id))
        })
        .or_else(|| {
            (version == 1 && schema_id.is_none())
                .then(|| table.get("schema"))
                .flatten()
        })
        .ok_or_else(|| failed("Iceberg selected schema ID is missing"))?;
    held.retain(selected_schema.to_string().len() as u64 * 32 + 8192)?;
    let fields = schema::iceberg(selected_schema)?;
    let mut snapshot = Snapshot::new(
        "iceberg",
        version,
        selected_id.map_or_else(|| "empty".into(), |v| v.to_string()),
        root,
        fields,
        ColumnMapping::FieldId,
        held,
    )?;
    for value in schemas
        .iter()
        .chain((schemas.is_empty()).then_some(selected_schema))
    {
        snapshot
            .metadata
            .retain(value.to_string().len() as u64 * 32 + 8192)?;
        snapshot.retain_historical(schema::iceberg(value)?)?;
    }
    snapshot.retain_historical(snapshot.fields.clone())?;
    let specs = partition_specs(table, version)?;
    let Some(selected) = selected else {
        return Ok(snapshot);
    };
    let snapshot_sequence = if version == 2 {
        metadata::unsigned(selected, "sequence-number")?
    } else {
        0
    };
    let mut manifests = Vec::new();
    if let Some(list) = selected.get("manifest-list").and_then(Value::as_str) {
        let path = metadata::local_path(&snapshot.root, list)?;
        let list = snapshot.metadata.records(&path, "avro")?;
        for row in &list.value().rows {
            let content = row.get("content").map_or(Ok(0), |v| {
                v.as_u64()
                    .ok_or_else(|| failed("invalid Iceberg manifest content"))
            })?;
            if content > 1 || (version == 1 && content != 0) {
                return Err(failed("unsupported Iceberg manifest content"));
            }
            if row.get("key_metadata").is_some_and(|v| !v.is_null()) {
                return Err(failed("encrypted Iceberg manifest is not admitted"));
            }
            let sequence = if version == 2 {
                metadata::unsigned(row, "sequence_number")?
            } else {
                row.get("sequence_number").map_or(Ok(0), |v| {
                    v.as_u64()
                        .ok_or_else(|| failed("invalid manifest sequence"))
                })?
            };
            if sequence > snapshot_sequence {
                return Err(failed(
                    "Iceberg manifest sequence exceeds selected snapshot",
                ));
            }
            let name = metadata::string(row, "manifest_path")?;
            snapshot.metadata.retain(name.len() as u64 * 4 + 512)?;
            manifests.push(Manifest {
                path: name.into(),
                spec: metadata::integer(row, "partition_spec_id")?,
                content,
                sequence,
                bytes: Some(metadata::unsigned(row, "manifest_length")?),
            });
        }
    } else if version == 1 {
        for path in metadata::array(selected, "manifests")? {
            let path = path
                .as_str()
                .ok_or_else(|| failed("Iceberg v1 manifest path must be a string"))?;
            snapshot.metadata.retain(path.len() as u64 * 4 + 512)?;
            manifests.push(Manifest {
                path: path.into(),
                spec: -1,
                content: 0,
                sequence: 0,
                bytes: None,
            });
        }
    } else {
        return Err(failed("Iceberg v2 snapshot requires a manifest list"));
    }
    let mut pending = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut partition_shapes = BTreeMap::new();
    for manifest in manifests {
        cancellation.check()?;
        let path = metadata::local_path(&snapshot.root, &manifest.path)?;
        if manifest
            .bytes
            .is_some_and(|bytes| std::fs::metadata(&path).is_ok_and(|v| v.len() != bytes))
        {
            return Err(failed("Iceberg manifest size differs from manifest list"));
        }
        if !seen.insert(path.clone()) {
            return Err(failed(
                "Iceberg snapshot references a manifest more than once",
            ));
        }
        let entries = snapshot.metadata.records(&path, "avro")?;
        let properties = &entries.value().properties;
        let file_version: u32 = properties
            .get("format-version")
            .map_or(Ok(1), |v| v.parse().map_err(failed))?;
        if !matches!(file_version, 1 | 2) || file_version > version {
            return Err(failed(
                "Iceberg manifest version is incompatible with table",
            ));
        }
        let spec: i64 = properties
            .get("partition-spec-id")
            .map_or(Ok(0), |v| v.parse().map_err(failed))?;
        if manifest.spec != -1 && spec != manifest.spec {
            return Err(failed(
                "Iceberg manifest partition-spec ID disagrees with list",
            ));
        }
        let spec_fields = specs
            .get(&spec)
            .ok_or_else(|| failed("Iceberg manifest references an unknown partition spec"))?;
        let shape = partition_shape(entries.value().schema.as_ref(), spec_fields)?;
        if let Some(previous) = partition_shapes.insert(spec, shape.clone())
            && previous != shape
        {
            return Err(failed(
                "Iceberg partition value type evolution within one spec is not admitted",
            ));
        }
        let encoded_spec = properties
            .get("partition-spec")
            .ok_or_else(|| failed("Iceberg manifest partition spec is absent"))?;
        let encoded_spec: Value = serde_json::from_str(encoded_spec).map_err(failed)?;
        if encoded_spec.as_array().map(Vec::as_slice) != Some(spec_fields.as_slice()) {
            return Err(failed(
                "Iceberg manifest partition spec differs from table metadata",
            ));
        }
        let manifest_schema = properties
            .get("schema")
            .ok_or_else(|| failed("Iceberg manifest schema is absent"))?;
        snapshot.retain_historical(schema::iceberg(
            &serde_json::from_str::<Value>(manifest_schema).map_err(failed)?,
        )?)?;
        if file_version == 2 {
            if !properties.contains_key("schema-id")
                || !properties.contains_key("partition-spec-id")
            {
                return Err(failed(
                    "Iceberg v2 manifest is missing schema/spec identity",
                ));
            }
            let content = if manifest.content == 0 {
                "data"
            } else {
                "deletes"
            };
            if properties.get("content").map(String::as_str) != Some(content) {
                return Err(failed(
                    "Iceberg manifest content property disagrees with manifest list",
                ));
            }
        } else if manifest.content != 0 {
            return Err(failed("Iceberg v1 manifest cannot contain delete files"));
        }
        for entry in &entries.value().rows {
            let status = metadata::unsigned(entry, "status")?;
            if status == 2 {
                continue;
            }
            if status > 2 {
                return Err(failed("unknown Iceberg manifest entry status"));
            }
            let sequence = if file_version == 1 {
                0
            } else {
                match entry.get("sequence_number").filter(|v| !v.is_null()) {
                    Some(value) => value
                        .as_u64()
                        .ok_or_else(|| failed("invalid Iceberg data sequence"))?,
                    None if status == 1 => manifest.sequence,
                    None => {
                        return Err(failed(
                            "existing Iceberg v2 entry has no data sequence number",
                        ));
                    }
                }
            };
            if sequence > manifest.sequence {
                return Err(failed(
                    "Iceberg data sequence exceeds its manifest sequence",
                ));
            }
            let data = entry
                .get("data_file")
                .ok_or_else(|| failed("Iceberg entry has no data_file"))?;
            let content = if file_version == 1 {
                0
            } else {
                metadata::unsigned(data, "content")?
            };
            if content > 2 || (content == 0) != (manifest.content == 0) {
                return Err(failed(
                    "Iceberg data/delete content disagrees with manifest",
                ));
            }
            if !metadata::string(data, "file_format")?.eq_ignore_ascii_case("parquet") {
                return Err(failed(
                    "Iceberg local reader admits Parquet data and delete files only",
                ));
            }
            if data.get("key_metadata").is_some_and(|v| !v.is_null()) {
                return Err(failed(
                    "encrypted Iceberg data/delete files are not admitted",
                ));
            }
            let location = metadata::string(data, "file_path")?;
            let path = metadata::local_path(&snapshot.root, location)?;
            let partitions = metadata::object(
                data.get("partition")
                    .ok_or_else(|| failed("Iceberg data file has no partition tuple"))?,
            )?;
            if partitions.len() != spec_fields.len()
                || spec_fields.iter().any(|field| {
                    metadata::string(field, "name")
                        .map_or(true, |name| !partitions.contains_key(name))
                })
            {
                return Err(failed("Iceberg file partition tuple differs from its spec"));
            }
            let mut file = DataFile::plain(path, location.into());
            file.sequence = sequence;
            file.spec_id = spec;
            file.partition_key = serde_json::to_string(partitions).map_err(failed)?;
            file.records = Some(metadata::unsigned(data, "record_count")?);
            file.bytes = Some(metadata::unsigned(data, "file_size_in_bytes")?);
            if content == 0 {
                snapshot.push_file(file)?;
            } else {
                let ids = if content == 2 {
                    Some(
                        metadata::array(data, "equality_ids")?
                            .iter()
                            .map(|v| {
                                v.as_i64()
                                    .ok_or_else(|| {
                                        failed("Iceberg equality field ID must be an integer")
                                    })
                                    .and_then(|v| i32::try_from(v).map_err(failed))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )
                } else {
                    None
                };
                snapshot.metadata.retain(8192 + location.len() as u64 * 4)?;
                let global = spec_fields
                    .iter()
                    .all(|field| field.get("transform").and_then(Value::as_str) == Some("void"));
                pending.push((file, ids, global));
            }
        }
    }
    let mut delete_paths = std::collections::BTreeSet::new();
    for (file, ids, global) in pending {
        if !delete_paths.insert(file.path.clone())
            || snapshot.files.iter().any(|data| data.path == file.path)
        {
            return Err(failed(
                "duplicate Iceberg content file in selected snapshot",
            ));
        }
        deletes::read(&mut snapshot, file, ids, global)?;
    }
    Ok(snapshot)
}

fn partition_shape(
    schema: &arrow_schema::Schema,
    spec: &[Value],
) -> Result<Vec<arrow_schema::DataType>> {
    use arrow_schema::DataType;
    let data = schema.field_with_name("data_file").map_err(failed)?;
    let DataType::Struct(data) = data.data_type() else {
        return Err(failed("Iceberg manifest data_file is not a struct"));
    };
    let partition = data
        .iter()
        .find(|field| field.name() == "partition")
        .ok_or_else(|| failed("Iceberg manifest partition schema is absent"))?;
    let DataType::Struct(partition) = partition.data_type() else {
        return Err(failed("Iceberg manifest partition is not a struct"));
    };
    if partition.len() != spec.len() {
        return Err(failed("Iceberg partition schema differs from its spec"));
    }
    spec.iter()
        .map(|field| {
            let name = metadata::string(field, "name")?;
            partition
                .iter()
                .find(|field| field.name() == name)
                .map(|field| field.data_type().clone())
                .ok_or_else(|| failed("Iceberg partition schema field is absent"))
        })
        .collect()
}

struct Manifest {
    path: String,
    spec: i64,
    content: u64,
    sequence: u64,
    bytes: Option<u64>,
}

fn partition_specs(table: &Value, version: u32) -> Result<BTreeMap<i64, Vec<Value>>> {
    let mut specs = BTreeMap::new();
    if let Some(values) = table.get("partition-specs") {
        for spec in values
            .as_array()
            .ok_or_else(|| failed("Iceberg partition-specs must be an array"))?
        {
            let id = metadata::integer(spec, "spec-id")?;
            if specs
                .insert(id, metadata::array(spec, "fields")?.to_vec())
                .is_some()
            {
                return Err(failed("duplicate Iceberg partition spec ID"));
            }
        }
    } else if version == 1 {
        specs.insert(0, metadata::array(table, "partition-spec")?.to_vec());
    }
    if specs.is_empty() {
        return Err(failed("Iceberg table has no partition specs"));
    }
    for (&id, fields) in &specs {
        if id < 0 {
            return Err(failed("Iceberg partition spec ID must be nonnegative"));
        }
        let mut names = std::collections::BTreeSet::new();
        let mut ids = std::collections::BTreeSet::new();
        for field in fields {
            if metadata::integer(field, "source-id")? <= 0
                || metadata::string(field, "transform")?.is_empty()
                || !names.insert(metadata::string(field, "name")?)
                || (version == 2 && !ids.insert(metadata::integer(field, "field-id")?))
            {
                return Err(failed("invalid or duplicate Iceberg partition field"));
            }
        }
    }
    Ok(specs)
}
