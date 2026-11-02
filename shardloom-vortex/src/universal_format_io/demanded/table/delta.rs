//! Delta snapshot reconciliation over local immutable commit/checkpoint files.

use super::{ColumnMapping, DataFile, Metadata, Result, Snapshot, failed, metadata, schema};
use serde_json::Value;
use shardloom_core::input::LocalTableSelection;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

#[derive(Default)]
struct State {
    protocol: Option<Value>,
    schema: Option<Value>,
    files: BTreeMap<String, Value>,
}

#[derive(Default)]
struct Checkpoint {
    single: Option<PathBuf>,
    parts: BTreeMap<u32, PathBuf>,
    total: Option<u32>,
}

impl Checkpoint {
    fn complete(&self) -> bool {
        self.single.is_some()
            || self.total.is_some_and(|total| {
                total > 0
                    && self.parts.len() == total as usize
                    && self.parts.keys().copied().eq(1..=total)
            })
    }
}

// Keep snapshot selection and ordered replay in one admission transaction.
#[allow(clippy::too_many_lines)]
pub(super) fn read(
    path: &Path,
    selection: &LocalTableSelection,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Snapshot> {
    let root = path.canonicalize().map_err(failed)?;
    if !root.is_dir() {
        return Err(failed("Delta input must be a local table root"));
    }
    let log = root.join("_delta_log");
    let mut held = Metadata::new(memory, cancellation)?;
    let mut commits = BTreeMap::new();
    let mut checkpoints: BTreeMap<u64, Checkpoint> = BTreeMap::new();
    let mut unsupported_checkpoints = BTreeSet::new();
    for entry in std::fs::read_dir(&log).map_err(failed)? {
        cancellation.check()?;
        let entry = entry.map_err(failed)?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| failed("Delta log filenames must be UTF8"))?;
        held.retain(1024 + name.len() as u64 * 4)?;
        let Some((prefix, suffix)) = name.split_once('.') else {
            continue;
        };
        if prefix.len() != 20 || !prefix.bytes().all(|v| v.is_ascii_digit()) {
            continue;
        }
        let version: u64 = prefix.parse().map_err(failed)?;
        if suffix == "json" {
            commits.insert(version, entry.path());
        } else if suffix == "checkpoint.parquet" {
            checkpoints.entry(version).or_default().single = Some(entry.path());
        } else if let Some(parts) = suffix
            .strip_prefix("checkpoint.")
            .and_then(|v| v.strip_suffix(".parquet"))
            && let Some((part, total)) = parts.split_once('.')
            && part.len() == 10
            && total.len() == 10
            && part
                .bytes()
                .chain(total.bytes())
                .all(|v| v.is_ascii_digit())
        {
            let part: u32 = part.parse().map_err(failed)?;
            let total: u32 = total.parse().map_err(failed)?;
            if part == 0 || part > total {
                return Err(failed("Delta checkpoint part index is invalid"));
            }
            let checkpoint = checkpoints.entry(version).or_default();
            if checkpoint.total.is_some_and(|v| v != total) {
                return Err(failed("Delta checkpoint has conflicting part totals"));
            }
            checkpoint.total = Some(total);
            if checkpoint.parts.insert(part, entry.path()).is_some() {
                return Err(failed("Delta checkpoint has duplicate parts"));
            }
        } else if suffix.starts_with("checkpoint.")
            && Path::new(suffix)
                .extension()
                .is_some_and(|extension| extension == "parquet" || extension == "json")
        {
            // Unknown checkpoint layouts still establish an observed version.
            // Default selection must not silently return an older snapshot.
            unsupported_checkpoints.insert(version);
        }
    }
    let latest = commits
        .keys()
        .copied()
        // Even an incomplete checkpoint establishes an observed committed
        // version. Reconstruction may fail, but must not select older data.
        .chain(checkpoints.keys().copied())
        .chain(unsupported_checkpoints.iter().copied())
        .max()
        .ok_or_else(|| failed("Delta log has no reconstructable local version"))?;
    let target = selection.version.unwrap_or(latest);
    if target > latest {
        return Err(failed("requested Delta version is not present"));
    }
    if unsupported_checkpoints.contains(&target)
        && !commits.contains_key(&target)
        && !checkpoints.get(&target).is_some_and(Checkpoint::complete)
    {
        return Err(failed(
            "selected Delta version requires an unsupported checkpoint layout",
        ));
    }
    let checkpoint = checkpoints
        .range(..=target)
        .rev()
        .find(|(_, v)| v.complete());
    let mut state = State::default();
    let start = if let Some((&version, checkpoint)) = checkpoint {
        let paths = checkpoint.single.iter().chain(
            checkpoint
                .parts
                .values()
                .filter(|_| checkpoint.single.is_none()),
        );
        let mut actions = Vec::new();
        for path in paths {
            let records = held.records(path, "parquet")?;
            for row in &records.value().rows {
                held.retain(row.to_string().len() as u64 * 32 + 512)?;
                actions.try_reserve(1).map_err(failed)?;
                actions.push(row.clone());
            }
        }
        state.apply(&actions, &mut held, true)?;
        version
            .checked_add(1)
            .ok_or_else(|| failed("Delta version exceeds replay domain"))?
    } else {
        0
    };
    if target.checked_sub(start).is_some_and(|n| n >= 1_000_000) {
        return Err(failed("Delta replay exceeds 1000000 commits"));
    }
    for version in start..=target {
        cancellation.check()?;
        let path = commits.get(&version).ok_or_else(|| failed(format!("Delta log is incomplete at version {version}; supply a complete checkpoint or retained commit history")))?;
        let bytes = held.bytes(path)?;
        let _workspace = memory.reserve(bytes.value().len() as u64 * 32 + 8192)?;
        let text = std::str::from_utf8(bytes.value()).map_err(failed)?;
        let mut actions = Vec::new();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            cancellation.check()?;
            actions.try_reserve(1).map_err(failed)?;
            actions.push(serde_json::from_str(line).map_err(failed)?);
        }
        if actions.is_empty() {
            return Err(failed("Delta commit contains no actions"));
        }
        state.apply(&actions, &mut held, false)?;
    }
    let protocol = state
        .protocol
        .as_ref()
        .ok_or_else(|| failed("Delta state has no protocol action"))?;
    let reader = validate_protocol(protocol)?;
    let features = features(protocol)?;
    let table = state
        .schema
        .as_ref()
        .ok_or_else(|| failed("Delta state has no metadata action"))?;
    let _id = metadata::string(table, "id")?;
    if table
        .get("format")
        .and_then(|v| v.get("provider"))
        .and_then(Value::as_str)
        != Some("parquet")
    {
        return Err(failed("Delta table data provider must be Parquet"));
    }
    let mode = table
        .get("configuration")
        .and_then(|v| v.get("delta.columnMapping.mode"))
        .and_then(Value::as_str)
        .unwrap_or("none");
    let mapping = match mode {
        "none" => ColumnMapping::Name,
        "name" if reader == 2 || features.contains("columnMapping") => ColumnMapping::PhysicalName,
        "id" if reader == 2 || features.contains("columnMapping") => ColumnMapping::FieldId,
        _ => {
            return Err(failed(
                "Delta column mapping is unknown or not enabled by its reader protocol",
            ));
        }
    };
    let encoded_schema = metadata::string(table, "schemaString")?;
    let _schema_workspace = memory.reserve(encoded_schema.len() as u64 * 32 + 8192)?;
    let logical: Value = serde_json::from_str(encoded_schema).map_err(failed)?;
    let fields = schema::delta(&logical, mapping, features.contains("timestampNtz"))?;
    held.retain(encoded_schema.len() as u64 * 32 + 8192)?;
    let partition_columns = metadata::array(table, "partitionColumns")?
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| failed("Delta partition column must be a string"))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if partition_columns.len() != metadata::array(table, "partitionColumns")?.len()
        || partition_columns
            .iter()
            .any(|name| !fields.iter().any(|f| f.field.name() == *name))
    {
        return Err(failed(
            "Delta partition declarations are duplicated or absent from schema",
        ));
    }
    let mut snapshot = Snapshot::new(
        "delta",
        reader,
        target.to_string(),
        root.clone(),
        fields,
        mapping,
        held,
    )?;
    for (location, add) in state.files {
        cancellation.check()?;
        let decoded = if location.starts_with("file://") {
            location.clone()
        } else {
            metadata::percent_decode(&location)?
        };
        let path = metadata::local_path(&root, &decoded)?;
        let mut file = DataFile::plain(path, location);
        file.bytes = Some(metadata::unsigned(&add, "size")?);
        let values = metadata::object(
            add.get("partitionValues")
                .ok_or_else(|| failed("Delta add has no partition values"))?,
        )?;
        if values.len() != partition_columns.len() {
            return Err(failed(
                "Delta add partition values do not match table partition columns",
            ));
        }
        for field in &snapshot.fields {
            if !partition_columns.contains(field.field.name().as_str()) {
                continue;
            }
            let key = match mapping {
                ColumnMapping::Name => field.field.name(),
                _ => &field.physical_name,
            };
            let value = values
                .get(key)
                .ok_or_else(|| failed("Delta add is missing a partition value"))?;
            let value = if value.is_null() {
                None
            } else {
                Some(
                    value
                        .as_str()
                        .ok_or_else(|| failed("Delta partition values must be strings or null"))?,
                )
            };
            let parsed = schema::partition(field.field.data_type(), value)?;
            if parsed.is_null() && !field.field.is_nullable() {
                return Err(failed("Delta non-null partition column has null value"));
            }
            file.partitions.insert(field.field.name().clone(), parsed);
        }
        snapshot.push_file(file)?;
    }
    Ok(snapshot)
}

impl State {
    fn apply(&mut self, actions: &[Value], held: &mut Metadata, checkpoint: bool) -> Result<()> {
        let mut adds = BTreeMap::new();
        let mut removes = BTreeSet::new();
        let mut protocol = None;
        let mut schema = None;
        for action in actions {
            held.cancellation.check()?;
            let object = metadata::object(action)?;
            let nonnull = object
                .iter()
                .filter(|(_, value)| !value.is_null())
                .collect::<Vec<_>>();
            if nonnull.len() != 1 {
                return Err(failed("Delta record must contain one non-null action"));
            }
            let (name, value) = nonnull[0];
            match name.as_str() {
                "protocol" => {
                    validate_protocol(value)?;
                    if protocol.replace(value).is_some() {
                        return Err(failed("Delta version contains duplicate protocol actions"));
                    }
                }
                "metaData" => {
                    if schema.replace(value).is_some() {
                        return Err(failed("Delta version contains duplicate metadata actions"));
                    }
                }
                "add" | "remove" => {
                    if value.get("deletionVector").is_some_and(|v| !v.is_null()) {
                        return Err(failed(
                            "Delta deletion vectors are not admitted; no deleted rows may be ignored",
                        ));
                    }
                    let path = metadata::string(value, "path")?;
                    held.retain(path.len() as u64 * 4 + 512)?;
                    if name == "add" {
                        held.retain(value.to_string().len() as u64 * 32 + 512)?;
                        if adds.insert(path.to_owned(), value.clone()).is_some() {
                            return Err(failed("duplicate Delta add path in one version"));
                        }
                    } else if !removes.insert(path.to_owned()) {
                        return Err(failed("duplicate Delta remove path in one version"));
                    }
                }
                "checkpointMetadata" | "sidecar" => {
                    return Err(failed("Delta V2 checkpoint/sidecar layout is not admitted"));
                }
                // Snapshot reads ignore CDC, transaction and optional domain
                // actions. Any reader obligation must be a supported feature.
                _ => {}
            }
        }
        if adds.keys().any(|path| removes.contains(path)) {
            return Err(failed(
                "Delta logical file is both added and removed in one version",
            ));
        }
        if checkpoint && (protocol.is_none() || schema.is_none()) {
            return Err(failed(
                "Delta checkpoint lacks complete protocol/metadata state",
            ));
        }
        for path in removes {
            self.files.remove(&path);
        }
        self.files.extend(adds);
        if let Some(value) = protocol {
            held.retain(value.to_string().len() as u64 * 32 + 512)?;
            self.protocol = Some(value.clone());
        }
        if let Some(value) = schema {
            held.retain(value.to_string().len() as u64 * 32 + 512)?;
            self.schema = Some(value.clone());
        }
        Ok(())
    }
}

fn features(value: &Value) -> Result<BTreeSet<&str>> {
    let Some(values) = value.get("readerFeatures") else {
        return Ok(BTreeSet::new());
    };
    let values = values
        .as_array()
        .ok_or_else(|| failed("Delta readerFeatures must be an array"))?;
    let set = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| failed("Delta reader feature must be a string"))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    if set.len() != values.len() {
        return Err(failed("Delta reader features contain duplicates"));
    }
    Ok(set)
}

fn validate_protocol(value: &Value) -> Result<u32> {
    let reader = u32::try_from(metadata::unsigned(value, "minReaderVersion")?).map_err(failed)?;
    let writer = metadata::unsigned(value, "minWriterVersion")?;
    if !(1..=3).contains(&reader) || writer == 0 || writer > 7 {
        return Err(failed(
            "Delta reader/writer protocol version is not admitted",
        ));
    }
    let features = features(value)?;
    if (writer == 7) != value.get("writerFeatures").is_some_and(Value::is_array)
        || (writer != 7 && value.get("writerFeatures").is_some())
    {
        return Err(failed(
            "Delta writer feature declarations require writer protocol 7",
        ));
    }
    if writer == 7 {
        let writers = metadata::array(value, "writerFeatures")?;
        let names = writers
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| failed("Delta writer feature must be a string"))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        if names.len() != writers.len() {
            return Err(failed("duplicate Delta writer feature"));
        }
    }
    if reader == 3 {
        if writer != 7
            || !value.get("readerFeatures").is_some_and(Value::is_array)
            || !value.get("writerFeatures").is_some_and(Value::is_array)
        {
            return Err(failed(
                "Delta reader v3 requires writer v7 and feature arrays",
            ));
        }
        let writers = metadata::array(value, "writerFeatures")?;
        for feature in &features {
            if !matches!(*feature, "columnMapping" | "timestampNtz")
                || !writers.iter().any(|value| value.as_str() == Some(feature))
            {
                return Err(failed(format!(
                    "required Delta reader feature {feature} is not admitted or absent from writer features"
                )));
            }
        }
    } else if value.get("readerFeatures").is_some() {
        return Err(failed("Delta reader features require reader v3"));
    }
    if reader == 2 && writer < 5 {
        return Err(failed(
            "Delta reader v2 requires writer protocol at least 5",
        ));
    }
    Ok(reader)
}
