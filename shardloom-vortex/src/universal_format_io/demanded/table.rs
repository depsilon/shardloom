//! Local table protocols resolve immutable visibility before native execution.
//! Format decoders are shared with ordinary file sources; no table provider
//! evaluates SQL, predicates, joins, ordering or aggregates.

use arrow_schema::{Field, Schema, SchemaRef};
use shardloom_core::{Result, ScalarValue, ShardLoomError, input::LocalTableSelection};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

#[path = "table/deletes.rs"]
mod deletes;
#[path = "table/delta.rs"]
mod delta;
#[path = "table/hudi.rs"]
mod hudi;
#[path = "table/iceberg.rs"]
mod iceberg;
#[path = "table/mapping.rs"]
mod mapping;
#[path = "table/metadata.rs"]
mod metadata;
#[path = "table/partition.rs"]
mod partition;
#[path = "table/schema.rs"]
mod schema;

pub(super) use mapping::FileMapping;
use metadata::Metadata;

#[derive(Clone, Copy)]
enum ColumnMapping {
    Name,
    FieldId,
    PhysicalName,
}

#[derive(Clone)]
struct TableField {
    field: Arc<Field>,
    id: Option<i32>,
    physical_name: String,
}

struct DataFile {
    path: PathBuf,
    location: String,
    partitions: BTreeMap<String, ScalarValue>,
    identity_partitions: BTreeMap<i32, partition::IdentityValue>,
    records: Option<u64>,
    bytes: Option<u64>,
    sequence: u64,
    spec_id: i64,
    partition_key: String,
}

impl DataFile {
    fn plain(path: PathBuf, location: String) -> Self {
        Self {
            path,
            location,
            partitions: BTreeMap::new(),
            identity_partitions: BTreeMap::new(),
            records: None,
            bytes: None,
            sequence: 0,
            spec_id: 0,
            partition_key: String::new(),
        }
    }
}

pub(crate) struct Snapshot {
    pub(super) format: &'static str,
    pub(super) version: u32,
    pub(super) id: String,
    pub(super) root: PathBuf,
    pub(super) schema: SchemaRef,
    fields: Vec<TableField>,
    historical_fields: BTreeMap<i32, TableField>,
    required_physical: std::collections::BTreeSet<String>,
    mapping: ColumnMapping,
    files: Vec<DataFile>,
    deletes: Vec<deletes::DeleteFile>,
    metadata: Metadata,
}

impl Snapshot {
    pub(super) fn admit(
        path: &Path,
        format: &str,
        selection: &LocalTableSelection,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Self>> {
        selection.validate_for(format)?;
        cancellation.check()?;
        let path = std::path::absolute(path).map_err(failed)?;
        let mut snapshot = match format {
            "iceberg" => iceberg::read(&path, selection, memory, cancellation)?,
            "delta" => delta::read(&path, selection, memory, cancellation)?,
            "hudi" => hudi::read(&path, selection, memory, cancellation)?,
            _ => return Err(failed("unknown table format")),
        };
        if snapshot.fields.is_empty() {
            return Err(failed(
                "table schema must contain at least one admitted field",
            ));
        }
        snapshot
            .files
            .sort_by(|left, right| left.path.cmp(&right.path));
        if snapshot
            .files
            .windows(2)
            .any(|pair| pair[0].path == pair[1].path)
        {
            return Err(failed(
                "selected table state contains duplicate data-file paths",
            ));
        }
        snapshot.validate(cancellation)?;
        Ok(Arc::new(snapshot))
    }

    fn new(
        format: &'static str,
        version: u32,
        id: String,
        root: PathBuf,
        fields: Vec<TableField>,
        mapping: ColumnMapping,
        mut metadata: Metadata,
    ) -> Result<Self> {
        metadata.retain(8192 + id.len() as u64 * 4 + root.as_os_str().len() as u64 * 4)?;
        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|field| Arc::clone(&field.field))
                .collect::<Vec<_>>(),
        ));
        Ok(Self {
            format,
            version,
            id,
            root,
            schema,
            fields,
            mapping,
            files: Vec::new(),
            historical_fields: BTreeMap::new(),
            required_physical: std::collections::BTreeSet::new(),
            deletes: Vec::new(),
            metadata,
        })
    }

    fn push_file(&mut self, file: DataFile) -> Result<()> {
        self.metadata.retain(
            4096 + (file.path.as_os_str().len() + file.location.len() + file.partition_key.len())
                as u64
                * 4
                + file
                    .partitions
                    .iter()
                    .map(|(key, value)| key.len() as u64 * 4 + metadata::scalar_bytes(value))
                    .sum::<u64>(),
        )?;
        self.files.try_reserve(1).map_err(failed)?;
        self.files.push(file);
        Ok(())
    }

    fn retain_historical(&mut self, fields: Vec<TableField>) -> Result<()> {
        for field in fields {
            let id = field
                .id
                .ok_or_else(|| failed("historical field identity is absent"))?;
            if let Some(previous) = self.historical_fields.get(&id) {
                let old = previous.field.data_type();
                let new = field.field.data_type();
                if !schema::compatible(old, new, true) {
                    if schema::compatible(new, old, true) {
                        continue;
                    }
                    return Err(failed(
                        "Iceberg field identity has incompatible historical types",
                    ));
                }
            }
            self.metadata
                .retain(field.field.name().len() as u64 * 4 + 4096)?;
            self.historical_fields.insert(id, field);
        }
        Ok(())
    }

    pub(super) fn paths(&self) -> impl Iterator<Item = &Path> {
        self.files.iter().map(|file| file.path.as_path())
    }

    pub(crate) fn validate(&self, cancellation: &CancellationToken) -> Result<()> {
        self.metadata.validate(cancellation)
    }

    pub(crate) fn validate_destination(&self, path: &Path) -> Result<()> {
        self.metadata.validate_destination(path)
    }

    pub(super) fn metadata_bytes(&self) -> u64 {
        self.metadata.read_bytes
    }
}

fn failed(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local table source: {message}; no fallback execution was attempted"
    ))
}
