//! Feature-gated local `vortex_ingest` lifecycle helpers.
//!
//! This module intentionally exposes a narrow local prepare-once path for flat
//! scalar rows. It writes a local Vortex artifact, reopens/scans the artifact
//! for row-count proof, and returns evidence fields that callers can surface as
//! a `VortexPreparedState`. It is not a broad Vortex writer, object-store sink,
//! table commit path, or query-engine integration.

use std::path::PathBuf;

#[cfg(feature = "vortex-write")]
use std::{collections::BTreeSet, path::Path, time::Instant};

use shardloom_core::{Result, ScalarValue, ShardLoomError};

/// Request to write one flat scalar local source into a local Vortex artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexPreparedStateWriteRequest {
    pub target_path: PathBuf,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<(String, ScalarValue)>>,
    pub allow_overwrite: bool,
}

impl VortexPreparedStateWriteRequest {
    /// Create a request for a local `VortexPreparedState` artifact write.
    #[must_use]
    pub fn new(
        target_path: impl Into<PathBuf>,
        columns: Vec<String>,
        rows: Vec<Vec<(String, ScalarValue)>>,
    ) -> Self {
        Self {
            target_path: target_path.into(),
            columns,
            rows,
            allow_overwrite: false,
        }
    }

    /// Allow overwriting an existing local target artifact.
    #[must_use]
    pub const fn allow_overwrite(mut self, allow_overwrite: bool) -> Self {
        self.allow_overwrite = allow_overwrite;
        self
    }
}

/// Evidence returned by the scoped local `vortex_ingest` helper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexPreparedStateWriteReport {
    pub target_path: PathBuf,
    pub row_count: u64,
    pub column_count: usize,
    pub column_families: Vec<(String, String)>,
    pub bytes_written: u64,
    pub artifact_digest: String,
    pub writer_row_count: u64,
    pub reopen_row_count: u64,
    pub write_micros: u128,
    pub reopen_scan_micros: u128,
    pub upstream_vortex_write_called: bool,
    pub upstream_vortex_scan_called: bool,
}

impl VortexPreparedStateWriteReport {
    /// Return a stable comma-separated column family summary.
    #[must_use]
    pub fn column_family_summary(&self) -> String {
        self.column_families
            .iter()
            .map(|(column, family)| format!("{column}:{family}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Return a compact schema/layout summary for evidence fields.
    #[must_use]
    pub fn layout_summary(&self) -> String {
        format!(
            "local_flat_struct;columns={};rows={}",
            self.column_count, self.row_count
        )
    }

    /// Return a compact encoding summary for evidence fields.
    #[must_use]
    pub fn encoding_summary(&self) -> String {
        format!(
            "upstream_vortex_writer_default;{}",
            self.column_family_summary()
        )
    }

    /// Return a compact statistics summary for evidence fields.
    #[must_use]
    pub fn statistics_summary(&self) -> String {
        format!(
            "writer_row_count={};reopen_row_count={};bytes_written={}",
            self.writer_row_count, self.reopen_row_count, self.bytes_written
        )
    }
}

/// Whether local Vortex artifact writing is compiled into this crate.
#[must_use]
pub const fn vortex_ingest_write_feature_enabled() -> bool {
    cfg!(feature = "vortex-write")
}

/// Write flat scalar rows into a local Vortex artifact and reopen/scan it.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the feature gate is not
/// enabled, row shape/family support is outside the scoped contract, the target
/// already exists without overwrite permission, or upstream Vortex write/reopen
/// APIs fail.
#[cfg(not(feature = "vortex-write"))]
pub fn write_flat_scalar_vortex_prepared_state(
    _request: VortexPreparedStateWriteRequest,
) -> Result<VortexPreparedStateWriteReport> {
    Err(ShardLoomError::InvalidOperation(
        "local vortex_ingest runtime requires building shardloom-cli with --features vortex-write; default builds expose vortex_ingest as a deterministic blocked prepare-once route"
            .to_string(),
    ))
}

/// Write flat scalar rows into a local Vortex artifact and reopen/scan it.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when row shape/family support is
/// outside the scoped contract, the target already exists without overwrite
/// permission, or upstream Vortex write/reopen APIs fail.
#[cfg(feature = "vortex-write")]
pub fn write_flat_scalar_vortex_prepared_state(
    request: VortexPreparedStateWriteRequest,
) -> Result<VortexPreparedStateWriteReport> {
    use std::fs;

    let row_count = validate_flat_rows(&request.columns, &request.rows)?;
    let column_families = scalar_column_families(&request.columns, &request.rows)?;
    if request.target_path.exists() && !request.allow_overwrite {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest target '{}' already exists; pass --allow-overwrite to replace it",
            request.target_path.display()
        )));
    }
    if let Some(parent) = request.target_path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local vortex_ingest target directory '{}': {error}",
                parent.display()
            ))
        })?;
    }

    let write_start = Instant::now();
    let array = flat_rows_to_vortex_struct(&request.columns, &request.rows, &column_families)?;
    let writer_row_count = write_vortex_array(&request.target_path, &array)?;
    let write_micros = write_start.elapsed().as_micros();

    let bytes = fs::read(&request.target_path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read local vortex_ingest artifact '{}' for digest: {error}",
            request.target_path.display()
        ))
    })?;
    let bytes_written = usize_to_u64(bytes.len())?;
    let artifact_digest = fnv64_digest_bytes(&bytes);

    let reopen_start = Instant::now();
    let reopen_row_count = reopen_vortex_row_count(&request.target_path)?;
    let reopen_scan_micros = reopen_start.elapsed().as_micros();
    if writer_row_count != row_count || reopen_row_count != row_count {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest row-count proof mismatch: source={row_count} writer={writer_row_count} reopen={reopen_row_count}"
        )));
    }

    Ok(VortexPreparedStateWriteReport {
        target_path: request.target_path,
        row_count,
        column_count: request.columns.len(),
        column_families,
        bytes_written,
        artifact_digest,
        writer_row_count,
        reopen_row_count,
        write_micros,
        reopen_scan_micros,
        upstream_vortex_write_called: true,
        upstream_vortex_scan_called: true,
    })
}

#[cfg(feature = "vortex-write")]
fn validate_flat_rows(columns: &[String], rows: &[Vec<(String, ScalarValue)>]) -> Result<u64> {
    validate_flat_columns(columns)?;
    for (row_index, row) in rows.iter().enumerate() {
        if row.len() != columns.len() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest row {} has {} columns, expected {}; no fallback execution was attempted",
                row_index + 1,
                row.len(),
                columns.len()
            )));
        }
        for (column_index, (name, _value)) in row.iter().enumerate() {
            let expected = &columns[column_index];
            if name != expected {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local vortex_ingest row {} column {} is '{}', expected '{}'; no fallback execution was attempted",
                    row_index + 1,
                    column_index + 1,
                    name,
                    expected
                )));
            }
        }
    }
    usize_to_u64(rows.len())
}

#[cfg(feature = "vortex-write")]
fn validate_flat_columns(columns: &[String]) -> Result<()> {
    if columns.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local vortex_ingest requires at least one column; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let mut seen = BTreeSet::new();
    for column in columns {
        if column.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "local vortex_ingest column names must not be empty; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        if !seen.insert(column) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local vortex_ingest contains duplicate column '{column}'; no fallback execution was attempted"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-write")]
fn scalar_column_families(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<(String, String)>> {
    columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            let mut family: Option<&'static str> = None;
            for row in rows {
                let value = &row[column_index].1;
                let candidate = scalar_family(value).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(format!(
                        "local vortex_ingest column '{column}' contains unsupported value {}; scoped Vortex ingest admits non-null int64, uint64, float64, utf8, date32, and timestamp_micros only; no fallback execution was attempted",
                        value.summary()
                    ))
                })?;
                if let Some(existing) = family {
                    if existing != candidate {
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "local vortex_ingest column '{column}' mixes scalar families {existing} and {candidate}; no fallback execution was attempted"
                        )));
                    }
                } else {
                    family = Some(candidate);
                }
            }
            Ok((column.clone(), family.unwrap_or("utf8").to_string()))
        })
        .collect()
}

#[cfg(feature = "vortex-write")]
fn scalar_family(value: &ScalarValue) -> Option<&'static str> {
    match value {
        ScalarValue::Int64(_) => Some("int64"),
        ScalarValue::UInt64(_) => Some("uint64"),
        ScalarValue::Float64(value) if value.is_finite() => Some("float64"),
        ScalarValue::Utf8(_) => Some("utf8"),
        ScalarValue::Date32(_) => Some("date32"),
        ScalarValue::TimestampMicros(_) => Some("timestamp_micros"),
        ScalarValue::Null
        | ScalarValue::Boolean(_)
        | ScalarValue::Binary(_)
        | ScalarValue::Float64(_) => None,
    }
}

#[cfg(feature = "vortex-write")]
fn flat_rows_to_vortex_struct(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
    column_families: &[(String, String)],
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::StructArray;
    use vortex::array::dtype::FieldNames;
    use vortex::array::validity::Validity;

    let fields = column_families
        .iter()
        .enumerate()
        .map(|(column_index, (_column, family))| {
            column_to_vortex_array(&columns[column_index], column_index, family, rows)
        })
        .collect::<Result<Vec<_>>>()?;

    let array = StructArray::try_new(
        FieldNames::from(columns.iter().map(String::as_str).collect::<Vec<_>>()),
        fields,
        rows.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    Ok(array)
}

#[cfg(feature = "vortex-write")]
fn column_to_vortex_array(
    column: &str,
    column_index: usize,
    family: &str,
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<vortex::array::ArrayRef> {
    use vortex::array::IntoArray as _;
    use vortex::array::arrays::{PrimitiveArray, VarBinViewArray};

    match family {
        "int64" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Int64(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "uint64" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::UInt64(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "float64" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Float64(value) if value.is_finite() => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "utf8" => {
            let values = rows
                .iter()
                .map(|row| match &row[column_index].1 {
                    ScalarValue::Utf8(value) => Ok(value.as_str()),
                    value => Err(unexpected_vortex_ingest_value(column, family, value)),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(VarBinViewArray::from_iter_str(values).into_array())
        }
        "date32" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::Date32(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        "timestamp_micros" => Ok(rows
            .iter()
            .map(|row| match &row[column_index].1 {
                ScalarValue::TimestampMicros(value) => Ok(*value),
                value => Err(unexpected_vortex_ingest_value(column, family, value)),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array()),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest column '{column}' has unsupported scalar family {other}; no fallback execution was attempted"
        ))),
    }
}

#[cfg(feature = "vortex-write")]
fn unexpected_vortex_ingest_value(
    column: &str,
    family: &str,
    value: &ScalarValue,
) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest column '{column}' expected {family}, found {}; no fallback execution was attempted",
        value.summary()
    ))
}

#[cfg(feature = "vortex-write")]
fn write_vortex_array(path: &Path, array: &vortex::array::ArrayRef) -> Result<u64> {
    use std::fs;

    use vortex::VortexSessionDefault as _;
    use vortex::file::WriteOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut bytes = Vec::new();
    let summary = runtime
        .block_on(
            session
                .write_options()
                .write(&mut bytes, array.to_array_stream()),
        )
        .map_err(vortex_error)?;
    let expected_rows = usize_to_u64(array.len())?;
    if summary.row_count() != expected_rows {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local vortex_ingest writer row count mismatch: wrote {}, expected {}; no fallback execution was attempted",
            summary.row_count(),
            expected_rows
        )));
    }
    fs::write(path, bytes).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write local vortex_ingest artifact '{}': {error}",
            path.display()
        ))
    })?;
    Ok(summary.row_count())
}

#[cfg(feature = "vortex-write")]
fn reopen_vortex_row_count(path: &Path) -> Result<u64> {
    use std::fs;

    use vortex::VortexSessionDefault as _;
    use vortex::array::stream::ArrayStreamExt as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let bytes = fs::read(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to reopen local vortex_ingest artifact '{}': {error}",
            path.display()
        ))
    })?;
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = session
        .open_options()
        .open_buffer(bytes)
        .map_err(vortex_error)?;
    let array = runtime
        .block_on(
            file.scan()
                .map_err(vortex_error)?
                .into_array_stream()
                .map_err(vortex_error)?
                .read_all(),
        )
        .map_err(vortex_error)?;
    usize_to_u64(array.len())
}

#[cfg(feature = "vortex-write")]
fn vortex_error(error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local vortex_ingest upstream Vortex API failed: {error}; no fallback execution was attempted"
    ))
}

#[cfg(feature = "vortex-write")]
fn fnv64_digest_bytes(value: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("fnv64:{hash:016x}")
}

#[cfg(feature = "vortex-write")]
fn usize_to_u64(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        ShardLoomError::InvalidOperation(
            "local vortex_ingest value does not fit in u64".to_string(),
        )
    })
}

#[cfg(all(test, feature = "vortex-write"))]
mod tests {
    use super::*;

    #[test]
    fn local_flat_scalar_rows_write_and_reopen_vortex_artifact() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-vortex-ingest-{}-{}.vortex",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let request = VortexPreparedStateWriteRequest::new(
            &path,
            vec!["id".to_string(), "label".to_string(), "metric".to_string()],
            vec![
                vec![
                    ("id".to_string(), ScalarValue::Int64(1)),
                    ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                    ("metric".to_string(), ScalarValue::Float64(1.5)),
                ],
                vec![
                    ("id".to_string(), ScalarValue::Int64(2)),
                    ("label".to_string(), ScalarValue::Utf8("beta".to_string())),
                    ("metric".to_string(), ScalarValue::Float64(2.5)),
                ],
            ],
        );

        let report = write_flat_scalar_vortex_prepared_state(request).expect("write report");

        assert_eq!(report.row_count, 2);
        assert_eq!(report.reopen_row_count, 2);
        assert!(report.artifact_digest.starts_with("fnv64:"));
        assert!(path.exists());
        std::fs::remove_file(path).expect("remove artifact");
    }
}
