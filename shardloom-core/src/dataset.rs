//! Dataset reference domain types for planning-time scan requests.
//!
//! These types model identifiers and references only. They do not perform reads
//! and they do not trigger fallback execution.

use crate::{Result, ShardLoomError};

/// Stable dataset identifier used by APIs, CLI, and planning.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasetId(String);

impl DatasetId {
    /// Creates a validated dataset identifier.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when the value is empty or whitespace only.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "dataset id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the dataset identifier value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// URI scheme classification without external URL parser dependencies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UriScheme {
    LocalPath,
    File,
    S3,
    Gcs,
    Adls,
    Other,
}

impl UriScheme {
    /// Returns the stable machine-readable scheme string.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::LocalPath => "local_path",
            Self::File => "file",
            Self::S3 => "s3",
            Self::Gcs => "gcs",
            Self::Adls => "adls",
            Self::Other => "other",
        }
    }
}

/// Dataset URI stored and inspected for planning, not for IO execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatasetUri(String);

impl DatasetUri {
    fn path_without_query_or_fragment(&self) -> &str {
        self.as_str()
            .split_once(['?', '#'])
            .map_or(self.as_str(), |(prefix, _)| prefix)
    }

    /// Creates a validated dataset URI.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when the value is empty or whitespace only.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "dataset uri must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the dataset URI value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Classifies URI scheme from string prefix only.
    #[must_use]
    pub fn scheme(&self) -> UriScheme {
        let s = self.as_str();
        if s.starts_with("file://") {
            UriScheme::File
        } else if s.starts_with("s3://") {
            UriScheme::S3
        } else if s.starts_with("gs://") {
            UriScheme::Gcs
        } else if s.starts_with("abfs://") || s.starts_with("abfss://") {
            UriScheme::Adls
        } else if s.contains("://") {
            UriScheme::Other
        } else {
            UriScheme::LocalPath
        }
    }

    /// Returns true when URI appears to reference Vortex-native storage.
    #[must_use]
    pub fn looks_like_vortex(&self) -> bool {
        let s = self.path_without_query_or_fragment();
        s.ends_with(".vortex") || s.contains(".vortex/")
    }
}

/// Declared or inferred dataset format for planning boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetFormat {
    Vortex,
    Parquet,
    ArrowIpc,
    IcebergCompatible,
    DeltaCompatible,
    JsonLines,
    Csv,
    Unknown,
    Extension(String),
}

impl DatasetFormat {
    /// Returns stable string for diagnostics and explain output.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Vortex => "vortex",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow_ipc",
            Self::IcebergCompatible => "iceberg_compatible",
            Self::DeltaCompatible => "delta_compatible",
            Self::JsonLines => "json_lines",
            Self::Csv => "csv",
            Self::Unknown => "unknown",
            Self::Extension(name) => name,
        }
    }

    /// Returns true when this format is native Vortex input/output.
    #[must_use]
    pub fn is_native_vortex(&self) -> bool {
        matches!(self, Self::Vortex)
    }

    /// Returns true for non-native compatibility translation formats.
    #[must_use]
    pub fn is_compatibility_format(&self) -> bool {
        matches!(
            self,
            Self::Parquet
                | Self::ArrowIpc
                | Self::IcebergCompatible
                | Self::DeltaCompatible
                | Self::JsonLines
                | Self::Csv
        )
    }

    /// Infers a format from the URI suffix and path structure only.
    #[must_use]
    pub fn infer_from_uri(uri: &DatasetUri) -> Self {
        let s = uri.path_without_query_or_fragment().to_ascii_lowercase();
        let ext = std::path::Path::new(&s)
            .extension()
            .and_then(|v| v.to_str())
            .unwrap_or_default();
        if s.ends_with(".vortex") || s.contains(".vortex/") {
            Self::Vortex
        } else if ext.eq_ignore_ascii_case("parquet") {
            Self::Parquet
        } else if ext.eq_ignore_ascii_case("arrow") || ext.eq_ignore_ascii_case("ipc") {
            Self::ArrowIpc
        } else if ext.eq_ignore_ascii_case("jsonl") {
            Self::JsonLines
        } else if ext.eq_ignore_ascii_case("csv") {
            Self::Csv
        } else {
            Self::Unknown
        }
    }

    /// Maps dataset-format terminology to output-target terminology.
    ///
    /// This helper preserves layer boundaries and does not perform translation.
    #[must_use]
    pub fn to_output_target_kind(&self) -> crate::OutputTargetKind {
        crate::OutputTargetKind::from_dataset_format(self)
    }
}

/// Snapshot identifier used in dataset planning references.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotId(String);
impl SnapshotId {
    /// Creates a validated snapshot identifier.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when the value is empty or whitespace only.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "snapshot id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }
    /// Returns the snapshot identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Manifest identifier used in dataset planning references.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ManifestId(String);
impl ManifestId {
    /// Creates a validated manifest identifier.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when the value is empty or whitespace only.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "manifest id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }
    /// Returns the manifest identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Planning-time dataset reference; does not perform any dataset reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetRef {
    pub id: DatasetId,
    pub uri: DatasetUri,
    pub format: DatasetFormat,
    pub snapshot: Option<SnapshotId>,
    pub manifest: Option<ManifestId>,
}

impl DatasetRef {
    #[must_use]
    pub fn new(id: DatasetId, uri: DatasetUri, format: DatasetFormat) -> Self {
        Self {
            id,
            uri,
            format,
            snapshot: None,
            manifest: None,
        }
    }

    /// Builds a dataset reference from a URI by deriving id and inferring format.
    ///
    /// # Errors
    /// Returns an error if derived dataset id validation fails.
    pub fn from_uri(uri: DatasetUri) -> Result<Self> {
        let id = DatasetId::new(uri.as_str().to_string())?;
        let format = DatasetFormat::infer_from_uri(&uri);
        Ok(Self::new(id, uri, format))
    }

    #[must_use]
    pub fn with_snapshot(mut self, snapshot: SnapshotId) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    #[must_use]
    pub fn with_manifest(mut self, manifest: ManifestId) -> Self {
        self.manifest = Some(manifest);
        self
    }

    #[must_use]
    pub fn is_native_vortex(&self) -> bool {
        self.format.is_native_vortex()
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "id={}; uri={}; format={}; scheme={}; has_snapshot={}; has_manifest={}; native_vortex={}",
            self.id.as_str(),
            self.uri.as_str(),
            self.format.as_str(),
            self.uri.scheme().as_str(),
            self.snapshot.is_some(),
            self.manifest.is_some(),
            self.is_native_vortex()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataset_id_rejects_empty_ids() {
        assert!(DatasetId::new("   ").is_err());
    }
    #[test]
    fn dataset_uri_rejects_empty_values() {
        assert!(DatasetUri::new("\n").is_err());
    }
    #[test]
    fn dataset_uri_identifies_local_path() {
        assert_eq!(
            DatasetUri::new("/tmp/a").unwrap().scheme(),
            UriScheme::LocalPath
        );
    }
    #[test]
    fn dataset_uri_identifies_s3_scheme() {
        assert_eq!(DatasetUri::new("s3://b/k").unwrap().scheme(), UriScheme::S3);
    }
    #[test]
    fn dataset_uri_identifies_gcs_scheme() {
        assert_eq!(
            DatasetUri::new("gs://b/k").unwrap().scheme(),
            UriScheme::Gcs
        );
    }
    #[test]
    fn dataset_uri_identifies_adls_scheme() {
        assert_eq!(
            DatasetUri::new("abfss://c@a/p").unwrap().scheme(),
            UriScheme::Adls
        );
    }
    #[test]
    fn dataset_uri_detects_vortex_extension() {
        assert!(DatasetUri::new("x.vortex").unwrap().looks_like_vortex());
    }
    #[test]
    fn dataset_uri_detects_vortex_directory() {
        assert!(
            DatasetUri::new("s3://b/p.vortex/part")
                .unwrap()
                .looks_like_vortex()
        );
    }
    #[test]
    fn dataset_uri_detects_vortex_extension_with_query() {
        assert!(
            DatasetUri::new("s3://bucket/table.vortex?versionId=abc")
                .unwrap()
                .looks_like_vortex()
        );
    }
    #[test]
    fn dataset_format_infers_vortex_from_uri() {
        assert_eq!(
            DatasetFormat::infer_from_uri(&DatasetUri::new("x.vortex").unwrap()),
            DatasetFormat::Vortex
        );
    }
    #[test]
    fn dataset_format_infers_parquet_from_uri() {
        assert_eq!(
            DatasetFormat::infer_from_uri(&DatasetUri::new("x.parquet").unwrap()),
            DatasetFormat::Parquet
        );
    }
    #[test]
    fn dataset_format_maps_to_output_target_kind() {
        assert_eq!(
            DatasetFormat::Vortex.to_output_target_kind(),
            crate::OutputTargetKind::Vortex
        );
        assert_eq!(
            DatasetFormat::Parquet.to_output_target_kind(),
            crate::OutputTargetKind::Parquet
        );
        assert_eq!(
            DatasetFormat::Extension("x".into()).to_output_target_kind(),
            crate::OutputTargetKind::Extension("x".into())
        );
    }
    #[test]
    fn dataset_format_infers_vortex_from_uri_with_query_and_fragment() {
        assert_eq!(
            DatasetFormat::infer_from_uri(
                &DatasetUri::new("s3://bucket/table.vortex?versionId=abc#frag").unwrap()
            ),
            DatasetFormat::Vortex
        );
    }
    #[test]
    fn dataset_format_recognizes_vortex_as_native() {
        assert!(DatasetFormat::Vortex.is_native_vortex());
    }
    #[test]
    fn dataset_format_recognizes_parquet_as_compatibility() {
        assert!(DatasetFormat::Parquet.is_compatibility_format());
    }
    #[test]
    fn snapshot_id_rejects_empty_ids() {
        assert!(SnapshotId::new("").is_err());
    }
    #[test]
    fn manifest_id_rejects_empty_ids() {
        assert!(ManifestId::new(" ").is_err());
    }
    #[test]
    fn dataset_ref_from_uri_infers_format() {
        assert_eq!(
            DatasetRef::from_uri(DatasetUri::new("file://a.vortex").unwrap())
                .unwrap()
                .format,
            DatasetFormat::Vortex
        );
    }
    #[test]
    fn dataset_ref_summary_includes_fallback_neutral_native_vortex_details() {
        let summary =
            DatasetRef::from_uri(DatasetUri::new("s3://bucket/path/table.vortex").unwrap())
                .unwrap()
                .summary();
        assert!(summary.contains("native_vortex=true"));
        assert!(summary.contains("scheme=s3"));
    }
}
