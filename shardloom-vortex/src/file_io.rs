//! Feature-gated metadata-only local `Vortex` file open contract for `ShardLoom`.

use std::fmt::Write as _;

use shardloom_core::{DatasetUri, Diagnostic, DiagnosticCode, Result, UriScheme};

use crate::VortexMetadataSummaryReport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexFileIoFeatureStatus {
    Disabled,
    Enabled,
    DeferredApiUnclear,
    DeferredApiUnstable,
    Unsupported,
}
impl VortexFileIoFeatureStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Enabled => "enabled",
            Self::DeferredApiUnclear => "deferred_api_unclear",
            Self::DeferredApiUnstable => "deferred_api_unstable",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled)
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataOpenMode {
    ReportOnly,
    LocalFileMetadataOnly,
    Unsupported,
}
impl VortexMetadataOpenMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::LocalFileMetadataOnly => "local_file_metadata_only",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn performs_file_io(&self) -> bool {
        matches!(self, Self::LocalFileMetadataOnly)
    }
    #[must_use]
    pub const fn performs_data_io(&self) -> bool {
        false
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataOpenStatus {
    Planned,
    OpenedMetadataOnly,
    FeatureDisabled,
    ApiDeferred,
    InvalidTarget,
    FileMissing,
    Unsupported,
}
impl VortexMetadataOpenStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::OpenedMetadataOnly => "opened_metadata_only",
            Self::FeatureDisabled => "feature_disabled",
            Self::ApiDeferred => "api_deferred",
            Self::InvalidTarget => "invalid_target",
            Self::FileMissing => "file_missing",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::InvalidTarget | Self::FileMissing | Self::Unsupported
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexMetadataOpenRequest {
    pub uri: DatasetUri,
    pub allow_file_io: bool,
    pub allow_data_io: bool,
    pub allow_object_store_io: bool,
    pub allow_write_io: bool,
}
impl VortexMetadataOpenRequest {
    #[must_use]
    pub fn metadata_only(uri: DatasetUri) -> Self {
        Self {
            uri,
            allow_file_io: true,
            allow_data_io: false,
            allow_object_store_io: false,
            allow_write_io: false,
        }
    }
    #[must_use]
    pub fn report_only(uri: DatasetUri) -> Self {
        Self {
            uri,
            allow_file_io: false,
            allow_data_io: false,
            allow_object_store_io: false,
            allow_write_io: false,
        }
    }
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.allow_data_io && !self.allow_object_store_io && !self.allow_write_io
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "uri={} allow_file_io={} allow_data_io={} allow_object_store_io={} allow_write_io={}",
            self.uri.as_str(),
            self.allow_file_io,
            self.allow_data_io,
            self.allow_object_store_io,
            self.allow_write_io
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexMetadataOpenReport {
    pub feature_status: VortexFileIoFeatureStatus,
    pub open_status: VortexMetadataOpenStatus,
    pub mode: VortexMetadataOpenMode,
    pub request: VortexMetadataOpenRequest,
    pub metadata_summary: Option<VortexMetadataSummaryReport>,
    pub api_name: Option<String>,
    pub file_io_performed: bool,
    pub data_io_performed: bool,
    pub object_store_io_performed: bool,
    pub write_io_performed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMetadataOpenReport {
    #[must_use]
    pub fn feature_disabled(request: VortexMetadataOpenRequest) -> Self {
        Self {
            feature_status: VortexFileIoFeatureStatus::Disabled,
            open_status: VortexMetadataOpenStatus::FeatureDisabled,
            mode: VortexMetadataOpenMode::ReportOnly,
            request,
            metadata_summary: None,
            api_name: None,
            file_io_performed: false,
            data_io_performed: false,
            object_store_io_performed: false,
            write_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn api_deferred(request: VortexMetadataOpenRequest, reason: impl Into<String>) -> Self {
        let reason = reason.into();
        let mut r = Self::feature_disabled(request);
        r.feature_status = VortexFileIoFeatureStatus::DeferredApiUnclear;
        r.open_status = VortexMetadataOpenStatus::ApiDeferred;
        r.add_diagnostic(Diagnostic::configuration_error(
            "vortex-file-io",
            reason,
            "Wait for stable public metadata-only Vortex file API support.",
        ));
        r
    }
    #[must_use]
    pub fn opened_metadata_only(
        request: VortexMetadataOpenRequest,
        api_name: impl Into<String>,
        metadata_summary: VortexMetadataSummaryReport,
    ) -> Self {
        Self {
            feature_status: VortexFileIoFeatureStatus::Enabled,
            open_status: VortexMetadataOpenStatus::OpenedMetadataOnly,
            mode: VortexMetadataOpenMode::LocalFileMetadataOnly,
            request,
            metadata_summary: Some(metadata_summary),
            api_name: Some(api_name.into()),
            file_io_performed: true,
            data_io_performed: false,
            object_store_io_performed: false,
            write_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn invalid_target(request: VortexMetadataOpenRequest, reason: impl Into<String>) -> Self {
        let mut r = Self::feature_disabled(request);
        r.open_status = VortexMetadataOpenStatus::InvalidTarget;
        r.add_diagnostic(Diagnostic::invalid_input(
            "vortex-file-metadata-open",
            reason.into(),
            "Provide a URI that looks like a local .vortex path.",
        ));
        r
    }
    #[must_use]
    pub fn unsupported(
        request: VortexMetadataOpenRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut r = Self::feature_disabled(request);
        r.feature_status = VortexFileIoFeatureStatus::Unsupported;
        r.open_status = VortexMetadataOpenStatus::Unsupported;
        r.mode = VortexMetadataOpenMode::Unsupported;
        r.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            None,
        ));
        r
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.open_status.is_error()
            || self
                .diagnostics
                .iter()
                .any(|d| matches!(d.severity.as_str(), "error" | "fatal"))
    }
    #[must_use]
    pub const fn is_metadata_only(&self) -> bool {
        !self.data_io_performed && !self.object_store_io_performed && !self.write_io_performed
    }
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        self.request.is_side_effect_free()
            && !self.data_io_performed
            && !self.object_store_io_performed
            && !self.write_io_performed
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = write!(
            out,
            "Vortex file metadata-only open\nfeature status: {}\nopen status: {}\nmode: {}\ntarget URI: {}\nfile IO performed: {}\ndata IO performed: {}\nobject-store IO performed: {}\nwrite IO performed: {}\nfallback execution allowed: {}",
            self.feature_status.as_str(),
            self.open_status.as_str(),
            self.mode.as_str(),
            self.request.uri.as_str(),
            self.file_io_performed,
            self.data_io_performed,
            self.object_store_io_performed,
            self.write_io_performed,
            self.fallback_execution_allowed
        );
        if self.diagnostics.is_empty() {
            out.push_str("\ndiagnostics: none");
        } else {
            out.push_str("\ndiagnostics:");
            for d in &self.diagnostics {
                let _ = write!(out, "\n- {}", d.to_human_text());
            }
        }
        out
    }
}

#[must_use]
pub const fn vortex_file_io_feature_enabled() -> bool {
    cfg!(feature = "vortex-file-io")
}

/// Opens local `Vortex` metadata only; no scan, decode, materialization, object-store IO, or writes.
///
/// # Errors
/// Returns errors only for invalid internal construction; unsupported/deferred states are returned in the report.
pub fn open_vortex_metadata_only(
    request: VortexMetadataOpenRequest,
) -> Result<VortexMetadataOpenReport> {
    if !request.uri.looks_like_vortex() {
        return Ok(VortexMetadataOpenReport::invalid_target(
            request,
            "target URI does not look like Vortex (.vortex).",
        ));
    }
    match request.uri.scheme() {
        UriScheme::S3 | UriScheme::Gcs | UriScheme::Adls | UriScheme::Other => {
            return Ok(VortexMetadataOpenReport::unsupported(
                request,
                "vortex-file-io",
                "object-store and remote URI metadata open is out of scope for this contract.",
            ));
        }
        UriScheme::LocalPath | UriScheme::File => {}
    }
    Ok(open_local_only(request))
}

fn open_local_only(request: VortexMetadataOpenRequest) -> VortexMetadataOpenReport {
    #[cfg(not(feature = "vortex-file-io"))]
    {
        VortexMetadataOpenReport::feature_disabled(request)
    }
    #[cfg(feature = "vortex-file-io")]
    {
        use std::path::Path;
        let path = if let Some(rest) = request.uri.as_str().strip_prefix("file://") {
            rest
        } else {
            request.uri.as_str()
        };
        if !Path::new(path).exists() {
            let mut report = VortexMetadataOpenReport::feature_disabled(request);
            report.feature_status = VortexFileIoFeatureStatus::Enabled;
            report.open_status = VortexMetadataOpenStatus::FileMissing;
            report.add_diagnostic(Diagnostic::invalid_input(
                "vortex-file-metadata-open",
                "local Vortex file path does not exist",
                "Provide an existing local .vortex file path.",
            ));
            return report;
        }
        VortexMetadataOpenReport::api_deferred(
            request,
            "public metadata-only upstream Vortex file API usage is deferred until stability/behavior guarantees are clearer.",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_modes() {
        let uri = DatasetUri::new("file://tmp/test.vortex").expect("uri");
        let m = VortexMetadataOpenRequest::metadata_only(uri.clone());
        assert!(
            m.allow_file_io
                && !m.allow_data_io
                && !m.allow_object_store_io
                && !m.allow_write_io
                && m.is_side_effect_free()
        );
        let r = VortexMetadataOpenRequest::report_only(uri);
        assert!(
            !r.allow_file_io && !r.allow_data_io && !r.allow_object_store_io && !r.allow_write_io
        );
    }
    #[test]
    fn feature_check_cfg() {
        #[cfg(feature = "vortex-file-io")]
        assert!(vortex_file_io_feature_enabled());
        #[cfg(not(feature = "vortex-file-io"))]
        assert!(!vortex_file_io_feature_enabled());
    }
    #[test]
    fn report_invariants() {
        let uri = DatasetUri::new("file://tmp/test.vortex").expect("uri");
        let req = VortexMetadataOpenRequest::metadata_only(uri);
        let fd = VortexMetadataOpenReport::feature_disabled(req.clone());
        assert!(
            !fd.file_io_performed
                && !fd.data_io_performed
                && !fd.object_store_io_performed
                && !fd.write_io_performed
                && !fd.fallback_execution_allowed
        );
        let ad = VortexMetadataOpenReport::api_deferred(req.clone(), "deferred");
        assert!(
            !ad.file_io_performed
                && !ad.data_io_performed
                && !ad.object_store_io_performed
                && !ad.write_io_performed
        );
        let bad = VortexMetadataOpenReport::invalid_target(req.clone(), "bad");
        assert!(bad.has_errors());
        let unsup = VortexMetadataOpenReport::unsupported(req, "f", "r");
        assert!(unsup.has_errors());
        assert!(!unsup.diagnostics[0].fallback.attempted);
        let txt = unsup.to_human_text();
        assert!(
            txt.contains("data IO performed: false")
                && txt.contains("object-store IO performed: false")
                && txt.contains("write IO performed: false")
                && txt.contains("fallback execution allowed: false")
        );
    }
    #[test]
    fn open_non_vortex_invalid_target() {
        let req = VortexMetadataOpenRequest::metadata_only(
            DatasetUri::new("file://tmp/not.parquet").expect("uri"),
        );
        let report = open_vortex_metadata_only(req).expect("report");
        assert!(matches!(
            report.open_status,
            VortexMetadataOpenStatus::InvalidTarget
        ));
        assert!(!report.object_store_io_performed);
    }
    #[test]
    fn open_object_store_unsupported() {
        let req = VortexMetadataOpenRequest::metadata_only(
            DatasetUri::new("s3://b/a.vortex").expect("uri"),
        );
        let report = open_vortex_metadata_only(req).expect("report");
        assert!(matches!(
            report.open_status,
            VortexMetadataOpenStatus::Unsupported | VortexMetadataOpenStatus::ApiDeferred
        ));
        assert!(!report.object_store_io_performed);
    }
    #[cfg(not(feature = "vortex-file-io"))]
    #[test]
    fn open_vortex_with_feature_disabled() {
        let req = VortexMetadataOpenRequest::metadata_only(
            DatasetUri::new("file://tmp/a.vortex").expect("uri"),
        );
        let report = open_vortex_metadata_only(req).expect("report");
        assert_eq!(
            report.open_status,
            VortexMetadataOpenStatus::FeatureDisabled
        );
    }
    #[cfg(feature = "vortex-file-io")]
    #[test]
    fn open_missing_local_vortex_safe() {
        let req = VortexMetadataOpenRequest::metadata_only(
            DatasetUri::new("file:///tmp/does-not-exist-shardloom.vortex").expect("uri"),
        );
        let report = open_vortex_metadata_only(req).expect("report");
        assert!(matches!(
            report.open_status,
            VortexMetadataOpenStatus::FileMissing | VortexMetadataOpenStatus::ApiDeferred
        ));
        assert!(
            !report.data_io_performed
                && !report.object_store_io_performed
                && !report.write_io_performed
                && !report.fallback_execution_allowed
        );
    }
}
