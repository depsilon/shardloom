#![allow(clippy::must_use_candidate, clippy::return_self_not_must_use)]

use std::fmt::Write as _;

use shardloom_core::{Diagnostic, DiagnosticSeverity, Result, ShardLoomError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadApiArea {
    FileOpen,
    FileMetadata,
    DType,
    Layout,
    Statistics,
    ScanSetup,
    ProjectionPushdown,
    PredicatePushdown,
    SplitPlanning,
    EncodedArrayAccess,
    DataRead,
    Decode,
    Materialization,
    ArrowInterop,
    ObjectStore,
    Write,
    Unknown,
}
impl VortexEncodedReadApiArea {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FileOpen => "file_open",
            Self::FileMetadata => "file_metadata",
            Self::DType => "dtype",
            Self::Layout => "layout",
            Self::Statistics => "statistics",
            Self::ScanSetup => "scan_setup",
            Self::ProjectionPushdown => "projection_pushdown",
            Self::PredicatePushdown => "predicate_pushdown",
            Self::SplitPlanning => "split_planning",
            Self::EncodedArrayAccess => "encoded_array_access",
            Self::DataRead => "data_read",
            Self::Decode => "decode",
            Self::Materialization => "materialization",
            Self::ArrowInterop => "arrow_interop",
            Self::ObjectStore => "object_store",
            Self::Write => "write",
            Self::Unknown => "unknown",
        }
    }
    pub const fn is_execution_related(&self) -> bool {
        matches!(
            self,
            Self::ScanSetup
                | Self::ProjectionPushdown
                | Self::PredicatePushdown
                | Self::SplitPlanning
                | Self::EncodedArrayAccess
                | Self::DataRead
                | Self::Decode
                | Self::Materialization
        )
    }
    pub const fn is_forbidden_for_now(&self) -> bool {
        matches!(
            self,
            Self::DataRead | Self::Decode | Self::Materialization | Self::ObjectStore | Self::Write
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadApiStatus {
    ConfirmedPublic,
    ConfirmedPublicButDeferred,
    Planned,
    ApiUnclear,
    ApiUnstable,
    Unsupported,
    ForbiddenForNow,
}
impl VortexEncodedReadApiStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ConfirmedPublic => "confirmed_public",
            Self::ConfirmedPublicButDeferred => "confirmed_public_but_deferred",
            Self::Planned => "planned",
            Self::ApiUnclear => "api_unclear",
            Self::ApiUnstable => "api_unstable",
            Self::Unsupported => "unsupported",
            Self::ForbiddenForNow => "forbidden_for_now",
        }
    }
    pub const fn is_usable_for_contract(&self) -> bool {
        matches!(self, Self::ConfirmedPublic)
    }
    pub const fn is_usable_for_execution(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadApiRisk {
    None,
    ApiInstability,
    DataRead,
    Decode,
    Materialization,
    ArrowDefaultPath,
    ObjectStoreIo,
    WriteIo,
    FallbackEngine,
    Unknown,
}
impl VortexEncodedReadApiRisk {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ApiInstability => "api_instability",
            Self::DataRead => "data_read",
            Self::Decode => "decode",
            Self::Materialization => "materialization",
            Self::ArrowDefaultPath => "arrow_default_path",
            Self::ObjectStoreIo => "object_store_io",
            Self::WriteIo => "write_io",
            Self::FallbackEngine => "fallback_engine",
            Self::Unknown => "unknown",
        }
    }
    pub const fn is_blocking(&self) -> bool {
        matches!(
            self,
            Self::DataRead
                | Self::Decode
                | Self::Materialization
                | Self::ArrowDefaultPath
                | Self::ObjectStoreIo
                | Self::WriteIo
                | Self::FallbackEngine
                | Self::Unknown
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexEncodedReadApiItem {
    pub area: VortexEncodedReadApiArea,
    pub name: String,
    pub status: VortexEncodedReadApiStatus,
    pub risk: VortexEncodedReadApiRisk,
    pub notes: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadApiItem {
    /// # Errors
    /// Returns an error when `name` is empty or only whitespace.
    pub fn new(
        area: VortexEncodedReadApiArea,
        name: impl Into<String>,
        status: VortexEncodedReadApiStatus,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "encoded-read API item name must be non-empty".to_string(),
            ));
        }
        Ok(Self {
            area,
            name,
            status,
            risk: VortexEncodedReadApiRisk::None,
            notes: None,
            diagnostics: vec![],
        })
    }
    pub fn with_risk(mut self, risk: VortexEncodedReadApiRisk) -> Self {
        self.risk = risk;
        self
    }
    pub fn with_notes(mut self, notes: impl Into<String>) -> Self {
        self.notes = Some(notes.into());
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub const fn is_contract_usable(&self) -> bool {
        self.status.is_usable_for_contract() && !self.risk.is_blocking()
    }
    pub const fn is_execution_usable(&self) -> bool {
        false
    }
    pub const fn is_blocked(&self) -> bool {
        self.risk.is_blocking()
            || matches!(
                self.status,
                VortexEncodedReadApiStatus::ForbiddenForNow
                    | VortexEncodedReadApiStatus::Unsupported
            )
            || self.area.is_forbidden_for_now()
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    pub fn summary(&self) -> String {
        format!(
            "area={} name={} status={} risk={} contract_usable={} execution_usable={}",
            self.area.as_str(),
            self.name,
            self.status.as_str(),
            self.risk.as_str(),
            self.is_contract_usable(),
            self.is_execution_usable()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadApiBoundaryStatus {
    ContractReady,
    ContractPartiallyReady,
    DeferredApiUnclear,
    BlockedByRisk,
    Unsupported,
}
impl VortexEncodedReadApiBoundaryStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ContractReady => "contract_ready",
            Self::ContractPartiallyReady => "contract_partially_ready",
            Self::DeferredApiUnclear => "deferred_api_unclear",
            Self::BlockedByRisk => "blocked_by_risk",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::BlockedByRisk | Self::Unsupported)
    }
    pub const fn allows_future_probe(&self) -> bool {
        matches!(self, Self::ContractReady | Self::ContractPartiallyReady)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexEncodedReadApiBoundaryReport {
    pub status: VortexEncodedReadApiBoundaryStatus,
    pub items: Vec<VortexEncodedReadApiItem>,
    pub contract_usable_count: usize,
    pub execution_usable_count: usize,
    pub blocked_count: usize,
    pub data_read_api_count: usize,
    pub decode_api_count: usize,
    pub materialization_api_count: usize,
    pub arrow_default_risk_count: usize,
    pub object_store_api_count: usize,
    pub write_api_count: usize,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadApiBoundaryReport {
    pub fn from_items(items: Vec<VortexEncodedReadApiItem>) -> Self {
        let mut s = Self {
            status: VortexEncodedReadApiBoundaryStatus::DeferredApiUnclear,
            items,
            contract_usable_count: 0,
            execution_usable_count: 0,
            blocked_count: 0,
            data_read_api_count: 0,
            decode_api_count: 0,
            materialization_api_count: 0,
            arrow_default_risk_count: 0,
            object_store_api_count: 0,
            write_api_count: 0,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        s.recompute_counts();
        s
    }
    pub fn default_deferred() -> Self {
        Self::from_items(vec![])
    }
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let mut s = Self::default_deferred();
        s.status = VortexEncodedReadApiBoundaryStatus::Unsupported;
        s.add_diagnostic(Diagnostic::unsupported(
            shardloom_core::DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    pub fn add_item(&mut self, item: VortexEncodedReadApiItem) {
        self.items.push(item);
        self.recompute_counts();
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn recompute_counts(&mut self) {
        self.contract_usable_count = 0;
        self.execution_usable_count = 0;
        self.blocked_count = 0;
        self.data_read_api_count = 0;
        self.decode_api_count = 0;
        self.materialization_api_count = 0;
        self.arrow_default_risk_count = 0;
        self.object_store_api_count = 0;
        self.write_api_count = 0;
        for i in &self.items {
            if i.is_contract_usable() {
                self.contract_usable_count += 1;
            }
            if i.is_execution_usable() {
                self.execution_usable_count += 1;
            }
            if i.is_blocked() {
                self.blocked_count += 1;
            }
            if i.area == VortexEncodedReadApiArea::DataRead {
                self.data_read_api_count += 1;
            }
            if i.area == VortexEncodedReadApiArea::Decode {
                self.decode_api_count += 1;
            }
            if i.area == VortexEncodedReadApiArea::Materialization {
                self.materialization_api_count += 1;
            }
            if i.area == VortexEncodedReadApiArea::ObjectStore {
                self.object_store_api_count += 1;
            }
            if i.area == VortexEncodedReadApiArea::Write {
                self.write_api_count += 1;
            }
            if i.risk == VortexEncodedReadApiRisk::ArrowDefaultPath {
                self.arrow_default_risk_count += 1;
            }
        }
        self.status = if self.blocked_count > 0 {
            VortexEncodedReadApiBoundaryStatus::BlockedByRisk
        } else if self.items.is_empty() {
            VortexEncodedReadApiBoundaryStatus::DeferredApiUnclear
        } else if self.contract_usable_count == self.items.len() {
            VortexEncodedReadApiBoundaryStatus::ContractReady
        } else {
            VortexEncodedReadApiBoundaryStatus::ContractPartiallyReady
        };
        self.execution_usable_count = 0;
        self.fallback_execution_allowed = false;
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(&mut out, "boundary status: {}", self.status.as_str());
        let _ = writeln!(
            &mut out,
            "contract usable count: {}",
            self.contract_usable_count
        );
        let _ = writeln!(
            &mut out,
            "execution usable count: {}",
            self.execution_usable_count
        );
        let _ = writeln!(&mut out, "blocked count: {}", self.blocked_count);
        let _ = writeln!(
            &mut out,
            "data-read API count: {}",
            self.data_read_api_count
        );
        let _ = writeln!(&mut out, "decode API count: {}", self.decode_api_count);
        let _ = writeln!(
            &mut out,
            "materialization API count: {}",
            self.materialization_api_count
        );
        let _ = writeln!(
            &mut out,
            "object-store API count: {}",
            self.object_store_api_count
        );
        let _ = writeln!(&mut out, "write API count: {}", self.write_api_count);
        let _ = writeln!(
            &mut out,
            "Arrow-default risk count: {}",
            self.arrow_default_risk_count
        );
        let _ = writeln!(
            &mut out,
            "fallback execution disabled: {}",
            !self.fallback_execution_allowed
        );
        if !self.diagnostics.is_empty() {
            let _ = writeln!(&mut out, "diagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(&mut out, "- {} {}", d.code.as_str(), d.message);
            }
        }
        out
    }
}

pub fn vortex_encoded_read_public_api_boundary() -> VortexEncodedReadApiBoundaryReport {
    let mut report = VortexEncodedReadApiBoundaryReport::default_deferred();
    let candidates = [
        VortexEncodedReadApiItem::new(
            VortexEncodedReadApiArea::FileOpen,
            "VortexOpenOptions",
            VortexEncodedReadApiStatus::ConfirmedPublicButDeferred,
        )
        .map(|item| item.with_risk(VortexEncodedReadApiRisk::ApiInstability)),
        VortexEncodedReadApiItem::new(
            VortexEncodedReadApiArea::ScanSetup,
            "OpenOptionsSessionExt",
            VortexEncodedReadApiStatus::ConfirmedPublicButDeferred,
        ),
        VortexEncodedReadApiItem::new(
            VortexEncodedReadApiArea::FileMetadata,
            "VortexFile::footer",
            VortexEncodedReadApiStatus::ConfirmedPublic,
        ),
        VortexEncodedReadApiItem::new(
            VortexEncodedReadApiArea::DType,
            "row_count/dtype metadata surfaces",
            VortexEncodedReadApiStatus::ConfirmedPublicButDeferred,
        ),
        VortexEncodedReadApiItem::new(
            VortexEncodedReadApiArea::DataRead,
            "scan/start-read APIs",
            VortexEncodedReadApiStatus::ForbiddenForNow,
        )
        .map(|item| item.with_risk(VortexEncodedReadApiRisk::DataRead)),
        VortexEncodedReadApiItem::new(
            VortexEncodedReadApiArea::ArrowInterop,
            "Arrow conversion/interoperability APIs",
            VortexEncodedReadApiStatus::ForbiddenForNow,
        )
        .map(|item| item.with_risk(VortexEncodedReadApiRisk::ArrowDefaultPath)),
    ];
    for item in candidates.into_iter().flatten() {
        report.add_item(item);
    }
    report
}

pub fn vortex_encoded_read_api_allows_future_probe(
    report: &VortexEncodedReadApiBoundaryReport,
) -> bool {
    report.status.allows_future_probe() && report.blocked_count == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn area_forbidden() {
        assert!(VortexEncodedReadApiArea::DataRead.is_forbidden_for_now());
        assert!(VortexEncodedReadApiArea::Decode.is_forbidden_for_now());
        assert!(VortexEncodedReadApiArea::Materialization.is_forbidden_for_now());
    }
    #[test]
    fn status_contract_not_exec() {
        assert!(VortexEncodedReadApiStatus::ConfirmedPublic.is_usable_for_contract());
        assert!(!VortexEncodedReadApiStatus::ConfirmedPublic.is_usable_for_execution());
    }
    #[test]
    fn risk_blocking() {
        assert!(VortexEncodedReadApiRisk::DataRead.is_blocking());
        assert!(VortexEncodedReadApiRisk::ArrowDefaultPath.is_blocking());
    }
    #[test]
    fn item_rejects_empty() {
        assert!(
            VortexEncodedReadApiItem::new(
                VortexEncodedReadApiArea::FileOpen,
                "   ",
                VortexEncodedReadApiStatus::ConfirmedPublic
            )
            .is_err()
        );
    }
    #[test]
    fn item_blocking_risk_not_contract() {
        let i = VortexEncodedReadApiItem::new(
            VortexEncodedReadApiArea::FileOpen,
            "x",
            VortexEncodedReadApiStatus::ConfirmedPublic,
        )
        .unwrap()
        .with_risk(VortexEncodedReadApiRisk::DataRead);
        assert!(!i.is_contract_usable());
    }
    #[test]
    fn item_execution_always_false() {
        let i = VortexEncodedReadApiItem::new(
            VortexEncodedReadApiArea::FileOpen,
            "x",
            VortexEncodedReadApiStatus::ConfirmedPublic,
        )
        .unwrap();
        assert!(!i.is_execution_usable());
    }
    #[test]
    fn report_default_deferred_exec_zero() {
        let r = VortexEncodedReadApiBoundaryReport::default_deferred();
        assert_eq!(r.execution_usable_count, 0);
    }
    #[test]
    fn report_unsupported_has_errors_and_no_fallback() {
        let r = VortexEncodedReadApiBoundaryReport::unsupported("x", "y");
        assert!(r.has_errors());
        assert!(!r.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn report_recompute_counts_blocking() {
        let mut r = VortexEncodedReadApiBoundaryReport::default_deferred();
        r.add_item(
            VortexEncodedReadApiItem::new(
                VortexEncodedReadApiArea::DataRead,
                "x",
                VortexEncodedReadApiStatus::ForbiddenForNow,
            )
            .unwrap()
            .with_risk(VortexEncodedReadApiRisk::DataRead),
        );
        assert_eq!(r.blocked_count, 1);
    }
    #[test]
    fn report_text_has_fallback_disabled_and_exec_zero() {
        let r = VortexEncodedReadApiBoundaryReport::default_deferred();
        let t = r.to_human_text();
        assert!(t.contains("fallback execution disabled"));
        assert!(t.contains("execution usable count: 0"));
    }
    #[test]
    fn boundary_does_not_allow_execution() {
        let r = vortex_encoded_read_public_api_boundary();
        assert_eq!(r.execution_usable_count, 0);
    }
    #[test]
    fn probe_blocked_false() {
        let mut r = VortexEncodedReadApiBoundaryReport::default_deferred();
        r.add_item(
            VortexEncodedReadApiItem::new(
                VortexEncodedReadApiArea::DataRead,
                "x",
                VortexEncodedReadApiStatus::ForbiddenForNow,
            )
            .unwrap()
            .with_risk(VortexEncodedReadApiRisk::DataRead),
        );
        assert!(!vortex_encoded_read_api_allows_future_probe(&r));
    }
}
