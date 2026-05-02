//! Release engineering, API compatibility, and packaging planning domain.
//!
//! This module is planning/reporting only. It does not build, sign, publish,
//! or create release artifacts. Publishing always requires explicit human approval.

#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::struct_excessive_bools,
    clippy::semicolon_if_nothing_returned
)]

use crate::{Diagnostic, Result, ShardLoomError};

fn validate_non_empty(value: impl Into<String>, field: &str) -> Result<String> {
    let value = value.into();
    if value.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{field} must not be empty"
        )));
    }
    Ok(value)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub pre_release: Option<String>,
}
impl ProjectVersion {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
            pre_release: None,
        }
    }
    pub fn current_crate() -> Self {
        let raw = env!("CARGO_PKG_VERSION");
        let core = raw.split('-').next().unwrap_or("0.1.0");
        let mut it = core.split('.');
        let major = it.next().and_then(|v| v.parse().ok()).unwrap_or(0);
        let minor = it.next().and_then(|v| v.parse().ok()).unwrap_or(1);
        let patch = it.next().and_then(|v| v.parse().ok()).unwrap_or(0);
        Self::new(major, minor, patch)
    }
    pub fn with_pre_release(mut self, pre_release: impl Into<String>) -> Result<Self> {
        self.pre_release = Some(validate_non_empty(pre_release, "pre_release")?);
        Ok(self)
    }
    pub fn is_zero_series(&self) -> bool {
        self.major == 0
    }
    pub fn summary(&self) -> String {
        match &self.pre_release {
            Some(p) => format!("{}.{}.{}-{}", self.major, self.minor, self.patch, p),
            None => format!("{}.{}.{}", self.major, self.minor, self.patch),
        }
    }
}

macro_rules! as_str_enum {($name:ident{$($v:ident=>$s:literal),* $(,)?})=>{impl $name{pub const fn as_str(&self)->&'static str{match self{$(Self::$v=>$s),*}}}}}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseChannel {
    Development,
    Experimental,
    Alpha,
    Beta,
    Stable,
    LongTermSupport,
}
as_str_enum!(ReleaseChannel{Development=>"development",Experimental=>"experimental",Alpha=>"alpha",Beta=>"beta",Stable=>"stable",LongTermSupport=>"long_term_support"});
impl ReleaseChannel {
    pub const fn is_stable(&self) -> bool {
        matches!(self, Self::Stable | Self::LongTermSupport)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiStabilityTier {
    Internal,
    Experimental,
    Stable,
    Deprecated,
    Removed,
}
as_str_enum!(ApiStabilityTier{Internal=>"internal",Experimental=>"experimental",Stable=>"stable",Deprecated=>"deprecated",Removed=>"removed"});
impl ApiStabilityTier {
    pub const fn is_publicly_supported(&self) -> bool {
        matches!(self, Self::Stable | Self::Deprecated)
    }
    pub const fn allows_breaking_changes(&self) -> bool {
        matches!(self, Self::Internal | Self::Experimental)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicSurfaceKind {
    RustCrate,
    Cli,
    PythonPackage,
    TypeScriptPackage,
    DockerImage,
    GhcrImage,
    MachineReadableSchema,
    Documentation,
    BenchmarkArtifact,
    ExtensionManifest,
    PlanIrSchema,
    DiagnosticSchema,
    Unknown,
}
as_str_enum!(PublicSurfaceKind{RustCrate=>"rust_crate",Cli=>"cli",PythonPackage=>"python_package",TypeScriptPackage=>"typescript_package",DockerImage=>"docker_image",GhcrImage=>"ghcr_image",MachineReadableSchema=>"machine_readable_schema",Documentation=>"documentation",BenchmarkArtifact=>"benchmark_artifact",ExtensionManifest=>"extension_manifest",PlanIrSchema=>"plan_ir_schema",DiagnosticSchema=>"diagnostic_schema",Unknown=>"unknown"});
impl PublicSurfaceKind {
    pub const fn is_machine_readable(&self) -> bool {
        matches!(
            self,
            Self::MachineReadableSchema
                | Self::ExtensionManifest
                | Self::PlanIrSchema
                | Self::DiagnosticSchema
        )
    }
    pub const fn is_package_artifact(&self) -> bool {
        matches!(
            self,
            Self::RustCrate
                | Self::PythonPackage
                | Self::TypeScriptPackage
                | Self::DockerImage
                | Self::GhcrImage
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicSurface {
    pub kind: PublicSurfaceKind,
    pub name: String,
    pub stability: ApiStabilityTier,
    pub version: ProjectVersion,
}
impl PublicSurface {
    pub fn new(
        kind: PublicSurfaceKind,
        name: impl Into<String>,
        stability: ApiStabilityTier,
        version: ProjectVersion,
    ) -> Result<Self> {
        Ok(Self {
            kind,
            name: validate_non_empty(name, "public surface name")?,
            stability,
            version,
        })
    }
    pub const fn is_stable(&self) -> bool {
        matches!(
            self.stability,
            ApiStabilityTier::Stable | ApiStabilityTier::Deprecated
        )
    }
    pub const fn allows_breaking_changes(&self) -> bool {
        self.stability.allows_breaking_changes()
    }
    pub fn summary(&self) -> String {
        format!(
            "{} {} ({}, v{})",
            self.kind.as_str(),
            self.name,
            self.stability.as_str(),
            self.version.summary()
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaStability {
    Unversioned,
    ExperimentalVersioned,
    StableVersioned,
    Deprecated,
}
as_str_enum!(SchemaStability{Unversioned=>"unversioned",ExperimentalVersioned=>"experimental_versioned",StableVersioned=>"stable_versioned",Deprecated=>"deprecated"});
impl SchemaStability {
    pub const fn is_versioned(&self) -> bool {
        matches!(self, Self::ExperimentalVersioned | Self::StableVersioned)
    }
    pub const fn is_stable(&self) -> bool {
        matches!(self, Self::StableVersioned)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MachineReadableSchemaKind {
    Diagnostics,
    Capabilities,
    ExplainReport,
    EstimateReport,
    DoctorReport,
    TranslationReport,
    BenchmarkReport,
    ExtensionManifest,
    PlanIr,
    OutputEnvelope,
    Unknown,
}
as_str_enum!(MachineReadableSchemaKind{Diagnostics=>"diagnostics",Capabilities=>"capabilities",ExplainReport=>"explain_report",EstimateReport=>"estimate_report",DoctorReport=>"doctor_report",TranslationReport=>"translation_report",BenchmarkReport=>"benchmark_report",ExtensionManifest=>"extension_manifest",PlanIr=>"plan_ir",OutputEnvelope=>"output_envelope",Unknown=>"unknown"});
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaCompatibilityPlan {
    pub kind: MachineReadableSchemaKind,
    pub stability: SchemaStability,
    pub version: Option<ProjectVersion>,
    pub breaking_change_allowed: bool,
}
impl SchemaCompatibilityPlan {
    pub fn experimental(kind: MachineReadableSchemaKind) -> Self {
        Self {
            kind,
            stability: SchemaStability::ExperimentalVersioned,
            version: None,
            breaking_change_allowed: true,
        }
    }
    pub fn stable(kind: MachineReadableSchemaKind, version: ProjectVersion) -> Self {
        Self {
            kind,
            stability: SchemaStability::StableVersioned,
            version: Some(version),
            breaking_change_allowed: false,
        }
    }
    pub const fn allows_breaking_change(&self) -> bool {
        self.breaking_change_allowed
    }
    pub fn summary(&self) -> String {
        format!(
            "{} {} breaking_change_allowed={}",
            self.kind.as_str(),
            self.stability.as_str(),
            self.breaking_change_allowed
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageTargetKind {
    CratesIo,
    PyPi,
    Npm,
    DockerHub,
    Ghcr,
    GitHubRelease,
    DocumentationSite,
    LocalArtifact,
    Unknown,
}
as_str_enum!(PackageTargetKind{CratesIo=>"crates_io",PyPi=>"pypi",Npm=>"npm",DockerHub=>"docker_hub",Ghcr=>"ghcr",GitHubRelease=>"github_release",DocumentationSite=>"documentation_site",LocalArtifact=>"local_artifact",Unknown=>"unknown"});
impl PackageTargetKind {
    pub const fn requires_external_publish(&self) -> bool {
        matches!(
            self,
            Self::CratesIo
                | Self::PyPi
                | Self::Npm
                | Self::DockerHub
                | Self::Ghcr
                | Self::GitHubRelease
                | Self::DocumentationSite
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageTarget {
    pub kind: PackageTargetKind,
    pub name: String,
    pub enabled: bool,
    pub publish_allowed: bool,
}
impl PackageTarget {
    pub fn planned(kind: PackageTargetKind, name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            kind,
            name: validate_non_empty(name, "package target name")?,
            enabled: true,
            publish_allowed: false,
        })
    }
    pub fn disabled(kind: PackageTargetKind, name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            kind,
            name: validate_non_empty(name, "package target name")?,
            enabled: false,
            publish_allowed: false,
        })
    }
    pub fn allow_publish(mut self, value: bool) -> Self {
        self.publish_allowed = value;
        self
    }
    pub const fn requires_human_approval(&self) -> bool {
        self.kind.requires_external_publish()
    }
    pub fn summary(&self) -> String {
        format!(
            "{} {} enabled={} publish_allowed={}",
            self.kind.as_str(),
            self.name,
            self.enabled,
            self.publish_allowed
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseArtifactKind {
    RustCrate,
    PythonWheel,
    SourceTarball,
    ContainerImage,
    CliBinary,
    Documentation,
    BenchmarkReport,
    Sbom,
    Checksum,
    Signature,
    Unknown,
}
as_str_enum!(ReleaseArtifactKind{RustCrate=>"rust_crate",PythonWheel=>"python_wheel",SourceTarball=>"source_tarball",ContainerImage=>"container_image",CliBinary=>"cli_binary",Documentation=>"documentation",BenchmarkReport=>"benchmark_report",Sbom=>"sbom",Checksum=>"checksum",Signature=>"signature",Unknown=>"unknown"});
impl ReleaseArtifactKind {
    pub const fn is_supply_chain_artifact(&self) -> bool {
        matches!(self, Self::Sbom | Self::Checksum | Self::Signature)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseArtifactPlan {
    pub kind: ReleaseArtifactKind,
    pub name: String,
    pub target: Option<PackageTargetKind>,
    pub built: bool,
    pub published: bool,
}
impl ReleaseArtifactPlan {
    pub fn planned(kind: ReleaseArtifactKind, name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            kind,
            name: validate_non_empty(name, "artifact name")?,
            target: None,
            built: false,
            published: false,
        })
    }
    pub fn with_target(mut self, target: PackageTargetKind) -> Self {
        self.target = Some(target);
        self
    }
    pub fn mark_built(mut self, built: bool) -> Self {
        self.built = built;
        self
    }
    pub fn mark_published(mut self, published: bool) -> Self {
        self.published = published;
        self
    }
    pub fn summary(&self) -> String {
        format!(
            "{} {} built={} published={}",
            self.kind.as_str(),
            self.name,
            self.built,
            self.published
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyLicenseClass {
    Apache2,
    Mit,
    Bsd,
    Isc,
    Zlib,
    UnicodeLike,
    Mpl2ReviewRequired,
    UnknownReviewRequired,
    Incompatible,
}
as_str_enum!(DependencyLicenseClass{Apache2=>"apache2",Mit=>"mit",Bsd=>"bsd",Isc=>"isc",Zlib=>"zlib",UnicodeLike=>"unicode_like",Mpl2ReviewRequired=>"mpl2_review_required",UnknownReviewRequired=>"unknown_review_required",Incompatible=>"incompatible"});
impl DependencyLicenseClass {
    pub const fn is_apache_compatible_candidate(&self) -> bool {
        matches!(
            self,
            Self::Apache2 | Self::Mit | Self::Bsd | Self::Isc | Self::Zlib | Self::UnicodeLike
        )
    }
    pub const fn requires_review(&self) -> bool {
        matches!(
            self,
            Self::Mpl2ReviewRequired | Self::UnknownReviewRequired | Self::Incompatible
        )
    }
    pub const fn is_incompatible(&self) -> bool {
        matches!(self, Self::Incompatible)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyReviewStatus {
    NotNeeded,
    Pending,
    Approved,
    Rejected,
    RequiresLegalReview,
    Unknown,
}
as_str_enum!(DependencyReviewStatus{NotNeeded=>"not_needed",Pending=>"pending",Approved=>"approved",Rejected=>"rejected",RequiresLegalReview=>"requires_legal_review",Unknown=>"unknown"});
impl DependencyReviewStatus {
    pub const fn is_blocking(&self) -> bool {
        matches!(
            self,
            Self::Rejected | Self::RequiresLegalReview | Self::Pending | Self::Unknown
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyReview {
    pub name: String,
    pub license: DependencyLicenseClass,
    pub status: DependencyReviewStatus,
    pub notes: Option<String>,
}
impl DependencyReview {
    pub fn new(name: impl Into<String>, license: DependencyLicenseClass) -> Result<Self> {
        Ok(Self {
            name: validate_non_empty(name, "dependency name")?,
            license,
            status: DependencyReviewStatus::Pending,
            notes: None,
        })
    }
    pub fn with_status(mut self, status: DependencyReviewStatus) -> Self {
        self.status = status;
        self
    }
    pub fn with_notes(mut self, notes: impl Into<String>) -> Self {
        self.notes = Some(notes.into());
        self
    }
    pub const fn is_blocking(&self) -> bool {
        self.status.is_blocking() || self.license.is_incompatible()
    }
    pub fn summary(&self) -> String {
        format!(
            "{} {} {}",
            self.name,
            self.license.as_str(),
            self.status.as_str()
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoFallbackReleaseCheck {
    pub spark_dependency_present: bool,
    pub datafusion_dependency_present: bool,
    pub duckdb_polars_velox_fallback_present: bool,
    pub fallback_execution_allowed: bool,
    pub docs_imply_fallback: bool,
}
impl NoFallbackReleaseCheck {
    pub const fn clean() -> Self {
        Self {
            spark_dependency_present: false,
            datafusion_dependency_present: false,
            duckdb_polars_velox_fallback_present: false,
            fallback_execution_allowed: false,
            docs_imply_fallback: false,
        }
    }
    pub const fn is_clean(&self) -> bool {
        !self.spark_dependency_present
            && !self.datafusion_dependency_present
            && !self.duckdb_polars_velox_fallback_present
            && !self.fallback_execution_allowed
            && !self.docs_imply_fallback
    }
    pub fn summary(&self) -> String {
        if self.is_clean() {
            "no-fallback check clean; fallback execution disabled".to_string()
        } else {
            "no-fallback check has policy violations".to_string()
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseChecklistItemKind {
    TestsPass,
    FormattingPasses,
    ClippyPasses,
    DocsUpdated,
    LicenseMetadataCorrect,
    NoticeUpdated,
    DependencyLicensesReviewed,
    SecurityReview,
    NoFallbackDependency,
    VersionBumped,
    ReleaseNotesWritten,
    BenchmarkClaimsVerified,
    PackagesBuilt,
    ChecksumsGenerated,
    ArtifactsSigned,
    HumanApproval,
    Unknown,
}
as_str_enum!(ReleaseChecklistItemKind{TestsPass=>"tests_pass",FormattingPasses=>"formatting_passes",ClippyPasses=>"clippy_passes",DocsUpdated=>"docs_updated",LicenseMetadataCorrect=>"license_metadata_correct",NoticeUpdated=>"notice_updated",DependencyLicensesReviewed=>"dependency_licenses_reviewed",SecurityReview=>"security_review",NoFallbackDependency=>"no_fallback_dependency",VersionBumped=>"version_bumped",ReleaseNotesWritten=>"release_notes_written",BenchmarkClaimsVerified=>"benchmark_claims_verified",PackagesBuilt=>"packages_built",ChecksumsGenerated=>"checksums_generated",ArtifactsSigned=>"artifacts_signed",HumanApproval=>"human_approval",Unknown=>"unknown"});
impl ReleaseChecklistItemKind {
    pub const fn is_required_before_publish(&self) -> bool {
        matches!(
            self,
            Self::HumanApproval
                | Self::TestsPass
                | Self::FormattingPasses
                | Self::ClippyPasses
                | Self::LicenseMetadataCorrect
                | Self::DependencyLicensesReviewed
                | Self::NoFallbackDependency
                | Self::VersionBumped
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecklistStatus {
    NotStarted,
    Passed,
    Failed,
    Waived,
    NotApplicable,
}
as_str_enum!(ChecklistStatus{NotStarted=>"not_started",Passed=>"passed",Failed=>"failed",Waived=>"waived",NotApplicable=>"not_applicable"});
impl ChecklistStatus {
    pub const fn is_blocking(&self) -> bool {
        matches!(self, Self::NotStarted | Self::Failed)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseChecklistItem {
    pub kind: ReleaseChecklistItemKind,
    pub status: ChecklistStatus,
    pub notes: Option<String>,
}
impl ReleaseChecklistItem {
    pub const fn new(kind: ReleaseChecklistItemKind) -> Self {
        Self {
            kind,
            status: ChecklistStatus::NotStarted,
            notes: None,
        }
    }
    pub fn with_status(mut self, status: ChecklistStatus) -> Self {
        self.status = status;
        self
    }
    pub fn with_notes(mut self, notes: impl Into<String>) -> Self {
        self.notes = Some(notes.into());
        self
    }
    pub const fn is_blocking(&self) -> bool {
        self.kind.is_required_before_publish() && self.status.is_blocking()
    }
    pub fn summary(&self) -> String {
        format!("{} {}", self.kind.as_str(), self.status.as_str())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseReadinessStatus {
    Draft,
    Blocked,
    ReadyForReview,
    ReadyForRelease,
    Released,
    Unsupported,
}
as_str_enum!(ReleaseReadinessStatus{Draft=>"draft",Blocked=>"blocked",ReadyForReview=>"ready_for_review",ReadyForRelease=>"ready_for_release",Released=>"released",Unsupported=>"unsupported"});
impl ReleaseReadinessStatus {
    pub const fn allows_publish(&self) -> bool {
        matches!(self, Self::ReadyForRelease)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReleasePlan {
    pub version: ProjectVersion,
    pub channel: ReleaseChannel,
    pub readiness: ReleaseReadinessStatus,
    pub public_surfaces: Vec<PublicSurface>,
    pub schemas: Vec<SchemaCompatibilityPlan>,
    pub package_targets: Vec<PackageTarget>,
    pub artifacts: Vec<ReleaseArtifactPlan>,
    pub dependency_reviews: Vec<DependencyReview>,
    pub no_fallback_check: NoFallbackReleaseCheck,
    pub checklist: Vec<ReleaseChecklistItem>,
    pub diagnostics: Vec<Diagnostic>,
}
impl ReleasePlan {
    pub fn draft(version: ProjectVersion) -> Self {
        Self {
            version,
            channel: ReleaseChannel::Experimental,
            readiness: ReleaseReadinessStatus::Draft,
            public_surfaces: vec![],
            schemas: vec![],
            package_targets: vec![],
            artifacts: vec![],
            dependency_reviews: vec![],
            no_fallback_check: NoFallbackReleaseCheck::clean(),
            checklist: vec![],
            diagnostics: vec![],
        }
    }
    pub fn default_foundation_plan() -> Self {
        let version = ProjectVersion::current_crate();
        let mut p = Self::draft(version.clone());
        p.add_public_surface(
            PublicSurface::new(
                PublicSurfaceKind::Cli,
                "shardloom",
                ApiStabilityTier::Experimental,
                version.clone(),
            )
            .expect("valid"),
        );
        p.add_public_surface(
            PublicSurface::new(
                PublicSurfaceKind::RustCrate,
                "shardloom-core",
                ApiStabilityTier::Experimental,
                version,
            )
            .expect("valid"),
        );
        p.add_schema(SchemaCompatibilityPlan::experimental(
            MachineReadableSchemaKind::OutputEnvelope,
        ));
        p.add_schema(SchemaCompatibilityPlan::experimental(
            MachineReadableSchemaKind::Diagnostics,
        ));
        p.add_package_target(
            PackageTarget::planned(PackageTargetKind::CratesIo, "crates.io").expect("valid"),
        );
        p.add_package_target(
            PackageTarget::planned(PackageTargetKind::GitHubRelease, "github-releases")
                .expect("valid"),
        );
        for k in [
            ReleaseChecklistItemKind::HumanApproval,
            ReleaseChecklistItemKind::TestsPass,
            ReleaseChecklistItemKind::FormattingPasses,
            ReleaseChecklistItemKind::ClippyPasses,
            ReleaseChecklistItemKind::LicenseMetadataCorrect,
            ReleaseChecklistItemKind::DependencyLicensesReviewed,
            ReleaseChecklistItemKind::NoFallbackDependency,
            ReleaseChecklistItemKind::VersionBumped,
        ] {
            p.add_checklist_item(ReleaseChecklistItem::new(k));
        }
        p
    }
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let mut p = Self::draft(ProjectVersion::current_crate());
        p.readiness = ReleaseReadinessStatus::Unsupported;
        p.add_diagnostic(Diagnostic::unsupported(
            crate::DiagnosticCode::NotImplemented,
            feature,
            "Release planning feature unsupported",
            Some(reason.into()),
        ));
        p
    }
    pub fn add_public_surface(&mut self, s: PublicSurface) {
        self.public_surfaces.push(s)
    }
    pub fn add_schema(&mut self, s: SchemaCompatibilityPlan) {
        self.schemas.push(s)
    }
    pub fn add_package_target(&mut self, t: PackageTarget) {
        self.package_targets.push(t)
    }
    pub fn add_artifact(&mut self, a: ReleaseArtifactPlan) {
        self.artifacts.push(a)
    }
    pub fn add_dependency_review(&mut self, r: DependencyReview) {
        self.dependency_reviews.push(r)
    }
    pub fn add_checklist_item(&mut self, i: ReleaseChecklistItem) {
        self.checklist.push(i)
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d)
    }
    pub fn has_blockers(&self) -> bool {
        self.checklist.iter().any(ReleaseChecklistItem::is_blocking)
            || self
                .dependency_reviews
                .iter()
                .any(DependencyReview::is_blocking)
            || !self.no_fallback_check.is_clean()
    }
    pub fn publish_allowed(&self) -> bool {
        self.readiness.allows_publish()
            && !self.has_blockers()
            && !self.has_errors()
            && self
                .package_targets
                .iter()
                .filter(|t| t.enabled && t.kind.requires_external_publish())
                .all(|t| t.publish_allowed)
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                crate::DiagnosticSeverity::Error | crate::DiagnosticSeverity::Fatal
            )
        })
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "release plan\nversion: {}\nchannel: {}\nreadiness: {}\npublish allowed: {}\nfallback execution disabled: {}\nblockers: {}\npackage targets: {}\ndiagnostics: {}",
            self.version.summary(),
            self.channel.as_str(),
            self.readiness.as_str(),
            self.publish_allowed(),
            !self.no_fallback_check.fallback_execution_allowed,
            self.has_blockers(),
            self.package_targets
                .iter()
                .map(PackageTarget::summary)
                .collect::<Vec<_>>()
                .join(", "),
            self.diagnostics.len()
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseReport {
    pub plan: ReleasePlan,
    pub published: bool,
    pub artifacts_published: usize,
    pub diagnostics: Vec<Diagnostic>,
    pub notes: Vec<String>,
}
impl ReleaseReport {
    pub fn not_published(plan: ReleasePlan) -> Self {
        Self {
            plan,
            published: false,
            artifacts_published: 0,
            diagnostics: vec![],
            notes: vec![],
        }
    }
    pub fn from_plan(plan: ReleasePlan) -> Self {
        Self::not_published(plan)
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d)
    }
    pub fn add_note(&mut self, n: impl Into<String>) {
        self.notes.push(n.into())
    }
    pub fn has_errors(&self) -> bool {
        self.plan.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    crate::DiagnosticSeverity::Error | crate::DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "{}\npublished: {}\nartifacts_published: {}\nfallback execution disabled: {}\nno publish occurred",
            self.plan.to_human_text(),
            self.published,
            self.artifacts_published,
            !self.plan.no_fallback_check.fallback_execution_allowed
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn t1() {
        assert!(ProjectVersion::new(0, 1, 0).with_pre_release(" ").is_err());
        assert!(ProjectVersion::new(0, 1, 0).is_zero_series());
        assert!(ReleaseChannel::Stable.is_stable());
        assert!(!ApiStabilityTier::Stable.allows_breaking_changes());
        assert!(ApiStabilityTier::Experimental.allows_breaking_changes());
        assert!(
            PublicSurface::new(
                PublicSurfaceKind::Cli,
                "",
                ApiStabilityTier::Experimental,
                ProjectVersion::new(0, 1, 0)
            )
            .is_err()
        );
        assert!(PublicSurfaceKind::DiagnosticSchema.is_machine_readable());
        assert!(
            SchemaStability::StableVersioned.is_versioned()
                && SchemaStability::StableVersioned.is_stable()
        );
        assert!(
            SchemaCompatibilityPlan::experimental(MachineReadableSchemaKind::Diagnostics)
                .allows_breaking_change()
        );
        assert!(
            !SchemaCompatibilityPlan::stable(
                MachineReadableSchemaKind::Diagnostics,
                ProjectVersion::new(1, 0, 0)
            )
            .allows_breaking_change()
        );
        assert!(
            !PackageTarget::planned(PackageTargetKind::CratesIo, "c")
                .unwrap()
                .publish_allowed
        );
        assert!(
            PackageTarget::planned(PackageTargetKind::CratesIo, "c")
                .unwrap()
                .requires_human_approval()
        );
        assert!(ReleaseArtifactPlan::planned(ReleaseArtifactKind::RustCrate, " ").is_err());
        assert!(DependencyLicenseClass::Apache2.is_apache_compatible_candidate());
        assert!(DependencyLicenseClass::Mpl2ReviewRequired.requires_review());
        assert!(DependencyReview::new("", DependencyLicenseClass::Mit).is_err());
        assert!(
            DependencyReview::new("x", DependencyLicenseClass::Mit)
                .unwrap()
                .with_status(DependencyReviewStatus::Pending)
                .is_blocking()
        );
        assert!(NoFallbackReleaseCheck::clean().is_clean());
        assert!(
            !NoFallbackReleaseCheck {
                spark_dependency_present: true,
                ..NoFallbackReleaseCheck::clean()
            }
            .is_clean()
        );
        assert!(ReleaseChecklistItemKind::HumanApproval.is_required_before_publish());
        assert!(ChecklistStatus::NotStarted.is_blocking());
        let p = ReleasePlan::default_foundation_plan();
        assert!(p.has_blockers());
        assert!(!p.publish_allowed());
        assert!(p.no_fallback_check.is_clean());
        let u = ReleasePlan::unsupported("x", "y");
        assert!(u.has_errors());
        assert!(!u.diagnostics[0].fallback.attempted);
        assert!(p.to_human_text().contains("fallback execution disabled"));
        let r = ReleaseReport::from_plan(p);
        assert!(!r.published && r.artifacts_published == 0);
        assert!(r.to_human_text().contains("no publish occurred"));
    }

    #[test]
    fn dependency_review_incompatible_license_is_always_blocking() {
        let review = DependencyReview::new("bad-dep", DependencyLicenseClass::Incompatible)
            .expect("dependency should be created")
            .with_status(DependencyReviewStatus::Approved);
        assert!(review.is_blocking());
    }

    #[test]
    fn publish_allowed_ignores_disabled_external_targets() {
        let mut plan = ReleasePlan::draft(ProjectVersion::new(1, 0, 0));
        plan.readiness = ReleaseReadinessStatus::ReadyForRelease;
        plan.add_package_target(
            PackageTarget::planned(PackageTargetKind::CratesIo, "crates")
                .expect("valid target")
                .allow_publish(true),
        );
        plan.add_package_target(
            PackageTarget::disabled(PackageTargetKind::GitHubRelease, "gh-release")
                .expect("valid target"),
        );
        plan.add_checklist_item(
            ReleaseChecklistItem::new(ReleaseChecklistItemKind::HumanApproval)
                .with_status(ChecklistStatus::Passed),
        );
        plan.add_checklist_item(
            ReleaseChecklistItem::new(ReleaseChecklistItemKind::TestsPass)
                .with_status(ChecklistStatus::Passed),
        );
        plan.add_checklist_item(
            ReleaseChecklistItem::new(ReleaseChecklistItemKind::FormattingPasses)
                .with_status(ChecklistStatus::Passed),
        );
        plan.add_checklist_item(
            ReleaseChecklistItem::new(ReleaseChecklistItemKind::ClippyPasses)
                .with_status(ChecklistStatus::Passed),
        );
        plan.add_checklist_item(
            ReleaseChecklistItem::new(ReleaseChecklistItemKind::LicenseMetadataCorrect)
                .with_status(ChecklistStatus::Passed),
        );
        plan.add_checklist_item(
            ReleaseChecklistItem::new(ReleaseChecklistItemKind::DependencyLicensesReviewed)
                .with_status(ChecklistStatus::Passed),
        );
        plan.add_checklist_item(
            ReleaseChecklistItem::new(ReleaseChecklistItemKind::NoFallbackDependency)
                .with_status(ChecklistStatus::Passed),
        );
        plan.add_checklist_item(
            ReleaseChecklistItem::new(ReleaseChecklistItemKind::VersionBumped)
                .with_status(ChecklistStatus::Passed),
        );

        assert!(plan.publish_allowed());
    }
}
