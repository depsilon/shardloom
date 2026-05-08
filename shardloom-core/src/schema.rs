//! Schema evolution, catalog reference, and table compatibility planning domain skeleton.
//!
//! This module is conservative by design. It defines explicit domain types and reporting
//! structures only; no catalog access, table metadata I/O, object-store I/O, or execution
//! occurs here. External table formats are compatibility targets and never fallback engines.

#![allow(clippy::must_use_candidate, clippy::missing_errors_doc)]

use crate::{
    CredentialScope, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity,
    FallbackStatus, LogicalDType, Nullability, Result, ShardLoomError,
};

fn validate_non_empty(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} must not be empty"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaId(String);
impl SchemaId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_non_empty("schema id", &value)?;
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchemaVersion(u64);
impl SchemaVersion {
    pub fn new(value: u64) -> Result<Self> {
        if value == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "schema version must be greater than zero".to_string(),
            ));
        }
        Ok(Self(value))
    }
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
    #[must_use]
    pub const fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldId(String);
impl FieldId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_non_empty("field id", &value)?;
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldName(String);
impl FieldName {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_non_empty("field name", &value)?;
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldPath {
    pub parts: Vec<FieldName>,
}
impl FieldPath {
    pub fn new(parts: Vec<FieldName>) -> Result<Self> {
        if parts.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "field path must not be empty".to_string(),
            ));
        }
        Ok(Self { parts })
    }
    #[must_use]
    pub fn single(name: FieldName) -> Self {
        Self { parts: vec![name] }
    }
    pub fn from_dot_separated(value: &str) -> Result<Self> {
        let mut parts = Vec::new();
        for raw in value.split('.') {
            parts.push(FieldName::new(raw)?);
        }
        Self::new(parts)
    }
    #[must_use]
    pub fn depth(&self) -> usize {
        self.parts.len()
    }
    #[must_use]
    pub fn as_dot_separated(&self) -> String {
        self.parts
            .iter()
            .map(FieldName::as_str)
            .collect::<Vec<_>>()
            .join(".")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaField {
    pub id: Option<FieldId>,
    pub name: FieldName,
    pub path: FieldPath,
    pub dtype: LogicalDType,
    pub nullability: Nullability,
    pub metadata: Vec<(String, String)>,
}
impl SchemaField {
    #[must_use]
    pub fn new(name: FieldName, dtype: LogicalDType, nullability: Nullability) -> Self {
        let path = FieldPath::single(name.clone());
        Self {
            id: None,
            name,
            path,
            dtype,
            nullability,
            metadata: vec![],
        }
    }
    #[must_use]
    pub fn with_id(mut self, id: FieldId) -> Self {
        self.id = Some(id);
        self
    }
    #[must_use]
    pub fn with_path(mut self, path: FieldPath) -> Self {
        self.path = path;
        self
    }
    pub fn add_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        let key = key.into();
        validate_non_empty("metadata key", &key)?;
        self.metadata.push((key, value.into()));
        Ok(())
    }
    #[must_use]
    pub fn has_field_id(&self) -> bool {
        self.id.is_some()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "field(name={}, path={}, dtype={}, nullability={}, has_id={})",
            self.name.as_str(),
            self.path.as_dot_separated(),
            self.dtype.as_str(),
            self.nullability.as_str(),
            self.has_field_id()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaDefinition {
    pub id: SchemaId,
    pub version: SchemaVersion,
    pub fields: Vec<SchemaField>,
    pub diagnostics: Vec<Diagnostic>,
}
impl SchemaDefinition {
    #[must_use]
    pub fn new(id: SchemaId, version: SchemaVersion) -> Self {
        Self {
            id,
            version,
            fields: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_field(&mut self, field: SchemaField) {
        self.fields.push(field);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
    #[must_use]
    pub fn find_field_by_name(&self, name: &str) -> Option<&SchemaField> {
        self.fields.iter().find(|f| f.name.as_str() == name)
    }
    #[must_use]
    pub fn find_field_by_id(&self, id: &FieldId) -> Option<&SchemaField> {
        self.fields.iter().find(|f| f.id.as_ref() == Some(id))
    }
    #[must_use]
    pub fn has_field_ids(&self) -> bool {
        !self.fields.is_empty() && self.fields.iter().all(SchemaField::has_field_id)
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "schema(id={}, version={}, field_count={}, field_ids_present={})",
            self.id.as_str(),
            self.version.as_u64(),
            self.field_count(),
            self.has_field_ids()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaChangeKind {
    AddField,
    DropField,
    RenameField,
    ReorderField,
    WidenType,
    NarrowType,
    ChangeNullability,
    ChangeMetadata,
    AddNestedField,
    DropNestedField,
    RenameNestedField,
    ChangePartitioning,
    Unknown,
}
impl SchemaChangeKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::AddField => "add_field",
            Self::DropField => "drop_field",
            Self::RenameField => "rename_field",
            Self::ReorderField => "reorder_field",
            Self::WidenType => "widen_type",
            Self::NarrowType => "narrow_type",
            Self::ChangeNullability => "change_nullability",
            Self::ChangeMetadata => "change_metadata",
            Self::AddNestedField => "add_nested_field",
            Self::DropNestedField => "drop_nested_field",
            Self::RenameNestedField => "rename_nested_field",
            Self::ChangePartitioning => "change_partitioning",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_potentially_safe(&self) -> bool {
        matches!(
            self,
            Self::AddField | Self::AddNestedField | Self::ChangeMetadata | Self::WidenType
        )
    }
    #[must_use]
    pub const fn requires_field_id_for_safety(&self) -> bool {
        matches!(self, Self::RenameField | Self::RenameNestedField)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaChange {
    pub kind: SchemaChangeKind,
    pub field_path: Option<FieldPath>,
    pub reason: String,
}
impl SchemaChange {
    #[must_use]
    pub fn new(kind: SchemaChangeKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            field_path: None,
            reason: reason.into(),
        }
    }
    #[must_use]
    pub fn with_field_path(mut self, field_path: FieldPath) -> Self {
        self.field_path = Some(field_path);
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "schema_change(kind={}, field_path={}, reason={})",
            self.kind.as_str(),
            self.field_path
                .as_ref()
                .map_or("none".to_string(), FieldPath::as_dot_separated),
            self.reason
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaCompatibilityLevel {
    Exact,
    ReadCompatible,
    WriteCompatible,
    RequiresProjection,
    RequiresCast,
    RequiresDefaultValues,
    Incompatible,
    Unknown,
}
impl SchemaCompatibilityLevel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::ReadCompatible => "read_compatible",
            Self::WriteCompatible => "write_compatible",
            Self::RequiresProjection => "requires_projection",
            Self::RequiresCast => "requires_cast",
            Self::RequiresDefaultValues => "requires_default_values",
            Self::Incompatible => "incompatible",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn allows_read(&self) -> bool {
        matches!(
            self,
            Self::Exact
                | Self::ReadCompatible
                | Self::RequiresProjection
                | Self::RequiresCast
                | Self::RequiresDefaultValues
        )
    }
    #[must_use]
    pub const fn allows_write(&self) -> bool {
        matches!(self, Self::Exact | Self::WriteCompatible)
    }
    #[must_use]
    pub const fn requires_transformation(&self) -> bool {
        matches!(
            self,
            Self::RequiresProjection | Self::RequiresCast | Self::RequiresDefaultValues
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaEvolutionPolicyKind {
    StrictExact,
    AllowAddNullableFields,
    AllowSafeWidening,
    AllowProjection,
    RequireFieldIdsForRename,
    CompatibilityExportOnly,
    RejectUnknownChanges,
}
impl SchemaEvolutionPolicyKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::StrictExact => "strict_exact",
            Self::AllowAddNullableFields => "allow_add_nullable_fields",
            Self::AllowSafeWidening => "allow_safe_widening",
            Self::AllowProjection => "allow_projection",
            Self::RequireFieldIdsForRename => "require_field_ids_for_rename",
            Self::CompatibilityExportOnly => "compatibility_export_only",
            Self::RejectUnknownChanges => "reject_unknown_changes",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaEvolutionPolicy {
    pub kinds: Vec<SchemaEvolutionPolicyKind>,
}
impl SchemaEvolutionPolicy {
    #[must_use]
    pub fn strict() -> Self {
        Self {
            kinds: vec![
                SchemaEvolutionPolicyKind::StrictExact,
                SchemaEvolutionPolicyKind::RejectUnknownChanges,
            ],
        }
    }
    #[must_use]
    pub fn default_conservative() -> Self {
        Self {
            kinds: vec![
                SchemaEvolutionPolicyKind::AllowAddNullableFields,
                SchemaEvolutionPolicyKind::AllowSafeWidening,
                SchemaEvolutionPolicyKind::AllowProjection,
                SchemaEvolutionPolicyKind::RequireFieldIdsForRename,
                SchemaEvolutionPolicyKind::RejectUnknownChanges,
            ],
        }
    }
    #[must_use]
    pub fn allows(&self, kind: SchemaEvolutionPolicyKind) -> bool {
        self.kinds.contains(&kind)
    }
    pub fn add(&mut self, kind: SchemaEvolutionPolicyKind) {
        if !self.allows(kind) {
            self.kinds.push(kind);
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "schema_evolution_policy(kinds={})",
            self.kinds
                .iter()
                .map(SchemaEvolutionPolicyKind::as_str)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaCompatibilityReport {
    pub from_schema: SchemaId,
    pub to_schema: SchemaId,
    pub level: SchemaCompatibilityLevel,
    pub changes: Vec<SchemaChange>,
    pub diagnostics: Vec<Diagnostic>,
}
impl SchemaCompatibilityReport {
    #[must_use]
    pub fn new(
        from_schema: SchemaId,
        to_schema: SchemaId,
        level: SchemaCompatibilityLevel,
    ) -> Self {
        Self {
            from_schema,
            to_schema,
            level,
            changes: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_change(&mut self, change: SchemaChange) {
        self.changes.push(change);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    #[must_use]
    pub const fn is_compatible_for_read(&self) -> bool {
        self.level.allows_read()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "schema_compatibility(from={}, to={}, level={}, changes={}, fallback_execution=disabled)",
            self.from_schema.as_str(),
            self.to_schema.as_str(),
            self.level.as_str(),
            self.changes.len()
        )
    }
}

/// Machine-readable CG-9 schema evolution compatibility evidence.
///
/// This report only compares typed schema definitions. It does not read data, write data,
/// contact a catalog, touch object storage, or attempt fallback execution.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct SchemaEvolutionCompatibilityReport {
    pub compatibility: SchemaCompatibilityReport,
    pub policy: SchemaEvolutionPolicy,
    pub safe_change_count: usize,
    pub unsafe_change_count: usize,
    pub field_id_required_count: usize,
    pub missing_field_id_count: usize,
    pub requires_projection: bool,
    pub requires_cast: bool,
    pub requires_default_values: bool,
    pub metadata_loss_reported: bool,
    pub read_supported: bool,
    pub write_supported: bool,
    pub data_read: bool,
    pub write_io: bool,
    pub catalog_io: bool,
    pub object_store_io: bool,
    pub fallback_execution_allowed: bool,
}

impl SchemaEvolutionCompatibilityReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.compatibility.has_errors() || self.unsafe_change_count > 0
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "schema_evolution_compatibility(level={}, safe_changes={}, unsafe_changes={}, read_supported={}, write_supported={}, data_read=false, write_io=false, catalog_io=false, object_store_io=false, fallback_execution=disabled)",
            self.compatibility.level.as_str(),
            self.safe_change_count,
            self.unsafe_change_count,
            self.read_supported,
            self.write_supported
        )
    }
}

#[derive(Debug, Default)]
#[allow(clippy::struct_excessive_bools)]
struct SchemaEvolutionAnalysis {
    safe_change_count: usize,
    unsafe_change_count: usize,
    field_id_required_count: usize,
    missing_field_id_count: usize,
    requires_projection: bool,
    requires_cast: bool,
    requires_default_values: bool,
    metadata_loss_reported: bool,
}

impl SchemaEvolutionAnalysis {
    fn add_safe(&mut self) {
        self.safe_change_count += 1;
    }

    fn add_unsafe(&mut self, report: &mut SchemaCompatibilityReport, diagnostic: Diagnostic) {
        self.unsafe_change_count += 1;
        report.add_diagnostic(diagnostic);
    }

    #[must_use]
    fn compatibility_level(&self, change_count: usize) -> SchemaCompatibilityLevel {
        if self.unsafe_change_count > 0 {
            SchemaCompatibilityLevel::Incompatible
        } else if change_count == 0 {
            SchemaCompatibilityLevel::Exact
        } else if self.requires_default_values {
            SchemaCompatibilityLevel::RequiresDefaultValues
        } else if self.requires_cast {
            SchemaCompatibilityLevel::RequiresCast
        } else if self.requires_projection {
            SchemaCompatibilityLevel::RequiresProjection
        } else {
            SchemaCompatibilityLevel::ReadCompatible
        }
    }
}

/// Evaluates a schema transition against the provided policy without performing I/O.
#[must_use]
pub fn evaluate_schema_evolution_compatibility(
    from: &SchemaDefinition,
    to: &SchemaDefinition,
    policy: &SchemaEvolutionPolicy,
) -> SchemaEvolutionCompatibilityReport {
    let mut compatibility = SchemaCompatibilityReport::new(
        from.id.clone(),
        to.id.clone(),
        SchemaCompatibilityLevel::Unknown,
    );
    let mut analysis = SchemaEvolutionAnalysis::default();
    let mut matched_to = vec![false; to.fields.len()];
    let mut unmatched_from = Vec::new();

    if from.has_errors() || to.has_errors() {
        analysis.add_unsafe(
            &mut compatibility,
            Diagnostic::invalid_input(
                "schema_evolution",
                "schema definitions contain existing error diagnostics",
                "Fix schema definition diagnostics before evaluating schema evolution.",
            ),
        );
    }

    for from_index in 0..from.fields.len() {
        let from_field = &from.fields[from_index];
        if let Some(to_index) = find_schema_match_index(from_field, to, &matched_to) {
            matched_to[to_index] = true;
            compare_matched_schema_fields(
                from_field,
                &to.fields[to_index],
                policy,
                &mut compatibility,
                &mut analysis,
            );
        } else {
            unmatched_from.push(from_index);
        }
    }

    for from_index in unmatched_from {
        let from_field = &from.fields[from_index];
        if let Some(to_index) = find_possible_rename_without_identity(from_field, to, &matched_to) {
            matched_to[to_index] = true;
            record_schema_rename(
                from_field,
                &to.fields[to_index],
                policy,
                &mut compatibility,
                &mut analysis,
            );
        } else {
            record_schema_drop(from_field, policy, &mut compatibility, &mut analysis);
        }
    }

    for (to_index, to_field) in to.fields.iter().enumerate() {
        if !matched_to[to_index] {
            record_schema_add(to_field, policy, &mut compatibility, &mut analysis);
        }
    }

    compatibility.level = analysis.compatibility_level(compatibility.changes.len());
    let read_supported = compatibility.level.allows_read() && !compatibility.has_errors();
    let write_supported = compatibility.level.allows_write() && !compatibility.has_errors();

    SchemaEvolutionCompatibilityReport {
        compatibility,
        policy: policy.clone(),
        safe_change_count: analysis.safe_change_count,
        unsafe_change_count: analysis.unsafe_change_count,
        field_id_required_count: analysis.field_id_required_count,
        missing_field_id_count: analysis.missing_field_id_count,
        requires_projection: analysis.requires_projection,
        requires_cast: analysis.requires_cast,
        requires_default_values: analysis.requires_default_values,
        metadata_loss_reported: analysis.metadata_loss_reported,
        read_supported,
        write_supported,
        data_read: false,
        write_io: false,
        catalog_io: false,
        object_store_io: false,
        fallback_execution_allowed: false,
    }
}

fn find_schema_match_index(
    from_field: &SchemaField,
    to: &SchemaDefinition,
    matched_to: &[bool],
) -> Option<usize> {
    if let Some(from_id) = &from_field.id {
        if let Some(index) = to
            .fields
            .iter()
            .enumerate()
            .find(|(index, field)| !matched_to[*index] && field.id.as_ref() == Some(from_id))
            .map(|(index, _)| index)
        {
            return Some(index);
        }
    }

    to.fields
        .iter()
        .enumerate()
        .find(|(index, field)| {
            !matched_to[*index] && field.name.as_str() == from_field.name.as_str()
        })
        .map(|(index, _)| index)
}

fn find_possible_rename_without_identity(
    from_field: &SchemaField,
    to: &SchemaDefinition,
    matched_to: &[bool],
) -> Option<usize> {
    to.fields
        .iter()
        .enumerate()
        .find(|(index, field)| {
            !matched_to[*index]
                && field.name != from_field.name
                && field.dtype == from_field.dtype
                && field.nullability == from_field.nullability
        })
        .map(|(index, _)| index)
}

fn compare_matched_schema_fields(
    from_field: &SchemaField,
    to_field: &SchemaField,
    policy: &SchemaEvolutionPolicy,
    compatibility: &mut SchemaCompatibilityReport,
    analysis: &mut SchemaEvolutionAnalysis,
) {
    if from_field.id != to_field.id && from_field.name == to_field.name {
        record_field_identity_change(from_field, to_field, compatibility, analysis);
    }
    if from_field.name != to_field.name {
        record_schema_rename(from_field, to_field, policy, compatibility, analysis);
    }
    if from_field.dtype != to_field.dtype {
        record_dtype_change(from_field, to_field, policy, compatibility, analysis);
    }
    if from_field.nullability != to_field.nullability {
        record_nullability_change(from_field, to_field, compatibility, analysis);
    }
    if from_field.metadata != to_field.metadata {
        record_metadata_change(from_field, to_field, compatibility, analysis);
    }
}

fn record_schema_add(
    to_field: &SchemaField,
    policy: &SchemaEvolutionPolicy,
    compatibility: &mut SchemaCompatibilityReport,
    analysis: &mut SchemaEvolutionAnalysis,
) {
    compatibility.add_change(
        SchemaChange::new(
            SchemaChangeKind::AddField,
            format!(
                "target field {} is not present in source schema",
                to_field.name.as_str()
            ),
        )
        .with_field_path(to_field.path.clone()),
    );

    if to_field.nullability == Nullability::Nullable
        && policy.allows(SchemaEvolutionPolicyKind::AllowAddNullableFields)
    {
        analysis.add_safe();
    } else if field_has_default(to_field)
        && policy.allows(SchemaEvolutionPolicyKind::AllowAddNullableFields)
    {
        analysis.add_safe();
        analysis.requires_default_values = true;
    } else {
        analysis.add_unsafe(
            compatibility,
            schema_evolution_unsupported_diagnostic(
                "schema_evolution_add_field",
                format!(
                    "adding non-nullable field {} without a declared default is unsupported",
                    to_field.name.as_str()
                ),
            ),
        );
    }
}

fn record_schema_drop(
    from_field: &SchemaField,
    policy: &SchemaEvolutionPolicy,
    compatibility: &mut SchemaCompatibilityReport,
    analysis: &mut SchemaEvolutionAnalysis,
) {
    compatibility.add_change(
        SchemaChange::new(
            SchemaChangeKind::DropField,
            format!(
                "source field {} is not present in target schema",
                from_field.name.as_str()
            ),
        )
        .with_field_path(from_field.path.clone()),
    );

    if policy.allows(SchemaEvolutionPolicyKind::AllowProjection) {
        analysis.add_safe();
        analysis.requires_projection = true;
    } else {
        analysis.add_unsafe(
            compatibility,
            schema_evolution_unsupported_diagnostic(
                "schema_evolution_drop_field",
                format!(
                    "dropping field {} requires explicit projection compatibility",
                    from_field.name.as_str()
                ),
            ),
        );
    }
}

fn record_schema_rename(
    from_field: &SchemaField,
    to_field: &SchemaField,
    policy: &SchemaEvolutionPolicy,
    compatibility: &mut SchemaCompatibilityReport,
    analysis: &mut SchemaEvolutionAnalysis,
) {
    compatibility.add_change(
        SchemaChange::new(
            SchemaChangeKind::RenameField,
            format!(
                "field {} was renamed to {}",
                from_field.name.as_str(),
                to_field.name.as_str()
            ),
        )
        .with_field_path(from_field.path.clone()),
    );
    analysis.field_id_required_count += 1;

    if policy.allows(SchemaEvolutionPolicyKind::RequireFieldIdsForRename)
        && from_field.id.is_some()
        && from_field.id == to_field.id
    {
        analysis.add_safe();
    } else {
        analysis.missing_field_id_count += 1;
        analysis.add_unsafe(
            compatibility,
            schema_evolution_unsupported_diagnostic(
                "schema_evolution_rename_field",
                format!(
                    "renaming {} to {} requires a stable field id on both schemas",
                    from_field.name.as_str(),
                    to_field.name.as_str()
                ),
            ),
        );
    }
}

fn record_field_identity_change(
    from_field: &SchemaField,
    to_field: &SchemaField,
    compatibility: &mut SchemaCompatibilityReport,
    analysis: &mut SchemaEvolutionAnalysis,
) {
    compatibility.add_change(
        SchemaChange::new(
            SchemaChangeKind::Unknown,
            format!(
                "field identity changed for {} from {} to {}",
                from_field.name.as_str(),
                from_field
                    .id
                    .as_ref()
                    .map_or("none".to_string(), |id| id.as_str().to_string()),
                to_field
                    .id
                    .as_ref()
                    .map_or("none".to_string(), |id| id.as_str().to_string())
            ),
        )
        .with_field_path(from_field.path.clone()),
    );
    analysis.add_unsafe(
        compatibility,
        schema_evolution_invalid_diagnostic(
            "schema_evolution_field_identity",
            format!(
                "field {} changed identity without a safe rename contract",
                from_field.name.as_str()
            ),
        ),
    );
}

fn record_dtype_change(
    from_field: &SchemaField,
    to_field: &SchemaField,
    policy: &SchemaEvolutionPolicy,
    compatibility: &mut SchemaCompatibilityReport,
    analysis: &mut SchemaEvolutionAnalysis,
) {
    let safe_widening = is_safe_schema_widening(&from_field.dtype, &to_field.dtype);
    let kind = if safe_widening {
        SchemaChangeKind::WidenType
    } else {
        SchemaChangeKind::NarrowType
    };
    compatibility.add_change(
        SchemaChange::new(
            kind,
            format!(
                "field {} dtype changed from {} to {}",
                from_field.name.as_str(),
                from_field.dtype.as_str(),
                to_field.dtype.as_str()
            ),
        )
        .with_field_path(from_field.path.clone()),
    );

    if safe_widening && policy.allows(SchemaEvolutionPolicyKind::AllowSafeWidening) {
        analysis.add_safe();
        analysis.requires_cast = true;
    } else {
        analysis.add_unsafe(
            compatibility,
            schema_evolution_unsupported_diagnostic(
                "schema_evolution_dtype_change",
                format!(
                    "dtype change from {} to {} is not a supported safe widening",
                    from_field.dtype.as_str(),
                    to_field.dtype.as_str()
                ),
            ),
        );
    }
}

fn record_nullability_change(
    from_field: &SchemaField,
    to_field: &SchemaField,
    compatibility: &mut SchemaCompatibilityReport,
    analysis: &mut SchemaEvolutionAnalysis,
) {
    compatibility.add_change(
        SchemaChange::new(
            SchemaChangeKind::ChangeNullability,
            format!(
                "field {} nullability changed from {} to {}",
                from_field.name.as_str(),
                from_field.nullability.as_str(),
                to_field.nullability.as_str()
            ),
        )
        .with_field_path(from_field.path.clone()),
    );

    if from_field.nullability == Nullability::NonNullable
        && to_field.nullability == Nullability::Nullable
    {
        analysis.add_safe();
    } else {
        analysis.add_unsafe(
            compatibility,
            schema_evolution_unsupported_diagnostic(
                "schema_evolution_nullability_change",
                format!(
                    "nullability change from {} to {} is not safely supported",
                    from_field.nullability.as_str(),
                    to_field.nullability.as_str()
                ),
            ),
        );
    }
}

fn record_metadata_change(
    from_field: &SchemaField,
    to_field: &SchemaField,
    compatibility: &mut SchemaCompatibilityReport,
    analysis: &mut SchemaEvolutionAnalysis,
) {
    compatibility.add_change(
        SchemaChange::new(
            SchemaChangeKind::ChangeMetadata,
            format!("field {} metadata changed", from_field.name.as_str()),
        )
        .with_field_path(from_field.path.clone()),
    );
    analysis.add_safe();

    if metadata_was_lost(from_field, to_field) {
        analysis.metadata_loss_reported = true;
        compatibility.add_diagnostic(Diagnostic::new(
            DiagnosticCode::MetadataLoss,
            DiagnosticSeverity::Warning,
            DiagnosticCategory::MetadataLoss,
            format!(
                "Schema evolution may lose metadata for field {}.",
                from_field.name.as_str()
            ),
            Some("schema_evolution_metadata".to_string()),
            Some("Target schema does not preserve every source metadata entry.".to_string()),
            Some("Preserve metadata explicitly or accept reported fidelity loss.".to_string()),
            FallbackStatus::disabled_by_policy(),
        ));
    }
}

fn schema_evolution_unsupported_diagnostic(
    feature: impl Into<String>,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::unsupported(
        DiagnosticCode::NotImplemented,
        feature,
        message,
        Some(
            "Use a compatible schema transition or add a native schema-evolution rule.".to_string(),
        ),
    )
}

fn schema_evolution_invalid_diagnostic(
    feature: impl Into<String>,
    reason: impl Into<String>,
) -> Diagnostic {
    Diagnostic::invalid_input(
        feature,
        reason,
        "Provide stable field identity and a supported schema transition.",
    )
}

fn field_has_default(field: &SchemaField) -> bool {
    field
        .metadata
        .iter()
        .any(|(key, _)| key.eq_ignore_ascii_case("default"))
}

fn metadata_was_lost(from_field: &SchemaField, to_field: &SchemaField) -> bool {
    from_field
        .metadata
        .iter()
        .any(|entry| !to_field.metadata.contains(entry))
}

fn is_safe_schema_widening(from: &LogicalDType, to: &LogicalDType) -> bool {
    matches!(
        (from, to),
        (
            LogicalDType::Int64 | LogicalDType::UInt64,
            LogicalDType::Float64
        ) | (LogicalDType::Date32, LogicalDType::TimestampMicros)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogKind {
    LocalManifest,
    ObjectStoreManifest,
    IcebergCompatible,
    DeltaCompatible,
    HiveStylePath,
    CustomEnterprise,
    FoundryCompatible,
    Unknown,
}
impl CatalogKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::LocalManifest => "local_manifest",
            Self::ObjectStoreManifest => "object_store_manifest",
            Self::IcebergCompatible => "iceberg_compatible",
            Self::DeltaCompatible => "delta_compatible",
            Self::HiveStylePath => "hive_style_path",
            Self::CustomEnterprise => "custom_enterprise",
            Self::FoundryCompatible => "foundry_compatible",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_external(&self) -> bool {
        !matches!(self, Self::LocalManifest)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRef {
    pub kind: CatalogKind,
    pub name: String,
    pub namespace: Option<String>,
    pub credential_scope: Option<CredentialScope>,
}
impl CatalogRef {
    pub fn new(kind: CatalogKind, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        validate_non_empty("catalog name", &name)?;
        Ok(Self {
            kind,
            name,
            namespace: None,
            credential_scope: None,
        })
    }
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Result<Self> {
        let ns = namespace.into();
        validate_non_empty("catalog namespace", &ns)?;
        self.namespace = Some(ns);
        Ok(self)
    }
    #[must_use]
    pub fn with_credential_scope(mut self, scope: CredentialScope) -> Self {
        self.credential_scope = Some(scope);
        self
    }
    #[must_use]
    pub fn requires_credentials(&self) -> bool {
        self.credential_scope.is_some()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "catalog_ref(kind={}, name={}, namespace={}, requires_credentials={})",
            self.kind.as_str(),
            self.name,
            self.namespace.as_deref().unwrap_or("none"),
            self.requires_credentials()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormatKind {
    NativeVortexManifest,
    IcebergCompatible,
    DeltaCompatible,
    HiveStyle,
    ExternalCatalogOnly,
    Unknown,
}
impl TableFormatKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeVortexManifest => "native_vortex_manifest",
            Self::IcebergCompatible => "iceberg_compatible",
            Self::DeltaCompatible => "delta_compatible",
            Self::HiveStyle => "hive_style",
            Self::ExternalCatalogOnly => "external_catalog_only",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_native_vortex(&self) -> bool {
        matches!(self, Self::NativeVortexManifest)
    }
    #[must_use]
    pub const fn is_compatibility_target(&self) -> bool {
        matches!(
            self,
            Self::IcebergCompatible
                | Self::DeltaCompatible
                | Self::HiveStyle
                | Self::ExternalCatalogOnly
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFeatureKind {
    SnapshotResolution,
    SchemaResolution,
    PartitionResolution,
    TimeTravel,
    FileListing,
    EqualityDeletes,
    PositionDeletes,
    RowLevelDeletes,
    AppendOnlyWrites,
    Overwrites,
    Transactions,
    Unknown,
}
impl TableFeatureKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SnapshotResolution => "snapshot_resolution",
            Self::SchemaResolution => "schema_resolution",
            Self::PartitionResolution => "partition_resolution",
            Self::TimeTravel => "time_travel",
            Self::FileListing => "file_listing",
            Self::EqualityDeletes => "equality_deletes",
            Self::PositionDeletes => "position_deletes",
            Self::RowLevelDeletes => "row_level_deletes",
            Self::AppendOnlyWrites => "append_only_writes",
            Self::Overwrites => "overwrites",
            Self::Transactions => "transactions",
            Self::Unknown => "unknown",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFeatureStatus {
    Supported,
    Planned,
    Unsupported,
    RequiresConfiguration,
    RequiresCredentials,
    Unknown,
}
impl TableFeatureStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Planned => "planned",
            Self::Unsupported => "unsupported",
            Self::RequiresConfiguration => "requires_configuration",
            Self::RequiresCredentials => "requires_credentials",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Supported)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableFeature {
    pub kind: TableFeatureKind,
    pub status: TableFeatureStatus,
    pub note: Option<String>,
}
impl TableFeature {
    #[must_use]
    pub fn new(kind: TableFeatureKind, status: TableFeatureStatus) -> Self {
        Self {
            kind,
            status,
            note: None,
        }
    }
    #[must_use]
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "table_feature(kind={}, status={}, note={})",
            self.kind.as_str(),
            self.status.as_str(),
            self.note.as_deref().unwrap_or("none")
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { buckets: u32 },
    Truncate { width: u32 },
    Unknown(String),
}
impl PartitionTransform {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::Year => "year",
            Self::Month => "month",
            Self::Day => "day",
            Self::Hour => "hour",
            Self::Bucket { .. } => "bucket",
            Self::Truncate { .. } => "truncate",
            Self::Unknown(_) => "unknown",
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        match self {
            Self::Bucket { buckets } => format!("bucket({buckets})"),
            Self::Truncate { width } => format!("truncate({width})"),
            Self::Unknown(v) => format!("unknown({v})"),
            _ => self.as_str().to_string(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionField {
    pub source: FieldPath,
    pub transform: PartitionTransform,
}
impl PartitionField {
    #[must_use]
    pub fn new(source: FieldPath, transform: PartitionTransform) -> Self {
        Self { source, transform }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "partition_field(source={}, transform={})",
            self.source.as_dot_separated(),
            self.transform.summary()
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSpec {
    pub fields: Vec<PartitionField>,
}
impl PartitionSpec {
    #[must_use]
    pub fn empty() -> Self {
        Self { fields: vec![] }
    }
    pub fn add_field(&mut self, field: PartitionField) {
        self.fields.push(field);
    }
    #[must_use]
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
    #[must_use]
    pub fn is_partitioned(&self) -> bool {
        !self.fields.is_empty()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("partition_spec(field_count={})", self.field_count())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionEvolutionCompatibilityLevel {
    Exact,
    ReadCompatible,
    RequiresPartitionRouting,
    RequiresMetadataRewrite,
    RequiresRepartition,
    Incompatible,
    Unknown,
}

impl PartitionEvolutionCompatibilityLevel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::ReadCompatible => "read_compatible",
            Self::RequiresPartitionRouting => "requires_partition_routing",
            Self::RequiresMetadataRewrite => "requires_metadata_rewrite",
            Self::RequiresRepartition => "requires_repartition",
            Self::Incompatible => "incompatible",
            Self::Unknown => "unknown",
        }
    }

    #[must_use]
    pub const fn allows_read(&self) -> bool {
        matches!(
            self,
            Self::Exact
                | Self::ReadCompatible
                | Self::RequiresPartitionRouting
                | Self::RequiresMetadataRewrite
                | Self::RequiresRepartition
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionEvolutionChangeKind {
    AddField,
    DropField,
    ChangeTransform,
    ReorderField,
    UnknownTransform,
}

impl PartitionEvolutionChangeKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::AddField => "add_field",
            Self::DropField => "drop_field",
            Self::ChangeTransform => "change_transform",
            Self::ReorderField => "reorder_field",
            Self::UnknownTransform => "unknown_transform",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionEvolutionChange {
    pub kind: PartitionEvolutionChangeKind,
    pub field_path: Option<FieldPath>,
    pub from_transform: Option<PartitionTransform>,
    pub to_transform: Option<PartitionTransform>,
    pub reason: String,
}

impl PartitionEvolutionChange {
    #[must_use]
    pub fn new(kind: PartitionEvolutionChangeKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            field_path: None,
            from_transform: None,
            to_transform: None,
            reason: reason.into(),
        }
    }

    #[must_use]
    pub fn with_field_path(mut self, field_path: FieldPath) -> Self {
        self.field_path = Some(field_path);
        self
    }

    #[must_use]
    pub fn with_transforms(
        mut self,
        from_transform: Option<PartitionTransform>,
        to_transform: Option<PartitionTransform>,
    ) -> Self {
        self.from_transform = from_transform;
        self.to_transform = to_transform;
        self
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "partition_evolution_change(kind={}, field_path={}, from_transform={}, to_transform={}, reason={})",
            self.kind.as_str(),
            self.field_path
                .as_ref()
                .map_or("none".to_string(), FieldPath::as_dot_separated),
            self.from_transform
                .as_ref()
                .map_or("none".to_string(), PartitionTransform::summary),
            self.to_transform
                .as_ref()
                .map_or("none".to_string(), PartitionTransform::summary),
            self.reason
        )
    }
}

/// Machine-readable CG-9 partition evolution compatibility evidence.
///
/// This report only compares typed partition specs. It does not read table metadata,
/// route real files, repartition data, write data, contact a catalog, or attempt fallback execution.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct PartitionEvolutionCompatibilityReport {
    pub from_spec: PartitionSpec,
    pub to_spec: PartitionSpec,
    pub level: PartitionEvolutionCompatibilityLevel,
    pub changes: Vec<PartitionEvolutionChange>,
    pub diagnostics: Vec<Diagnostic>,
    pub preserved_field_count: usize,
    pub added_field_count: usize,
    pub dropped_field_count: usize,
    pub transform_change_count: usize,
    pub reorder_count: usize,
    pub unsafe_change_count: usize,
    pub requires_partition_router: bool,
    pub requires_metadata_rewrite: bool,
    pub requires_repartition: bool,
    pub read_supported: bool,
    pub write_supported: bool,
    pub data_read: bool,
    pub write_io: bool,
    pub catalog_io: bool,
    pub object_store_io: bool,
    pub fallback_execution_allowed: bool,
}

impl PartitionEvolutionCompatibilityReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.unsafe_change_count > 0
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "partition_evolution_compatibility(level={}, changes={}, unsafe_changes={}, read_supported={}, write_supported={}, data_read=false, write_io=false, catalog_io=false, object_store_io=false, fallback_execution=disabled)",
            self.level.as_str(),
            self.changes.len(),
            self.unsafe_change_count,
            self.read_supported,
            self.write_supported
        )
    }
}

#[derive(Debug, Default)]
struct PartitionEvolutionAnalysis {
    preserved_field_count: usize,
    added_field_count: usize,
    dropped_field_count: usize,
    transform_change_count: usize,
    reorder_count: usize,
    unsafe_change_count: usize,
    requires_partition_router: bool,
    requires_metadata_rewrite: bool,
    requires_repartition: bool,
}

impl PartitionEvolutionAnalysis {
    fn record_unsafe(&mut self, diagnostics: &mut Vec<Diagnostic>, diagnostic: Diagnostic) {
        self.unsafe_change_count += 1;
        diagnostics.push(diagnostic);
    }

    #[must_use]
    fn compatibility_level(&self, change_count: usize) -> PartitionEvolutionCompatibilityLevel {
        if self.unsafe_change_count > 0 {
            PartitionEvolutionCompatibilityLevel::Incompatible
        } else if change_count == 0 {
            PartitionEvolutionCompatibilityLevel::Exact
        } else if self.requires_repartition {
            PartitionEvolutionCompatibilityLevel::RequiresRepartition
        } else if self.requires_metadata_rewrite {
            PartitionEvolutionCompatibilityLevel::RequiresMetadataRewrite
        } else if self.requires_partition_router {
            PartitionEvolutionCompatibilityLevel::RequiresPartitionRouting
        } else {
            PartitionEvolutionCompatibilityLevel::ReadCompatible
        }
    }
}

/// Evaluates a partition spec transition without performing table metadata or data I/O.
#[must_use]
pub fn evaluate_partition_evolution_compatibility(
    from: &PartitionSpec,
    to: &PartitionSpec,
) -> PartitionEvolutionCompatibilityReport {
    let mut changes = Vec::new();
    let mut diagnostics = Vec::new();
    let mut analysis = PartitionEvolutionAnalysis::default();
    let mut matched_to = vec![false; to.fields.len()];

    for from_field in &from.fields {
        if let Some(to_index) = find_partition_field_index(from_field, to, &matched_to) {
            matched_to[to_index] = true;
            compare_partition_field(
                from_field,
                &to.fields[to_index],
                &mut changes,
                &mut diagnostics,
                &mut analysis,
            );
        } else {
            record_partition_drop(from_field, &mut changes, &mut analysis);
        }
    }

    for (to_index, to_field) in to.fields.iter().enumerate() {
        if !matched_to[to_index] {
            record_partition_add(to_field, &mut changes, &mut diagnostics, &mut analysis);
        }
    }

    if partition_field_order_changed(from, to) {
        changes.push(PartitionEvolutionChange::new(
            PartitionEvolutionChangeKind::ReorderField,
            "partition fields use the same sources and transforms but appear in a different order",
        ));
        analysis.reorder_count += 1;
        analysis.requires_metadata_rewrite = true;
    }

    for field in from.fields.iter().chain(to.fields.iter()) {
        if partition_transform_is_unknown(&field.transform) {
            changes.push(
                PartitionEvolutionChange::new(
                    PartitionEvolutionChangeKind::UnknownTransform,
                    format!(
                        "partition field {} uses an unknown transform",
                        field.source.as_dot_separated()
                    ),
                )
                .with_field_path(field.source.clone())
                .with_transforms(Some(field.transform.clone()), None),
            );
            analysis.record_unsafe(
                &mut diagnostics,
                partition_evolution_unsupported_diagnostic(
                    "partition_evolution_unknown_transform",
                    format!(
                        "partition transform for {} is unknown",
                        field.source.as_dot_separated()
                    ),
                ),
            );
        }
    }

    let level = analysis.compatibility_level(changes.len());
    let read_supported = level.allows_read() && diagnostics_are_not_errors(&diagnostics);
    let write_supported = read_supported
        && !to
            .fields
            .iter()
            .any(|field| partition_transform_is_unknown(&field.transform));

    PartitionEvolutionCompatibilityReport {
        from_spec: from.clone(),
        to_spec: to.clone(),
        level,
        changes,
        diagnostics,
        preserved_field_count: analysis.preserved_field_count,
        added_field_count: analysis.added_field_count,
        dropped_field_count: analysis.dropped_field_count,
        transform_change_count: analysis.transform_change_count,
        reorder_count: analysis.reorder_count,
        unsafe_change_count: analysis.unsafe_change_count,
        requires_partition_router: analysis.requires_partition_router,
        requires_metadata_rewrite: analysis.requires_metadata_rewrite,
        requires_repartition: analysis.requires_repartition,
        read_supported,
        write_supported,
        data_read: false,
        write_io: false,
        catalog_io: false,
        object_store_io: false,
        fallback_execution_allowed: false,
    }
}

fn find_partition_field_index(
    from_field: &PartitionField,
    to: &PartitionSpec,
    matched_to: &[bool],
) -> Option<usize> {
    to.fields
        .iter()
        .enumerate()
        .find(|(index, to_field)| !matched_to[*index] && to_field.source == from_field.source)
        .map(|(index, _)| index)
}

fn compare_partition_field(
    from_field: &PartitionField,
    to_field: &PartitionField,
    changes: &mut Vec<PartitionEvolutionChange>,
    _diagnostics: &mut Vec<Diagnostic>,
    analysis: &mut PartitionEvolutionAnalysis,
) {
    if from_field.transform == to_field.transform {
        analysis.preserved_field_count += 1;
        return;
    }

    changes.push(
        PartitionEvolutionChange::new(
            PartitionEvolutionChangeKind::ChangeTransform,
            format!(
                "partition transform for {} changed from {} to {}",
                from_field.source.as_dot_separated(),
                from_field.transform.summary(),
                to_field.transform.summary()
            ),
        )
        .with_field_path(from_field.source.clone())
        .with_transforms(
            Some(from_field.transform.clone()),
            Some(to_field.transform.clone()),
        ),
    );
    analysis.transform_change_count += 1;
    analysis.requires_partition_router = true;
    analysis.requires_metadata_rewrite = true;
    analysis.requires_repartition = true;
}

fn record_partition_drop(
    from_field: &PartitionField,
    changes: &mut Vec<PartitionEvolutionChange>,
    analysis: &mut PartitionEvolutionAnalysis,
) {
    changes.push(
        PartitionEvolutionChange::new(
            PartitionEvolutionChangeKind::DropField,
            format!(
                "partition field {} is not present in target spec",
                from_field.source.as_dot_separated()
            ),
        )
        .with_field_path(from_field.source.clone())
        .with_transforms(Some(from_field.transform.clone()), None),
    );
    analysis.dropped_field_count += 1;
    analysis.requires_partition_router = true;
    analysis.requires_metadata_rewrite = true;
}

fn record_partition_add(
    to_field: &PartitionField,
    changes: &mut Vec<PartitionEvolutionChange>,
    diagnostics: &mut Vec<Diagnostic>,
    analysis: &mut PartitionEvolutionAnalysis,
) {
    changes.push(
        PartitionEvolutionChange::new(
            PartitionEvolutionChangeKind::AddField,
            format!(
                "partition field {} is new in target spec",
                to_field.source.as_dot_separated()
            ),
        )
        .with_field_path(to_field.source.clone())
        .with_transforms(None, Some(to_field.transform.clone())),
    );
    analysis.added_field_count += 1;
    analysis.requires_partition_router = true;
    analysis.requires_metadata_rewrite = true;
    analysis.requires_repartition = true;

    if partition_transform_is_unknown(&to_field.transform) {
        analysis.record_unsafe(
            diagnostics,
            partition_evolution_unsupported_diagnostic(
                "partition_evolution_add_field",
                format!(
                    "new partition field {} uses an unknown transform",
                    to_field.source.as_dot_separated()
                ),
            ),
        );
    }
}

fn partition_field_order_changed(from: &PartitionSpec, to: &PartitionSpec) -> bool {
    if from.fields.len() != to.fields.len() {
        return false;
    }

    let from_order = partition_field_keys(from);
    let to_order = partition_field_keys(to);
    let mut from_sorted = from_order.clone();
    let mut to_sorted = to_order.clone();
    from_sorted.sort();
    to_sorted.sort();

    from_sorted == to_sorted && from_order != to_order
}

fn partition_field_keys(spec: &PartitionSpec) -> Vec<String> {
    spec.fields
        .iter()
        .map(|field| {
            format!(
                "{}:{}",
                field.source.as_dot_separated(),
                field.transform.summary()
            )
        })
        .collect()
}

fn partition_transform_is_unknown(transform: &PartitionTransform) -> bool {
    matches!(transform, PartitionTransform::Unknown(_))
}

fn diagnostics_are_not_errors(diagnostics: &[Diagnostic]) -> bool {
    !diagnostics.iter().any(|diagnostic| {
        matches!(
            diagnostic.severity,
            DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
        )
    })
}

fn partition_evolution_unsupported_diagnostic(
    feature: impl Into<String>,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::unsupported(
        DiagnosticCode::NotImplemented,
        feature,
        message,
        Some(
            "Use known partition transforms or add a native partition-evolution rule.".to_string(),
        ),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteModel {
    None,
    FileLevelDelete,
    SegmentLevelTombstone,
    RowLevelDelete,
    PositionDelete,
    EqualityDelete,
    ExternalTableMetadata,
    Unknown,
}
impl DeleteModel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::FileLevelDelete => "file_level_delete",
            Self::SegmentLevelTombstone => "segment_level_tombstone",
            Self::RowLevelDelete => "row_level_delete",
            Self::PositionDelete => "position_delete",
            Self::EqualityDelete => "equality_delete",
            Self::ExternalTableMetadata => "external_table_metadata",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_supported_initially(&self) -> bool {
        matches!(self, Self::None | Self::FileLevelDelete)
    }
    #[must_use]
    pub const fn requires_explicit_handling(&self) -> bool {
        !self.is_supported_initially()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteTombstoneCompatibilityLevel {
    Exact,
    FileLevelCompatible,
    RequiresTombstoneFilter,
    RequiresRowIdentity,
    RequiresPositionIdentity,
    RequiresEqualityPredicate,
    RequiresExternalTableMetadata,
    Incompatible,
    Unknown,
}
impl DeleteTombstoneCompatibilityLevel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::FileLevelCompatible => "file_level_compatible",
            Self::RequiresTombstoneFilter => "requires_tombstone_filter",
            Self::RequiresRowIdentity => "requires_row_identity",
            Self::RequiresPositionIdentity => "requires_position_identity",
            Self::RequiresEqualityPredicate => "requires_equality_predicate",
            Self::RequiresExternalTableMetadata => "requires_external_table_metadata",
            Self::Incompatible => "incompatible",
            Self::Unknown => "unknown",
        }
    }

    #[must_use]
    pub const fn allows_read(&self) -> bool {
        matches!(self, Self::Exact | Self::FileLevelCompatible)
    }

    #[must_use]
    pub const fn allows_write(&self) -> bool {
        matches!(self, Self::Exact | Self::FileLevelCompatible)
    }
}

/// Machine-readable CG-9 delete/tombstone compatibility evidence.
///
/// This report compares declared delete/tombstone models only. It does not read table metadata,
/// apply delete files, filter tombstones, write data, contact a catalog, or attempt fallback execution.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct DeleteTombstoneCompatibilityReport {
    pub source_model: DeleteModel,
    pub target_model: DeleteModel,
    pub level: DeleteTombstoneCompatibilityLevel,
    pub diagnostics: Vec<Diagnostic>,
    pub delete_semantics_preserved: bool,
    pub tombstone_semantics_preserved: bool,
    pub requires_explicit_delete_handling: bool,
    pub requires_file_delete_filter: bool,
    pub requires_tombstone_filter: bool,
    pub requires_row_identity: bool,
    pub requires_position_identity: bool,
    pub requires_equality_predicate: bool,
    pub requires_external_table_metadata: bool,
    pub metadata_loss_reported: bool,
    pub unsupported_model_count: usize,
    pub unsafe_change_count: usize,
    pub read_supported: bool,
    pub write_supported: bool,
    pub data_read: bool,
    pub write_io: bool,
    pub catalog_io: bool,
    pub object_store_io: bool,
    pub fallback_execution_allowed: bool,
}

impl DeleteTombstoneCompatibilityReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.unsafe_change_count > 0
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "delete_tombstone_compatibility(level={}, source_model={}, target_model={}, unsafe_changes={}, read_supported={}, write_supported={}, data_read=false, write_io=false, catalog_io=false, object_store_io=false, fallback_execution=disabled)",
            self.level.as_str(),
            self.source_model.as_str(),
            self.target_model.as_str(),
            self.unsafe_change_count,
            self.read_supported,
            self.write_supported
        )
    }
}

/// Evaluates declared delete/tombstone semantics without performing table metadata or data I/O.
#[must_use]
pub fn evaluate_delete_tombstone_compatibility(
    source_model: DeleteModel,
    target_model: DeleteModel,
) -> DeleteTombstoneCompatibilityReport {
    let mut diagnostics = Vec::new();
    let requires_explicit_delete_handling =
        source_model.requires_explicit_handling() || target_model.requires_explicit_handling();
    let requires_file_delete_filter = source_model == DeleteModel::FileLevelDelete
        || target_model == DeleteModel::FileLevelDelete;
    let requires_tombstone_filter = source_model == DeleteModel::SegmentLevelTombstone
        || target_model == DeleteModel::SegmentLevelTombstone;
    let requires_row_identity =
        source_model == DeleteModel::RowLevelDelete || target_model == DeleteModel::RowLevelDelete;
    let requires_position_identity =
        source_model == DeleteModel::PositionDelete || target_model == DeleteModel::PositionDelete;
    let requires_equality_predicate =
        source_model == DeleteModel::EqualityDelete || target_model == DeleteModel::EqualityDelete;
    let requires_external_table_metadata = source_model == DeleteModel::ExternalTableMetadata
        || target_model == DeleteModel::ExternalTableMetadata;
    let unsupported_model_count = [source_model, target_model]
        .iter()
        .filter(|model| model.requires_explicit_handling())
        .count();
    let metadata_loss_reported = source_model != target_model && source_model != DeleteModel::None;
    let source_or_target_unknown =
        source_model == DeleteModel::Unknown || target_model == DeleteModel::Unknown;

    let level = if source_or_target_unknown {
        diagnostics.push(delete_tombstone_unsupported_diagnostic(
            "delete_tombstone_unknown_model",
            "delete/tombstone compatibility cannot be proven for an unknown delete model",
        ));
        DeleteTombstoneCompatibilityLevel::Incompatible
    } else if source_model.is_supported_initially() && target_model.is_supported_initially() {
        if source_model == target_model {
            DeleteTombstoneCompatibilityLevel::Exact
        } else if source_model == DeleteModel::None && target_model == DeleteModel::FileLevelDelete
        {
            DeleteTombstoneCompatibilityLevel::FileLevelCompatible
        } else {
            diagnostics.push(delete_tombstone_unsupported_diagnostic(
                "delete_tombstone_metadata_loss",
                "dropping declared file-level delete semantics requires an explicit metadata-loss rule",
            ));
            DeleteTombstoneCompatibilityLevel::Incompatible
        }
    } else if source_model == target_model {
        let level = delete_tombstone_required_level(source_model);
        diagnostics.push(delete_tombstone_unsupported_diagnostic(
            source_model.as_str(),
            format!(
                "delete model {} requires a native delete/tombstone rule before table compatibility can be certified",
                source_model.as_str()
            ),
        ));
        level
    } else {
        diagnostics.push(delete_tombstone_unsupported_diagnostic(
            "delete_tombstone_model_transition",
            format!(
                "delete model transition from {} to {} requires an explicit native translation rule",
                source_model.as_str(),
                target_model.as_str()
            ),
        ));
        DeleteTombstoneCompatibilityLevel::Incompatible
    };

    let unsafe_change_count = usize::from(source_or_target_unknown)
        + usize::from(requires_explicit_delete_handling)
        + usize::from(metadata_loss_reported);
    let read_supported = level.allows_read() && diagnostics_are_not_errors(&diagnostics);
    let write_supported = level.allows_write() && diagnostics_are_not_errors(&diagnostics);

    DeleteTombstoneCompatibilityReport {
        source_model,
        target_model,
        level,
        diagnostics,
        delete_semantics_preserved: source_model == target_model,
        tombstone_semantics_preserved: source_model == DeleteModel::SegmentLevelTombstone
            && target_model == DeleteModel::SegmentLevelTombstone,
        requires_explicit_delete_handling,
        requires_file_delete_filter,
        requires_tombstone_filter,
        requires_row_identity,
        requires_position_identity,
        requires_equality_predicate,
        requires_external_table_metadata,
        metadata_loss_reported,
        unsupported_model_count,
        unsafe_change_count,
        read_supported,
        write_supported,
        data_read: false,
        write_io: false,
        catalog_io: false,
        object_store_io: false,
        fallback_execution_allowed: false,
    }
}

const fn delete_tombstone_required_level(model: DeleteModel) -> DeleteTombstoneCompatibilityLevel {
    match model {
        DeleteModel::SegmentLevelTombstone => {
            DeleteTombstoneCompatibilityLevel::RequiresTombstoneFilter
        }
        DeleteModel::RowLevelDelete => DeleteTombstoneCompatibilityLevel::RequiresRowIdentity,
        DeleteModel::PositionDelete => DeleteTombstoneCompatibilityLevel::RequiresPositionIdentity,
        DeleteModel::EqualityDelete => DeleteTombstoneCompatibilityLevel::RequiresEqualityPredicate,
        DeleteModel::ExternalTableMetadata => {
            DeleteTombstoneCompatibilityLevel::RequiresExternalTableMetadata
        }
        DeleteModel::None | DeleteModel::FileLevelDelete => {
            DeleteTombstoneCompatibilityLevel::Exact
        }
        DeleteModel::Unknown => DeleteTombstoneCompatibilityLevel::Unknown,
    }
}

fn delete_tombstone_unsupported_diagnostic(
    feature: impl Into<String>,
    message: impl Into<String>,
) -> Diagnostic {
    Diagnostic::unsupported(
        DiagnosticCode::NotImplemented,
        feature,
        message,
        Some(
            "Add a native delete/tombstone compatibility rule before enabling this table path."
                .to_string(),
        ),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableCompatibilityStatus {
    Planned,
    Compatible,
    PartiallyCompatible,
    Incompatible,
    Unsupported,
}
impl TableCompatibilityStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Compatible => "compatible",
            Self::PartiallyCompatible => "partially_compatible",
            Self::Incompatible => "incompatible",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Incompatible | Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableCompatibilityPlan {
    pub table_format: TableFormatKind,
    pub catalog: Option<CatalogRef>,
    pub schema: Option<SchemaDefinition>,
    pub partition_spec: Option<PartitionSpec>,
    pub delete_model: DeleteModel,
    pub features: Vec<TableFeature>,
    pub status: TableCompatibilityStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl TableCompatibilityPlan {
    #[must_use]
    pub fn new(table_format: TableFormatKind) -> Self {
        Self {
            table_format,
            catalog: None,
            schema: None,
            partition_spec: None,
            delete_model: DeleteModel::None,
            features: vec![],
            status: TableCompatibilityStatus::Planned,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn native_vortex() -> Self {
        Self::new(TableFormatKind::NativeVortexManifest)
    }
    #[must_use]
    pub fn compatibility_target(table_format: TableFormatKind) -> Self {
        let mut plan = Self::new(table_format);
        plan.add_feature(TableFeature::new(
            TableFeatureKind::SchemaResolution,
            TableFeatureStatus::Planned,
        ));
        plan.add_feature(TableFeature::new(
            TableFeatureKind::PartitionResolution,
            TableFeatureStatus::Planned,
        ));
        plan.add_feature(TableFeature::new(
            TableFeatureKind::EqualityDeletes,
            TableFeatureStatus::Unsupported,
        ));
        plan
    }
    #[must_use]
    pub fn unsupported(
        table_format: TableFormatKind,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut plan = Self::new(table_format);
        plan.status = TableCompatibilityStatus::Unsupported;
        plan.add_diagnostic(Diagnostic::unsupported(
            crate::DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Use native Vortex format or supported compatibility planning paths.".to_string()),
        ));
        plan
    }
    #[must_use]
    pub fn with_catalog(mut self, catalog: CatalogRef) -> Self {
        self.catalog = Some(catalog);
        self
    }
    #[must_use]
    pub fn with_schema(mut self, schema: SchemaDefinition) -> Self {
        self.schema = Some(schema);
        self
    }
    #[must_use]
    pub fn with_partition_spec(mut self, partition_spec: PartitionSpec) -> Self {
        self.partition_spec = Some(partition_spec);
        self
    }
    #[must_use]
    pub fn with_delete_model(mut self, delete_model: DeleteModel) -> Self {
        self.delete_model = delete_model;
        self
    }
    pub fn add_feature(&mut self, feature: TableFeature) {
        self.features.push(feature);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub const fn requires_explicit_delete_handling(&self) -> bool {
        self.delete_model.requires_explicit_handling()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "table_compatibility_plan(format={}, status={}, delete_model={}, fallback execution: disabled, external table formats are compatibility targets, not fallback engines)",
            self.table_format.as_str(),
            self.status.as_str(),
            self.delete_model.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableCompatibilityReport {
    pub plan: TableCompatibilityPlan,
    pub schema_report: Option<SchemaCompatibilityReport>,
    pub diagnostics: Vec<Diagnostic>,
}
impl TableCompatibilityReport {
    #[must_use]
    pub fn from_plan(plan: TableCompatibilityPlan) -> Self {
        Self {
            plan,
            schema_report: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn with_schema_report(mut self, schema_report: SchemaCompatibilityReport) -> Self {
        self.schema_report = Some(schema_report);
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.plan.has_errors()
            || self
                .schema_report
                .as_ref()
                .is_some_and(SchemaCompatibilityReport::has_errors)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "{}; report_diagnostics={}; fallback execution: disabled",
            self.plan.to_human_text(),
            self.diagnostics.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DiagnosticCode;
    #[test]
    fn schema_id_rejects_empty_ids() {
        assert!(SchemaId::new("   ").is_err());
    }
    #[test]
    fn schema_version_rejects_zero() {
        assert!(SchemaVersion::new(0).is_err());
    }
    #[test]
    fn schema_version_next_increments() {
        assert_eq!(SchemaVersion::new(1).expect("ok").next().as_u64(), 2);
    }
    #[test]
    fn field_id_rejects_empty_ids() {
        assert!(FieldId::new("").is_err());
    }
    #[test]
    fn field_name_rejects_empty_names() {
        assert!(FieldName::new(" ").is_err());
    }
    #[test]
    fn field_path_rejects_empty_paths() {
        assert!(FieldPath::new(vec![]).is_err());
    }
    #[test]
    fn field_path_from_dot_separated_parses_nested_path() {
        let path = FieldPath::from_dot_separated("a.b.c").expect("ok");
        assert_eq!(path.depth(), 3);
    }
    #[test]
    fn schema_field_new_creates_single_field_path() {
        let name = FieldName::new("x").expect("ok");
        let field = SchemaField::new(name.clone(), LogicalDType::Int64, Nullability::Nullable);
        assert_eq!(field.path, FieldPath::single(name));
    }
    #[test]
    fn schema_field_add_metadata_rejects_empty_key() {
        let name = FieldName::new("x").expect("ok");
        let mut field = SchemaField::new(name, LogicalDType::Int64, Nullability::Nullable);
        assert!(field.add_metadata("  ", "v").is_err());
    }
    #[test]
    fn schema_definition_counts_fields() {
        let mut s = SchemaDefinition::new(
            SchemaId::new("s").expect("ok"),
            SchemaVersion::new(1).expect("ok"),
        );
        s.add_field(SchemaField::new(
            FieldName::new("a").expect("ok"),
            LogicalDType::Int64,
            Nullability::Nullable,
        ));
        assert_eq!(s.field_count(), 1);
    }
    #[test]
    fn schema_definition_has_field_ids_returns_true_only_when_all_fields_have_ids() {
        let mut s = SchemaDefinition::new(
            SchemaId::new("s").expect("ok"),
            SchemaVersion::new(1).expect("ok"),
        );
        s.add_field(
            SchemaField::new(
                FieldName::new("a").expect("ok"),
                LogicalDType::Int64,
                Nullability::Nullable,
            )
            .with_id(FieldId::new("1").expect("ok")),
        );
        s.add_field(SchemaField::new(
            FieldName::new("b").expect("ok"),
            LogicalDType::Int64,
            Nullability::Nullable,
        ));
        assert!(!s.has_field_ids());
    }
    #[test]
    fn schema_change_kind_rename_field_requires_field_id_for_safety() {
        assert!(SchemaChangeKind::RenameField.requires_field_id_for_safety());
    }
    #[test]
    fn schema_change_kind_add_field_is_potentially_safe() {
        assert!(SchemaChangeKind::AddField.is_potentially_safe());
    }
    #[test]
    fn schema_compatibility_level_read_compatible_allows_read() {
        assert!(SchemaCompatibilityLevel::ReadCompatible.allows_read());
    }
    #[test]
    fn schema_compatibility_level_incompatible_does_not_allow_read() {
        assert!(!SchemaCompatibilityLevel::Incompatible.allows_read());
    }
    #[test]
    fn schema_evolution_policy_default_conservative_allows_safe_widening() {
        assert!(
            SchemaEvolutionPolicy::default_conservative()
                .allows(SchemaEvolutionPolicyKind::AllowSafeWidening)
        );
    }
    #[test]
    fn schema_compatibility_report_is_compatible_for_read_delegates_to_level() {
        let r = SchemaCompatibilityReport::new(
            SchemaId::new("a").expect("ok"),
            SchemaId::new("b").expect("ok"),
            SchemaCompatibilityLevel::ReadCompatible,
        );
        assert!(r.is_compatible_for_read());
    }
    #[test]
    fn catalog_kind_iceberg_compatible_is_external() {
        assert!(CatalogKind::IcebergCompatible.is_external());
    }
    #[test]
    fn catalog_ref_rejects_empty_name() {
        assert!(CatalogRef::new(CatalogKind::LocalManifest, " ").is_err());
    }
    #[test]
    fn catalog_ref_summary_does_not_expose_secret_values() {
        let scope = CredentialScope::new(crate::CredentialScopeKind::CatalogRead, "x").expect("ok");
        let c = CatalogRef::new(CatalogKind::IcebergCompatible, "cat")
            .expect("ok")
            .with_credential_scope(scope);
        assert!(!c.summary().contains("secret"));
    }
    #[test]
    fn table_format_kind_native_vortex_manifest_is_native_vortex() {
        assert!(TableFormatKind::NativeVortexManifest.is_native_vortex());
    }
    #[test]
    fn table_format_kind_iceberg_compatible_is_compatibility_target() {
        assert!(TableFormatKind::IcebergCompatible.is_compatibility_target());
    }
    #[test]
    fn table_feature_status_supported_is_usable() {
        assert!(TableFeatureStatus::Supported.is_usable());
    }
    #[test]
    fn partition_transform_bucket_summary_includes_bucket_count() {
        assert!(
            PartitionTransform::Bucket { buckets: 16 }
                .summary()
                .contains("16")
        );
    }
    #[test]
    fn partition_spec_empty_is_not_partitioned() {
        assert!(!PartitionSpec::empty().is_partitioned());
    }

    fn partition_path(value: &str) -> FieldPath {
        FieldPath::from_dot_separated(value).expect("partition path")
    }

    fn partition_field(source: &str, transform: PartitionTransform) -> PartitionField {
        PartitionField::new(partition_path(source), transform)
    }

    fn partition_spec(fields: Vec<PartitionField>) -> PartitionSpec {
        let mut spec = PartitionSpec::empty();
        for field in fields {
            spec.add_field(field);
        }
        spec
    }

    #[test]
    fn partition_evolution_same_spec_is_exact_and_side_effect_free() {
        let spec = partition_spec(vec![partition_field("created_at", PartitionTransform::Day)]);

        let report = evaluate_partition_evolution_compatibility(&spec, &spec);

        assert_eq!(report.level, PartitionEvolutionCompatibilityLevel::Exact);
        assert_eq!(report.preserved_field_count, 1);
        assert_eq!(report.changes.len(), 0);
        assert!(report.read_supported);
        assert!(report.write_supported);
        assert!(!report.data_read);
        assert!(!report.write_io);
        assert!(!report.catalog_io);
        assert!(!report.object_store_io);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn partition_evolution_add_field_requires_routing_and_repartition() {
        let from = partition_spec(vec![partition_field("created_at", PartitionTransform::Day)]);
        let to = partition_spec(vec![
            partition_field("created_at", PartitionTransform::Day),
            partition_field("customer_id", PartitionTransform::Bucket { buckets: 16 }),
        ]);

        let report = evaluate_partition_evolution_compatibility(&from, &to);

        assert_eq!(
            report.level,
            PartitionEvolutionCompatibilityLevel::RequiresRepartition
        );
        assert_eq!(report.added_field_count, 1);
        assert!(report.requires_partition_router);
        assert!(report.requires_metadata_rewrite);
        assert!(report.requires_repartition);
        assert!(report.read_supported);
        assert!(report.write_supported);
    }

    #[test]
    fn partition_evolution_transform_change_requires_repartition() {
        let from = partition_spec(vec![partition_field("created_at", PartitionTransform::Day)]);
        let to = partition_spec(vec![partition_field(
            "created_at",
            PartitionTransform::Month,
        )]);

        let report = evaluate_partition_evolution_compatibility(&from, &to);

        assert_eq!(report.transform_change_count, 1);
        assert_eq!(
            report.level,
            PartitionEvolutionCompatibilityLevel::RequiresRepartition
        );
        assert!(report.requires_partition_router);
        assert!(report.requires_repartition);
    }

    #[test]
    fn partition_evolution_reorder_requires_metadata_rewrite() {
        let from = partition_spec(vec![
            partition_field("created_at", PartitionTransform::Day),
            partition_field("customer_id", PartitionTransform::Bucket { buckets: 16 }),
        ]);
        let to = partition_spec(vec![
            partition_field("customer_id", PartitionTransform::Bucket { buckets: 16 }),
            partition_field("created_at", PartitionTransform::Day),
        ]);

        let report = evaluate_partition_evolution_compatibility(&from, &to);

        assert_eq!(
            report.level,
            PartitionEvolutionCompatibilityLevel::RequiresMetadataRewrite
        );
        assert_eq!(report.reorder_count, 1);
        assert!(report.requires_metadata_rewrite);
        assert_eq!(report.unsafe_change_count, 0);
    }

    #[test]
    fn partition_evolution_unknown_transform_is_rejected() {
        let from = partition_spec(vec![partition_field("created_at", PartitionTransform::Day)]);
        let to = partition_spec(vec![partition_field(
            "created_at",
            PartitionTransform::Unknown("vendor_specific".to_string()),
        )]);

        let report = evaluate_partition_evolution_compatibility(&from, &to);

        assert_eq!(
            report.level,
            PartitionEvolutionCompatibilityLevel::Incompatible
        );
        assert_eq!(report.unsafe_change_count, 1);
        assert!(report.has_errors());
        assert_eq!(report.diagnostics[0].code, DiagnosticCode::NotImplemented);
        assert!(!report.diagnostics[0].fallback.attempted);
    }

    #[test]
    fn delete_model_none_is_initially_supported() {
        assert!(DeleteModel::None.is_supported_initially());
    }
    #[test]
    fn delete_model_equality_delete_requires_explicit_handling() {
        assert!(DeleteModel::EqualityDelete.requires_explicit_handling());
    }
    #[test]
    fn delete_tombstone_none_is_exact_and_side_effect_free() {
        let report = evaluate_delete_tombstone_compatibility(DeleteModel::None, DeleteModel::None);

        assert_eq!(report.level, DeleteTombstoneCompatibilityLevel::Exact);
        assert!(report.delete_semantics_preserved);
        assert!(!report.requires_explicit_delete_handling);
        assert!(report.read_supported);
        assert!(report.write_supported);
        assert!(!report.data_read);
        assert!(!report.write_io);
        assert!(!report.catalog_io);
        assert!(!report.object_store_io);
        assert!(!report.fallback_execution_allowed);
    }
    #[test]
    fn delete_tombstone_file_level_is_initially_compatible() {
        let report = evaluate_delete_tombstone_compatibility(
            DeleteModel::None,
            DeleteModel::FileLevelDelete,
        );

        assert_eq!(
            report.level,
            DeleteTombstoneCompatibilityLevel::FileLevelCompatible
        );
        assert!(report.requires_file_delete_filter);
        assert_eq!(report.unsafe_change_count, 0);
        assert!(report.read_supported);
        assert!(report.write_supported);
    }
    #[test]
    fn delete_tombstone_segment_tombstone_requires_native_filter() {
        let report = evaluate_delete_tombstone_compatibility(
            DeleteModel::SegmentLevelTombstone,
            DeleteModel::SegmentLevelTombstone,
        );

        assert_eq!(
            report.level,
            DeleteTombstoneCompatibilityLevel::RequiresTombstoneFilter
        );
        assert!(report.tombstone_semantics_preserved);
        assert!(report.requires_explicit_delete_handling);
        assert!(report.requires_tombstone_filter);
        assert!(!report.read_supported);
        assert!(!report.write_supported);
        assert!(report.has_errors());
        assert_eq!(report.diagnostics[0].code, DiagnosticCode::NotImplemented);
        assert!(!report.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn delete_tombstone_equality_delete_requires_native_predicate_rule() {
        let report = evaluate_delete_tombstone_compatibility(
            DeleteModel::EqualityDelete,
            DeleteModel::EqualityDelete,
        );

        assert_eq!(
            report.level,
            DeleteTombstoneCompatibilityLevel::RequiresEqualityPredicate
        );
        assert!(report.requires_equality_predicate);
        assert_eq!(report.unsupported_model_count, 2);
        assert!(report.has_errors());
    }
    #[test]
    fn delete_tombstone_model_transition_reports_metadata_loss() {
        let report = evaluate_delete_tombstone_compatibility(
            DeleteModel::FileLevelDelete,
            DeleteModel::None,
        );

        assert_eq!(
            report.level,
            DeleteTombstoneCompatibilityLevel::Incompatible
        );
        assert!(report.metadata_loss_reported);
        assert_eq!(report.unsafe_change_count, 1);
        assert!(report.has_errors());
    }
    #[test]
    fn delete_tombstone_unknown_model_is_rejected() {
        let report =
            evaluate_delete_tombstone_compatibility(DeleteModel::Unknown, DeleteModel::Unknown);

        assert_eq!(
            report.level,
            DeleteTombstoneCompatibilityLevel::Incompatible
        );
        assert!(report.requires_explicit_delete_handling);
        assert!(report.has_errors());
        assert_eq!(report.diagnostics[0].code, DiagnosticCode::NotImplemented);
        assert!(!report.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn table_compatibility_plan_native_vortex_has_native_format() {
        assert_eq!(
            TableCompatibilityPlan::native_vortex().table_format,
            TableFormatKind::NativeVortexManifest
        );
    }
    #[test]
    fn table_compatibility_plan_unsupported_has_errors_and_fallback_attempted_false() {
        let p = TableCompatibilityPlan::unsupported(
            TableFormatKind::Unknown,
            "unknown_format",
            "unsupported format",
        );
        assert!(p.has_errors());
        assert_eq!(p.diagnostics[0].code, DiagnosticCode::NotImplemented);
        assert!(!p.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn table_compatibility_plan_human_text_includes_fallback_execution_disabled() {
        assert!(
            TableCompatibilityPlan::native_vortex()
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
    #[test]
    fn table_compatibility_report_has_errors_when_schema_report_has_errors() {
        let plan = TableCompatibilityPlan::native_vortex();
        let mut schema_report = SchemaCompatibilityReport::new(
            SchemaId::new("from").expect("id"),
            SchemaId::new("to").expect("id"),
            SchemaCompatibilityLevel::Incompatible,
        );
        schema_report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedDType,
            "schema_compatibility",
            "incompatible schema",
            Some("Update schema mappings.".to_string()),
        ));
        let report = TableCompatibilityReport::from_plan(plan).with_schema_report(schema_report);
        assert!(report.has_errors());
    }
    #[test]
    fn table_compatibility_report_from_plan_does_not_perform_io_and_preserves_plan() {
        let p = TableCompatibilityPlan::native_vortex();
        let r = TableCompatibilityReport::from_plan(p.clone());
        assert_eq!(r.plan, p);
    }

    fn schema_with_fields(version: u64, fields: Vec<SchemaField>) -> SchemaDefinition {
        let mut schema = SchemaDefinition::new(
            SchemaId::new("orders").expect("id"),
            SchemaVersion::new(version).expect("version"),
        );
        for field in fields {
            schema.add_field(field);
        }
        schema
    }

    fn field(id: &str, name: &str, dtype: LogicalDType, nullability: Nullability) -> SchemaField {
        SchemaField::new(FieldName::new(name).expect("name"), dtype, nullability)
            .with_id(FieldId::new(id).expect("field id"))
    }

    #[test]
    fn schema_evolution_add_nullable_field_is_read_compatible_without_io_or_fallback() {
        let from = schema_with_fields(
            1,
            vec![field(
                "f1",
                "order_id",
                LogicalDType::Int64,
                Nullability::NonNullable,
            )],
        );
        let to = schema_with_fields(
            2,
            vec![
                field(
                    "f1",
                    "order_id",
                    LogicalDType::Int64,
                    Nullability::NonNullable,
                ),
                field("f2", "region", LogicalDType::Utf8, Nullability::Nullable),
            ],
        );

        let report = evaluate_schema_evolution_compatibility(
            &from,
            &to,
            &SchemaEvolutionPolicy::default_conservative(),
        );

        assert_eq!(
            report.compatibility.level,
            SchemaCompatibilityLevel::ReadCompatible
        );
        assert!(report.read_supported);
        assert!(!report.write_supported);
        assert_eq!(report.safe_change_count, 1);
        assert_eq!(report.unsafe_change_count, 0);
        assert!(!report.data_read);
        assert!(!report.write_io);
        assert!(!report.catalog_io);
        assert!(!report.object_store_io);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn schema_evolution_rename_with_stable_field_id_is_safe() {
        let from = schema_with_fields(
            1,
            vec![field(
                "f1",
                "status",
                LogicalDType::Utf8,
                Nullability::Nullable,
            )],
        );
        let to = schema_with_fields(
            2,
            vec![field(
                "f1",
                "order_status",
                LogicalDType::Utf8,
                Nullability::Nullable,
            )],
        );

        let report = evaluate_schema_evolution_compatibility(
            &from,
            &to,
            &SchemaEvolutionPolicy::default_conservative(),
        );

        assert_eq!(report.field_id_required_count, 1);
        assert_eq!(report.missing_field_id_count, 0);
        assert_eq!(report.safe_change_count, 1);
        assert!(!report.has_errors());
    }

    #[test]
    fn schema_evolution_rename_without_field_id_is_rejected() {
        let from = schema_with_fields(
            1,
            vec![SchemaField::new(
                FieldName::new("status").expect("name"),
                LogicalDType::Utf8,
                Nullability::Nullable,
            )],
        );
        let to = schema_with_fields(
            2,
            vec![SchemaField::new(
                FieldName::new("order_status").expect("name"),
                LogicalDType::Utf8,
                Nullability::Nullable,
            )],
        );

        let report = evaluate_schema_evolution_compatibility(
            &from,
            &to,
            &SchemaEvolutionPolicy::default_conservative(),
        );

        assert_eq!(
            report.compatibility.level,
            SchemaCompatibilityLevel::Incompatible
        );
        assert_eq!(report.field_id_required_count, 1);
        assert_eq!(report.missing_field_id_count, 1);
        assert_eq!(report.unsafe_change_count, 1);
        assert_eq!(
            report.compatibility.diagnostics[0].code,
            DiagnosticCode::NotImplemented
        );
        assert!(!report.compatibility.diagnostics[0].fallback.attempted);
    }

    #[test]
    fn schema_evolution_safe_widening_requires_cast() {
        let from = schema_with_fields(
            1,
            vec![field(
                "f1",
                "amount",
                LogicalDType::Int64,
                Nullability::Nullable,
            )],
        );
        let to = schema_with_fields(
            2,
            vec![field(
                "f1",
                "amount",
                LogicalDType::Float64,
                Nullability::Nullable,
            )],
        );

        let report = evaluate_schema_evolution_compatibility(
            &from,
            &to,
            &SchemaEvolutionPolicy::default_conservative(),
        );

        assert_eq!(
            report.compatibility.level,
            SchemaCompatibilityLevel::RequiresCast
        );
        assert!(report.requires_cast);
        assert_eq!(report.safe_change_count, 1);
        assert_eq!(report.unsafe_change_count, 0);
    }

    #[test]
    fn schema_evolution_drop_field_requires_projection() {
        let from = schema_with_fields(
            1,
            vec![
                field(
                    "f1",
                    "order_id",
                    LogicalDType::Int64,
                    Nullability::NonNullable,
                ),
                field("f2", "status", LogicalDType::Utf8, Nullability::Nullable),
            ],
        );
        let to = schema_with_fields(
            2,
            vec![field(
                "f1",
                "order_id",
                LogicalDType::Int64,
                Nullability::NonNullable,
            )],
        );

        let report = evaluate_schema_evolution_compatibility(
            &from,
            &to,
            &SchemaEvolutionPolicy::default_conservative(),
        );

        assert_eq!(
            report.compatibility.level,
            SchemaCompatibilityLevel::RequiresProjection
        );
        assert!(report.requires_projection);
        assert_eq!(report.safe_change_count, 1);
    }
}
