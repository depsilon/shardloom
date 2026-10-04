//! Every parsed source leaf normalizes once; strings in SQL values are untouched.

use super::super::PublicSourcePreparations;
use super::super::{declared_sql_source_identifier_matches, infer_input_format_from_ref};
use super::{
    PreparedVortexRelational, PublicWorkflowRouteRequest, ShardLoomError, native_relational,
    native_vortex_materializing_policy,
};
use shardloom_core::DatasetUri;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
use super::super::{
    PreparationFacadeError, auto_prepared_vortex_target_path_with_schema,
    prepare_local_source_for_public_workflow, public_workflow_effective_resource_envelope,
};

pub(super) fn prepare_with_source(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
    source: Option<shardloom_vortex::resident_session::PreparedVortexSource>,
    preparations: PublicSourcePreparations,
) -> Result<(PreparedVortexRelational, usize), ShardLoomError> {
    #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
    let _ = preparations;
    validate_bindings(statement, request)?;
    let mut sources = Sources::default();
    let policy = native_vortex_materializing_policy(request)?;
    let operation = if let Some(source) = source {
        let uri = DatasetUri::new(
            request
                .input_uri
                .clone()
                .ok_or_else(|| failed("source URI is absent"))?,
        )?;
        native_relational::prepare_from_source(statement, policy, uri, source, |path| {
            sources.resolve(path, request)
        })?
    } else {
        native_relational::prepare(statement, policy, |path| sources.resolve(path, request))?
    };
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    sources.preparations.extend(preparations.sources);
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    let operation = operation.with_preparation_sources(sources.preparations)?;
    #[cfg(feature = "vortex-write")]
    let operation = if let Some(spill) = &request.spill {
        operation.with_spill(spill.relational()?)?
    } else {
        operation
    };
    Ok((operation, sources.normalized))
}

pub(super) fn validate_bindings(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
) -> Result<(), ShardLoomError> {
    if request.source_bindings.is_empty() {
        return Ok(());
    }
    let paths = native_relational::source_leaves(statement)?
        .iter()
        .map(|leaf| declared_path(leaf, request))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    for (uri, binding) in &request.source_bindings {
        if !paths.contains(Path::new(uri)) {
            return Err(failed(
                "source declaration is not referenced by the SQL statement",
            ));
        }
        if request.input_uri.as_deref() == Some(uri.as_str())
            && (request
                .input_format
                .as_ref()
                .is_some_and(|format| format != &binding.input_format)
                || request
                    .source_schema
                    .as_ref()
                    .is_some_and(|schema| binding.source_schema.as_ref() != Some(schema)))
        {
            return Err(failed(
                "source declaration conflicts with the primary input format or schema",
            ));
        }
    }
    Ok(())
}

pub(super) fn normalization_required(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
) -> Result<bool, ShardLoomError> {
    let mut required = false;
    for leaf in native_relational::source_leaves(statement)? {
        let path = declared_path(&leaf, request)?;
        let raw = path
            .to_str()
            .ok_or_else(|| failed("source paths must be UTF8"))?;
        let format = declared_format(raw, request)
            .ok_or_else(|| failed("source format is not declared or recognized"))?;
        required |= format != "vortex";
    }
    Ok(required)
}

/// Resolve only parsed, unquoted identifiers against explicit input contracts.
/// Quoted source paths and SQL values are never rewritten or rebound.
fn declared_path(
    leaf: &native_relational::ParsedRelationLeaf,
    request: &PublicWorkflowRouteRequest,
) -> Result<PathBuf, ShardLoomError> {
    if !leaf.declared_identifier {
        return Ok(leaf.path.clone());
    }
    let name = leaf
        .path
        .to_str()
        .ok_or_else(|| failed("source paths must be UTF8"))?;
    let candidates = request
        .input_uri
        .as_deref()
        .into_iter()
        .chain(request.source_bindings.keys().map(String::as_str))
        .filter(|uri| *uri == name || declared_sql_source_identifier_matches(name, uri))
        .collect::<std::collections::BTreeSet<_>>();
    if candidates.len() > 1 {
        return Err(failed(
            "declared SQL source identifier is ambiguous; use an exact quoted source path",
        ));
    }
    Ok(candidates
        .first()
        .map_or_else(|| leaf.path.clone(), PathBuf::from))
}

fn declared_format<'a>(source: &str, request: &'a PublicWorkflowRouteRequest) -> Option<&'a str> {
    request
        .source_bindings
        .get(source)
        .map(|binding| binding.input_format.as_str())
        .or_else(|| {
            (request.input_uri.as_deref() == Some(source))
                .then_some(request.input_format.as_deref())
                .flatten()
        })
        .or_else(|| infer_input_format_from_ref(source))
}

#[derive(Default)]
struct Sources {
    bound: BTreeMap<PathBuf, DatasetUri>,
    normalized: usize,
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    preparations:
        Vec<std::sync::Arc<shardloom_vortex::prepared_source_binding::LocalPreparationIdentity>>,
}

impl Sources {
    fn resolve(
        &mut self,
        leaf: &native_relational::ParsedRelationLeaf,
        request: &PublicWorkflowRouteRequest,
    ) -> Result<DatasetUri, ShardLoomError> {
        let path = declared_path(leaf, request)?;
        if let Some(uri) = self.bound.get(&path) {
            return Ok(uri.clone());
        }
        if self.bound.len() >= 128 {
            return Err(failed("SQL exceeds 128 source leaves"));
        }
        let raw = path
            .to_str()
            .ok_or_else(|| failed("source paths must be UTF8"))?;
        let format = declared_format(raw, request)
            .ok_or_else(|| failed("source format is not declared or recognized"))?;
        let uri = if format == "vortex" {
            DatasetUri::new(raw)?
        } else {
            self.prepare_compatibility(raw, format, request)?
        };
        self.bound.insert(path, uri.clone());
        Ok(uri)
    }

    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    fn prepare_compatibility(
        &mut self,
        source: &str,
        format: &str,
        request: &PublicWorkflowRouteRequest,
    ) -> Result<DatasetUri, ShardLoomError> {
        let schema = request
            .source_bindings
            .get(source)
            .and_then(|binding| binding.source_schema.as_deref())
            .or_else(|| {
                (request.input_uri.as_deref() == Some(source))
                    .then_some(request.source_schema.as_deref())
                    .flatten()
            });
        let target = auto_prepared_vortex_target_path_with_schema(source, format, schema);
        let (memory, parallelism) = public_workflow_effective_resource_envelope(request)?;
        let preparation = prepare_local_source_for_public_workflow(
            source,
            format,
            &target,
            false,
            memory,
            parallelism,
            request.source_fingerprint_policy.as_deref(),
            schema,
        )
        .map_err(|error| match error {
            PreparationFacadeError::Runtime(error) => error,
            PreparationFacadeError::FeatureGated => {
                failed("compatibility preparation is feature gated")
            }
        })?;
        self.preparations.push(
            preparation
                .identity
                .ok_or_else(|| failed("preparation source generation proof is absent"))?,
        );
        self.normalized += 1;
        DatasetUri::new(preparation.target_path.to_string_lossy().into_owned())
    }

    #[cfg(not(all(feature = "vortex-write", feature = "universal-format-io")))]
    #[allow(clippy::unused_self)]
    fn prepare_compatibility(
        &mut self,
        _source: &str,
        _format: &str,
        _request: &PublicWorkflowRouteRequest,
    ) -> Result<DatasetUri, ShardLoomError> {
        Err(failed(
            "compatibility normalization requires vortex-write and universal-format-io",
        ))
    }
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native SQL source preparation: {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::super::super::PublicSourceBinding;
    use super::*;

    #[test]
    fn native_relational_declared_identifiers_resolve_inertly_across_nested_sources() {
        let mut request = PublicWorkflowRouteRequest::new("sql".into());
        request.input_uri = Some("missing/cargo.vortex".into());
        request.input_format = Some("vortex".into());
        request.source_bindings.insert(
            "missing/cargo.vortex".into(),
            PublicSourceBinding {
                input_format: "vortex".into(),
                source_schema: None,
            },
        );
        let sql = "SELECT cargo_id FROM (SELECT cargo_id FROM cargo LIMIT 2) AS q WHERE cargo_id IN (SELECT cargo_id FROM cargo)";
        validate_bindings(sql, &request).unwrap();
        assert!(!normalization_required(sql, &request).unwrap());
        let leaves = native_relational::source_leaves(sql).unwrap();
        assert_eq!(leaves.len(), 1);
        assert_eq!(
            declared_path(leaves.first().unwrap(), &request).unwrap(),
            Path::new("missing/cargo.vortex")
        );

        let quoted = "SELECT * FROM (SELECT cargo_id FROM 'cargo') AS q";
        assert!(
            validate_bindings(quoted, &request)
                .unwrap_err()
                .to_string()
                .contains("not referenced")
        );
        let literal = native_relational::source_leaves(quoted).unwrap();
        assert!(!literal.first().unwrap().declared_identifier);
        assert_eq!(
            declared_path(literal.first().unwrap(), &request).unwrap(),
            Path::new("cargo")
        );

        request.source_bindings.insert(
            "another/cargo.csv".into(),
            PublicSourceBinding {
                input_format: "csv".into(),
                source_schema: Some("cargo_id:utf8".into()),
            },
        );
        assert!(
            validate_bindings(sql, &request)
                .unwrap_err()
                .to_string()
                .contains("ambiguous")
        );
        let exact = "SELECT * FROM (SELECT cargo_id FROM 'missing/cargo.vortex' UNION ALL SELECT cargo_id FROM 'another/cargo.csv') AS q";
        validate_bindings(exact, &request).unwrap();
        assert!(normalization_required(exact, &request).unwrap());
    }
}
