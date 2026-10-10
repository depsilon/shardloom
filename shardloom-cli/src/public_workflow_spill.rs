//! Inert public spill permission, attached to the already selected native provider.

use super::{
    CommandStatus, DatasetUri, Diagnostic, DiagnosticCode, PublicVortexPrimitive,
    PublicWorkflowRoutePlan, PublicWorkflowRouteRequest, ShardLoomError, blocked_route,
    native_vortex_primitive_arg_for_request, normalized_vortex_primitive,
    public_workflow_effective_resource_envelope, vortex_primitive_execution,
};
use shardloom_vortex::{
    VortexAggregateSpillPolicy, VortexQueryPrimitiveRequest, VortexSortSpillPolicy,
};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Options {
    workspace: PathBuf,
    quota_bytes: u64,
    buffer_bytes: u64,
}

impl Options {
    pub(super) fn parse(raw: &str) -> Result<Self, ShardLoomError> {
        if raw.len() > 32 * 1024 {
            return Err(invalid("spill configuration exceeds 32 KiB"));
        }
        let options: Self = serde_json::from_str(raw)
            .map_err(|error| invalid(&format!("invalid spill configuration: {error}")))?;
        // Construction validates values without probing or creating the path.
        options.relational()?;
        Ok(options)
    }

    pub(super) fn relational(
        &self,
    ) -> Result<shardloom_vortex::relational_query::VortexRelationalSpillPolicy, ShardLoomError>
    {
        shardloom_vortex::relational_query::VortexRelationalSpillPolicy::new(
            self.workspace.clone(),
            self.quota_bytes,
            self.buffer_bytes,
        )
    }

    fn attach(&self, primitive: &mut VortexQueryPrimitiveRequest) -> Result<(), ShardLoomError> {
        if let Some(sort) = &mut primitive.sort_rows {
            if sort.spill.is_some() {
                return Err(invalid(
                    "spill is declared in both --spill and the sort payload",
                ));
            }
            sort.spill = Some(VortexSortSpillPolicy::new(
                self.workspace.clone(),
                self.quota_bytes,
                self.buffer_bytes,
            )?);
        } else if let Some(aggregate) = &mut primitive.simple_aggregate {
            if aggregate.spill.is_some() {
                return Err(invalid(
                    "spill is declared in both --spill and the aggregate payload",
                ));
            }
            aggregate.spill = Some(VortexAggregateSpillPolicy::new(
                self.workspace.clone(),
                self.quota_bytes,
                self.buffer_bytes,
            )?);
        } else {
            return Err(invalid(
                "this native provider has no admitted spill contract",
            ));
        }
        Ok(())
    }
}

fn invalid(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "public workflow spill: {reason}; no fallback execution was attempted"
    ))
}

pub(super) fn attach(
    request: &PublicWorkflowRouteRequest,
    primitive: &mut VortexQueryPrimitiveRequest,
) -> Result<(), ShardLoomError> {
    if let Some(options) = &request.spill {
        options.attach(primitive)?;
    }
    Ok(())
}

pub(super) fn validate_route(
    request: &PublicWorkflowRouteRequest,
    plan: PublicWorkflowRoutePlan,
) -> PublicWorkflowRoutePlan {
    if request.spill.is_none() || plan.status != CommandStatus::Success {
        return plan;
    }
    match validate(request, &plan) {
        Ok(()) => plan,
        Err(error) => blocked_route(
            "cg21.route.spill_not_admitted",
            "the declared spill request is not admitted by the selected native provider",
            Diagnostic::unsupported(
                DiagnosticCode::UnsupportedEffect,
                "public_workflow_route.spill",
                error.to_string(),
                Some("use an admitted relational ordering or specialized sort/aggregate spill request with an existing local workspace and explicit byte limits".into()),
            ),
        ),
    }
}

fn validate(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> Result<(), ShardLoomError> {
    let Some(options) = &request.spill else {
        return Ok(());
    };
    if !cfg!(all(
        feature = "vortex-local-primitives",
        feature = "vortex-write",
        unix
    )) {
        return Err(invalid(
            "spill requires vortex-local-primitives, vortex-write and Unix file identity",
        ));
    }
    let budget = public_workflow_effective_resource_envelope(request)?.memory_bytes();
    if options.buffer_bytes > budget {
        return Err(invalid(
            "spill buffer_bytes exceeds the declared query memory budget",
        ));
    }
    match plan.route_id {
        // The nested native route has already validated the same spill options.
        "native_vortex_relational_collect"
        | "native_vortex_relational_write"
        | "local_file_prepare_once_first_query" => Ok(()),
        "native_vortex_sort_rows"
        | "native_vortex_aggregate"
        | "native_vortex_primitive_row_export" => {
            let primitive = normalized_vortex_primitive(request)
                .filter(|primitive| {
                    matches!(
                        primitive,
                        PublicVortexPrimitive::SortRows | PublicVortexPrimitive::Aggregate
                    )
                })
                .ok_or_else(|| invalid("this native provider has no admitted spill contract"))?;
            let arg = native_vortex_primitive_arg_for_request(request, primitive)?;
            let mut typed = vortex_primitive_execution::parse_vortex_primitive_request(
                DatasetUri::new("public-spill-validation.vortex")?,
                &arg,
            )?;
            options.attach(&mut typed)
        }
        _ => Err(invalid(
            "this public route has no admitted native spill contract",
        )),
    }
}

pub(super) fn append_request_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
) {
    fields.push((
        "public_workflow_spill_requested".into(),
        request.spill.is_some().to_string(),
    ));
    if let Some(options) = &request.spill {
        fields.extend([
            (
                "public_workflow_spill_workspace".into(),
                options.workspace.display().to_string(),
            ),
            (
                "public_workflow_spill_quota_bytes".into(),
                options.quota_bytes.to_string(),
            ),
            (
                "public_workflow_spill_buffer_bytes".into(),
                options.buffer_bytes.to_string(),
            ),
            (
                "public_workflow_spill_scope".into(),
                "selected_native_provider_with_shared_query_resource_admission".into(),
            ),
        ]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> Options {
        Options::parse(r#"{"workspace":"/not-created/shardloom-spill-test","quota_bytes":67108864,"buffer_bytes":2097152}"#).unwrap()
    }

    #[test]
    fn public_spill_configuration_is_typed_bounded_and_inert() {
        let declared = options();
        assert!(!declared.workspace.exists());
        for invalid in [
            r#"{"workspace":"relative","quota_bytes":67108864,"buffer_bytes":2097152}"#,
            r#"{"workspace":"/absent","quota_bytes":0,"buffer_bytes":2097152}"#,
            r#"{"workspace":"/absent","quota_bytes":67108864,"buffer_bytes":1}"#,
            r#"{"workspace":"/absent","quota_bytes":67108864,"buffer_bytes":2097152,"fallback":true}"#,
            r#"{"workspace":"/absent","quota_bytes":67108864,"buffer_bytes":-1}"#,
            r#"{"workspace":"/absent","quota_bytes":67108864,"buffer_bytes":true}"#,
        ] {
            assert!(Options::parse(invalid).is_err(), "{invalid}");
        }
        assert!(Options::parse(&" ".repeat(32 * 1024 + 1)).is_err());
        let text = r#"{"workspace":"/absent","quota_bytes":67108864,"buffer_bytes":2097152}"#;
        assert!(
            PublicWorkflowRouteRequest::parse(
                ["sql", "--spill", text, "--spill", text]
                    .into_iter()
                    .map(str::to_owned)
            )
            .is_err()
        );
    }

    #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
    #[test]
    fn public_spill_keeps_specialized_provider_and_denies_duplicate_permission() {
        use super::super::{effective_public_workflow_request, plan_public_workflow_route};
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "/not-created/source.vortex",
                "--input-format",
                "vortex",
                "--bounded",
                "true",
                "--vortex-primitive",
                "sort_rows",
                "--vortex-sort-rows",
                r#"{"order_by":[{"column":"identifier","descending":false}]}"#,
                "--vortex-source-order-limit",
                "10",
                "--memory-gb",
                "1",
                "--max-parallelism",
                "2",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        let mut request = effective_public_workflow_request(&request);
        request.spill = Some(options());
        let route = plan_public_workflow_route(&request);
        assert_eq!(
            route.status,
            CommandStatus::Success,
            "{:?}",
            route.diagnostics
        );
        assert_eq!(route.route_id, "native_vortex_sort_rows");
        let arg =
            native_vortex_primitive_arg_for_request(&request, PublicVortexPrimitive::SortRows)
                .unwrap();
        let mut typed = vortex_primitive_execution::parse_vortex_primitive_request(
            DatasetUri::new("/not-created/source.vortex").unwrap(),
            &arg,
        )
        .unwrap();
        attach(&request, &mut typed).unwrap();
        assert_eq!(
            typed
                .sort_rows
                .as_ref()
                .unwrap()
                .spill
                .as_ref()
                .unwrap()
                .memory_bytes,
            2 << 20
        );
        assert!(attach(&request, &mut typed).is_err());
        request.spill.as_mut().unwrap().buffer_bytes = 2 << 30;
        assert_eq!(
            plan_public_workflow_route(&request).blocker_id,
            "cg21.route.spill_not_admitted"
        );
        assert!(!options().workspace.exists());
    }

    #[cfg(all(feature = "vortex-local-primitives", feature = "vortex-write", unix))]
    #[test]
    fn public_spill_relational_inspection_and_unsupported_routes_have_no_file_effects() {
        use super::super::{
            effective_public_workflow_request, execution_attachment_fields,
            plan_public_workflow_route, route_fields,
        };
        let request = PublicWorkflowRouteRequest::parse([
            "sql", "--sql", "SELECT identifier FROM (SELECT identifier FROM '/not-created/source.vortex') AS derived ORDER BY identifier NULLS LAST",
            "--bounded", "true", "--memory-gb", "1", "--max-parallelism", "7",
        ].into_iter().map(str::to_owned)).unwrap();
        let mut request = effective_public_workflow_request(&request);
        request.spill = Some(options());
        let route = plan_public_workflow_route(&request);
        assert_eq!(
            route.status,
            CommandStatus::Success,
            "{:?}",
            route.diagnostics
        );
        assert_eq!(route.route_id, "native_vortex_relational_collect");
        let fields = route_fields(&request, &route);
        assert!(fields.contains(&("public_workflow_spill_requested".into(), "true".into())));
        assert!(fields.contains(&("memory_gb".into(), "1".into())));
        assert!(fields.contains(&("max_parallelism".into(), "7".into())));
        let executed = execution_attachment_fields("run", &request, &route);
        for entry in [
            ("public_workflow_spill_requested", "true"),
            ("public_workflow_spill_quota_bytes", "67108864"),
            ("public_workflow_spill_buffer_bytes", "2097152"),
            ("public_workflow_memory_gb", "1"),
            ("public_workflow_max_parallelism", "7"),
        ] {
            assert!(executed.contains(&(entry.0.into(), entry.1.into())));
        }
        let mut plain = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "/not-created/source.vortex",
                "--input-format",
                "vortex",
                "--bounded",
                "true",
                "--vortex-primitive",
                "project",
                "--vortex-columns",
                "identifier",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        plain.spill = Some(options());
        assert_eq!(
            plan_public_workflow_route(&plain).blocker_id,
            "cg21.route.spill_not_admitted"
        );
        assert!(!options().workspace.exists());
    }
}
