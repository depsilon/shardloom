//! Strict, deliberately loaded resource declarations for every executable CLI path.

use shardloom_core::{
    ExecutionResourceLimits, ExecutionResourceOrigin, ExecutionResourceRequest, ExecutionResources,
    OutputFormat, Result, ShardLoomError,
};

/// Optional command arguments are inert until a complete allocation is resolved.
/// `Default` constructs an empty declaration, never a numeric resource grant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ResourceArguments {
    pub memory_gb: Option<String>,
    pub memory_bytes: Option<String>,
    pub max_parallelism: Option<String>,
    memory_origin: Option<String>,
    parallelism_origin: Option<String>,
    memory_limit_bytes: Option<String>,
    parallelism_limit: Option<String>,
    inherited: Option<ExecutionResources>,
}

impl ResourceArguments {
    /// Extract named resource options without performing command-specific work.
    pub(crate) fn take_required(
        mut args: impl Iterator<Item = String>,
        value_options: &[&str],
    ) -> Result<(std::vec::IntoIter<String>, ExecutionResources)> {
        let mut arguments = Self::default();
        let mut remaining = Vec::new();
        while let Some(argument) = args.next() {
            // Command option values are opaque, even when their spelling is a
            // resource flag (for example an idempotency key).
            if value_options.contains(&argument.as_str()) {
                remaining.push(argument);
                if let Some(value) = args.next() {
                    remaining.push(value);
                }
                continue;
            }
            if !arguments.parse_flag(&argument, &mut args)? {
                remaining.push(argument);
            }
        }
        Ok((remaining.into_iter(), arguments.resolve()?))
    }

    /// Read a complete resource tail. Historical explicit GiB/lane positionals
    /// remain accepted; named options provide bytes, origins and ceilings.
    pub(crate) fn parse_complete(args: impl Iterator<Item = String>) -> Result<ExecutionResources> {
        let mut args = args.peekable();
        let mut arguments = Self::default();
        arguments.parse_legacy_prefix(&mut args)?;
        while let Some(flag) = args.next() {
            if !arguments.parse_flag(&flag, &mut args)? {
                return Err(configuration_error(format!(
                    "unknown resource option: {flag}"
                )));
            }
        }
        arguments.resolve()
    }

    pub(crate) fn parse_legacy_prefix(
        &mut self,
        args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    ) -> Result<()> {
        if args.peek().is_some_and(|value| !value.starts_with("--")) {
            if self.memory_gb.is_some()
                || self.memory_bytes.is_some()
                || self.max_parallelism.is_some()
            {
                return Err(configuration_error(
                    "resource values may be declared only once",
                ));
            }
            self.memory_gb = args.next();
            self.max_parallelism = args.next();
        }
        Ok(())
    }

    /// Consume only recognized flags; the caller retains its own command grammar.
    pub(crate) fn parse_flag(
        &mut self,
        flag: &str,
        args: &mut impl Iterator<Item = String>,
    ) -> Result<bool> {
        let destination = match flag {
            "--memory-gb" => &mut self.memory_gb,
            "--memory-bytes" => &mut self.memory_bytes,
            "--max-parallelism" => &mut self.max_parallelism,
            "--memory-origin" => &mut self.memory_origin,
            "--parallelism-origin" => &mut self.parallelism_origin,
            "--memory-limit-bytes" => &mut self.memory_limit_bytes,
            "--parallelism-limit" => &mut self.parallelism_limit,
            "--resources-from-env" => {
                if self.inherited.is_some() {
                    return Err(configuration_error(
                        "--resources-from-env may be declared only once",
                    ));
                }
                self.inherited = Some(Self::environment(|name| std::env::var(name))?);
                return Ok(true);
            }
            _ => return Ok(false),
        };
        if destination.is_some() {
            return Err(configuration_error(format!(
                "{flag} may be declared only once"
            )));
        }
        *destination =
            Some(args.next().ok_or_else(|| {
                configuration_error(format!("{flag} requires an explicit value"))
            })?);
        Ok(true)
    }

    pub(crate) fn optional(&self) -> Result<Option<ExecutionResources>> {
        if self == &Self::default() {
            Ok(None)
        } else {
            self.resolve().map(Some)
        }
    }

    pub(crate) fn resolve(&self) -> Result<ExecutionResources> {
        let request = ExecutionResourceRequest {
            memory_gb: integer(self.memory_gb.as_deref(), "memory_gb (GiB)")?,
            memory_bytes: integer(self.memory_bytes.as_deref(), "memory_bytes")?,
            max_parallelism: integer(self.max_parallelism.as_deref(), "max_parallelism")?,
            origin: ExecutionResourceOrigin::ExecutionCall,
        };
        let limits = if self.memory_limit_bytes.is_some() || self.parallelism_limit.is_some() {
            Some(ExecutionResourceLimits::new(
                integer(self.memory_limit_bytes.as_deref(), "memory_bytes ceiling")?,
                integer(self.parallelism_limit.as_deref(), "max_parallelism ceiling")?,
            )?)
        } else {
            None
        };
        let resources = ExecutionResources::resolve(request, self.inherited, limits)?;
        if self.memory_origin.is_some()
            && request.memory_gb.is_none()
            && request.memory_bytes.is_none()
        {
            return Err(configuration_error(
                "--memory-origin requires an explicit memory declaration",
            ));
        }
        if self.parallelism_origin.is_some() && request.max_parallelism.is_none() {
            return Err(configuration_error(
                "--parallelism-origin requires --max-parallelism",
            ));
        }
        ExecutionResources::from_declaration(
            resources.memory_bytes(),
            resources.max_parallelism(),
            self.memory_origin
                .as_deref()
                .map(str::parse)
                .transpose()?
                .unwrap_or(resources.memory_origin()),
            self.parallelism_origin
                .as_deref()
                .map(str::parse)
                .transpose()?
                .unwrap_or(resources.parallelism_origin()),
            resources.limits(),
        )
    }

    fn environment(
        mut read: impl FnMut(&str) -> std::result::Result<String, std::env::VarError>,
    ) -> Result<ExecutionResources> {
        let mut value = |name| match read(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(configuration_error(format!(
                "{name} must contain a positive ASCII integer"
            ))),
        };
        let memory_gb = value("SHARDLOOM_MEMORY_GB")?;
        let memory_bytes = value("SHARDLOOM_MEMORY_BYTES")?;
        let max_parallelism = value("SHARDLOOM_MAX_PARALLELISM")?;
        ExecutionResources::resolve(
            ExecutionResourceRequest {
                memory_gb: integer(memory_gb.as_deref(), "SHARDLOOM_MEMORY_GB")?,
                memory_bytes: integer(memory_bytes.as_deref(), "SHARDLOOM_MEMORY_BYTES")?,
                max_parallelism: integer(max_parallelism.as_deref(), "SHARDLOOM_MAX_PARALLELISM")?,
                origin: ExecutionResourceOrigin::Environment,
            },
            None,
            None,
        )
    }
}

/// Admit actual fixture/file work before its handler can inspect any input.
pub(crate) fn require_for_command(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
    command: &str,
    value_options: &[&str],
) -> std::result::Result<(std::vec::IntoIter<String>, ExecutionResources), std::process::ExitCode> {
    ResourceArguments::take_required(args, value_options).map_err(|error| {
        crate::cli_output::emit_error(command, format, "execution resources are required", &error)
    })
}

pub(crate) fn with_declaration_fields(
    mut fields: Vec<(String, String)>,
    resources: ExecutionResources,
) -> Vec<(String, String)> {
    append_declaration_fields(&mut fields, resources);
    fields
}

pub(crate) fn command_args(resources: ExecutionResources) -> Vec<String> {
    let mut args = vec![
        "--memory-bytes".into(),
        resources.memory_bytes().to_string(),
        "--max-parallelism".into(),
        resources.max_parallelism().to_string(),
        "--memory-origin".into(),
        resources.memory_origin().as_str().into(),
        "--parallelism-origin".into(),
        resources.parallelism_origin().as_str().into(),
    ];
    if let Some(limits) = resources.limits() {
        if let Some(bytes) = limits.memory_bytes() {
            args.extend(["--memory-limit-bytes".into(), bytes.to_string()]);
        }
        if let Some(lanes) = limits.max_parallelism() {
            args.extend(["--parallelism-limit".into(), lanes.to_string()]);
        }
    }
    args
}

/// Attach a declaration without turning missing measurements into zero usage.
pub(crate) fn append_declaration_fields(
    fields: &mut Vec<(String, String)>,
    resources: ExecutionResources,
) {
    replace_fields(fields, resources.evidence_fields());
    for (key, value) in [
        ("admission_status", "not_observed"),
        ("admitted_memory_bytes", "unavailable"),
        ("admitted_max_parallelism", "unavailable"),
        ("admission_policy", "not_observed"),
        ("observed_native_reserved_bytes", "unavailable"),
        ("observed_native_peak_reserved_bytes", "unavailable"),
        ("memory_observation_scope", "not_instrumented"),
        ("observed_peak_active_lanes", "unavailable"),
        ("observed_spill_io_performed", "unavailable"),
        ("observed_spill_bytes", "unavailable"),
        ("spill_observation_scope", "not_instrumented"),
        ("observed_process_peak_rss_bytes", "unavailable"),
    ] {
        let key = format!("execution_resource_{key}");
        if !fields.iter().any(|(existing, _)| existing == &key) {
            fields.push((key, value.into()));
        }
    }
}

pub(crate) fn append_admission_fields(
    fields: &mut Vec<(String, String)>,
    memory_bytes: u64,
    max_parallelism: usize,
    policy: &str,
) {
    replace_fields(
        fields,
        [
            (
                "execution_resource_admission_status".into(),
                "admitted".into(),
            ),
            (
                "execution_resource_admitted_memory_bytes".into(),
                memory_bytes.to_string(),
            ),
            (
                "execution_resource_admitted_max_parallelism".into(),
                max_parallelism.to_string(),
            ),
            ("execution_resource_admission_policy".into(), policy.into()),
        ],
    );
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) fn append_resident_snapshot_fields(
    fields: &mut Vec<(String, String)>,
    snapshot: &shardloom_vortex::resident_session::ResidentSessionSnapshot,
) {
    append_admission_fields(
        fields,
        snapshot.memory.limit_bytes,
        snapshot.admitted_max_parallelism,
        "shared_native_session;integer_lanes_bounded_by_declared_allocation_and_local_capacity",
    );
    append_memory_observation_fields(
        fields,
        Some(snapshot.memory.reserved_bytes),
        snapshot.memory.peak_reserved_bytes,
        "session_pool_lifetime_including_preparation_and_retained_owners;excludes_cli_envelope_formatting_provider_bypass_allocations_and_process_rss",
    );
}

pub(crate) fn append_memory_observation_fields(
    fields: &mut Vec<(String, String)>,
    reserved_bytes: Option<u64>,
    peak_reserved_bytes: u64,
    scope: &str,
) {
    replace_fields(
        fields,
        [
            (
                "execution_resource_observed_native_reserved_bytes".into(),
                reserved_bytes.map_or_else(|| "unavailable".into(), |value| value.to_string()),
            ),
            (
                "execution_resource_observed_native_peak_reserved_bytes".into(),
                peak_reserved_bytes.to_string(),
            ),
            (
                "execution_resource_memory_observation_scope".into(),
                scope.into(),
            ),
        ],
    );
}

pub(crate) fn append_spill_observation_fields(
    fields: &mut Vec<(String, String)>,
    performed: bool,
    bytes_written: Option<u64>,
) {
    let scope = if !performed {
        "no_spill_io"
    } else if bytes_written.is_some() {
        "cumulative_native_spill_payload_bytes_written;excludes_workspace_markers_and_filesystem_overhead"
    } else {
        "spill_io_observed;byte_count_not_instrumented"
    };
    replace_fields(
        fields,
        [
            (
                "execution_resource_observed_spill_io_performed".into(),
                performed.to_string(),
            ),
            (
                "execution_resource_observed_spill_bytes".into(),
                bytes_written
                    .or((!performed).then_some(0))
                    .map_or_else(|| "unavailable".into(), |value| value.to_string()),
            ),
            (
                "execution_resource_spill_observation_scope".into(),
                scope.into(),
            ),
        ],
    );
}

fn replace_fields(
    fields: &mut Vec<(String, String)>,
    values: impl IntoIterator<Item = (String, String)>,
) {
    for (key, value) in values {
        fields.retain(|(existing, _)| existing != &key);
        fields.push((key, value));
    }
}

fn integer<T: std::str::FromStr>(value: Option<&str>, name: &str) -> Result<Option<T>> {
    value
        .map(|value| {
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(configuration_error(format!(
                    "{name} must be a positive ASCII integer"
                )));
            }
            value.parse().map_err(|_| {
                configuration_error(format!("{name} exceeds the supported integer range"))
            })
        })
        .transpose()
}

fn configuration_error(message: impl Into<String>) -> ShardLoomError {
    ShardLoomError::new(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_extraction_preserves_opaque_command_values() {
        let (remaining, resources) = ResourceArguments::take_required(
            [
                "source",
                "--idempotency-key",
                "--memory-gb",
                "--memory-bytes",
                "1500000001",
                "--max-parallelism",
                "3",
            ]
            .into_iter()
            .map(str::to_owned),
            &["--idempotency-key"],
        )
        .unwrap();
        assert_eq!(
            remaining.collect::<Vec<_>>(),
            ["source", "--idempotency-key", "--memory-gb"]
        );
        assert_eq!(resources.memory_bytes(), 1_500_000_001);
        assert_eq!(resources.max_parallelism(), 3);
    }

    #[test]
    fn resource_evidence_keeps_permission_admission_and_measurement_distinct() {
        let resources = ExecutionResources::from_declaration(
            1_500_000_001,
            17,
            ExecutionResourceOrigin::Platform,
            ExecutionResourceOrigin::ExecutionCall,
            Some(ExecutionResourceLimits::new(Some(2_000_000_000), Some(24)).unwrap()),
        )
        .unwrap();
        let mut fields = Vec::new();
        append_declaration_fields(&mut fields, resources);
        let get = |fields: &[(String, String)], key: &str| {
            fields
                .iter()
                .find(|(name, _)| name == key)
                .unwrap()
                .1
                .clone()
        };
        assert_eq!(
            get(&fields, "execution_resource_admitted_memory_bytes"),
            "unavailable"
        );
        assert_eq!(
            get(
                &fields,
                "execution_resource_observed_native_peak_reserved_bytes"
            ),
            "unavailable"
        );
        append_admission_fields(&mut fields, 1_500_000_001, 8, "shared_native_session");
        append_memory_observation_fields(&mut fields, Some(256), 1024, "shared_pool_lifetime");
        append_spill_observation_fields(&mut fields, true, None);
        // Attaching a later declaration must preserve observations and unique keys.
        append_declaration_fields(&mut fields, resources);
        assert_eq!(
            get(&fields, "execution_resource_declared_memory_bytes"),
            "1500000001"
        );
        assert_eq!(
            get(&fields, "execution_resource_declared_memory_gb"),
            "unavailable"
        );
        assert_eq!(
            get(&fields, "execution_resource_declared_max_parallelism"),
            "17"
        );
        assert_eq!(get(&fields, "execution_resource_memory_origin"), "platform");
        assert_eq!(
            get(&fields, "execution_resource_parallelism_origin"),
            "execution_call"
        );
        assert_eq!(
            get(&fields, "execution_resource_admitted_max_parallelism"),
            "8"
        );
        assert_eq!(
            get(
                &fields,
                "execution_resource_observed_native_peak_reserved_bytes"
            ),
            "1024"
        );
        assert_eq!(
            get(&fields, "execution_resource_observed_spill_io_performed"),
            "true"
        );
        assert_eq!(
            get(&fields, "execution_resource_observed_spill_bytes"),
            "unavailable"
        );
        assert_eq!(
            get(&fields, "execution_resource_observed_peak_active_lanes"),
            "unavailable"
        );
        assert_eq!(
            get(
                &fields,
                "execution_resource_whole_process_memory_limit_enforced"
            ),
            "false"
        );
        let keys = fields
            .iter()
            .map(|(key, _)| key)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(keys.len(), fields.len());
    }

    fn parse(values: &[&str]) -> Result<ResourceArguments> {
        let mut arguments = ResourceArguments::default();
        let mut values = values.iter().map(|value| (*value).to_string());
        while let Some(flag) = values.next() {
            assert!(arguments.parse_flag(&flag, &mut values)?);
        }
        Ok(arguments)
    }

    #[test]
    fn empty_descriptions_are_inert_but_execution_requires_both_values() {
        assert_eq!(ResourceArguments::default().optional().unwrap(), None);
        for values in [
            vec![],
            vec!["--memory-gb", "16"],
            vec!["--max-parallelism", "8"],
        ] {
            let error = parse(&values).unwrap().resolve().unwrap_err();
            assert_eq!(
                error.to_diagnostic().code,
                shardloom_core::DiagnosticCode::ConfigurationError
            );
            assert!(
                error
                    .message()
                    .contains("missing required execution resources")
            );
        }
    }

    #[test]
    fn exact_bytes_origins_and_authorization_survive_transport() {
        let declaration = parse(&[
            "--memory-bytes",
            "1500000001",
            "--max-parallelism",
            "3",
            "--memory-origin",
            "platform",
            "--parallelism-origin",
            "execution_call",
            "--memory-limit-bytes",
            "2000000000",
            "--parallelism-limit",
            "4",
        ])
        .unwrap()
        .resolve()
        .unwrap();
        let args = command_args(declaration);
        assert_eq!(
            parse(&args.iter().map(String::as_str).collect::<Vec<_>>())
                .unwrap()
                .resolve()
                .unwrap(),
            declaration
        );
        assert_eq!(declaration.whole_gib(), None);
        assert_eq!(
            declaration.memory_origin(),
            ExecutionResourceOrigin::Platform
        );
        let mut excessive = parse(&[
            "--memory-bytes",
            "2000000001",
            "--max-parallelism",
            "3",
            "--memory-limit-bytes",
            "2000000000",
        ])
        .unwrap();
        assert!(excessive.resolve().is_err());
        excessive.memory_bytes = Some("1".into());
        excessive.parallelism_limit = Some("2".into());
        assert!(excessive.resolve().is_err());
    }

    #[test]
    fn malformed_zero_conflicting_and_overflowing_values_are_never_repaired() {
        for value in [
            "",
            "eight",
            "+2",
            " 2",
            "2.0",
            "-1",
            "0",
            "18446744073709551616",
        ] {
            assert!(
                parse(&["--memory-bytes", value, "--max-parallelism", "2"])
                    .unwrap()
                    .resolve()
                    .is_err()
            );
            assert!(
                parse(&["--memory-bytes", "1", "--max-parallelism", value])
                    .unwrap()
                    .resolve()
                    .is_err()
            );
        }
        assert!(
            parse(&[
                "--memory-gb",
                "18446744073709551615",
                "--max-parallelism",
                "2"
            ])
            .unwrap()
            .resolve()
            .is_err()
        );
        assert!(
            parse(&[
                "--memory-gb",
                "1",
                "--memory-bytes",
                "1073741824",
                "--max-parallelism",
                "2"
            ])
            .unwrap()
            .resolve()
            .is_err()
        );
        assert!(parse(&["--max-parallelism", "2", "--max-parallelism", "3"]).is_err());
    }

    #[test]
    fn deliberate_environment_loading_is_strict_and_overrides_retain_origin() {
        let environment = |entries: &[(&str, &str)]| {
            ResourceArguments::environment(|name| {
                entries
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| (*value).into())
                    .ok_or(std::env::VarError::NotPresent)
            })
        };
        assert!(environment(&[]).is_err());
        assert!(environment(&[("SHARDLOOM_MEMORY_GB", "16")]).is_err());
        assert!(
            environment(&[
                ("SHARDLOOM_MEMORY_GB", "16"),
                ("SHARDLOOM_MAX_PARALLELISM", "eight")
            ])
            .is_err()
        );
        let inherited = environment(&[
            ("SHARDLOOM_MEMORY_BYTES", "1500000001"),
            ("SHARDLOOM_MAX_PARALLELISM", "8"),
        ])
        .unwrap();
        let overridden = ResourceArguments {
            inherited: Some(inherited),
            max_parallelism: Some("2".into()),
            ..ResourceArguments::default()
        }
        .resolve()
        .unwrap();
        assert_eq!(overridden.memory_bytes(), 1_500_000_001);
        assert_eq!(
            overridden.memory_origin(),
            ExecutionResourceOrigin::Environment
        );
        assert_eq!(
            overridden.parallelism_origin(),
            ExecutionResourceOrigin::ExecutionCall
        );
    }
}
