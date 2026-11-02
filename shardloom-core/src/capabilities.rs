#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityStatus {
    Supported,
    PartiallySupported,
    Planned,
    Disabled,
    RequiresExplicitEnablement,
    RequiresConfiguration,
    Unsupported,
}

impl CapabilityStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::PartiallySupported => "partially_supported",
            Self::Planned => "planned",
            Self::Disabled => "disabled",
            Self::RequiresExplicitEnablement => "requires_explicit_enablement",
            Self::RequiresConfiguration => "requires_configuration",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capability {
    pub name: String,
    pub status: CapabilityStatus,
    pub notes: Option<String>,
}

impl Capability {
    #[must_use]
    pub fn new(name: impl Into<String>, status: CapabilityStatus) -> Self {
        Self {
            name: name.into(),
            status,
            notes: None,
        }
    }

    #[must_use]
    pub fn with_notes(
        name: impl Into<String>,
        status: CapabilityStatus,
        notes: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            status,
            notes: Some(notes.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineCapabilities {
    pub engine: String,
    pub version: String,
    pub fallback_execution_allowed: bool,
    pub native_inputs: Vec<Capability>,
    pub native_outputs: Vec<Capability>,
    pub compatibility_outputs: Vec<Capability>,
    pub frontends: Vec<Capability>,
    pub extensions: Vec<Capability>,
}

/// Provider availability supplied by the embedding runtime without probing.
/// Each flag admits only the corresponding implemented local execution scope;
/// it does not certify arbitrary queries, formats, platforms, or resource grants.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // Independent compile/platform admissions.
pub struct RuntimeCapabilityAvailability {
    pub vortex_read: bool,
    pub vortex_write: bool,
    pub local_queries: bool,
    pub compatibility_io: bool,
}

impl EngineCapabilities {
    /// Core-only discovery has no execution providers. An embedding runtime
    /// supplies its actual feature/platform admissions through `for_runtime`.
    #[must_use]
    pub fn current() -> Self {
        Self::for_runtime(RuntimeCapabilityAvailability::default())
    }

    /// Describe admitted local execution using explicit runtime availability.
    /// This remains independent of broad certification and performs no I/O.
    #[must_use]
    pub fn for_runtime(available: RuntimeCapabilityAvailability) -> Self {
        let text_output = available.local_queries && available.vortex_write;
        let binary_output = text_output && available.compatibility_io;
        Self {
            engine: "shardloom".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            fallback_execution_allowed: false,
            native_inputs: vec![runtime_capability(
                "vortex",
                available.vortex_read,
                "admitted local Vortex reads; query, type, generation and resource checks still apply",
            )],
            native_outputs: vec![runtime_capability(
                "vortex",
                available.vortex_write,
                "admitted local Vortex preparation and output; writes require explicit policy",
            )],
            compatibility_outputs: vec![
                runtime_capability(
                    "arrow_ipc",
                    binary_output,
                    "admitted local typed result export",
                ),
                runtime_capability(
                    "parquet",
                    binary_output,
                    "admitted local typed result export",
                ),
                runtime_capability("csv", text_output, "admitted local scalar result export"),
                runtime_capability(
                    "jsonl",
                    text_output,
                    "admitted local structured result export",
                ),
                runtime_capability(
                    "json",
                    text_output,
                    "admitted local structured result export",
                ),
                runtime_capability("avro", binary_output, "admitted local typed result export"),
                runtime_capability("orc", binary_output, "admitted local typed result export"),
                Capability::new("iceberg_compatible", CapabilityStatus::Planned),
                Capability::new("delta_compatible", CapabilityStatus::Planned),
            ],
            frontends: vec![
                Capability::new("cli", CapabilityStatus::PartiallySupported),
                runtime_capability(
                    "sql",
                    available.local_queries,
                    "admitted SQL through the shared local native plan; broader language certification is separate",
                ),
                runtime_capability(
                    "dataframe_api",
                    available.local_queries,
                    "admitted DataFrame declarations through the shared local native plan; broader API certification is separate",
                ),
            ],
            extensions: vec![
                Capability::new("udfs", CapabilityStatus::Planned),
                Capability::new("llm_calls", CapabilityStatus::Planned),
                Capability::new("api_calls", CapabilityStatus::Planned),
                Capability::new("embeddings", CapabilityStatus::Planned),
                Capability::new("vector_search", CapabilityStatus::Planned),
            ],
        }
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let fallback_text = if self.fallback_execution_allowed {
            "enabled"
        } else {
            "disabled"
        };
        format!(
            "engine: {}\nversion: {}\nfallback execution: {}\nnative inputs: {}\nnative outputs: {}\ncompatibility outputs: {}\nfrontends: {}\nscope: admitted local execution; query/type/resource admission and certification remain separate",
            self.engine,
            self.version,
            fallback_text,
            self.native_inputs
                .iter()
                .map(|c| format!("{} ({})", c.name, c.status.as_str()))
                .collect::<Vec<_>>()
                .join(", "),
            self.native_outputs
                .iter()
                .map(|c| format!("{} ({})", c.name, c.status.as_str()))
                .collect::<Vec<_>>()
                .join(", "),
            self.compatibility_outputs
                .iter()
                .map(|c| format!("{} ({})", c.name, c.status.as_str()))
                .collect::<Vec<_>>()
                .join(", "),
            self.frontends
                .iter()
                .map(|c| format!("{} ({})", c.name, c.status.as_str()))
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}

fn runtime_capability(name: &str, available: bool, scope: &str) -> Capability {
    Capability::with_notes(
        name,
        if available {
            CapabilityStatus::PartiallySupported
        } else {
            CapabilityStatus::Disabled
        },
        if available {
            scope.to_string()
        } else {
            format!(
                "execution provider is not enabled for this runtime/platform; scope when enabled: {scope}"
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_execution_is_false() {
        assert!(!EngineCapabilities::current().fallback_execution_allowed());
    }

    #[test]
    fn vortex_in_native_inputs() {
        let capabilities = EngineCapabilities::current();
        assert!(
            capabilities
                .native_inputs
                .iter()
                .any(|c| c.name == "vortex")
        );
    }

    #[test]
    fn vortex_in_native_outputs() {
        let capabilities = EngineCapabilities::current();
        assert!(
            capabilities
                .native_outputs
                .iter()
                .any(|c| c.name == "vortex")
        );
    }

    #[test]
    fn planned_extensions_are_present() {
        let capabilities = EngineCapabilities::current();
        assert!(
            capabilities
                .extensions
                .iter()
                .any(|c| c.name == "llm_calls" && c.status == CapabilityStatus::Planned)
        );
        assert!(
            capabilities
                .extensions
                .iter()
                .any(|c| c.name == "vector_search" && c.status == CapabilityStatus::Planned)
        );
    }

    #[test]
    fn human_text_mentions_fallback_disabled() {
        let text = EngineCapabilities::current().to_human_text();
        assert!(text.contains("fallback execution: disabled"));
    }

    #[test]
    fn runtime_availability_distinguishes_implemented_disabled_and_planned() {
        let core = EngineCapabilities::current();
        assert_eq!(core.native_inputs[0].status, CapabilityStatus::Disabled);
        assert_eq!(core.native_outputs[0].status, CapabilityStatus::Disabled);
        assert_eq!(core.frontends[1].status, CapabilityStatus::Disabled);
        assert_eq!(core.frontends[2].status, CapabilityStatus::Disabled);
        let native = EngineCapabilities::for_runtime(RuntimeCapabilityAvailability {
            vortex_read: true,
            vortex_write: true,
            local_queries: true,
            compatibility_io: true,
        });
        for capability in native
            .native_inputs
            .iter()
            .chain(&native.native_outputs)
            .chain(native.frontends.iter().filter(|c| c.name != "cli"))
        {
            assert_eq!(capability.status, CapabilityStatus::PartiallySupported);
            assert!(capability.notes.is_some());
        }
        assert!(
            native
                .compatibility_outputs
                .iter()
                .filter(|c| !c.name.ends_with("_compatible"))
                .all(|c| c.status == CapabilityStatus::PartiallySupported)
        );
        assert!(
            native
                .compatibility_outputs
                .iter()
                .filter(|c| c.name.ends_with("_compatible"))
                .all(|c| c.status == CapabilityStatus::Planned)
        );
        assert!(!native.fallback_execution_allowed());
    }

    #[test]
    fn writer_and_query_admissions_are_independent() {
        let preparation = EngineCapabilities::for_runtime(RuntimeCapabilityAvailability {
            vortex_write: true,
            ..RuntimeCapabilityAvailability::default()
        });
        assert_eq!(
            preparation.native_outputs[0].status,
            CapabilityStatus::PartiallySupported
        );
        assert_eq!(preparation.frontends[1].status, CapabilityStatus::Disabled);
        assert!(
            preparation
                .compatibility_outputs
                .iter()
                .filter(|c| !c.name.ends_with("_compatible"))
                .all(|c| c.status == CapabilityStatus::Disabled)
        );
        let queries = EngineCapabilities::for_runtime(RuntimeCapabilityAvailability {
            vortex_read: true,
            local_queries: true,
            ..RuntimeCapabilityAvailability::default()
        });
        assert_eq!(
            queries.frontends[1].status,
            CapabilityStatus::PartiallySupported
        );
        assert_eq!(queries.native_outputs[0].status, CapabilityStatus::Disabled);
    }
}
