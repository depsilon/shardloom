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

impl EngineCapabilities {
    #[must_use]
    pub fn current() -> Self {
        Self {
            engine: "shardloom".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            fallback_execution_allowed: false,
            native_inputs: vec![Capability::new("vortex", CapabilityStatus::Planned)],
            native_outputs: vec![Capability::new("vortex", CapabilityStatus::Planned)],
            compatibility_outputs: vec![
                Capability::new("arrow_ipc", CapabilityStatus::Planned),
                Capability::new("parquet", CapabilityStatus::Planned),
                Capability::new("iceberg_compatible", CapabilityStatus::Planned),
                Capability::new("delta_compatible", CapabilityStatus::Planned),
            ],
            frontends: vec![
                Capability::new("cli", CapabilityStatus::PartiallySupported),
                Capability::new("sql", CapabilityStatus::Planned),
                Capability::new("dataframe_api", CapabilityStatus::Planned),
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
            "engine: {}\nversion: {}\nfallback execution: {}\nnative inputs: {}\nnative outputs: {}",
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
        )
    }
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
}
