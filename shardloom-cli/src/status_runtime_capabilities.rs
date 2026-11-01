//! Shared runtime availability for default and scoped discovery. Build/platform
//! facts do not execute a provider or replace query admission/certification.

use shardloom_core::{EngineCapabilities, RuntimeCapabilityAvailability};

pub(super) fn current() -> EngineCapabilities {
    EngineCapabilities::for_runtime(RuntimeCapabilityAvailability {
        vortex_read: cfg!(all(
            unix,
            any(
                feature = "vortex-local-primitives",
                feature = "vortex-encoded-read-spike"
            )
        )),
        vortex_write: cfg!(all(unix, feature = "vortex-write")),
        local_queries: cfg!(all(unix, feature = "vortex-local-primitives")),
        compatibility_io: cfg!(feature = "universal-format-io"),
    })
}

pub(super) fn append_fields(fields: &mut Vec<(String, String)>) {
    let capabilities = current();
    fields.extend([
        (
            "runtime_capabilities_schema_version".into(),
            "shardloom.runtime_capabilities.v1".into(),
        ),
        (
            "runtime_capabilities_scope".into(),
            "admitted_local_execution_not_certification".into(),
        ),
    ]);
    for (group, entries) in [
        ("native_input", &capabilities.native_inputs),
        ("native_output", &capabilities.native_outputs),
        ("compatibility_output", &capabilities.compatibility_outputs),
        ("frontend", &capabilities.frontends),
    ] {
        for entry in entries {
            let prefix = format!("runtime_capability_{group}_{}", entry.name);
            fields.push((format!("{prefix}_status"), entry.status.as_str().into()));
            fields.push((
                format!("{prefix}_scope"),
                entry.notes.clone().unwrap_or_else(|| {
                    "declared scope; no runtime or certification expansion".into()
                }),
            ));
        }
    }
}

pub(super) fn with_text(details: &str) -> String {
    format!("{}\n\n{details}", current().to_human_text())
}
