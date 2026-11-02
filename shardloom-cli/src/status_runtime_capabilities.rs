//! Shared runtime availability for default and scoped discovery. Build/platform
//! facts do not execute a provider or replace query admission/certification.

use shardloom_core::{
    Capability, CapabilityStatus, EngineCapabilities, RuntimeCapabilityAvailability,
};

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
    let tables = local_table_inputs();
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
        ("local_table_input", &tables),
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
    format!("{}\n\n{details}", to_human_text())
}

pub(super) fn to_human_text() -> String {
    let tables = local_table_inputs()
        .iter()
        .map(|entry| format!("{} ({})", entry.name, entry.status.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{}\nlocal table inputs: {tables}\nlocal table scope: read-only snapshots over local Parquet files; catalogs, remote storage and table writes are unavailable; broad table certification is separate",
        current().to_human_text()
    )
}

fn local_table_inputs() -> Vec<Capability> {
    let enabled = cfg!(all(
        unix,
        feature = "vortex-local-primitives",
        feature = "vortex-write",
        feature = "universal-format-io"
    ));
    let status = if enabled {
        CapabilityStatus::PartiallySupported
    } else {
        CapabilityStatus::Disabled
    };
    [
        ("iceberg", "v1/v2 explicit metadata JSON and current/snapshot-ID selection; Parquet field IDs, primitive evolution and position/equality deletes"),
        ("delta", "reader v1/v2 and admitted v3 features; JSON/classic/multipart checkpoint replay, none/name/id mapping and version selection; no deletion vectors or V2 checkpoints"),
        ("hudi", "Copy-on-Write table v8/v9, layout-v2 complete active timeline, completed instant selection and replacements; no Merge-on-Read, bootstrap or archived history"),
    ]
    .into_iter()
    .map(|(name, profile)| Capability::with_notes(name, status, format!(
        "{profile}; local read-only Parquet data, top-level primitive schemas; no catalogs, remote reads, table writes or prepare_once; query/type/resource admission still applies{}",
        if enabled { "" } else { "; disabled: requires Unix and vortex-local-primitives, vortex-write, universal-format-io build features" }
    )))
    .collect()
}
