//! Explicit aggregate spill policy parsing. Parsing does not inspect or modify
//! the workspace; native execution owns source/schema and filesystem admission.

use shardloom_core::{Result, ShardLoomError};
use shardloom_vortex::VortexAggregateSpillPolicy;

pub(super) fn parse(
    value: Option<&serde_json::Value>,
) -> Result<Option<VortexAggregateSpillPolicy>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let object = value
        .as_object()
        .ok_or_else(|| invalid("must be an object"))?;
    for name in object.keys() {
        if !matches!(name.as_str(), "workspace" | "quota_bytes" | "memory_bytes") {
            return Err(invalid(&format!("contains unsupported field {name:?}")));
        }
    }
    let workspace = object
        .get("workspace")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid("requires string workspace"))?;
    let bytes = |field: &str| {
        object
            .get(field)
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| invalid(&format!("requires unsigned integer {field}")))
    };
    VortexAggregateSpillPolicy::new(workspace, bytes("quota_bytes")?, bytes("memory_bytes")?)
        .map(Some)
}

fn invalid(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native aggregate spill policy {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::parse;
    use serde_json::json;

    #[test]
    fn exact_distinct_spill_policy_is_explicit_and_side_effect_free() {
        assert!(parse(None).unwrap().is_none());
        let root = std::env::temp_dir().join(format!(
            "shardloom-distinct-policy-only-{}",
            std::process::id()
        ));
        let before = root.exists();
        let policy = parse(Some(
            &json!({"workspace": root, "quota_bytes": 16_777_216_u64,
            "memory_bytes": 4_194_304_u64}),
        ))
        .unwrap()
        .unwrap();
        assert_eq!(policy.workspace, root);
        assert_eq!(policy.memory_bytes, 4_194_304);
        assert_eq!(policy.quota_bytes, 16_777_216);
        assert_eq!(root.exists(), before);
    }

    #[test]
    fn exact_distinct_spill_policy_rejects_malformed_effect_admission() {
        for value in [
            json!(null),
            json!(true),
            json!([]),
            json!({"workspace": "/tmp", "quota_bytes": 1_000_000, "memory_bytes": 4_194_304, "force": true}),
            json!({"workspace": 123, "quota_bytes": 1_000_000, "memory_bytes": 4_194_304}),
            json!({"workspace": "/tmp", "quota_bytes": -1, "memory_bytes": 4_194_304}),
            json!({"workspace": "/tmp", "quota_bytes": 1.5, "memory_bytes": 4_194_304}),
            json!({"workspace": "/tmp", "quota_bytes": "1000000", "memory_bytes": 4_194_304}),
            json!({"workspace": "/tmp", "quota_bytes": 1_000_000}),
            json!({"workspace": "relative", "quota_bytes": 1_000_000, "memory_bytes": 4_194_304}),
            json!({"workspace": "/tmp", "quota_bytes": 0, "memory_bytes": 4_194_304}),
            json!({"workspace": "/tmp", "quota_bytes": 1_000_000, "memory_bytes": 1_048_576}),
        ] {
            let error = parse(Some(&value)).unwrap_err().to_string();
            assert!(error.contains("aggregate spill"), "{error}");
            assert!(!error.contains("native sort"), "{error}");
        }
    }
}
