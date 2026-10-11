use super::*;
use shardloom_core::{ExecutionResourceOrigin, ExecutionResources};
use shardloom_exec::compute_pool::CancellationToken;
use std::{path::Path, time::Instant};

const MEMORY_BYTES: u64 = 8 << 20;

fn resources() -> ExecutionResources {
    ExecutionResources::from_bytes(MEMORY_BYTES, 1, ExecutionResourceOrigin::Context)
        .expect("explicit test allocation")
}

fn owner() -> EmbeddedExecutionOwner {
    EmbeddedExecutionOwner::new(Some(resources()), None).expect("configured native owner")
}

fn fixture_path() -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")
        .display()
        .to_string()
}

fn run_primitive_args(path: &str, primitive: &str, payload: &[&str]) -> Vec<String> {
    let family = if primitive == "count" {
        "count"
    } else {
        "filter_project_limit"
    };
    let mut args = vec![
        "run",
        "cli",
        "--input",
        path,
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--native-vortex-operation-family",
        family,
        "--vortex-primitive",
        primitive,
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();
    args.extend(payload.iter().map(|value| (*value).to_string()));
    args.extend([
        "--memory-bytes".into(),
        MEMORY_BYTES.to_string(),
        "--max-parallelism".into(),
        "1".into(),
    ]);
    args
}

#[cfg(feature = "vortex-encoded-read-spike")]
fn direct_encoded_count_args(path: &str) -> Vec<String> {
    vec![
        "vortex-count".into(),
        path.into(),
        "--execute-local-encoded-count".into(),
        "--memory-bytes".into(),
        MEMORY_BYTES.to_string(),
        "--max-parallelism".into(),
        "1".into(),
    ]
}

fn envelope(json: &str) -> serde_json::Value {
    serde_json::from_str(json).expect("command returns a JSON envelope")
}

fn field<'a>(envelope: &'a serde_json::Value, name: &str) -> &'a str {
    envelope["result"]["fields"]
        .as_array()
        .expect("typed result fields are present")
        .iter()
        .chain(envelope["fields"].as_array().unwrap())
        .find(|field| field["key"] == name)
        .and_then(|field| field["value"].as_str())
        .unwrap_or_else(|| panic!("missing command field {name}: {envelope}"))
}

fn has_field(envelope: &serde_json::Value, name: &str) -> bool {
    [
        envelope["result"]["fields"].as_array(),
        envelope["fields"].as_array(),
    ]
    .into_iter()
    .flatten()
    .flatten()
    .any(|field| field["key"] == name)
}

fn assert_no_fallback(envelope: &serde_json::Value) {
    assert_eq!(envelope["fallback"]["attempted"], false);
    assert_eq!(envelope["fallback"]["allowed"], false);
    assert!(envelope["fallback"]["engine"].is_null());
}

fn assert_admitted_resources(envelope: &serde_json::Value) {
    assert_eq!(
        field(envelope, "execution_resource_admission_status"),
        "admitted"
    );
    assert_eq!(
        field(envelope, "execution_resource_admitted_memory_bytes"),
        MEMORY_BYTES.to_string()
    );
    assert_eq!(
        field(envelope, "execution_resource_admitted_max_parallelism"),
        "1"
    );
    assert_eq!(
        field(envelope, "execution_resource_memory_observation_scope"),
        "session_pool_lifetime_including_preparation_and_retained_owners;excludes_cli_envelope_formatting_provider_bypass_allocations_and_process_rss"
    );
}

fn retain_almost_all_memory(
    owner: &EmbeddedExecutionOwner,
) -> shardloom_exec::live_memory::MemoryLease {
    let token = CancellationToken::default();
    owner
        .controller(resources(), &token)
        .expect("controller")
        .with_resources(resources(), &token, |session, _| {
            session.reserve_input_scratch(MEMORY_BYTES - 1)
        })
        .expect("retain lease in the owner's shared pool")
}

#[test]
fn smaller_command_allocation_reports_its_own_peak_and_keeps_lifetime_history() {
    let owner = owner();
    let token = CancellationToken::default();
    let larger =
        ExecutionResources::from_bytes(2 * MEMORY_BYTES, 1, ExecutionResourceOrigin::ExecutionCall)
            .unwrap();
    let controller = owner.controller(larger, &token).unwrap();
    let retained = controller
        .with_resources(larger, &token, |session, _| {
            let mut retained = session.reserve_input_scratch(MEMORY_BYTES + 4096)?;
            retained.resize(1024)?;
            Ok(retained)
        })
        .unwrap();
    for _ in 0..2 {
        let result = envelope(
            &command_json_in_owner(
                run_primitive_args(&fixture_path(), "count", &[]),
                &owner,
                &token,
            )
            .unwrap(),
        );
        assert_eq!(result["status"], "success", "{result}");
        assert_admitted_resources(&result);
        assert_no_fallback(&result);
        let peak = field(
            &result,
            "execution_resource_observed_native_allocation_peak_reserved_bytes",
        )
        .parse::<u64>()
        .unwrap();
        assert!((1024..=MEMORY_BYTES).contains(&peak));
        assert_eq!(
            field(
                &result,
                "execution_resource_observed_native_peak_reserved_bytes"
            ),
            (MEMORY_BYTES + 4096).to_string()
        );
        assert_eq!(
            field(&result, "resident_peak_reserved_buffer_bytes"),
            (MEMORY_BYTES + 4096).to_string()
        );
        assert_eq!(
            field(
                &result,
                "execution_resource_allocation_memory_observation_scope"
            ),
            "current_allocation_including_preparation_and_retained_owners;excludes_cli_envelope_formatting_provider_bypass_allocations_and_process_rss"
        );
        controller
            .with_resources(resources(), &token, |session, _| {
                assert!(session.shared_memory_pool().owns(&retained));
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn cancelled_and_expired_public_run_commands_reject_missing_sources_before_open() {
    let owner = owner();
    let missing = std::env::temp_dir()
        .join(format!(
            "shardloom-command-admission-missing-{}",
            std::process::id()
        ))
        .display()
        .to_string();
    let args = run_primitive_args(&missing, "count", &[]);
    let before = owner.snapshot().expect("configured owner snapshot");

    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let expired = CancellationToken::default().with_deadline(Instant::now());
    for (token, diagnostic) in [
        (&cancelled, "execution cancelled"),
        (&expired, "execution deadline exceeded"),
    ] {
        let response = command_json_in_owner(args.clone(), &owner, token).unwrap();
        let json = envelope(&response);
        assert_eq!(json["status"], "error");
        assert!(
            json["diagnostics"].as_array().unwrap().iter().any(|item| {
                item["reason"]
                    .as_str()
                    .is_some_and(|message| message.contains(diagnostic))
            }),
            "{json}"
        );
        assert_no_fallback(&json);
        let after = owner.snapshot().expect("owner remains initialized");
        assert_eq!(after.prepared_source_opens, before.prepared_source_opens);
        assert_eq!(after.completed_executions, before.completed_executions);
        assert_eq!(after.memory.reserved_bytes, before.memory.reserved_bytes);
    }
}

#[test]
fn public_run_count_project_and_filter_project_share_the_retained_memory_grant() {
    let owner = owner();
    let path = fixture_path();
    let sources = [
        ("count", Vec::new(), "{\"count_all\":5}\n".to_string()),
        (
            "project",
            vec!["--vortex-columns", "metric"],
            "{\"metric\":10}\n{\"metric\":20}\n{\"metric\":30}\n{\"metric\":40}\n{\"metric\":50}\n"
                .to_string(),
        ),
        (
            "filter_project",
            vec![
                "--vortex-predicate",
                "gte:value:3",
                "--vortex-columns",
                "metric",
            ],
            "{\"metric\":30}\n{\"metric\":40}\n{\"metric\":50}\n".to_string(),
        ),
    ];

    for (primitive, payload, expected_jsonl) in sources {
        let lease = retain_almost_all_memory(&owner);
        let before = owner.snapshot().expect("owner snapshot before run");
        assert_eq!(before.memory.reserved_bytes, MEMORY_BYTES - 1);
        let denied = envelope(
            &command_json_in_owner(
                run_primitive_args(&path, primitive, &payload),
                &owner,
                &CancellationToken::default(),
            )
            .unwrap(),
        );
        assert_eq!(denied["status"], "error");
        assert!(
            denied["diagnostics"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| {
                    item["reason"]
                        .as_str()
                        .is_some_and(|message| message.contains("memory reservation denied"))
                }),
            "{denied}"
        );
        assert_admitted_resources(&denied);
        assert_eq!(
            field(&denied, "execution_resource_observed_native_reserved_bytes"),
            (MEMORY_BYTES - 1).to_string()
        );
        assert_no_fallback(&denied);
        let after_denial = owner.snapshot().expect("owner snapshot after denial");
        assert_eq!(
            after_denial.prepared_source_opens,
            before.prepared_source_opens
        );
        assert_eq!(
            after_denial.completed_executions,
            before.completed_executions
        );
        assert_eq!(after_denial.memory.reserved_bytes, MEMORY_BYTES - 1);
        assert!(after_denial.memory.peak_reserved_bytes <= after_denial.memory.limit_bytes);
        drop(lease);

        let completed = envelope(
            &command_json_in_owner(
                run_primitive_args(&path, primitive, &payload),
                &owner,
                &CancellationToken::default(),
            )
            .unwrap(),
        );
        assert_eq!(completed["status"], "success");
        let expected = expected_jsonl
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        let actual = if primitive == "count" {
            field(&completed, "result_jsonl")
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect::<Vec<_>>()
        } else {
            serde_json::from_str::<Vec<serde_json::Value>>(field(&completed, "result_values_json"))
                .unwrap()
        };
        assert_eq!(actual, expected);
        assert_admitted_resources(&completed);
        assert_no_fallback(&completed);
        let after_success = owner.snapshot().expect("owner snapshot after success");
        assert!(after_success.completed_executions > before.completed_executions);
        assert_eq!(after_success.memory.reserved_bytes, 0);
        assert!(after_success.memory.peak_reserved_bytes <= after_success.memory.limit_bytes);
    }
}

#[test]
fn command_resource_observation_does_not_leak_into_resource_free_capabilities() {
    let owner = owner();
    let successful = envelope(
        &command_json_in_owner(
            run_primitive_args(&fixture_path(), "count", &[]),
            &owner,
            &CancellationToken::default(),
        )
        .unwrap(),
    );
    assert_eq!(successful["status"], "success");
    assert_eq!(
        field(&successful, "execution_resource_admission_status"),
        "admitted"
    );
    assert_eq!(
        field(&successful, "execution_resource_admitted_memory_bytes"),
        MEMORY_BYTES.to_string()
    );
    assert_eq!(
        field(&successful, "execution_resource_admitted_max_parallelism"),
        "1"
    );

    // command_json_in_owner appends the public JSON format option, so this is
    // the resource-free `capabilities --format json` invocation.
    let capabilities = envelope(
        &command_json_in_owner(
            vec!["capabilities".into()],
            &owner,
            &CancellationToken::default(),
        )
        .unwrap(),
    );
    assert_eq!(capabilities["status"], "success");
    assert_eq!(field(&capabilities, "fallback_execution_allowed"), "false");
    for key in [
        "execution_resource_admission_status",
        "execution_resource_admitted_memory_bytes",
        "execution_resource_admitted_max_parallelism",
        "execution_resource_observed_native_reserved_bytes",
        "execution_resource_observed_native_peak_reserved_bytes",
        "execution_resource_memory_observation_scope",
    ] {
        assert!(
            !has_field(&capabilities, key),
            "unexpected {key}: {capabilities}"
        );
    }
}

#[test]
fn joined_commands_preserve_parent_admission_without_blocking_discovery_or_other_owners() {
    let busy = owner();
    let available = owner();
    let token = CancellationToken::default();
    busy.controller(resources(), &token)
        .unwrap()
        .with_resources(resources(), &token, |_, _| {
            // A deadline keeps a broken implementation bounded. The expected
            // diagnosis is the lock dependency, not expiration while waiting.
            let bounded = token
                .clone()
                .with_deadline(Instant::now() + std::time::Duration::from_secs(5));
            let denied = envelope(&command_json_in_owner(
                run_primitive_args(&fixture_path(), "count", &[]),
                &busy,
                &bounded,
            )?);
            assert_eq!(denied["status"], "error");
            assert!(
                denied["diagnostics"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| {
                        item["reason"]
                            .as_str()
                            .is_some_and(|reason| reason.contains("nested operation cannot wait"))
                    }),
                "{denied}"
            );
            let discovery = envelope(&command_json_in_owner(
                vec!["capabilities".into()],
                &busy,
                &bounded,
            )?);
            assert_eq!(discovery["status"], "success");
            let completed = envelope(&command_json_in_owner(
                run_primitive_args(&fixture_path(), "count", &[]),
                &available,
                &bounded,
            )?);
            assert_eq!(completed["status"], "success");
            assert_eq!(field(&completed, "result_jsonl"), "{\"count_all\":5}\n");
            Ok(())
        })
        .unwrap();
    // The joined dependency scope must not outlive the command.
    let completed = envelope(
        &command_json_in_owner(
            run_primitive_args(&fixture_path(), "count", &[]),
            &busy,
            &token,
        )
        .unwrap(),
    );
    assert_eq!(completed["status"], "success");
    assert_eq!(busy.snapshot().unwrap().memory.reserved_bytes, 0);
}

#[cfg(feature = "vortex-encoded-read-spike")]
#[test]
fn direct_local_encoded_count_observes_the_retained_owner_grant_and_recovers() {
    let owner = owner();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("shardloom-vortex/tests/fixtures/metadata_footer_u64_20000.vortex")
        .display()
        .to_string();
    let lease = retain_almost_all_memory(&owner);
    let before = owner
        .snapshot()
        .expect("owner snapshot before direct command");
    let denied = envelope(
        &command_json_in_owner(
            direct_encoded_count_args(&path),
            &owner,
            &CancellationToken::default(),
        )
        .unwrap(),
    );
    assert_ne!(denied["status"], "success");
    assert!(
        denied["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| {
                item["reason"]
                    .as_str()
                    .is_some_and(|message| message.contains("memory reservation denied"))
            }),
        "{denied}"
    );
    assert_no_fallback(&denied);
    let after_denial = owner.snapshot().expect("owner snapshot after denial");
    assert_eq!(
        after_denial.prepared_source_opens,
        before.prepared_source_opens
    );
    assert_eq!(
        after_denial.completed_executions,
        before.completed_executions
    );
    assert_eq!(after_denial.memory.reserved_bytes, MEMORY_BYTES - 1);
    assert!(after_denial.memory.peak_reserved_bytes <= after_denial.memory.limit_bytes);
    drop(lease);

    let completed = envelope(
        &command_json_in_owner(
            direct_encoded_count_args(&path),
            &owner,
            &CancellationToken::default(),
        )
        .unwrap(),
    );
    assert_eq!(completed["status"], "success");
    assert_eq!(field(&completed, "count"), "20000");
    assert_eq!(field(&completed, "result_known"), "true");
    assert_eq!(
        field(&completed, "explicit_local_encoded_count_requested"),
        "true"
    );
    assert_no_fallback(&completed);
    let after_success = owner.snapshot().expect("owner snapshot after success");
    assert!(after_success.completed_executions > before.completed_executions);
    assert_eq!(after_success.memory.reserved_bytes, 0);
    assert!(after_success.memory.peak_reserved_bytes <= after_success.memory.limit_bytes);
}
