//! A reduced writer build must not enter an unowned compatibility reader.

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn compatibility_preparation_admits_shared_control_before_malformed_input() {
    use std::{fs, process::Command, time::SystemTime};
    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-prepare-owned-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    let existing = root.join("existing.vortex");
    let missing = root.join("missing-parent").join("output.vortex");
    fs::write(&existing, b"preserve this artifact").unwrap();
    for extension in ["csv", "json", "jsonl", "parquet", "arrow"] {
        let source = root.join(format!("malformed.{extension}"));
        fs::write(&source, [0xff, 0xfe]).unwrap();
        for target in [&existing, &missing] {
            let result = Command::new(env!("CARGO_BIN_EXE_shardloom"))
                .arg("vortex-prepare")
                .arg(&source)
                .arg(target)
                .args([
                    "--allow-overwrite",
                    "--memory-bytes",
                    "1",
                    "--max-parallelism",
                    "1",
                    "--format",
                    "json",
                ])
                .output()
                .unwrap();
            assert!(!result.status.success());
            let stdout = String::from_utf8(result.stdout).unwrap();
            assert!(
                stdout.contains("memory reservation denied"),
                "{extension}: {stdout}"
            );
            assert_eq!(fs::read(&existing).unwrap(), b"preserve this artifact");
            assert!(!missing.parent().unwrap().exists());
        }
    }
    fs::remove_dir_all(root).unwrap();
}

#[cfg(all(feature = "vortex-write", not(feature = "universal-format-io")))]
#[test]
fn compatibility_preparation_refuses_unowned_feature_profile_before_data_or_output() {
    use std::{fs, process::Command, time::SystemTime};
    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shardloom-prepare-resources-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    let source = root.join("malformed.csv");
    // The refusal must win over text decoding, including with a large valid grant.
    fs::write(&source, [0xff, 0xfe]).unwrap();
    let existing = root.join("existing.vortex");
    fs::write(&existing, b"preserve this artifact").unwrap();
    let missing = root.join("missing-parent").join("output.vortex");
    for memory in ["1", "4294967296"] {
        for output in [&existing, &missing] {
            let result = Command::new(env!("CARGO_BIN_EXE_shardloom"))
                .arg("vortex-prepare")
                .arg(&source)
                .arg(output)
                .args([
                    "--allow-overwrite",
                    "--memory-bytes",
                    memory,
                    "--max-parallelism",
                    "1",
                    "--format",
                    "json",
                ])
                .output()
                .unwrap();
            assert!(!result.status.success());
            let stdout = String::from_utf8(result.stdout).unwrap();
            assert!(
                stdout.contains("requires universal-format-io for shared resource admission"),
                "{stdout}"
            );
            assert!(stdout.contains("no input was read"), "{stdout}");
            assert!(
                !stdout.contains("execution_resource_admission_status\":\"admitted"),
                "{stdout}"
            );
            assert_eq!(fs::read(&existing).unwrap(), b"preserve this artifact");
            assert!(!missing.parent().unwrap().exists());
        }
    }
    fs::remove_dir_all(root).unwrap();
}
