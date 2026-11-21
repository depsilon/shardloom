"""Verify patched-provider test commands against the frozen CI recipes."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import tomllib
import unittest
from unittest.mock import patch

import ci_provider_tests as providers


# Transcribed from .github/workflows/ci.yml at frozen baseline
# 782c636c59a6686090bebcb07fe27b4641644e13 (native_vortex_tests lane).
FROZEN_CRATES = ("vortex-array", "vortex-fsst", "vortex-runend", "vortex-layout",
                 "vortex-file", "vortex-io", "vortex-buffer", "vortex-zstd")
NEW_PROVIDERS = (
    "vortex-datetime-parts", "orc-rust", "arrow-avro", "arrow-ipc", "arrow-data",
    "arrow-array", "parquet", "vortex-arrow", "flatbuffers", "prost", "vortex-alp",
    "vortex-bytebool", "vortex-decimal-byte-parts", "vortex-fastlanes", "vortex-onpair",
    "vortex-parquet-variant", "vortex-pco", "vortex-sparse", "vortex-zigzag", "parquet-variant",
    "arrow-schema", "arrow-json", "arrow-row",
)
RETIRED_PROVIDERS = {"vortex-runend"}  # 0.93 moves its implementation/tests into vortex-array.
CURRENT_PROVIDERS = (set(FROZEN_CRATES) | set(NEW_PROVIDERS)) - RETIRED_PROVIDERS


# Cargo commands as they appeared in the frozen workflow, before the intentional
# addition of --locked. Keeping these recipes here makes the comparison work in
# shallow checkouts and source archives without invoking Git.
FROZEN_COMMANDS = {
    "vortex-array": [
        "cargo test --manifest-path vendor/vortex-array/Cargo.toml --features _test-harness,table-display --lib -- --test-threads=2",
        "cargo clippy --manifest-path vendor/vortex-array/Cargo.toml --features _test-harness,table-display --lib -- -D warnings",
    ],
    "vortex-fsst": [
        'cargo test --manifest-path vendor/vortex-fsst/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-fsst/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- -D warnings',
    ],
    "vortex-runend": [
        'cargo test --manifest-path vendor/vortex-runend/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-runend/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- -D warnings',
    ],
    "vortex-layout": [
        'cargo test --manifest-path vendor/vortex-layout/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --config patch.crates-io.vortex-io.path="vendor/vortex-io" --features tokio,vortex-io/tokio,vortex-array/_test-harness --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-layout/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --config patch.crates-io.vortex-io.path="vendor/vortex-io" --features tokio,vortex-io/tokio,vortex-array/_test-harness --lib --tests -- -D warnings',
    ],
    "vortex-file": [
        'cargo test --manifest-path vendor/vortex-file/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --config patch.crates-io.vortex-io.path="vendor/vortex-io" --config patch.crates-io.vortex-layout.path="vendor/vortex-layout" --features tokio,vortex-array/table-display --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-file/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --config patch.crates-io.vortex-io.path="vendor/vortex-io" --config patch.crates-io.vortex-layout.path="vendor/vortex-layout" --features tokio,vortex-array/table-display --lib --tests -- -D warnings',
    ],
    "vortex-io": [
        'cargo test --manifest-path vendor/vortex-io/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features object_store --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-io/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features object_store --lib --tests -- -D warnings',
    ],
    "vortex-buffer": [
        "cargo test --manifest-path vendor/vortex-buffer/Cargo.toml --all-features",
        "cargo clippy --locked --manifest-path vendor/vortex-buffer/Cargo.toml --all-features --all-targets -- -D warnings",
    ],
    "vortex-zstd": [
        'cargo test --manifest-path vendor/vortex-zstd/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- --test-threads=2',
        'cargo clippy --manifest-path vendor/vortex-zstd/Cargo.toml --config patch.crates-io.vortex-array.path="vendor/vortex-array" --features vortex-array/table-display --lib -- -D warnings',
    ],
}


def normalize_ownership_command(command: list[str]) -> list[str]:
    """Add only source replacements present in this exact standalone graph."""
    command = list(command)
    position = command.index("--manifest-path") + 1
    crate = command[position].removeprefix("vendor/").removesuffix("/Cargo.toml")
    frozen = Path(__file__).resolve().parents[1] / ".github/provider-locks" / f"{crate}.lock"
    packages = tomllib.loads(frozen.read_text())["package"]
    names = {p["name"] for p in packages if p["version"] == "59.3.2"}
    position += 1
    while position < len(command) and command[position] == "--config":
        position += 2
    added = []
    for name in ("arrow-schema", "arrow-array", "arrow-data", "arrow-json", "arrow-row"):
        value = f'patch.crates-io.{name}.path="vendor/{name}"'
        if name in names and name != crate and value not in command:
            added += ["--config", value]
    return [*command[:position], *added, *command[position:]]


def normalize_frozen_command(command: list[str]) -> list[str]:
    """Apply locked-resolution and reviewed same-version provider patch deltas."""
    if command[0] != "cargo":
        raise AssertionError(command)
    if "--locked" not in command:
        command = [command[0], command[1], "--locked", *command[2:]]
    if "--features" in command:
        position = command.index("--features")
        command = [*command[:position], "--config",
                   'patch.crates-io.flatbuffers.path="vendor/flatbuffers"', *command[position:]]
    manifest_position = command.index("--manifest-path")
    crate = command[manifest_position + 1].removeprefix("vendor/").removesuffix("/Cargo.toml")
    if crate in (*FROZEN_CRATES, "vortex-datetime-parts") and crate not in RETIRED_PROVIDERS:
        position = manifest_position + 2
        while position < len(command) and command[position] == "--config":
            position += 2
        command = [*command[:position], "--config",
                   'patch.crates-io.prost.path="vendor/prost"', *command[position:]]
    if crate in {"vortex-layout", "vortex-file"}:
        position = command.index('patch.crates-io.prost.path="vendor/prost"') - 1
        command = [*command[:position], "--config",
                   'patch.crates-io.vortex-datetime-parts.path="vendor/vortex-datetime-parts"',
                   *command[position:]]
    return normalize_ownership_command(command)


class ProviderCommandTests(unittest.TestCase):
    def test_dtype_owner_consumers_keep_the_matching_datetime_patch(self) -> None:
        root = Path(__file__).resolve().parents[1]
        covered = set()
        for crate in CURRENT_PROVIDERS - {"vortex-datetime-parts"}:
            packages = tomllib.loads((root / ".github/provider-locks" / f"{crate}.lock").read_text())["package"]
            datetime = next((p for p in packages if p["name"] == "vortex-datetime-parts"), None)
            if datetime is None:
                continue
            with self.subTest(crate=crate):
                covered.add(crate)
                self.assertIn("vortex-array", providers.provider_patches(crate))
                self.assertIn("vortex-datetime-parts", providers.provider_patches(crate))
                self.assertEqual(datetime["version"], "0.93.0")
                self.assertNotIn("source", datetime)
                self.assertNotIn("checksum", datetime)
        self.assertEqual(covered, {"vortex-layout", "vortex-file", "vortex-parquet-variant"})

    def test_same_version_arrow_graph_uses_the_reviewed_ownership_sources(self) -> None:
        owned = {"arrow-schema", "arrow-array", "arrow-data", "arrow-json", "arrow-row"}
        root = Path(__file__).resolve().parents[1]
        for crate in CURRENT_PROVIDERS:
            with self.subTest(crate=crate):
                packages = tomllib.loads((root / ".github/provider-locks" / f"{crate}.lock").read_text())["package"]
                matching = [p for p in packages if p["name"] in owned and p["version"] == "59.3.2"]
                self.assertEqual(set(providers.provider_patches(crate)) & owned,
                                 {p["name"] for p in matching} - {crate})
                for package in matching:
                    self.assertNotIn("source", package)
                    self.assertNotIn("checksum", package)

    def test_new_arrow_providers_run_complete_libraries_and_strict_test_lint(self) -> None:
        for crate in ("arrow-schema", "arrow-json", "arrow-row"):
            test, lint = providers.commands(crate)
            self.assertIn("--all-features", test)
            self.assertEqual(test[-3:], ["--lib", "--", "--test-threads=2"])
            self.assertEqual(lint[-5:], ["--lib", "--tests", "--", "-D", "warnings"])
            self.assertNotIn("arrow_test_data", providers.CONFIGURATIONS[crate])

    def test_all_providers_are_assigned_once_to_four_groups(self) -> None:
        assigned = [crate for group in providers.GROUPS.values() for crate in group]
        self.assertEqual(len(assigned), 30)
        self.assertEqual(set(assigned), CURRENT_PROVIDERS)
        self.assertEqual(len(set(assigned)), len(assigned))
        self.assertEqual(set(providers.CONFIGURATIONS), CURRENT_PROVIDERS)
        self.assertEqual(set(providers.GROUPS), {"arrays", "io", "buffer", "formats"})

    def test_new_datetime_parts_provider_is_configured_without_rewriting_frozen_history(self) -> None:
        self.assertEqual(providers.GROUPS["arrays"][:4], (
            "vortex-array", "vortex-fsst", "vortex-zstd", "vortex-datetime-parts"))
        self.assertEqual(providers.CONFIGURATIONS["vortex-datetime-parts"], {
            "features": "vortex-array/table-display", "patches": ["vortex-array", "flatbuffers", "prost"]})
        test_command, clippy_command = providers.commands("vortex-datetime-parts")
        expected = normalize_frozen_command(FROZEN_COMMANDS["vortex-zstd"][0].replace(
            "vortex-zstd", "vortex-datetime-parts").split())
        self.assertEqual(test_command, expected)
        expected_clippy = normalize_frozen_command(FROZEN_COMMANDS["vortex-zstd"][1].replace(
            "vortex-zstd", "vortex-datetime-parts").split())
        self.assertEqual(clippy_command, expected_clippy)

    def test_arrow_boundary_provider_checks_shared_owners_and_all_library_tests(self) -> None:
        self.assertIn("vortex-arrow", providers.GROUPS["io"])
        config = providers.CONFIGURATIONS["vortex-arrow"]
        self.assertEqual(config["patches"],
                         ["vortex-array", "vortex-buffer", "arrow-array", "arrow-data", "vortex-fsst", "flatbuffers", "prost"])
        test_command, lint_command = providers.commands("vortex-arrow")
        self.assertIn("vortex-array/_test-harness,vortex-array/table-display", test_command)
        self.assertEqual(test_command[-3:], ["--lib", "--", "--test-threads=2"])
        self.assertIn("--tests", lint_command)

    def test_orc_provider_matches_admitted_runtime_features_and_checks_tests(self) -> None:
        self.assertEqual(providers.GROUPS["formats"],
                         ("orc-rust", "arrow-avro", "arrow-ipc", "arrow-data", "arrow-array", "parquet", "parquet-variant",
                          "arrow-schema", "arrow-json", "arrow-row"))
        self.assertEqual(providers.commands("orc-rust"), [normalize_ownership_command(c) for c in [
            ["cargo", "test", "--locked", "--manifest-path", "vendor/orc-rust/Cargo.toml",
             "--config", 'patch.crates-io.flatbuffers.path="vendor/flatbuffers"',
             "--no-default-features", "--lib", "--", "--test-threads=2"],
            ["cargo", "clippy", "--locked", "--manifest-path", "vendor/orc-rust/Cargo.toml",
             "--config", 'patch.crates-io.flatbuffers.path="vendor/flatbuffers"',
             "--no-default-features", "--lib", "--tests", "--", "-D", "warnings"],
        ]])
        self.assertEqual(providers.CONFIGURATIONS["orc-rust"]["patches"], ["flatbuffers"])
        orc_lock = Path(__file__).resolve().parents[1] / ".github/provider-locks/orc-rust.lock"
        self.assertIn('name = "prost"\nversion = "0.13.5"', orc_lock.read_text())
        self.assertNotIn("prost", providers.CONFIGURATIONS["orc-rust"]["patches"])

    def test_avro_runs_complete_library_with_only_the_admitted_codecs(self) -> None:
        self.assertEqual(providers.commands("arrow-avro"), [normalize_ownership_command(c) for c in [
            ["cargo", "test", "--locked", "--manifest-path", "vendor/arrow-avro/Cargo.toml",
             "--no-default-features", "--features", "deflate,snappy", "--lib", "--",
             "--test-threads=2"],
            ["cargo", "clippy", "--locked", "--manifest-path", "vendor/arrow-avro/Cargo.toml",
             "--no-default-features", "--features", "deflate,snappy", "--lib", "--tests",
             "--", "-D", "warnings"],
        ]])

    def test_generated_commands_preserve_frozen_feature_patch_target_and_thread_behavior(self) -> None:
        self.assertEqual(set(FROZEN_COMMANDS), set(FROZEN_CRATES))
        for crate in FROZEN_CRATES:
            if crate in RETIRED_PROVIDERS:
                continue
            with self.subTest(crate=crate):
                self.assertEqual(providers.commands(crate),
                                 [normalize_ownership_command(c) for c in [normalize_frozen_command(row.split())
                                  for row in FROZEN_COMMANDS[crate]]])

        for crate in ("vortex-array", "vortex-fsst", "vortex-zstd"):
            test_command, clippy_command = providers.commands(crate)
            self.assertEqual(test_command[-3:], ["--lib", "--", "--test-threads=2"])
            self.assertIn("--lib", clippy_command)
            self.assertNotIn("--tests", clippy_command)
        for crate in ("vortex-layout", "vortex-file", "vortex-io"):
            self.assertIn("--tests", providers.commands(crate)[1])
        buffer_test, buffer_clippy = providers.commands("vortex-buffer")
        self.assertIn("--all-features", buffer_test)
        self.assertNotIn("--lib", buffer_test)
        self.assertNotIn("--test-threads=2", buffer_test)
        self.assertIn("--all-targets", buffer_clippy)

    def test_ipc_runs_both_codecs_and_complete_library_without_external_fixtures(self) -> None:
        self.assertEqual(providers.commands("arrow-ipc"), [normalize_ownership_command(c) for c in [
            ["cargo", "test", "--locked", "--manifest-path", "vendor/arrow-ipc/Cargo.toml",
             "--config", 'patch.crates-io.flatbuffers.path="vendor/flatbuffers"',
             "--features", "lz4,zstd", "--lib", "--", "--test-threads=2"],
            ["cargo", "clippy", "--locked", "--manifest-path", "vendor/arrow-ipc/Cargo.toml",
             "--config", 'patch.crates-io.flatbuffers.path="vendor/flatbuffers"',
             "--features", "lz4,zstd", "--lib", "--tests", "--", "-D", "warnings"],
        ]])
        self.assertNotIn("arrow_test_data", providers.CONFIGURATIONS["arrow-ipc"])

    def test_arrow_array_and_data_run_complete_libraries_with_the_patched_slice_provider(self) -> None:
        for crate in ("arrow-data", "arrow-array"):
            with self.subTest(crate=crate):
                arguments = ["--manifest-path", f"vendor/{crate}/Cargo.toml"]
                if crate == "arrow-array":
                    arguments += ["--config", 'patch.crates-io.arrow-data.path="vendor/arrow-data"']
                arguments += ["--features", "ffi"]
                self.assertEqual(providers.commands(crate), [normalize_ownership_command(c) for c in [
                    ["cargo", "test", "--locked", *arguments, "--lib", "--", "--test-threads=2"],
                    ["cargo", "clippy", "--locked", *arguments, "--lib", "--tests", "--", "-D", "warnings"],
                ]])
                self.assertNotIn("arrow_test_data", providers.CONFIGURATIONS[crate])

    def test_parquet_checks_default_codecs_and_integration_with_patched_arrow_providers(self) -> None:
        arguments = ["--manifest-path", "vendor/parquet/Cargo.toml"]
        for crate in ("arrow-array", "arrow-data", "arrow-ipc", "flatbuffers", "parquet-variant"):
            arguments += ["--config", f'patch.crates-io.{crate}.path="vendor/{crate}"']
        arguments += ["--features", "default", "--lib", "--tests"]
        self.assertEqual(providers.commands("parquet"), [normalize_ownership_command(c) for c in [
            ["cargo", "test", "--locked", *arguments, "--", "--test-threads=2"],
            ["cargo", "clippy", "--locked", *arguments, "--", "-D", "warnings"],
        ]])

    def test_flatbuffers_checks_standard_tests_and_no_std_without_changing_other_features(self) -> None:
        self.assertEqual(providers.commands("flatbuffers"), [normalize_ownership_command(c) for c in [
            ["cargo", "test", "--locked", "--manifest-path", "vendor/flatbuffers/Cargo.toml", "--all-features"],
            ["cargo", "clippy", "--locked", "--manifest-path", "vendor/flatbuffers/Cargo.toml",
             "--all-features", "--all-targets", "--", "-D", "warnings"],
            ["cargo", "check", "--locked", "--manifest-path", "vendor/flatbuffers/Cargo.toml",
             "--no-default-features"],
        ]])
        for crate in ("vortex-buffer", "arrow-array", "arrow-data", "arrow-avro"):
            self.assertNotIn("flatbuffers", providers.CONFIGURATIONS[crate]["patches"])

    def test_prost_checks_default_bounded_and_no_std_configurations(self) -> None:
        self.assertIn("prost", providers.GROUPS["buffer"])
        self.assertEqual(providers.CONFIGURATIONS["prost"], {
            "features": None, "patches": [], "clippy_conf_dir": "vendor/prost"})
        self.assertEqual(providers.commands("prost"), [normalize_ownership_command(c) for c in [
            ["cargo", "test", "--locked", "--manifest-path", "vendor/prost/Cargo.toml",
             "--lib", "--", "--test-threads=2"],
            ["cargo", "test", "--locked", "--manifest-path", "vendor/prost/Cargo.toml",
             "--lib", "--features", "no-recursion-limit", "--", "--test-threads=2"],
            ["cargo", "clippy", "--locked", "--manifest-path", "vendor/prost/Cargo.toml",
             "--lib", "--tests", "--", "-D", "warnings"],
            ["cargo", "clippy", "--locked", "--manifest-path", "vendor/prost/Cargo.toml",
             "--lib", "--tests", "--features", "no-recursion-limit", "--", "-D", "warnings"],
            ["cargo", "check", "--locked", "--manifest-path", "vendor/prost/Cargo.toml",
             "--no-default-features"],
            ["cargo", "check", "--locked", "--manifest-path", "vendor/prost/Cargo.toml",
             "--no-default-features", "--features", "no-recursion-limit"],
        ]])
        patched = ("vortex-array", "vortex-fsst", "vortex-zstd", "vortex-datetime-parts",
                   "vortex-layout", "vortex-file", "vortex-io", "vortex-arrow", "vortex-buffer")
        for crate in patched:
            self.assertEqual(providers.CONFIGURATIONS[crate]["patches"][-1], "prost")
        self.assertNotIn("prost", providers.CONFIGURATIONS["orc-rust"]["patches"])

    def test_nine_codec_providers_use_their_exact_reviewed_feature_and_patch_recipes(self) -> None:
        expected = {
            "vortex-alp": ["vortex-array", "vortex-fastlanes", "flatbuffers", "prost"],
            "vortex-bytebool": ["vortex-array", "flatbuffers", "prost"],
            "vortex-decimal-byte-parts": ["vortex-array", "flatbuffers", "prost"],
            "vortex-fastlanes": ["vortex-array", "flatbuffers", "prost"],
            "vortex-onpair": ["vortex-array", "flatbuffers", "prost"],
            "vortex-parquet-variant": ["vortex-array", "vortex-file", "vortex-layout",
                                        "vortex-io", "vortex-datetime-parts", "flatbuffers", "prost", "parquet-variant"],
            "vortex-pco": ["vortex-array", "flatbuffers", "prost"],
            "vortex-sparse": ["vortex-array", "flatbuffers", "prost"],
            "vortex-zigzag": ["vortex-array", "flatbuffers", "prost"],
        }
        for crate, patches in expected.items():
            with self.subTest(crate=crate):
                features = {
                    "vortex-decimal-byte-parts": "vortex-array/_test-harness,vortex-array/table-display",
                    "vortex-fastlanes": "_test-harness,vortex-array/table-display",
                }.get(crate, "vortex-array/table-display")
                self.assertEqual(providers.CONFIGURATIONS[crate], {
                    "features": features,
                    "patches": patches,
                    "clippy_tests": True,
                })
                patch_args = [arg for patch in patches for arg in (
                    "--config", f'patch.crates-io.{patch}.path="vendor/{patch}"')]
                manifest = ["--locked", "--manifest-path", f"vendor/{crate}/Cargo.toml"]
                self.assertEqual(providers.commands(crate), [normalize_ownership_command(c) for c in [
                    ["cargo", "test", *manifest, *patch_args,
                     "--features", features, "--lib", "--",
                     "--test-threads=2"],
                    ["cargo", "clippy", *manifest, *patch_args,
                     "--features", features, "--lib", "--tests",
                     "--", "-D", "warnings"],
                ]])


class RunGroupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / ".github/provider-locks").mkdir(parents=True)
        (self.root / "vendor").mkdir()
        self.group = "buffer"
        for crate in providers.GROUPS[self.group]:
            (self.root / ".github/provider-locks" / f"{crate}.lock").write_text(f"{crate} lock\n")
            (self.root / "vendor" / crate).mkdir()

    def test_missing_lock_is_rejected_and_failed_report_written(self) -> None:
        (self.root / ".github/provider-locks/vortex-buffer.lock").unlink()
        output = self.root / "out"
        with self.assertRaisesRegex(ValueError, "missing reviewed provider lockfile"):
            providers.run_group(self.group, output, root=self.root)
        report = json.loads((output / "report.json").read_text())
        self.assertEqual(report["status"], "failed")
        self.assertIn("missing reviewed provider lockfile", report["error"])

    def test_successful_mocked_commands_copy_reviewed_lock_and_write_pass_report(self) -> None:
        output = self.root / "out"
        with patch.object(providers.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run:
            report = providers.run_group(self.group, output, root=self.root)
        self.assertEqual(report["status"], "passed")
        self.assertEqual(run.call_count, 11)
        self.assertEqual(run.call_args.kwargs["env"]["CLIPPY_CONF_DIR"],
                         str(self.root / "vendor/prost"))
        self.assertEqual((self.root / "vendor/vortex-buffer/Cargo.lock").read_text(), "vortex-buffer lock\n")
        saved = json.loads((output / "report.json").read_text())
        self.assertEqual(saved["status"], "passed")
        self.assertEqual(len(saved["commands"]), 11)

    def test_prost_repeated_verbs_get_distinct_write_once_logs_and_receipts(self) -> None:
        output = self.root / "out"
        with patch.object(providers.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run:
            report = providers.run_group(self.group, output, root=self.root)
            self.assertEqual(run.call_count, 11)
            prost_entries = [entry for entry in report["commands"] if entry["crate"] == "prost"]
            expected_logs = {
                "prost-01-test.log", "prost-02-test.log",
                "prost-01-clippy.log", "prost-02-clippy.log",
                "prost-01-check.log", "prost-02-check.log",
            }
            self.assertEqual({entry["log"] for entry in prost_entries}, expected_logs)
            original = {name: (output / name).read_bytes() for name in expected_logs}
            with self.assertRaisesRegex(ValueError, "evidence directory must be new"):
                providers.run_group(self.group, output, root=self.root)
            self.assertEqual(run.call_count, 11)
        self.assertEqual(len(prost_entries), 6)
        for name in expected_logs:
            path = output / name
            self.assertTrue(path.is_file())
        self.assertTrue(all(data == b"" for data in original.values()))
        self.assertEqual({name: (output / name).read_bytes() for name in expected_logs}, original)

    def test_format_commands_have_scoped_lint_and_fixture_paths(self) -> None:
        for crate in providers.GROUPS["formats"]:
            (self.root / ".github/provider-locks" / f"{crate}.lock").write_text(f"{crate} lock\n")
            (self.root / "vendor" / crate).mkdir()
        fixtures = self.root / "vendor/arrow-avro/test/arrow-testing/data"
        fixtures.mkdir(parents=True)
        parquet_arrow = self.root / "vendor/parquet/test/arrow-testing/data"
        parquet_arrow.mkdir(parents=True)
        parquet_data = self.root / "vendor/parquet/test/parquet-testing/data"
        parquet_data.mkdir(parents=True)
        variant_data = self.root / "vendor/parquet-variant/test/parquet-testing/data"
        variant_data.mkdir(parents=True)
        with patch.dict(providers.os.environ, {"CLIPPY_CONF_DIR": "/inherited/vortex-policy",
                                               "ARROW_TEST_DATA": "/inherited/fixtures",
                                               "PARQUET_TEST_DATA": "/inherited/parquet"}):
            with patch.object(providers.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run:
                report = providers.run_group("formats", self.root / "out", root=self.root)
        self.assertEqual(report["status"], "passed")
        self.assertEqual(run.call_count, 20)
        for call, command in zip(run.call_args_list, report["commands"]):
            crate = command["crate"]
            expected = str(self.root / "vendor" / crate)
            self.assertEqual(call.kwargs["env"]["CLIPPY_CONF_DIR"], expected)
            self.assertEqual(command["clippy_conf_dir"], expected)
            if crate == "arrow-avro":
                self.assertEqual(call.kwargs["env"]["ARROW_TEST_DATA"], str(fixtures))
                self.assertEqual(command["arrow_test_data"], str(fixtures))
            elif crate == "parquet":
                self.assertEqual(call.kwargs["env"]["ARROW_TEST_DATA"], str(parquet_arrow))
                self.assertEqual(command["arrow_test_data"], str(parquet_arrow))
                self.assertEqual(call.kwargs["env"]["PARQUET_TEST_DATA"], str(parquet_data))
                self.assertEqual(command["parquet_test_data"], str(parquet_data))
            elif crate == "parquet-variant":
                self.assertEqual(call.kwargs["env"]["PARQUET_TEST_DATA"], str(variant_data))
                self.assertEqual(command["parquet_test_data"], str(variant_data))
                self.assertEqual(call.kwargs["env"]["ARROW_TEST_DATA"], "/inherited/fixtures")
                self.assertNotIn("arrow_test_data", command)
            else:
                self.assertEqual(call.kwargs["env"]["ARROW_TEST_DATA"], "/inherited/fixtures")
                self.assertNotIn("arrow_test_data", command)

    def test_missing_either_parquet_fixture_set_fails_before_commands(self) -> None:
        for missing in ("arrow_test_data", "parquet_test_data"):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                present = "parquet_test_data" if missing == "arrow_test_data" else "arrow_test_data"
                (root / providers.CONFIGURATIONS["parquet"][present]).mkdir(parents=True)
                with patch.object(providers, "GROUPS", {"formats": ("parquet",)}):
                    with patch.object(providers.subprocess, "run") as run:
                        with self.assertRaisesRegex(ValueError, f"parquet {missing}"):
                            providers.run_group("formats", root / "out", root=root)
                run.assert_not_called()

    def test_missing_avro_fixtures_fail_before_commands(self) -> None:
        with patch.object(providers, "GROUPS", {"formats": ("arrow-avro",)}):
            with patch.object(providers.subprocess, "run") as run:
                with self.assertRaisesRegex(ValueError, "missing committed provider test fixtures"):
                    providers.run_group("formats", self.root / "out", root=self.root)
        run.assert_not_called()

    def test_command_failure_is_rejected_and_failed_report_written(self) -> None:
        output = self.root / "out"
        with patch.object(providers.subprocess, "run", return_value=subprocess.CompletedProcess([], 1)):
            with self.assertRaisesRegex(RuntimeError, "failed"):
                providers.run_group(self.group, output, root=self.root)
        report = json.loads((output / "report.json").read_text())
        self.assertEqual(report["status"], "failed")

    def test_lock_modified_by_command_is_rejected_and_failed_report_written(self) -> None:
        output = self.root / "out"
        def mutate_lock(command, **kwargs):
            (self.root / "vendor/vortex-buffer/Cargo.lock").write_text("changed by command\n")
            return subprocess.CompletedProcess(command, 0)

        with patch.object(providers.subprocess, "run", side_effect=mutate_lock):
            with self.assertRaisesRegex(RuntimeError, "failed"):
                providers.run_group(self.group, output, root=self.root)
        report = json.loads((output / "report.json").read_text())
        self.assertEqual(report["status"], "failed")


if __name__ == "__main__":
    unittest.main()
