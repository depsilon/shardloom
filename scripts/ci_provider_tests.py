#!/usr/bin/env python3
"""Run the existing patched-provider configurations with committed lockfiles."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

from ci_build_artifact import ROOT, sha, write_json

GROUPS = {
    "arrays": ("vortex-array", "vortex-fsst", "vortex-zstd",
               "vortex-datetime-parts", "vortex-alp", "vortex-bytebool",
               "vortex-decimal-byte-parts", "vortex-fastlanes", "vortex-onpair",
               "vortex-parquet-variant", "vortex-pco", "vortex-sparse", "vortex-zigzag"),
    "io": ("vortex-layout", "vortex-file", "vortex-io", "vortex-arrow"),
    "buffer": ("vortex-buffer", "flatbuffers", "prost"),
    "formats": ("orc-rust", "arrow-avro", "arrow-ipc", "arrow-data", "arrow-array", "parquet", "parquet-variant",
                "arrow-schema", "arrow-json", "arrow-row"),
}
CONFIGURATIONS = {
    "vortex-array": {"features": "_test-harness,table-display", "patches": ["flatbuffers", "prost"]},
    "vortex-fsst": {"features": "vortex-array/table-display", "patches": ["vortex-array", "flatbuffers", "prost"]},
    "vortex-zstd": {"features": "vortex-array/table-display", "patches": ["vortex-array", "flatbuffers", "prost"]},
    "vortex-datetime-parts": {"features": "vortex-array/table-display",
                               "patches": ["vortex-array", "flatbuffers", "prost"]},
    "vortex-alp": {"features": "vortex-array/table-display",
                   "patches": ["vortex-array", "vortex-fastlanes", "flatbuffers", "prost"],
                   "clippy_tests": True},
    "vortex-bytebool": {"features": "vortex-array/table-display",
                        "patches": ["vortex-array", "flatbuffers", "prost"],
                        "clippy_tests": True},
    "vortex-decimal-byte-parts": {"features": "vortex-array/_test-harness,vortex-array/table-display",
                                  "patches": ["vortex-array", "flatbuffers", "prost"],
                                  "clippy_tests": True},
    "vortex-fastlanes": {"features": "_test-harness,vortex-array/table-display",
                         "patches": ["vortex-array", "flatbuffers", "prost"],
                         "clippy_tests": True},
    "vortex-onpair": {"features": "vortex-array/table-display",
                      "patches": ["vortex-array", "flatbuffers", "prost"],
                      "clippy_tests": True},
    "vortex-parquet-variant": {"features": "vortex-array/table-display",
                               "patches": ["vortex-array", "vortex-file", "vortex-layout",
                                           "vortex-io", "vortex-datetime-parts", "flatbuffers", "prost",
                                           "parquet-variant"],
                               "clippy_tests": True},
    "vortex-pco": {"features": "vortex-array/table-display",
                   "patches": ["vortex-array", "flatbuffers", "prost"],
                   "clippy_tests": True},
    "vortex-sparse": {"features": "vortex-array/table-display",
                      "patches": ["vortex-array", "flatbuffers", "prost"],
                      "clippy_tests": True},
    "vortex-zigzag": {"features": "vortex-array/table-display",
                      "patches": ["vortex-array", "flatbuffers", "prost"],
                      "clippy_tests": True},
    "vortex-layout": {"features": "tokio,vortex-io/tokio,vortex-array/_test-harness",
                      "patches": ["vortex-array", "vortex-io", "flatbuffers", "prost"], "clippy_tests": True},
    "vortex-file": {"features": "tokio,vortex-array/table-display",
                    "patches": ["vortex-array", "vortex-io", "vortex-layout", "flatbuffers", "prost"], "clippy_tests": True},
    "vortex-io": {"features": "object_store", "patches": ["vortex-array", "flatbuffers", "prost"], "clippy_tests": True},
    "vortex-arrow": {"features": "vortex-array/_test-harness,vortex-array/table-display",
                     "patches": ["vortex-array", "vortex-buffer", "arrow-array", "arrow-data", "vortex-fsst", "flatbuffers", "prost"],
                     "clippy_tests": True},
    "vortex-buffer": {"features": None, "patches": ["prost"]},
    "flatbuffers": {"features": None, "patches": [], "clippy_conf_dir": "vendor/flatbuffers"},
    "prost": {"features": None, "patches": [], "clippy_conf_dir": "vendor/prost"},
    "orc-rust": {"features": None, "patches": ["flatbuffers"], "no_default_features": True,
                 "clippy_tests": True, "clippy_conf_dir": "vendor/orc-rust"},
    "arrow-avro": {"features": "deflate,snappy", "patches": [], "no_default_features": True,
                   "clippy_tests": True, "clippy_conf_dir": "vendor/arrow-avro",
                   "arrow_test_data": "vendor/arrow-avro/test/arrow-testing/data"},
    "arrow-ipc": {"features": "lz4,zstd", "patches": ["flatbuffers"],
                  "clippy_tests": True, "clippy_conf_dir": "vendor/arrow-ipc"},
    "arrow-data": {"features": "ffi", "patches": [],
                   "clippy_tests": True, "clippy_conf_dir": "vendor/arrow-data"},
    "arrow-array": {"features": "ffi", "patches": ["arrow-data"],
                    "clippy_tests": True, "clippy_conf_dir": "vendor/arrow-array"},
    "arrow-schema": {"features": None, "patches": [],
                     "clippy_tests": True, "clippy_conf_dir": "vendor/arrow-schema"},
    "arrow-json": {"features": None, "patches": [],
                   "clippy_tests": True, "clippy_conf_dir": "vendor/arrow-json"},
    "arrow-row": {"features": None, "patches": [],
                  "clippy_tests": True, "clippy_conf_dir": "vendor/arrow-row"},
    "parquet": {"features": "default", "patches": ["arrow-array", "arrow-data", "arrow-ipc", "flatbuffers", "parquet-variant"],
                "integration_tests": True, "clippy_tests": True,
                "clippy_conf_dir": "vendor/parquet",
                "arrow_test_data": "vendor/parquet/test/arrow-testing/data",
                "parquet_test_data": "vendor/parquet/test/parquet-testing/data"},
    "parquet-variant": {"features": None, "patches": ["arrow-array", "arrow-data"],
                        "integration_tests": True, "clippy_tests": True,
                        "clippy_conf_dir": "vendor/parquet-variant",
                        "parquet_test_data": "vendor/parquet-variant/test/parquet-testing/data"},
}


# Same-version ownership graph reviewed from every committed standalone lock.
# Preserve each provider's existing codecs, feature flags and unrelated patches.
# ORC's existing Arrow 58 test graph remains registry-owned; these 59.3.2
# patches apply only to matching package versions.
ARROW_ARRAY_CONSUMERS = frozenset({
    "vortex-layout", "vortex-file", "vortex-arrow", "vortex-pco", "vortex-sparse",
    "vortex-parquet-variant", "orc-rust", "arrow-avro", "arrow-ipc", "parquet",
    "parquet-variant", "arrow-json", "arrow-row",
})
ARROW_OWNERSHIP_CONSUMERS = {
    "arrow-schema": frozenset(CONFIGURATIONS) - {"flatbuffers", "prost", "arrow-schema"},
    "arrow-array": ARROW_ARRAY_CONSUMERS,
    "arrow-data": ARROW_ARRAY_CONSUMERS | {"arrow-array"},
    "arrow-json": frozenset({"orc-rust", "parquet"}),
    "arrow-row": frozenset({"orc-rust", "arrow-avro", "parquet", "parquet-variant", "vortex-parquet-variant"}),
}


def provider_patches(crate: str) -> list[str]:
    return list(dict.fromkeys([
        *CONFIGURATIONS[crate]["patches"],
        *(name for name, consumers in ARROW_OWNERSHIP_CONSUMERS.items() if crate in consumers),
    ]))


def configuration_arguments(crate: str) -> list[str]:
    config = CONFIGURATIONS[crate]
    args = ["--manifest-path", f"vendor/{crate}/Cargo.toml"]
    for patch in provider_patches(crate):
        args += ["--config", f'patch.crates-io.{patch}.path="vendor/{patch}"']
    if config.get("no_default_features"):
        args.append("--no-default-features")
    if config["features"]:
        args += ["--features", config["features"]]
    elif not config.get("no_default_features"):
        args.append("--all-features")
    return args


def commands(crate: str) -> list[list[str]]:
    if crate == "prost":
        manifest = ["--locked", "--manifest-path", "vendor/prost/Cargo.toml"]
        return [
            ["cargo", "test", *manifest, "--lib", "--", "--test-threads=2"],
            ["cargo", "test", *manifest, "--lib", "--features", "no-recursion-limit",
             "--", "--test-threads=2"],
            ["cargo", "clippy", *manifest, "--lib", "--tests", "--", "-D", "warnings"],
            ["cargo", "clippy", *manifest, "--lib", "--tests", "--features", "no-recursion-limit",
             "--", "-D", "warnings"],
            ["cargo", "check", *manifest, "--no-default-features"],
            ["cargo", "check", *manifest, "--no-default-features", "--features", "no-recursion-limit"],
        ]
    args = configuration_arguments(crate)
    if crate == "flatbuffers":
        return [["cargo", "test", "--locked", *args],
                ["cargo", "clippy", "--locked", *args, "--all-targets", "--", "-D", "warnings"],
                ["cargo", "check", "--locked", "--manifest-path", "vendor/flatbuffers/Cargo.toml",
                 "--no-default-features"]]
    if crate == "vortex-buffer":
        # No --lib: retain buffer's integration targets and Cargo doctests.
        return [["cargo", "test", "--locked", *args],
                ["cargo", "clippy", "--locked", *args, "--all-targets", "--", "-D", "warnings"]]
    targets = ["--lib", "--tests"] if CONFIGURATIONS[crate].get("clippy_tests") else ["--lib"]
    test_targets = ["--lib", "--tests"] if CONFIGURATIONS[crate].get("integration_tests") else ["--lib"]
    return [["cargo", "test", "--locked", *args, *test_targets, "--", "--test-threads=2"],
            ["cargo", "clippy", "--locked", *args, *targets, "--", "-D", "warnings"]]


def run_group(group: str, output: Path, *, root: Path = ROOT) -> dict:
    if output.exists() or output.is_symlink():
        raise ValueError("provider evidence directory must be new")
    output.mkdir(parents=True)
    env = dict(os.environ)
    target = Path(env.get("CARGO_TARGET_DIR", root / "target/providers"))
    env["CARGO_TARGET_DIR"] = str(target.resolve())
    report = {"schema_version": "shardloom.ci_provider_tests.v1", "group": group,
              "status": "running", "target_directory": str(target.resolve()), "commands": []}
    try:
        for crate in GROUPS[group]:
            # Vortex's aliases policy belongs only to its provider family.
            # Other codecs retain their own lint configuration, even if the
            # calling environment selected Vortex's configuration globally.
            crate_env = dict(env)
            crate_env["CLIPPY_CONF_DIR"] = str(root / CONFIGURATIONS[crate].get(
                "clippy_conf_dir", "vendor/vortex-array"))
            fixture_paths = {}
            for fixture_key in ("arrow_test_data", "parquet_test_data"):
                if fixture_path := CONFIGURATIONS[crate].get(fixture_key):
                    fixture_root = root / fixture_path
                    if not fixture_root.is_dir():
                        raise ValueError(f"missing committed provider test fixtures: {crate} {fixture_key}")
                    fixture_paths[fixture_key] = str(fixture_root)
                    crate_env[fixture_key.upper()] = str(fixture_root)
            frozen = root / ".github/provider-locks" / f"{crate}.lock"
            if frozen.is_symlink() or not frozen.is_file():
                raise ValueError(f"missing reviewed provider lockfile: {crate}")
            lock = root / "vendor" / crate / "Cargo.lock"
            if lock.is_symlink():
                raise ValueError("provider lockfile must not be a symlink")
            shutil.copyfile(frozen, lock)
            provider_commands = commands(crate)
            verb_totals = {}
            for command in provider_commands:
                verb_totals[command[1]] = verb_totals.get(command[1], 0) + 1
            verb_ordinals = {}
            for command in provider_commands:
                verb = command[1]
                verb_ordinals[verb] = verb_ordinals.get(verb, 0) + 1
                if verb_totals[verb] > 1:
                    log_name = f"{crate}-{verb_ordinals[verb]:02d}-{verb}.log"
                else:
                    log_name = f"{crate}-{verb}.log"
                log = output / log_name
                started = time.monotonic()
                entry = {"crate": crate, "command": command, "lock_sha256": sha(frozen),
                         "clippy_conf_dir": crate_env["CLIPPY_CONF_DIR"]}
                entry.update(fixture_paths)
                report["commands"].append(entry)
                print("Running " + " ".join(command), flush=True)
                with log.open("xb") as stream:
                    result = subprocess.run(command, cwd=root, env=crate_env, stdout=stream,
                                            stderr=subprocess.STDOUT, check=False)
                entry.update(returncode=result.returncode, seconds=time.monotonic() - started,
                             log=log.name, log_sha256=sha(log))
                if result.returncode or sha(lock) != sha(frozen):
                    raise RuntimeError(f"provider {crate} {command[1]} failed; see {log}")
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        write_json(output / "report.json", report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", choices=GROUPS, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run_group(args.group, args.output), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
