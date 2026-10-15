#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Observe the existing 0.6.0 registry distributions without rebuilding/uploading.

Downloads twelve existing Actions artifacts, binds each distribution to the
passed channel proof AND live registry inventory, inspects package/bundled-CLI
bytes without executing them, and writes channel-specific checksums, CycloneDX
file inventories and unsigned post-publication provenance. This is not a
cryptographic attestation verifier or a complete compiled dependency SBOM.
"""
from __future__ import annotations

import argparse
import ast
import datetime as dt
import email.parser
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import urllib.parse
import urllib.request
import zipfile

from local_uat_storage import available_bytes, require_local_path

ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.6.0"
RELEASE_SOURCE = "936313473fedca78176d1fd983446673ed8ae62b"
WORKFLOW = ".github/workflows/pypi-publish-draft.yml"
CHANNELS = {
    "testpypi": (38668844039, RELEASE_SOURCE, "https://test.pypi.org"),
    "pypi": (38671076441, "9f73f12d2acc25b0b245d9f50ee679610db27d03", "https://pypi.org"),
}
KINDS = {
    "python-dist-macos": ("shardloom-0.6.0-cp310-abi3-macosx_26_0_arm64.whl", "macos-aarch64", "shardloom"),
    "python-dist-linux": ("shardloom-0.6.0-cp310-abi3-manylinux_2_39_x86_64.whl", "linux-x86_64", "shardloom"),
    "python-dist-windows": ("shardloom-0.6.0-cp313-cp313-win_amd64.whl", "windows-x86_64", "shardloom.exe"),
    "python-dist-sdist": ("shardloom-0.6.0.tar.gz", None, None),
}
NATIVE_PROOF_KINDS = {
    "native-registry-proof-macos": "python-dist-macos",
    "native-registry-proof-linux": "python-dist-linux",
}
MAX_ARCHIVE = 64 << 20
MAX_CLI = 128 << 20
MAX_NATIVE = 128 << 20
MAX_NATIVE_PROOF = 1 << 20
MAX_JSON = 4 << 20
MAX_WORKSPACE = 384 << 20


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha_bytes(value):
    return hashlib.sha256(value).hexdigest()


def stream_sha(stream, maximum):
    digest, size = hashlib.sha256(), 0
    while block := stream.read(1 << 20):
        size += len(block)
        require(size <= maximum, "stream exceeds declared bound")
        digest.update(block)
    return digest.hexdigest(), size


def file_sha(path):
    with Path(path).open("rb") as stream:
        return stream_sha(stream, MAX_WORKSPACE)[0]


def json_bytes(value):
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key: " + key)
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def read_small(path, limit=MAX_JSON):
    require(path.is_file() and path.stat().st_size <= limit, "missing or oversized input: " + str(path))
    raw = path.read_bytes()
    require(len(raw) <= limit, "input grew beyond bound")
    return raw


def safe_member(name):
    path = PurePosixPath(name)
    require(not path.is_absolute() and ".." not in path.parts and "\\" not in name,
            "unsafe archive member")
    return path


def package_metadata(raw):
    require(len(raw) <= MAX_JSON, "package metadata too large")
    metadata = email.parser.BytesParser().parsebytes(raw)
    require(metadata["Name"] == "shardloom" and metadata["Version"] == VERSION,
            "distribution package identity mismatch")
    return {"name": metadata["Name"], "version": metadata["Version"],
            "requires_python": metadata["Requires-Python"],
            "license_expression": metadata["License-Expression"],
            "requires_dist": metadata.get_all("Requires-Dist", []),
            "metadata_sha256": sha_bytes(raw)}


def inspect_distribution(path, artifact_name, source_pyproject):
    _, platform, executable = KINDS[artifact_name]
    if platform is not None:
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            names = [row.filename for row in members]
            require(len(names) == len(set(names)) and len(names) <= 4096, "ambiguous wheel inventory")
            for name in names:
                safe_member(name)
            metadata_name = "shardloom-0.6.0.dist-info/METADATA"
            wheel_name = "shardloom-0.6.0.dist-info/WHEEL"
            require(sum(name.endswith(".dist-info/METADATA") for name in names) == 1,
                    "ambiguous package metadata")
            require(archive.getinfo(metadata_name).file_size <= MAX_JSON and
                    archive.getinfo(wheel_name).file_size <= 65536, "oversized wheel metadata")
            metadata = package_metadata(archive.read(metadata_name))
            wheel = archive.read(wheel_name)
            expected_tag = path.name[len("shardloom-0.6.0-"):-len(".whl")]
            wheel_metadata = email.parser.BytesParser().parsebytes(wheel)
            require(wheel_metadata["Root-Is-Purelib"] == "false" and
                    wheel_metadata.get_all("Tag") == [expected_tag], "wheel platform contract differs")
            binary_suffix = f"shardloom/bin/{platform}/{executable}"
            # Platform wheels may retain setuptools' purelib relocation tree;
            # pip installs that tree into site-packages without changing bytes.
            allowed = {binary_suffix, "shardloom-0.6.0.data/purelib/" + binary_suffix}
            binary_members = [name for name in names if
                (name.startswith("shardloom/bin/") or "/shardloom/bin/" in name) and not name.endswith("/")]
            require(len(binary_members) == 1 and binary_members[0] in allowed,
                    "wheel must contain exactly its platform CLI")
            binary_name = binary_members[0]
            require(archive.getinfo(binary_name).file_size <= MAX_CLI, "oversized bundled CLI")
            with archive.open(binary_name) as stream:
                digest, size = stream_sha(stream, MAX_CLI)
            native_names = [name for name in names if name.endswith((".so", ".pyd", ".dylib", ".dll"))]
            native = None
            if platform in {"macos-aarch64", "linux-x86_64"}:
                require(native_names == ["shardloom/_native.abi3.so"] and
                        expected_tag.startswith("cp310-abi3-"), "native wheel ABI/module inventory differs")
                native_name = native_names[0]
                require(archive.getinfo(native_name).file_size <= MAX_NATIVE, "oversized native module")
                with archive.open(native_name) as stream:
                    native_digest, native_size = stream_sha(stream, MAX_NATIVE)
                native = {"member": native_name, "sha256": native_digest,
                          "size_bytes": native_size, "python_abi": "cp310-abi3"}
            else:
                require(not native_names, "Windows CLI package unexpectedly contains a native Python module")
            return {"package_metadata": metadata, "wheel_metadata_sha256": sha_bytes(wheel),
                    "wheel_tag": expected_tag, "bundled_cli": {"member": binary_name,
                    "sha256": digest, "size_bytes": size, "platform": platform},
                    "native_library": native,
                    "inspection_scope": "package_metadata_and_bundled_CLI_and_native_module_bytes;no_execution"}
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        require(len(members) <= 4096, "sdist inventory exceeds bound")
        names = [row.name for row in members]
        require(len(names) == len(set(names)), "duplicate sdist members")
        require(sum(row.size for row in members) <= 32 << 20, "sdist expansion exceeds bound")
        for row in members:
            safe_member(row.name)
            require(row.isdir() or row.isfile(), "sdist contains link/device member")
            require("/shardloom/bin/" not in row.name and not row.name.endswith("/shardloom.exe"),
                    "source distribution unexpectedly bundles native CLI")
            require(not row.name.endswith((".so", ".pyd", ".dylib", ".dll", ".exe")),
                    "source distribution unexpectedly contains a compiled native module")
        def member_bytes(name):
            row = archive.getmember(name)
            require(row.isfile() and row.size <= MAX_JSON, "invalid sdist metadata member")
            with archive.extractfile(row) as stream:
                return stream.read(MAX_JSON + 1)
        metadata = package_metadata(member_bytes("shardloom-0.6.0/PKG-INFO"))
        pyproject = member_bytes("shardloom-0.6.0/pyproject.toml")
        require(pyproject == source_pyproject, "sdist pyproject differs from actual build source")
        return {"package_metadata": metadata, "clean_sdist_no_bundled_cli": True,
                "source_pyproject_sha256": sha_bytes(pyproject),
                "inspection_scope": "source_package_metadata_and_no_bundled_CLI;no_execution"}


def validate_inventory(proof, live, channel):
    require(proof.get("schema_version") == "shardloom.python_registry_package_proof.v1" and
            proof.get("proof_status") == "passed" and proof.get("package_version") == VERSION and
            proof.get("channel_id") == channel and proof.get("fallback_attempted") is False and
            proof.get("external_engine_invoked") is False, "prior channel proof is not accepted")
    expected = proof.get("registry_release_artifacts", [])
    actual = live.get("urls", [])
    names = {value[0] for value in KINDS.values()}
    require(len(expected) == len(actual) == 4 and {row["filename"] for row in expected} == names and
            {row["filename"] for row in actual} == names, "registry inventory is not exact four distributions")
    for row in expected:
        found = next(item for item in actual if item["filename"] == row["filename"])
        require(found["digests"]["sha256"] == row["sha256"] and found["size"] == row["size"]
                and found["url"] == row["url"] and not found.get("yanked", False), "registry digest/URL/size changed")
        require(re.fullmatch("[0-9a-f]{64}", row["sha256"]) is not None and
                type(row["size"]) is int and 0 < row["size"] <= MAX_ARCHIVE, "invalid distribution identity")
    return {row["filename"]: row for row in expected}


def resolve_cargo_target(root):
    """Read Cargo configuration without fetching dependencies or building code."""
    command = ["cargo", "metadata", "--offline", "--no-deps", "--format-version", "1"]
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        process = subprocess.Popen(command, cwd=root, stdin=subprocess.DEVNULL,
            stdout=out, stderr=err, start_new_session=True)
        started = time.monotonic()
        try:
            while process.poll() is None:
                require(time.monotonic() - started < 30, "Cargo metadata deadline exceeded")
                require(out.tell() <= MAX_JSON and err.tell() <= 65536, "Cargo metadata output exceeds bound")
                time.sleep(0.05)
            require(process.returncode == 0, "offline Cargo metadata failed; no observation started")
            require(out.tell() <= MAX_JSON and err.tell() <= 65536, "Cargo metadata output exceeds bound")
        finally:
            for action in (signal.SIGTERM, signal.SIGKILL):
                try:
                    os.killpg(process.pid, action)
                except ProcessLookupError:
                    break
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
            process.wait(timeout=2)
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                pass
            else:
                raise ValueError("Cargo metadata process-group cleanup unproved")
        out.seek(0)
        metadata = strict_json(out.read(MAX_JSON + 1))
    value = metadata.get("target_directory") if isinstance(metadata, dict) else None
    require(isinstance(value, str) and Path(value).is_absolute(), "Cargo metadata requires an absolute target_directory")
    target = require_local_path(Path(value), Path.home(), sys.platform)
    require(not any(part.casefold() in {"clouddocs", "icloud drive"} for part in target.parts),
            "generated artifacts require a local nonsynced target directory")
    return target


class Observer:
    def __init__(self, root):
        self.root = root
        self.target = resolve_cargo_target(root)
        require(available_bytes(self.target) >= (12 << 30) + MAX_WORKSPACE,
                "insufficient free space for observation and12GiB headroom")
        self.target.mkdir(parents=True, exist_ok=True)
        self.directory = Path(tempfile.mkdtemp(prefix="registry-evidence-060-", dir=self.target))
        self.deadline = time.monotonic() + 900
        self.commands = []

    def local_ref(self, path):
        """Keep historical checkout-relative refs; identify external Cargo outputs."""
        return str(path.relative_to(self.root)) if path.is_relative_to(self.root) else str(path)

    def guard(self):
        require(time.monotonic() < self.deadline, "observation deadline exceeded")
        require(shutil.disk_usage(self.directory).free >= 12 << 30, "less than12GiB free space")
        require(sum(path.stat().st_size for path in self.directory.rglob("*") if path.is_file()) <= MAX_WORKSPACE,
                "observation workspace exceeds384MiB")

    def command(self, command, destination, limit=MAX_JSON):
        self.guard()
        error_path = destination.with_name(destination.name + ".stderr")
        record = {"command": list(map(str, command)), "stdout_ref": self.local_ref(destination),
                  "stderr_ref": self.local_ref(error_path), "process_group_drained": False}
        self.commands.append(record)
        process = None
        started = time.monotonic()
        try:
            with destination.open("xb") as out, error_path.open("xb") as err:
                process = subprocess.Popen(command, cwd=self.root, stdin=subprocess.DEVNULL,
                    stdout=out, stderr=err, start_new_session=True)
                record["pid"] = process.pid
                while process.poll() is None:
                    self.guard()
                    require(time.monotonic() - started < 120, "download/source command deadline exceeded")
                    require(out.tell() <= limit and err.tell() <= 65536, "command output limit exceeded")
                    time.sleep(0.05)
                require(destination.stat().st_size <= limit and error_path.stat().st_size <= 65536,
                        "command output limit exceeded")
                require(process.returncode == 0, "read-only command failed; preserved command stderr")
        finally:
            if process is not None:
                for action in (signal.SIGTERM, signal.SIGKILL):
                    try:
                        os.killpg(process.pid, action)
                    except ProcessLookupError:
                        break
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        pass
                process.poll()
                record["returncode"] = process.returncode
                try:
                    os.killpg(process.pid, 0)
                except ProcessLookupError:
                    record["process_group_drained"] = True
                record["direct_child_reaped"] = process.returncode is not None
            record["seconds"] = time.monotonic() - started
            (self.directory / "commands.json").write_bytes(json_bytes(self.commands))
        require(record["process_group_drained"] and record["direct_child_reaped"], "command cleanup unproved")
        self.guard()
        return destination

    def gh_json(self, endpoint, name):
        path = self.command(["gh", "api", endpoint], self.directory / (name + ".json"))
        return strict_json(read_small(path)), path

    def registry_json(self, url, name):
        self.guard()
        require(urllib.parse.urlsplit(url).hostname in {"pypi.org", "test.pypi.org"}, "unexpected registry host")
        with urllib.request.urlopen(url, timeout=30) as response:
            require(response.status == 200, "registry JSON unavailable")
            raw = response.read(MAX_JSON + 1)
        require(len(raw) <= MAX_JSON, "registry JSON exceeds bound")
        path = self.directory / (name + ".json")
        path.write_bytes(raw)
        self.guard()
        return strict_json(raw), path

    def source(self, commit, name, channel):
        path = self.directory / (channel + "-" + name.replace("/", "_"))
        self.command(["git", "show", commit + ":" + name], path)
        return read_small(path), {"path": name, "source_commit": commit, "sha256": file_sha(path)}


def cyclonedx(channel, artifacts, source_inputs, timestamp):
    components, dependencies = [], []
    for artifact in artifacts:
        ref = "sha256:" + artifact["sha256"]
        components.append({"type": "file", "bom-ref": ref, "name": artifact["filename"],
                           "hashes": [{"alg": "SHA-256", "content": artifact["sha256"]}],
                           "externalReferences": [{"type": "distribution", "url": artifact["url"]}]})
        children = []
        for field, suffix, scope in (("bundled_cli", "bundled-cli", "executable"),
                                     ("native_library", "native-library", "native Python module")):
            binary = artifact.get(field)
            if binary:
                child = ref + ":" + suffix
                components.append({"type": "file", "bom-ref": child,
                                   "name": artifact["filename"] + "!/" + binary["member"],
                                   "hashes": [{"alg": "SHA-256", "content": binary["sha256"]}],
                                   "properties": [{"name": "shardloom:inventory-scope", "value":
                                       "observed bundled " + scope + " bytes;not complete compiled dependencies"}]})
                children.append(child)
        if children:
            dependencies.append({"ref": ref, "dependsOn": children})
    return {"bomFormat": "CycloneDX", "specVersion": "1.5", "version": 1,
            "metadata": {"timestamp": timestamp,
                "tools": [{"vendor": "ShardLoom", "name": "registry_release_evidence.py", "version": "1"}],
                "component": {"type": "application", "name": "shardloom", "version": VERSION,
                              "purl": "pkg:pypi/shardloom@0.6.0", "licenses": [{"license": {"id": "Apache-2.0"}}]},
                "properties": [{"name": "shardloom:channel", "value": channel},
                    {"name": "shardloom:inventory-scope", "value": "post-publication distribution files and observed bundled CLI/native module bytes;not a build attestation or complete compiled dependency SBOM"},
                    {"name": "shardloom:source-inputs", "value": json.dumps(source_inputs, sort_keys=True)}]},
            "components": components, "dependencies": dependencies}


def native_proof_programs(source):
    programs = {}
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {"IMPORT_CHECK", "RUN_TESTS"}:
                    require(target.id not in programs, "duplicate native proof program")
                    programs[target.id] = ast.literal_eval(node.value)
    require(set(programs) == {"IMPORT_CHECK", "RUN_TESTS"} and
            all(isinstance(value, str) for value in programs.values()), "missing native proof programs")
    return programs


def verify_native_proof(files, distribution, programs, *, package_version=VERSION):
    """Bind actual build/test logs to the observed wheel; execute no package code."""
    required_json = {"native-build.json", "proof/report.json", "proof/integration.json"}
    require(required_json <= files.keys(), "missing native installed-wheel proof files")
    require(sum(len(raw) for raw in files.values()) <= MAX_NATIVE_PROOF, "native proof expansion exceeds bound")
    build = strict_json(files["native-build.json"])
    proof = strict_json(files["proof/report.json"])
    integration = strict_json(files["proof/integration.json"])
    require(build.get("schema_version") == "shardloom.native_python_build.v1" and
            build.get("status") == "passed" and build.get("package_version") == package_version and
            build.get("python_abi") == "cp310-abi3", "native build report is not accepted")
    require(proof.get("schema_version") == "shardloom.native_python_wheel_proof.v1" and
            proof.get("status") == proof.get("uninstall_status") == "passed" and
            proof.get("publication_attempted") is False and proof.get("performance_claim") is False,
            "native installed-wheel proof is not accepted")
    expected_integration = {"tests": 80, "failures": 0, "errors": 0, "skipped": 0, "status": "passed"}
    require(proof.get("integration") == integration == expected_integration and
            all(type(record[field]) is int
                for record in (integration, proof["integration"])
                for field in ("tests", "failures", "errors", "skipped")),
            "native binding suite lacks complete zero-skip acceptance")
    native = distribution.get("native_library")
    require(isinstance(native, dict), "native proof has no observed native module")
    integrity, stage = proof.get("integrity", {}), proof.get("stage", {})
    require(integrity.get("status") == stage.get("status") == "passed" and
            integrity.get("wheel") == distribution["filename"] and
            integrity.get("wheel_sha256") == distribution["sha256"] and
            integrity.get("wheel_bytes") == distribution["size_bytes"] and
            integrity.get("tag") == distribution["wheel_tag"], "native proof wheel identity differs")
    require(integrity.get("native_library_sha256") == stage.get("native_library_sha256") ==
            build.get("library_sha256") == native["sha256"] and
            build.get("library_bytes") == native["size_bytes"], "native proof module identity differs")
    require(integrity.get("cli_sha256") == stage.get("cli_sha256") == distribution["bundled_cli"]["sha256"],
            "native proof CLI identity differs")
    build_command = build.get("command", [])
    require(len(build_command) == 9 and build_command[1:] == ["build", "--locked", "-p", "shardloom-python",
            "--features", "python-native", "--message-format=json-render-diagnostics", "--release"],
            "native build command differs from approved release recipe")
    installed = proof.get("installed_load", {})
    require(installed.get("version") == package_version and installed.get("isolated_install") is True,
            "native package was not isolated during load")
    linux = distribution["bundled_cli"]["platform"] == "linux-x86_64"
    expected_steps = {"build", "create_environment", "install_optional_consumer", "install",
                      "load", "integration", "uninstall", "verify_uninstalled"}
    if linux:
        expected_steps.add("linux_compatibility_audit")
    rows = proof.get("steps", [])
    steps = {row["name"]: row for row in rows}
    require(len(steps) == len(rows) and set(steps) == expected_steps, "native proof step inventory differs")
    expected_files = required_json | {"proof/" + name + ".log" for name in expected_steps}
    require(set(files) == expected_files, "native proof artifact inventory differs")
    for name, row in steps.items():
        require(type(row.get("returncode")) is int and row["returncode"] == 0 and
                row.get("log") == name + ".log" and
                row.get("log_sha256") == sha_bytes(files["proof/" + name + ".log"]),
                "native proof step failed or its log changed: " + name)
    require(steps["load"]["command"][1:] == ["-I", "-c", programs["IMPORT_CHECK"],
            native["sha256"], distribution["bundled_cli"]["sha256"]], "native load program/hash binding differs")
    require(steps["integration"]["command"][1:4] == ["-I", "-c", programs["RUN_TESTS"]] and
            len(steps["integration"]["command"]) == 6, "native integration program differs")
    require(strict_json(files["proof/load.log"]) == installed, "native load output differs from proof")
    require(len(steps["install"]["command"]) == 8 and steps["install"]["command"][1:7] ==
            ["-I", "-m", "pip", "install", "--no-index", "--no-deps"] and
            PurePosixPath(steps["install"]["command"][-1]).name == distribution["filename"],
            "native install did not select the observed wheel")
    require(steps["uninstall"]["command"][1:] == ["-I", "-m", "pip", "uninstall", "--yes", "shardloom"],
            "native uninstall command differs")
    require(steps["verify_uninstalled"]["command"][1:] == ["-I", "-c",
            "import importlib.util, pathlib, sys; "
            "assert importlib.util.find_spec('shardloom') is None; "
            "assert all(not pathlib.Path(path).exists() for path in sys.argv[1:])",
            installed["native_library"], installed["cli"]], "native uninstall verification differs")
    if linux:
        audit = proof.get("linux_audit", {})
        require(audit.get("status") == "passed" and audit.get("auditwheel_version") == "6.8.2" and
                audit.get("platform") == "manylinux_2_39_x86_64" and audit.get("payloads_unchanged") is True and
                audit.get("unchanged_payload_count") == 232 and audit.get("wheel_sha256") == distribution["sha256"] and
                integrity.get("linux_audit_verified") is True, "native Linux portability proof differs")
        require(steps["linux_compatibility_audit"]["command"][1:10] == ["-m", "auditwheel", "repair", "--only-plat",
                "--plat", "manylinux_2_39_x86_64", "--patcher", "none", "--wheel-dir"], "native Linux audit recipe differs")
    else:
        require("linux_audit" not in proof and integrity.get("linux_audit_verified") is False,
                "macOS wheel unexpectedly claims Linux portability")
    return {"status": "passed", "wheel": distribution["filename"], "wheel_sha256": distribution["sha256"],
            "native_library_sha256": native["sha256"], "cli_sha256": distribution["bundled_cli"]["sha256"],
            "integration": integration, "uninstall_status": proof["uninstall_status"],
            "source_program_sha256": {name: sha_bytes(value.encode()) for name, value in programs.items()},
            "native_build": build, "proof_report": proof,
            "original_files": [{"name": name, "size_bytes": len(raw), "sha256": sha_bytes(raw),
                                "text": raw.decode("utf-8")} for name, raw in sorted(files.items())],
            "fresh_execution_performed_by_observer": False}


def observe_native_proof(observer, item, distribution, channel, run_id, source, programs):
    name = item["name"]
    require(name in NATIVE_PROOF_KINDS and item["expired"] is False and
            item["workflow_run"]["id"] == run_id and item["workflow_run"]["head_sha"] == source,
            "native proof workflow identity differs")
    require(type(item["size_in_bytes"]) is int and 0 < item["size_in_bytes"] <= MAX_NATIVE_PROOF,
            "native proof archive exceeds bound")
    archive_path = observer.directory / f"{channel}-{name}.zip"
    observer.command(["gh", "api", f"repos/depsilon/shardloom/actions/artifacts/{item['id']}/zip"],
                     archive_path, MAX_NATIVE_PROOF)
    digest = file_sha(archive_path)
    require(item["digest"] == "sha256:" + digest and archive_path.stat().st_size == item["size_in_bytes"],
            "native proof archive digest/size mismatch")
    with zipfile.ZipFile(archive_path) as archive:
        members = archive.infolist()
        names = [row.filename for row in members]
        require(len(names) == len(set(names)) and len(names) <= 32, "ambiguous native proof archive")
        require(sum(row.file_size for row in members) <= MAX_NATIVE_PROOF, "native proof expansion exceeds bound")
        for row in members:
            safe_member(row.filename)
            require(not row.is_dir(), "unexpected native proof directory entry")
        files = {row.filename: archive.read(row) for row in members}
    result = verify_native_proof(files, distribution, programs)
    result.update(workflow_artifact_id=item["id"], workflow_artifact_name=name,
                  workflow_run_id=run_id, source_commit=source,
                  workflow_artifact_archive_sha256=digest,
                  workflow_artifact_archive_size_bytes=item["size_in_bytes"])
    return result


def observe_channel(observer, channel):
    root = observer.root
    run_id, source, registry = CHANNELS[channel]
    proof_path = root / f"docs/release/channel-proofs/{channel}-v0.6.0-transcript.json"
    proof_raw = read_small(proof_path)
    proof = strict_json(proof_raw)
    live, live_path = observer.registry_json(registry + "/pypi/shardloom/0.6.0/json", channel + "-registry")
    distributions = validate_inventory(proof, live, channel)
    workflow, workflow_path = observer.gh_json(f"repos/depsilon/shardloom/actions/runs/{run_id}", channel + "-workflow")
    require(workflow["id"] == run_id and workflow["head_sha"] == source and
            workflow["path"] == WORKFLOW and workflow["status"] == "completed" and
            workflow["conclusion"] == "success" and workflow["run_attempt"] == 1, "workflow identity/status differs")
    inventory, inventory_path = observer.gh_json(f"repos/depsilon/shardloom/actions/runs/{run_id}/artifacts", channel + "-artifacts")
    build_artifacts = inventory["artifacts"]
    require(inventory["total_count"] == len(build_artifacts) == 6 and
            {row["name"] for row in build_artifacts} == set(KINDS) | set(NATIVE_PROOF_KINDS),
            "workflow artifact inventory differs")
    lock_raw, lock_ref = observer.source(source, "Cargo.lock", channel)
    project_raw, project_ref = observer.source(source, "python/pyproject.toml", channel)
    workflow_raw, workflow_ref = observer.source(source, WORKFLOW, channel)
    prover_raw, prover_ref = observer.source(source, "scripts/prove_native_python_wheel.py", channel)
    source_inputs = [lock_ref, project_ref, workflow_ref, prover_ref]
    programs = native_proof_programs(prover_raw)
    lock = tomllib.loads(lock_raw.decode())
    project = tomllib.loads(project_raw.decode())["project"]
    require(project["name"] == "shardloom" and project["license"] == "Apache-2.0", "source package metadata differs")
    diff_path = observer.command(["git", "diff", "--name-only", RELEASE_SOURCE, source], observer.directory / (channel + "-source-delta.txt"))
    source_delta = read_small(diff_path).decode().splitlines()
    require(all(name.startswith("docs/") for name in source_delta), "registry build source differs beyond documentation")
    artifacts = []
    for item in sorted(build_artifacts, key=lambda row: row["name"]):
        observer.guard()
        name = item["name"]
        if name in NATIVE_PROOF_KINDS:
            continue
        filename = KINDS[name][0]
        expected = distributions[filename]
        require(item["expired"] is False and item["workflow_run"]["id"] == run_id and
                item["workflow_run"]["head_sha"] == source and type(item["id"]) is int,
                "expired or mismatched workflow artifact")
        require(type(item["size_in_bytes"]) is int and 0 < item["size_in_bytes"] <= MAX_ARCHIVE,
                "artifact archive size exceeds bound")
        archive_path = observer.directory / f"{channel}-{name}.zip"
        observer.command(["gh", "api", f"repos/depsilon/shardloom/actions/artifacts/{item['id']}/zip"], archive_path, MAX_ARCHIVE)
        digest = file_sha(archive_path)
        require(item["digest"] == "sha256:" + digest and archive_path.stat().st_size == item["size_in_bytes"],
                "downloaded workflow archive digest/size mismatch")
        directory = observer.directory / channel
        directory.mkdir(exist_ok=True)
        distribution_path = directory / filename
        with zipfile.ZipFile(archive_path) as archive:
            files = [row for row in archive.infolist() if not row.is_dir()]
            require(len(files) == 1 and files[0].filename == filename, "workflow archive does not contain exact distribution")
            require(files[0].file_size == expected["size"], "workflow distribution size differs")
            with archive.open(files[0]) as incoming, distribution_path.open("xb") as outgoing:
                shutil.copyfileobj(incoming, outgoing, 1 << 20)
        require(file_sha(distribution_path) == expected["sha256"] and distribution_path.stat().st_size == expected["size"],
                "workflow distribution does not match registry SHA/size")
        artifact = {"filename": filename, "sha256": expected["sha256"], "size_bytes": expected["size"],
                    "url": expected["url"], "workflow_artifact_id": item["id"],
                    "workflow_artifact_name": name, "workflow_artifact_archive_sha256": digest,
                    "workflow_artifact_archive_size_bytes": item["size_in_bytes"],
                    "local_path": observer.local_ref(distribution_path), "registry_digest_match": True,
                    "retrieval_source": "existing_GitHub_Actions_workflow_artifact"}
        artifact.update(inspect_distribution(distribution_path, name, project_raw))
        # The retained distribution has passed complete extraction, CRC, size,
        # registry hash and payload checks. Retire only its duplicate wrapper
        # before downloading the next archive; keep the total workspace bound.
        archive_path.unlink()
        artifact["workflow_archive_retired_after_verified_extraction"] = True
        artifacts.append(artifact)
    by_name = {row["workflow_artifact_name"]: row for row in artifacts}
    native_proofs = [observe_native_proof(observer, item,
        by_name[NATIVE_PROOF_KINDS[item["name"]]], channel, run_id, source, programs)
        for item in sorted(build_artifacts, key=lambda row: row["name"])
        if item["name"] in NATIVE_PROOF_KINDS]
    require(len(native_proofs) == 2, "missing native platform installation evidence")
    require(read_small(proof_path) == proof_raw, "channel proof changed during observation")
    timestamp = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    prefix = f"docs/release/channel-proofs/{channel}-v0.6.0-"
    sbom_path, checksums_path = prefix + "sbom.cdx.json", prefix + "checksums.sha256"
    sbom = json_bytes(cyclonedx(channel, artifacts, source_inputs, timestamp))
    checksums = "".join(row["sha256"] + "  " + row["filename"] + "\n" for row in sorted(artifacts, key=lambda row: row["filename"])).encode()
    provenance = {"schema_version": "shardloom.registry_release_evidence.v1", "channel_id": channel,
        "package_name": "shardloom", "package_version": VERSION, "source_commit": source,
        "workflow_run_id": run_id, "workflow_url": workflow["html_url"], "workflow_path": WORKFLOW,
        "workflow_attempt": 1, "proof_status": "passed",
        "workflow_observation": {key: workflow[key] for key in
            ("id", "head_sha", "path", "event", "status", "conclusion", "run_attempt", "html_url")},
        "provenance_status": "unsigned_post_publication_observation", "observed_at_utc": timestamp,
        "artifact_refs": artifacts, "sbom_ref": {"path": sbom_path, "sha256": sha_bytes(sbom)},
        "checksum_ref": {"path": checksums_path, "sha256": sha_bytes(checksums)},
        "channel_proof_ref": {"path": str(proof_path.relative_to(root)), "sha256": sha_bytes(proof_raw)},
        "registry_inventory_ref": {"path": observer.local_ref(live_path), "sha256": file_sha(live_path)},
        "workflow_metadata_ref": {"path": observer.local_ref(workflow_path), "sha256": file_sha(workflow_path)},
        "workflow_artifact_inventory_ref": {"path": observer.local_ref(inventory_path), "sha256": file_sha(inventory_path)},
        "native_installed_wheel_proofs": native_proofs,
        "source_inputs": source_inputs, "source_changes_from_release": source_delta,
        "runtime_and_packaging_source_equal_to_release": True, "release_source_commit": RELEASE_SOURCE,
        "source_declared_inventory": {"scope": "actual build source declarations;not compiled or linked dependency proof",
            "cargo_lock_package_count": len(lock["package"]), "cargo_lock_registry_package_count": sum("checksum" in p for p in lock["package"]),
            "python_required_dependencies": project.get("dependencies", []),
            "python_optional_dependencies": project.get("optional-dependencies", {})},
        "build_recipe": "locked release-user-surfaces CLI; Linux/macOS python-native ABI3 module and complete installed tests; Linux compatibility audit; Windows CLI-only wheel; clean source sdist",
        "crypto_attestation_verification_performed": False, "complete_compiled_dependency_inventory_claimed": False,
        "local_build_or_package_execution_performed": False, "publication_attempted": False,
        "package_upload_attempted": False, "fallback_attempted": False, "external_engine_invoked": False,
        "generator": {"path": "scripts/registry_release_evidence.py", "sha256": file_sha(Path(__file__))}}
    provenance_bytes = json_bytes(provenance)
    require(len(provenance_bytes) <= MAX_JSON, "registry provenance exceeds its checked-in evidence bound")
    return {sbom_path: sbom, checksums_path: checksums, prefix + "provenance.json": provenance_bytes}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    args = parser.parse_args()
    root = args.repo_root.resolve()
    require(os.name == "posix" and root == ROOT, "use this generator's POSIX publication worktree")
    require(re.fullmatch("[0-9a-f]{40}", RELEASE_SOURCE) is not None and
            all(type(run) is int and run > 0 and re.fullmatch("[0-9a-f]{40}", source) is not None
                for run, source, _ in CHANNELS.values()), "bind actual successful publication identities first")
    outputs = [root / f"docs/release/channel-proofs/{channel}-v0.6.0-{suffix}"
               for channel in CHANNELS for suffix in ("sbom.cdx.json", "checksums.sha256", "provenance.json")]
    require(not any(path.exists() for path in outputs), "preserve existing registry evidence; refusing overwrite")
    observer = Observer(root)
    try:
        prepared = {}
        for channel in CHANNELS:
            prepared.update(observe_channel(observer, channel))
        observer.guard()
        for path, raw in prepared.items():
            with (root / path).open("xb") as output:
                output.write(raw)
        summary = {"proof_status": "passed", "observation_directory": str(observer.directory),
                   "cargo_target_directory": str(observer.target),
                   "distribution_count": 8, "workflow_artifact_count": 12,
                   "native_installed_wheel_proof_count": 4,
                   "outputs": [{"path": path, "sha256": sha_bytes(raw)} for path, raw in prepared.items()],
                   "commands": observer.commands, "publication_attempted": False, "local_build_performed": False}
        (observer.directory / "observation.json").write_bytes(json_bytes(summary))
        print(json.dumps(summary, indent=2))
        return 0
    except BaseException as error:
        (observer.directory / "observation.json").write_bytes(json_bytes({"proof_status": "failed",
            "error": type(error).__name__ + ": " + str(error), "commands": observer.commands,
            "publication_attempted": False, "local_build_performed": False}))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
