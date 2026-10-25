#!/usr/bin/env python3
"""Build and verify exact-revision CI artifacts; never publish a package."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "shardloom.ci_build_artifact.v1"
PRODUCTS = {
    "cli-debug": ("dev", "shardloom-cli", ["--bin", "shardloom"], ["release-user-surfaces"]),
    "cli-release": ("release", "shardloom-cli", ["--bin", "shardloom"], ["release-user-surfaces"]),
    "native-tests": ("ci-native", "shardloom-vortex", ["--lib"], ["release-user-surfaces"]),
    "native-extension": ("release", "shardloom-python", ["--lib"], ["python-native"]),
    "observation-extension": ("release", "shardloom-python", ["--lib"],
                              ["release-user-surfaces", "output-window-observation"]),
}


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def command_output(command: list[str], root: Path = ROOT) -> str:
    return subprocess.check_output(command, cwd=root, text=True).strip()


def identity(root: Path = ROOT) -> dict:
    return {"revision": command_output(["git", "rev-parse", "HEAD"], root),
            "cargo_lock_sha256": sha(root / "Cargo.lock"),
            "os": platform.system(), "architecture": platform.machine(),
            "workspace_root": str(root.resolve()),
            "rustc": command_output(["rustc", "-vV"], root),
            "cargo": command_output(["cargo", "--version"], root)}


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def safe_file(root: Path, name: str) -> Path:
    if not name or Path(name).name != name or name in {".", "..", "manifest.json"}:
        raise ValueError(f"invalid CI artifact filename: {name!r}")
    path = root / name
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"CI artifact must be a regular file: {name}")
    return path


def record_files(destination: Path, product: str, files: dict[str, Path], *,
                 build: dict, root: Path = ROOT) -> dict:
    if destination.exists() or destination.is_symlink():
        raise ValueError("CI artifact destination must be new")
    if product not in PRODUCTS:
        raise ValueError("unknown CI artifact product")
    destination.mkdir(parents=True)
    manifest = {"schema_version": SCHEMA, "product": product, "identity": identity(root),
                "profile": PRODUCTS[product][0], "requested_features": PRODUCTS[product][3],
                "abi": "cp310-abi3" if "extension" in product else None,
                "build": build, "files": {}}
    for name, source in files.items():
        source = safe_file(source.parent, source.name)
        if Path(name).name != name or name in {".", "..", "manifest.json"}:
            raise ValueError("invalid CI artifact destination filename")
        target = destination / name
        shutil.copy2(source, target)
        manifest["files"][name] = {"sha256": sha(target), "bytes": target.stat().st_size,
                                   "executable": bool(source.stat().st_mode & 0o111)}
    if not manifest["files"]:
        raise ValueError("empty CI artifact")
    write_json(destination / "manifest.json", manifest)
    return manifest


def verify_bundle(directory: Path, product: str, *, root: Path = ROOT,
                  require_toolchain: bool = True) -> dict:
    manifest_path = directory / "manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("CI artifact manifest must be a regular file")
    manifest = json.loads(manifest_path.read_text())
    if (manifest.get("schema_version") != SCHEMA or manifest.get("product") != product
            or product not in PRODUCTS or manifest.get("profile") != PRODUCTS[product][0]
            or manifest.get("requested_features") != PRODUCTS[product][3]
            or manifest.get("abi") != ("cp310-abi3" if "extension" in product else None)):
        raise ValueError("CI artifact product/configuration mismatch")
    current = identity(root) if require_toolchain else {
        "revision": command_output(["git", "rev-parse", "HEAD"], root),
        "cargo_lock_sha256": sha(root / "Cargo.lock"),
        "os": platform.system(), "architecture": platform.machine(),
    }
    # Test fixtures include compile-time CARGO_MANIFEST_DIR references. Require
    # the same checkout location on build and execution runners, not path repair.
    keys = ["revision", "cargo_lock_sha256", "os", "architecture"]
    if require_toolchain:
        keys += ["rustc", "cargo"]
    if product == "native-tests":
        keys += ["workspace_root"]
    if any(manifest["identity"].get(key) != current.get(key) for key in keys):
        raise ValueError("CI artifact revision, platform, toolchain or workspace mismatch")
    verify_compiler_configuration(product, manifest["build"]["compiler_features"],
                                  manifest["build"]["compiler_profile"])
    expected = manifest.get("files", {})
    if not expected or set(p.name for p in directory.iterdir()) != {*expected, "manifest.json"}:
        raise ValueError("CI artifact has missing or unexpected files")
    for name, evidence in expected.items():
        path = safe_file(directory, name)
        if path.stat().st_size != evidence["bytes"] or sha(path) != evidence["sha256"]:
            raise ValueError(f"CI artifact content mismatch: {name}")
    return manifest


def build_command(product: str) -> list[str]:
    profile, package, targets, features = PRODUCTS[product]
    command = ["cargo", "test" if product == "native-tests" else "build", "--locked",
               "--profile", profile, "-p", package, *targets, "--features", ",".join(features)]
    if product == "native-tests":
        command += ["--no-run"]
    return [*command, "--message-format=json"]


def compiler_artifact(messages: str, product: str) -> dict:
    rows = [json.loads(line) for line in messages.splitlines() if line.startswith("{")]
    name = "shardloom_vortex" if product == "native-tests" else (
        "_native" if "extension" in product else "shardloom")
    artifacts = [row for row in rows if row.get("reason") == "compiler-artifact"
                 and row.get("target", {}).get("name") == name
                 and row.get("profile", {}).get("test") == (product == "native-tests")]
    if len(artifacts) != 1:
        raise ValueError(f"expected one compiler artifact for {product}")
    artifact = artifacts[0]
    verify_compiler_configuration(product, artifact["features"], artifact["profile"])
    return artifact


def verify_compiler_configuration(product: str, compiled_features: list[str], profile: dict) -> None:
    features = set(compiled_features)
    if not set(PRODUCTS[product][3]) <= features:
        raise ValueError("compiled artifact lacks requested features")
    if ("output-window-observation" in features) != (product == "observation-extension"):
        raise ValueError("observation and ordinary products must stay separate")
    if product == "native-tests" and any(profile.get(key) != value for key, value in {
        "opt_level": "2", "debuginfo": 1, "debug_assertions": True, "overflow_checks": True,
        "test": True,
    }.items()):
        raise ValueError("native test compiler settings violate the checked optimization profile")
    if product != "native-tests":
        debug = product == "cli-debug"
        expected = {"opt_level": "0" if debug else "3", "debug_assertions": debug,
                    "overflow_checks": debug, "test": False}
        if any(profile.get(key) != value for key, value in expected.items()):
            raise ValueError("CLI/extension compiler profile does not match the requested product")


def build(product: str, destination: Path, *, root: Path = ROOT) -> dict:
    command = build_command(product)
    env = dict(os.environ)
    if "extension" in product:
        env["PYO3_BUILD_EXTENSION_MODULE"] = "1"
        env["PYO3_PYTHON"] = sys.executable
    started = time.monotonic()
    result = subprocess.run(command, cwd=root, env=env, text=True, stdout=subprocess.PIPE, check=False)
    if result.returncode:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.with_suffix(".failed-build.jsonl").write_text(result.stdout)
        for line in result.stdout.splitlines():
            if line.startswith("{"):
                diagnostic = json.loads(line).get("message", {}).get("rendered")
                if diagnostic:
                    print(diagnostic, file=sys.stderr, end="")
        raise subprocess.CalledProcessError(result.returncode, command)
    artifact = compiler_artifact(result.stdout, product)
    if "extension" in product:
        from build_native_python import native_artifact
        source = native_artifact([artifact])
        name = "_native.abi3.so"
    else:
        source = Path(artifact["executable"])
        name = "native-tests" if product == "native-tests" else "shardloom"
    return record_files(destination, product, {name: source}, root=root, build={
        "command": command, "seconds": time.monotonic() - started,
        "compiler_profile": artifact["profile"], "compiler_features": artifact["features"],
        "compiler_messages_sha256": hashlib.sha256(result.stdout.encode()).hexdigest(),
    })


def restore_bundle(directory: Path, product: str, destination: Path, *,
                   replace_existing: bool = False, root: Path = ROOT) -> dict:
    manifest = verify_bundle(directory, product, root=root)
    if len(manifest["files"]) != 1:
        raise ValueError("restore requires exactly one artifact")
    name, entry = next(iter(manifest["files"].items()))
    previous = None
    if destination.is_symlink():
        raise ValueError("refusing to replace a symlink")
    if destination.exists():
        if not replace_existing or not destination.is_file():
            raise ValueError("refusing to replace an existing artifact")
        # CI evidence merging may have restored the default-feature CLI. This
        # explicit replacement uses the verified user-surface product and keeps
        # the prior downloaded artifact and its evidence intact.
        previous = sha(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(directory / name, destination)
    destination.chmod(0o755 if entry["executable"] else 0o644)
    if sha(destination) != entry["sha256"]:
        raise ValueError("restored CI artifact content changed")
    return {"product": product, "destination": str(destination), "sha256": entry["sha256"],
            "previous_sha256": previous}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("build", "verify"))
    parser.add_argument("--product", choices=PRODUCTS, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--restore", type=Path)
    parser.add_argument("--replace-existing", action="store_true")
    args = parser.parse_args()
    if args.action == "build":
        manifest = build(args.product, args.directory)
    else:
        manifest = verify_bundle(args.directory, args.product)
        if args.restore:
            manifest = restore_bundle(args.directory, args.product, args.restore,
                                      replace_existing=args.replace_existing)
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
