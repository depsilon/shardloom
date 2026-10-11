"""Verify a native wheel's artifact identities, notices, tags and complete RECORD.

This is an archive integrity check. Import and execution in a clean environment
are separate proof steps; this command neither installs nor publishes a package.
"""

from __future__ import annotations

import argparse
import base64
import csv
from email.parser import BytesParser
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import zipfile

from check_python_license_bundle import tomllib, verify as verify_notices
from release_dry_run_proof import bundled_cli_platform_tag
from stage_native_python_package import sha


def member_identity(archive: zipfile.ZipFile, name: str) -> tuple[str, int]:
    digest, size = hashlib.sha256(), 0
    with archive.open(name) as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size


def verify_record(archive: zipfile.ZipFile, record: str) -> None:
    names = {item.filename for item in archive.infolist() if not item.is_dir()}
    rows = list(csv.reader(io.StringIO(archive.read(record).decode("utf-8"))))
    if any(len(row) != 3 for row in rows) or len(rows) != len(names):
        raise ValueError("wheel RECORD does not cover every payload exactly once")
    if {row[0] for row in rows} != names:
        raise ValueError("wheel RECORD contains missing, duplicate or unknown payloads")
    for name, declared_hash, declared_size in rows:
        if name == record:
            if declared_hash or declared_size:
                raise ValueError("wheel RECORD must not contain its own digest")
            continue
        digest, size = member_identity(archive, name)
        encoded = base64.urlsafe_b64encode(bytes.fromhex(digest)).decode().rstrip("=")
        if declared_hash != "sha256=" + encoded or declared_size != str(size):
            raise ValueError(f"wheel RECORD identity mismatch: {name}")


def verify(root: Path, wheel: Path, library: Path, binary: Path) -> dict:
    licenses = verify_notices(root)
    for path in (wheel, library, binary):
        if path.is_symlink() or not path.is_file():
            raise ValueError("wheel proof requires regular built artifact files")
    filename = wheel.name.removesuffix(".whl").split("-")
    if len(filename) != 5 or filename[0] != "shardloom" or filename[2:4] != ["cp310", "abi3"]:
        raise ValueError("native wheel must declare the cp310-abi3 contract")
    version, platform = filename[1], filename[4]
    expected_version = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    if version != expected_version:
        raise ValueError("native wheel version differs from its source workspace")
    if not platform.startswith(("linux_", "macosx_")):
        raise ValueError("native wheel has no audited portable platform contract")
    dist_info = f"shardloom-{version}.dist-info"
    library_name = "shardloom/_native.abi3.so"
    binary_name = f"shardloom/bin/{bundled_cli_platform_tag()}/shardloom"
    with zipfile.ZipFile(wheel) as archive:
        members = archive.infolist()
        names = {item.filename for item in members}
        if len(names) != len(members):
            raise ValueError("wheel contains duplicate archive paths")
        for item in members:
            path = PurePosixPath(item.filename)
            if path.is_absolute() or ".." in path.parts or "\\" in item.filename:
                raise ValueError("wheel contains an unsafe archive path")
            if (item.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError("wheel payloads must not be symbolic links")
        wheel_metadata = BytesParser().parsebytes(archive.read(f"{dist_info}/WHEEL"))
        if (wheel_metadata.get_all("Tag") != [f"cp310-abi3-{platform}"]
                or wheel_metadata.get("Root-Is-Purelib") != "false"):
            raise ValueError("native wheel metadata differs from its filename tag")
        metadata = BytesParser().parsebytes(archive.read(f"{dist_info}/METADATA"))
        if metadata.get("Name") != "shardloom" or metadata.get("Version") != version:
            raise ValueError("native wheel package identity is inconsistent")
        native_sha, _ = member_identity(archive, library_name)
        cli_sha, _ = member_identity(archive, binary_name)
        if native_sha != sha(library) or cli_sha != sha(binary):
            raise ValueError("native wheel contains a different extension or CLI")
        source_names = {f"shardloom/{path.relative_to(root / 'python/src/shardloom').as_posix()}": path
                        for path in (root / "python/src/shardloom").rglob("*")
                        if path.is_file() and (path.suffix == ".py" or path.name == "py.typed")}
        for name, path in source_names.items():
            if archive.read(name) != path.read_bytes():
                raise ValueError(f"native wheel contains stale Python source: {name}")
        legal = {"LICENSE", "NOTICE", "THIRD_PARTY_LICENSES/manifest.json"}
        legal.update(f"THIRD_PARTY_LICENSES/texts/{path.name}"
                     for path in (root / "python/THIRD_PARTY_LICENSES/texts").iterdir())
        if set(metadata.get_all("License-File", [])) != legal:
            raise ValueError("native wheel does not declare every bundled notice")
        for name in legal:
            if archive.read(f"{dist_info}/licenses/{name}") != (root / "python" / name).read_bytes():
                raise ValueError(f"native wheel contains missing or altered notice: {name}")
        verify_record(archive, f"{dist_info}/RECORD")
    return {"schema_version": "shardloom.native_python_wheel_integrity.v1", "status": "passed",
            "wheel": wheel.name, "wheel_sha256": sha(wheel), "wheel_bytes": wheel.stat().st_size,
            "native_library_sha256": native_sha, "cli_sha256": cli_sha,
            "tag": f"cp310-abi3-{platform}", "python_source_files": len(source_names),
            "licenses": licenses, "load_verified": False, "published": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--native-library", type=Path, required=True)
    parser.add_argument("--cli-binary", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.repo_root, args.wheel, args.native_library, args.cli_binary), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
