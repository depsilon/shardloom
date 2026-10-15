"""Audit the Linux native wheel without changing or adding runtime payloads.

The installed-wheel prover supplies the logged subprocess runner. Auditwheel
checks both ELF artifacts, symbols, shared libraries and ISA policy. Its `none`
patcher rejects required binary changes; archive comparison independently rejects
any changed payload or added library. Only WHEEL and RECORD metadata may change.
"""

from __future__ import annotations

from importlib.metadata import version
from pathlib import Path
import sys
import zipfile

from check_native_python_wheel import (
    AUDITED_LINUX_PLATFORM, AUDITWHEEL_VERSION, member_identity, verify_record,
)
from stage_native_python_package import sha


def verify_payload_preservation(original: Path, audited: Path) -> int:
    expected_name = original.name.rsplit("-", 1)[0] + f"-{AUDITED_LINUX_PLATFORM}.whl"
    if not original.name.endswith("-cp310-abi3-linux_x86_64.whl") or audited.name != expected_name:
        raise ValueError("Linux audit must preserve package and ABI identity under the reviewed policy")
    for path in (original, audited):
        if path.is_symlink() or not path.is_file():
            raise ValueError("Linux audit requires regular wheel artifacts")
    with zipfile.ZipFile(original) as before, zipfile.ZipFile(audited) as after:
        inventories = []
        for archive in (before, after):
            names = [item.filename for item in archive.infolist() if not item.is_dir()]
            if len(names) != len(set(names)):
                raise ValueError("Linux audit wheel contains duplicate payloads")
            inventories.append(set(names))
        if inventories[0] != inventories[1]:
            raise ValueError("Linux audit added or removed wheel payloads")
        records = [name for name in inventories[0] if name.endswith(".dist-info/RECORD")]
        if len(records) != 1:
            raise ValueError("Linux audit requires exactly one package RECORD")
        record = records[0]
        metadata = record.removesuffix("RECORD") + "WHEEL"
        for archive in (before, after):
            verify_record(archive, record)
        payloads = inventories[0] - {record, metadata}
        for name in sorted(payloads):
            if member_identity(before, name) != member_identity(after, name):
                raise ValueError(f"Linux audit changed a runtime payload: {name}")
    return len(payloads)


def audit(wheel: Path, destination: Path, run) -> tuple[Path, dict]:
    if sys.platform != "linux" or not wheel.name.endswith("-cp310-abi3-linux_x86_64.whl"):
        raise ValueError("the reviewed Linux audit currently covers native x86_64 wheels")
    if version("auditwheel") != AUDITWHEEL_VERSION:
        raise ValueError("Linux audit requires the hash-pinned reviewed auditwheel version")
    if destination.is_symlink() or destination.exists():
        raise ValueError("Linux audit requires a new wheel destination")
    input_sha = sha(wheel)
    destination.mkdir(parents=True)
    run("linux_compatibility_audit", [sys.executable, "-m", "auditwheel", "repair",
        "--only-plat", "--plat", AUDITED_LINUX_PLATFORM, "--patcher", "none",
        "--wheel-dir", destination, wheel])
    wheels = list(destination.glob("*.whl"))
    if len(wheels) != 1 or sha(wheel) != input_sha:
        raise ValueError("Linux audit must produce one wheel and preserve its input")
    audited = wheels[0]
    payloads = verify_payload_preservation(wheel, audited)
    return audited, {"schema_version": "shardloom.native_python_linux_audit.v1",
                     "status": "passed", "platform": AUDITED_LINUX_PLATFORM,
                     "auditwheel_version": AUDITWHEEL_VERSION,
                     "input_wheel": wheel.name, "input_wheel_sha256": input_sha,
                     "wheel": audited.name, "wheel_sha256": sha(audited),
                     "unchanged_payload_count": payloads, "payloads_unchanged": True}
