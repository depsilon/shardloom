# SPDX-License-Identifier: Apache-2.0
"""Compact closed UAT gzip envelopes without changing their original bytes."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import stat
import tarfile


def _identity(path: Path) -> tuple[int, ...]:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"envelope is not a regular file: {path}")
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def archive_envelopes(directory: Path, entries: list[dict], number: int) -> dict:
    """Verify a bounded archive and every source before removing redundant files.

    The caller owns an exclusive UAT lock and passes only closed envelope files.
    Failed validation leaves every original in place. A partial/failed archive
    also remains visible to storage accounting and failed-run inspection.
    """
    if not entries or len(entries) > 128 or number < 1:
        raise ValueError("an archive needs 1..128 envelopes and a positive sequence")
    directory = directory.resolve(strict=True)
    archive_path = directory / f"envelopes-{number:06d}.tar.xz"
    members, sources, names = [], [], set()
    for entry in entries:
        path = Path(entry["path"])
        if path.parent != directory or not path.name.endswith(".envelope.json.gz"):
            raise ValueError("archive input must be a local gzip envelope")
        if path.name in names or "archive_path" in entry:
            raise ValueError("duplicate or already archived envelope")
        names.add(path.name)
        identity = _identity(path)
        raw = path.read_bytes()
        if (len(raw) != entry["stored_bytes"] or
                hashlib.sha256(raw).hexdigest() != entry["stored_sha256"] or
                _identity(path) != identity):
            raise ValueError(f"envelope changed before archival: {path.name}")
        members.append({"name": path.name, "bytes": len(raw),
                        "sha256": entry["stored_sha256"], "source_identity": identity})
        sources.append((path, identity, raw))

    with archive_path.open("xb") as destination:
        with tarfile.open(fileobj=destination, mode="w:xz", preset=3) as archive:
            for path, _, raw in sources:
                info = tarfile.TarInfo(path.name)
                info.size = len(raw)
                info.mode = 0o600
                archive.addfile(info, io.BytesIO(raw))
    with tarfile.open(archive_path, "r:xz") as archive:
        archived = archive.getmembers()
        if [item.name for item in archived] != [item["name"] for item in members]:
            raise ValueError("archive member names differ from the closed envelopes")
        for member, (_, _, original) in zip(archived, sources):
            if not member.isfile() or archive.extractfile(member).read() != original:
                raise ValueError(f"archive readback differs for {member.name}")
    for path, identity, _ in sources:
        if _identity(path) != identity:
            raise ValueError(f"envelope changed during archival: {path.name}")
    archive_bytes = archive_path.read_bytes()
    manifest = {"path": str(archive_path), "bytes": len(archive_bytes),
                "sha256": hashlib.sha256(archive_bytes).hexdigest(), "members": members,
                "original_bytes_verified_before_removal": True}
    manifest_path = archive_path.with_suffix(".manifest.json")
    manifest_bytes = (json.dumps(manifest, indent=2) + "\n").encode()
    with manifest_path.open("xb") as destination:
        destination.write(manifest_bytes)
    if manifest_path.read_bytes() != manifest_bytes:
        raise ValueError("archive manifest readback differs")
    manifest.update(manifest_path=str(manifest_path), manifest_bytes=len(manifest_bytes),
                    manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest())
    for entry, (path, _, _) in zip(entries, sources):
        entry["archive_path"] = str(archive_path)
        entry["archive_member"] = path.name
    for path, _, _ in sources:
        path.unlink()
    return manifest
