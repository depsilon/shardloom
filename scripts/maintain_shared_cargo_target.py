"""Resolve shared Cargo target locations and replace superseded build hashes in place.

Without shared location configuration, separate git worktrees create isolated
100+ GiB `target/` directories, and repeated Cargo builds accumulate older
`<crate>-<16hex>*` binaries alongside the newest one. This utility keeps builds
reusing one non-synced local target directory and prunes superseded hashes
after builds with no artificial byte cap.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

DEFAULT_SHARED_TARGET = Path.home() / ".cache" / "shardloom" / "cargo-target"
_HASHED_ARTIFACT_RE = re.compile(r"^(?:lib)?(.+?)-([0-9a-f]{16})(?:\.|$)")
_WORKSPACE_PREFIX_RE = re.compile(
    r"^(?:shardloom|sql_|cg\d+|local_|public_|v1_|native_|weighted_|exact_|"
    r"typed_|delta_|sqlite_|session_|version_|vortex_prepare|adaptive_|agent_|"
    r"api_|backpressure_|benchmark_|capability_|ci_|claim_|commit_|compute_|"
    r"correctness_|cpu_|dynamic_|effect_|encoded_|sizing_|stateful_|streaming_|"
    r"table_|universal_|workflow_|workload_|non_unix_|_native\b)"
)


def resolve_cargo_target_dir(repo_root: Path, cargo: str = "cargo") -> Path:
    """Resolve Cargo's active target_directory via `cargo metadata` or CARGO_TARGET_DIR."""
    env_target = os.environ.get("CARGO_TARGET_DIR")
    if env_target:
        candidate = Path(env_target)
        return (candidate if candidate.is_absolute() else repo_root / candidate).resolve()
    for extra in (["--offline"], []):
        try:
            raw = subprocess.check_output(
                [cargo, "metadata", *extra, "--no-deps", "--format-version", "1"],
                cwd=repo_root,
                stderr=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
            )
            data = json.loads(raw)
            target_dir = data.get("target_directory")
            if target_dir:
                return Path(target_dir).resolve()
        except (OSError, subprocess.CalledProcessError, json.JSONDecodeError):
            continue
    return (repo_root / "target").resolve()


def ensure_worktree_cargo_config(
    worktree_root: Path, shared_target_dir: Path = DEFAULT_SHARED_TARGET
) -> Path:
    """Point a checkout/worktree `.cargo/config.toml` at `shared_target_dir`."""
    config_dir = worktree_root.resolve() / ".cargo"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "config.toml"
    content = (
        "# Local-only build output: share one non-synced Cargo target directory across checkouts/worktrees.\n"
        "[build]\n"
        f'target-dir = "{shared_target_dir.expanduser().resolve()}"\n'
        "incremental = false\n"
    )
    if config_path.exists() and config_path.read_text(encoding="utf-8") == content:
        return config_path
    config_path.write_text(content, encoding="utf-8")
    return config_path


def _open_paths_under(root: Path) -> set[str]:
    if sys.platform not in {"darwin", "linux"} or not root.exists():
        return set()
    try:
        completed = subprocess.run(
            ["lsof", "+D", str(root)],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return set()
    busy: set[str] = set()
    for line in completed.stdout.splitlines()[1:]:
        parts = line.split(None, 8)
        if len(parts) >= 9:
            busy.add(parts[8])
    return busy


def _remove_file(path: Path, busy: set[str]) -> tuple[int, int]:
    resolved = str(path)
    if resolved in busy:
        return 0, 0
    try:
        st = path.lstat()
        if path.is_symlink() or not path.is_file():
            return 0, 0
        size = st.st_size
        path.unlink()
        return 1, size
    except OSError:
        return 0, 0


def _remove_tree(path: Path, busy: set[str]) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    prefix = str(path)
    if any(b == prefix or b.startswith(prefix + os.sep) for b in busy):
        return 0, 0
    files_removed = 0
    bytes_reclaimed = 0
    for root, dirs, files in os.walk(path, topdown=False, followlinks=False):
        root_path = Path(root)
        for name in files:
            cnt, sz = _remove_file(root_path / name, busy)
            files_removed += cnt
            bytes_reclaimed += sz
        for name in dirs:
            dp = root_path / name
            try:
                if dp.is_symlink():
                    dp.unlink()
                else:
                    dp.rmdir()
            except OSError:
                pass
    try:
        shutil.rmtree(path, ignore_errors=True)
    except OSError:
        pass
    return files_removed, bytes_reclaimed


def _prune_profile_dir(
    profile_dir: Path,
    *,
    keep_workspace_hashes: int,
    keep_dep_hashes: int,
    busy: set[str],
) -> tuple[int, int]:
    files_removed = 0
    bytes_reclaimed = 0

    inc_dir = profile_dir / "incremental"
    if inc_dir.is_dir():
        for entry in inc_dir.iterdir():
            cnt, sz = _remove_tree(entry, busy)
            files_removed += cnt
            bytes_reclaimed += sz

    deps_dir = profile_dir / "deps"
    fp_dir = profile_dir / ".fingerprint"
    if not deps_dir.is_dir():
        return files_removed, bytes_reclaimed

    grouped: dict[str, dict[str, tuple[float, list[Path]]]] = {}
    for entry in deps_dir.iterdir():
        if entry.is_symlink() or not entry.is_file():
            continue
        if entry.name.endswith((".rcgu.o", ".o")):
            cnt, sz = _remove_file(entry, busy)
            files_removed += cnt
            bytes_reclaimed += sz
            continue
        match = _HASHED_ARTIFACT_RE.match(entry.name)
        if not match:
            continue
        prefix, hash_hex = match.group(1), match.group(2)
        try:
            mtime = entry.lstat().st_mtime
        except OSError:
            continue
        bucket = grouped.setdefault(prefix, {})
        prev_mtime, paths = bucket.get(hash_hex, (0.0, []))
        paths.append(entry)
        bucket[hash_hex] = (max(prev_mtime, mtime), paths)

    for prefix, by_hash in grouped.items():
        keep_n = (
            keep_workspace_hashes
            if _WORKSPACE_PREFIX_RE.match(prefix)
            else keep_dep_hashes
        )
        ordered = sorted(by_hash.items(), key=lambda kv: kv[1][0], reverse=True)
        for hash_hex, (_, paths) in ordered[keep_n:]:
            for artifact in paths:
                cnt, sz = _remove_file(artifact, busy)
                files_removed += cnt
                bytes_reclaimed += sz
            if fp_dir.is_dir():
                fp_entry = fp_dir / f"{prefix}-{hash_hex}"
                if fp_entry.is_dir():
                    cnt, sz = _remove_tree(fp_entry, busy)
                    files_removed += cnt
                    bytes_reclaimed += sz

    return files_removed, bytes_reclaimed


def prune_superseded_artifacts(
    target_dir: Path,
    *,
    keep_workspace_hashes: int = 2,
    keep_dep_hashes: int = 3,
    check_open_handles: bool = True,
) -> dict[str, object]:
    """Prune superseded hashed build artifacts inside `target_dir` in place."""
    target_dir = target_dir.resolve()
    busy = _open_paths_under(target_dir) if check_open_handles else set()
    total_files = 0
    total_bytes = 0
    scanned_profiles: list[str] = []

    if target_dir.is_dir():
        # Handle both `<target>/<profile>` and `<target>/<triple>/<profile>`.
        candidates: list[Path] = []
        for child in target_dir.iterdir():
            if not child.is_dir() or child.is_symlink():
                continue
            if (child / "deps").is_dir():
                candidates.append(child)
            else:
                for nested in child.iterdir():
                    if nested.is_dir() and not nested.is_symlink() and (nested / "deps").is_dir():
                        candidates.append(nested)
        for profile_dir in sorted(candidates):
            cnt, sz = _prune_profile_dir(
                profile_dir,
                keep_workspace_hashes=keep_workspace_hashes,
                keep_dep_hashes=keep_dep_hashes,
                busy=busy,
            )
            total_files += cnt
            total_bytes += sz
            scanned_profiles.append(str(profile_dir.relative_to(target_dir)))

    return {
        "schema_version": "shardloom.cargo_target_maintenance.v1",
        "target_directory": str(target_dir),
        "profiles_scanned": scanned_profiles,
        "keep_workspace_hashes": keep_workspace_hashes,
        "keep_dep_hashes": keep_dep_hashes,
        "removed_files": total_files,
        "reclaimed_bytes": total_bytes,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--shared-target-dir", type=Path, default=DEFAULT_SHARED_TARGET)
    parser.add_argument("--ensure-config", action="store_true")
    parser.add_argument("--keep-workspace-hashes", type=int, default=2)
    parser.add_argument("--keep-dep-hashes", type=int, default=3)
    parser.add_argument("--skip-lsof", action="store_true")
    args = parser.parse_args(argv)

    repo_root = args.repo_root.resolve()
    config_path = None
    if args.ensure_config:
        config_path = str(ensure_worktree_cargo_config(repo_root, args.shared_target_dir))
    target_dir = resolve_cargo_target_dir(repo_root)
    report = prune_superseded_artifacts(
        target_dir,
        keep_workspace_hashes=args.keep_workspace_hashes,
        keep_dep_hashes=args.keep_dep_hashes,
        check_open_handles=not args.skip_lsof,
    )
    if config_path is not None:
        report["cargo_config_path"] = config_path
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
