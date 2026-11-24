"""Resolve shared Cargo target locations and safely prune superseded build hashes.

Without shared location configuration, separate git worktrees create isolated
100+ GiB `target/` directories, and repeated Cargo builds accumulate older
`<crate>-<16hex>*` binaries alongside the newest one. This utility keeps builds
reusing one non-synced local target directory and prunes superseded hashes
only when explicitly invoked and no active Cargo build lock holds the target.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import subprocess
import sys

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None  # type: ignore[assignment]

try:
    import tomllib
except ImportError:  # pragma: no cover - Python 3.10 fallback
    tomllib = None  # type: ignore[assignment]

DEFAULT_SHARED_TARGET = Path.home() / ".cache" / "shardloom" / "cargo-target"
_HASHED_ARTIFACT_RE = re.compile(r"^(?:lib)?(.+?)-([0-9a-f]{16})(?:\.|$)")
_WORKSPACE_PREFIX_RE = re.compile(
    r"^(?:shardloom|sql_|cg\d+|local_|public_|v1_|native_|weighted_|exact_|"
    r"typed_|delta_|sqlite_|session_|version_|vortex_prepare|adaptive_|agent_|"
    r"api_|backpressure_|benchmark_|capability_|ci_|claim_|commit_|compute_|"
    r"correctness_|cpu_|dynamic_|effect_|encoded_|sizing_|stateful_|streaming_|"
    r"table_|universal_|workflow_|workload_|non_unix_|_native\b)"
)
_TOML_TABLE_RE = re.compile(r"^\s*\[([^\[\]]+)\]\s*(?:#.*)?$")
_TOML_KV_RE = re.compile(r"^\s*([A-Za-z0-9_-]+)\s*=\s*(.+?)\s*$")


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


def _strip_inline_comment(raw: str) -> str:
    quote: str | None = None
    escaped = False
    for idx, ch in enumerate(raw):
        if escaped:
            escaped = False
            continue
        if quote == '"':
            if ch == "\\":
                escaped = True
            elif ch == '"':
                quote = None
            continue
        if quote == "'":
            if ch == "'":
                quote = None
            continue
        if ch in {'"', "'"}:
            quote = ch
        elif ch == "#":
            return raw[:idx].rstrip()
    return raw.rstrip()


def _parse_toml_string_literal(raw: str) -> str:
    value = _strip_inline_comment(raw).strip()
    if tomllib is not None and (
        (len(value) >= 2 and value[0] == '"' and value[-1] == '"')
        or (len(value) >= 2 and value[0] == "'" and value[-1] == "'")
    ):
        return str(tomllib.loads(f"v = {value}")["v"])
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return str(json.loads(value))
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        return value[1:-1]
    raise ValueError(f"expected TOML string literal, found: {raw!r}")


def _toml_basic_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _validate_toml_text(text: str) -> None:
    if tomllib is not None:
        tomllib.loads(text)
        return
    bracket_depth = 0
    for lineno, raw_line in enumerate(text.splitlines(), start=1):
        stripped = _strip_inline_comment(raw_line).strip()
        if not stripped:
            continue
        if bracket_depth == 0:
            if stripped.startswith("["):
                if not _TOML_TABLE_RE.match(raw_line):
                    raise ValueError(f"invalid TOML table header at line {lineno}: {raw_line!r}")
                continue
            if "=" not in stripped:
                raise ValueError(f"invalid TOML entry at line {lineno}: {raw_line!r}")
        for ch in stripped:
            if ch == "[":
                bracket_depth += 1
            elif ch == "]":
                bracket_depth -= 1
                if bracket_depth < 0:
                    raise ValueError(f"unmatched `]` in TOML at line {lineno}")
    if bracket_depth != 0:
        raise ValueError("unclosed `[` in TOML configuration")


def _update_build_table_toml(
    existing_text: str,
    desired_target: str,
    *,
    force: bool,
) -> tuple[str, bool]:
    _validate_toml_text(existing_text)
    lines = existing_text.splitlines(keepends=True)
    build_start: int | None = None
    build_end = len(lines)
    bracket_depth = 0
    for idx, raw_line in enumerate(lines):
        stripped = _strip_inline_comment(raw_line).strip()
        if bracket_depth == 0:
            table_match = _TOML_TABLE_RE.match(raw_line)
            if table_match:
                section_name = table_match.group(1).strip()
                if section_name == "build":
                    if build_start is not None:
                        raise ValueError("duplicate [build] section in .cargo/config.toml")
                    build_start = idx
                elif build_start is not None:
                    build_end = idx
                    break
        for ch in stripped:
            if ch == "[":
                bracket_depth += 1
            elif ch == "]":
                bracket_depth = max(0, bracket_depth - 1)

    desired_line = f"target-dir = {_toml_basic_string(desired_target)}\n"
    incremental_line = "incremental = false\n"

    if build_start is None:
        prefix = existing_text
        if prefix and not prefix.endswith("\n"):
            prefix += "\n"
        if prefix and not prefix.endswith("\n\n"):
            prefix += "\n"
        updated = (
            prefix
            + "# Local-only build output: share one non-synced Cargo target directory across checkouts/worktrees.\n"
            + "[build]\n"
            + desired_line
            + incremental_line
        )
        _validate_toml_text(updated)
        return updated, True

    target_idx: int | None = None
    incremental_idx: int | None = None
    in_multiline = 0
    for idx in range(build_start + 1, build_end):
        stripped = _strip_inline_comment(lines[idx]).strip()
        if in_multiline == 0:
            kv = _TOML_KV_RE.match(_strip_inline_comment(lines[idx]))
            if kv:
                key, val = kv.group(1), kv.group(2)
                if key == "target-dir":
                    current_target = str(Path(_parse_toml_string_literal(val)).expanduser().resolve())
                    if current_target != desired_target and not force:
                        raise ValueError(
                            f".cargo/config.toml already sets [build].target-dir = {current_target!r} "
                            f"(differs from {desired_target!r}); pass force=True to update it"
                        )
                    target_idx = idx
                elif key == "incremental":
                    incremental_idx = idx
        for ch in stripped:
            if ch in {"[", "{"}:
                in_multiline += 1
            elif ch in {"]", "}"}:
                in_multiline = max(0, in_multiline - 1)

    changed = False
    new_lines = list(lines)
    if target_idx is not None:
        if new_lines[target_idx].strip() != desired_line.strip():
            new_lines[target_idx] = desired_line
            changed = True
    else:
        new_lines.insert(build_start + 1, desired_line)
        changed = True
        if incremental_idx is not None and incremental_idx > build_start:
            incremental_idx += 1
        build_end += 1

    if incremental_idx is not None:
        if _strip_inline_comment(new_lines[incremental_idx]).strip() != "incremental = false":
            new_lines[incremental_idx] = incremental_line
            changed = True
    else:
        insert_at = (target_idx + 1) if target_idx is not None else (build_start + 2)
        new_lines.insert(insert_at, incremental_line)
        changed = True

    updated = "".join(new_lines)
    _validate_toml_text(updated)
    return updated, changed


def ensure_worktree_cargo_config(
    worktree_root: Path,
    shared_target_dir: Path = DEFAULT_SHARED_TARGET,
    *,
    force: bool = False,
) -> Path:
    """Point a checkout/worktree `.cargo/config.toml` at `shared_target_dir`."""
    resolved_root = worktree_root.resolve()
    config_dir = resolved_root / ".cargo"
    if config_dir.is_symlink():
        raise ValueError(".cargo directory must not be a symlink")
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "config.toml"
    if config_path.is_symlink():
        raise ValueError(".cargo/config.toml must not be a symlink")

    desired_target = str(shared_target_dir.expanduser().resolve())
    if config_path.exists():
        existing_text = config_path.read_text(encoding="utf-8")
        updated_text, changed = _update_build_table_toml(
            existing_text, desired_target, force=force
        )
        if not changed:
            return config_path
        config_path.write_text(updated_text, encoding="utf-8")
        return config_path

    content = (
        "# Local-only build output: share one non-synced Cargo target directory across checkouts/worktrees.\n"
        "[build]\n"
        f"target-dir = {_toml_basic_string(desired_target)}\n"
        "incremental = false\n"
    )
    _validate_toml_text(content)
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


@contextmanager
def _try_nonblocking_flock(lock_path: Path, *, create: bool):
    if fcntl is None:
        yield True
        return
    if lock_path.is_symlink():
        yield False
        return
    if not create and not lock_path.exists():
        yield True
        return
    mode = "a+b" if create else "r+b"
    try:
        handle = lock_path.open(mode)
    except OSError:
        yield False
        return
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
    finally:
        handle.close()


def _is_safe_contained_path(path: Path, real_target: Path) -> bool:
    if path.is_symlink():
        return False
    try:
        resolved = path.resolve(strict=True)
    except OSError:
        return False
    return resolved != real_target and resolved.is_relative_to(real_target)


def _remove_file(path: Path, busy: set[str], real_target: Path) -> tuple[int, int]:
    if path.is_symlink():
        return 0, 0
    resolved_str = str(path)
    if resolved_str in busy:
        return 0, 0
    if not _is_safe_contained_path(path, real_target):
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


def _remove_tree_scandir(
    dir_path: Path,
    busy: set[str],
    real_target: Path,
    skipped_symlinks: list[str],
) -> tuple[int, int, bool]:
    if not _is_safe_contained_path(dir_path, real_target):
        return 0, 0, False
    files_removed = 0
    bytes_reclaimed = 0
    had_skipped = False
    try:
        with os.scandir(dir_path) as it:
            entries = list(it)
    except OSError:
        return 0, 0, False

    for entry in entries:
        entry_path = Path(entry.path)
        if entry.is_symlink():
            skipped_symlinks.append(entry.path)
            had_skipped = True
            continue
        if str(entry_path) in busy:
            had_skipped = True
            continue
        if entry.is_dir(follow_symlinks=False):
            cnt, sz, sub_clean = _remove_tree_scandir(
                entry_path, busy, real_target, skipped_symlinks
            )
            files_removed += cnt
            bytes_reclaimed += sz
            if not sub_clean:
                had_skipped = True
        elif entry.is_file(follow_symlinks=False):
            cnt, sz = _remove_file(entry_path, busy, real_target)
            files_removed += cnt
            bytes_reclaimed += sz
            if cnt == 0:
                had_skipped = True

    if not had_skipped:
        try:
            dir_path.rmdir()
        except OSError:
            had_skipped = True
    return files_removed, bytes_reclaimed, not had_skipped


def _remove_tree(
    path: Path,
    busy: set[str],
    real_target: Path,
    skipped_symlinks: list[str],
) -> tuple[int, int]:
    if path.is_symlink():
        skipped_symlinks.append(str(path))
        return 0, 0
    if not path.exists():
        return 0, 0
    prefix = str(path)
    if any(b == prefix or b.startswith(prefix + os.sep) for b in busy):
        return 0, 0
    if not _is_safe_contained_path(path, real_target):
        return 0, 0
    if path.is_file():
        return _remove_file(path, busy, real_target)
    cnt, sz, _ = _remove_tree_scandir(path, busy, real_target, skipped_symlinks)
    return cnt, sz


def _prune_profile_dir(
    profile_dir: Path,
    real_target: Path,
    *,
    keep_workspace_hashes: int,
    keep_dep_hashes: int,
    busy: set[str],
    skipped_symlinks: list[str],
) -> tuple[int, int]:
    files_removed = 0
    bytes_reclaimed = 0

    inc_dir = profile_dir / "incremental"
    if not inc_dir.is_symlink() and _is_safe_contained_path(inc_dir, real_target) and inc_dir.is_dir():
        for entry in inc_dir.iterdir():
            cnt, sz = _remove_tree(entry, busy, real_target, skipped_symlinks)
            files_removed += cnt
            bytes_reclaimed += sz

    deps_dir = profile_dir / "deps"
    fp_dir = profile_dir / ".fingerprint"
    if deps_dir.is_symlink() or not _is_safe_contained_path(deps_dir, real_target) or not deps_dir.is_dir():
        return files_removed, bytes_reclaimed

    grouped: dict[str, dict[str, tuple[float, list[Path]]]] = {}
    for entry in deps_dir.iterdir():
        if entry.is_symlink() or not entry.is_file():
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

    fp_safe = (
        not fp_dir.is_symlink()
        and _is_safe_contained_path(fp_dir, real_target)
        and fp_dir.is_dir()
    )
    for prefix, by_hash in grouped.items():
        keep_n = (
            keep_workspace_hashes
            if _WORKSPACE_PREFIX_RE.match(prefix)
            else keep_dep_hashes
        )
        ordered = sorted(by_hash.items(), key=lambda kv: kv[1][0], reverse=True)
        for hash_hex, (_, paths) in ordered[keep_n:]:
            for artifact in paths:
                cnt, sz = _remove_file(artifact, busy, real_target)
                files_removed += cnt
                bytes_reclaimed += sz
            if fp_safe:
                fp_entry = fp_dir / f"{prefix}-{hash_hex}"
                if fp_entry.is_symlink() or fp_entry.exists():
                    cnt, sz = _remove_tree(fp_entry, busy, real_target, skipped_symlinks)
                    files_removed += cnt
                    bytes_reclaimed += sz

    return files_removed, bytes_reclaimed


def _discover_profile_dirs(real_target: Path) -> list[Path]:
    candidates: list[Path] = []
    for child in real_target.iterdir():
        if child.is_symlink() or not child.is_dir():
            continue
        deps = child / "deps"
        if not deps.is_symlink() and deps.is_dir():
            candidates.append(child)
            continue
        for nested in child.iterdir():
            if nested.is_symlink() or not nested.is_dir():
                continue
            nested_deps = nested / "deps"
            if not nested_deps.is_symlink() and nested_deps.is_dir():
                candidates.append(nested)
    return sorted(candidates)


def prune_superseded_artifacts(
    target_dir: Path,
    *,
    keep_workspace_hashes: int = 2,
    keep_dep_hashes: int = 3,
    check_open_handles: bool = True,
) -> dict[str, object]:
    """Prune superseded hashed build artifacts inside `target_dir` in place."""
    if target_dir.is_symlink():
        raise ValueError("Cargo target directory must not be a symlink")
    real_target = target_dir.resolve()
    total_files = 0
    total_bytes = 0
    scanned_profiles: list[str] = []
    skipped_locked_profiles: list[str] = []
    skipped_symlinks: list[str] = []
    skipped_busy = False

    if real_target.is_dir():
        maintain_lock = real_target / ".shardloom-maintain.lock"
        cargo_root_lock = real_target / ".cargo-lock"
        with _try_nonblocking_flock(maintain_lock, create=True) as have_maintain, _try_nonblocking_flock(
            cargo_root_lock, create=False
        ) as have_root_cargo:
            if not have_maintain or not have_root_cargo:
                skipped_busy = True
            else:
                busy = _open_paths_under(real_target) if check_open_handles else set()
                for profile_dir in _discover_profile_dirs(real_target):
                    rel_name = str(profile_dir.relative_to(real_target))
                    profile_lock = profile_dir / ".cargo-lock"
                    with _try_nonblocking_flock(profile_lock, create=False) as have_profile:
                        if not have_profile:
                            skipped_locked_profiles.append(rel_name)
                            continue
                        cnt, sz = _prune_profile_dir(
                            profile_dir,
                            real_target,
                            keep_workspace_hashes=keep_workspace_hashes,
                            keep_dep_hashes=keep_dep_hashes,
                            busy=busy,
                            skipped_symlinks=skipped_symlinks,
                        )
                        total_files += cnt
                        total_bytes += sz
                        scanned_profiles.append(rel_name)

    return {
        "schema_version": "shardloom.cargo_target_maintenance.v1",
        "target_directory": str(real_target),
        "profiles_scanned": scanned_profiles,
        "skipped_locked_profiles": skipped_locked_profiles,
        "skipped_symlinks": skipped_symlinks,
        "skipped_busy": skipped_busy,
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
    parser.add_argument("--force-config", action="store_true")
    parser.add_argument("--keep-workspace-hashes", type=int, default=2)
    parser.add_argument("--keep-dep-hashes", type=int, default=3)
    parser.add_argument("--skip-lsof", action="store_true")
    args = parser.parse_args(argv)

    repo_root = args.repo_root.resolve()
    config_path = None
    if args.ensure_config:
        config_path = str(
            ensure_worktree_cargo_config(
                repo_root, args.shared_target_dir, force=args.force_config
            )
        )
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
