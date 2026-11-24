"""Unit tests for shared Cargo target resolution, config wiring, and superseded-hash pruning."""

import fcntl
import os
from pathlib import Path
import tempfile
import unittest

from maintain_shared_cargo_target import (
    ensure_worktree_cargo_config,
    prune_superseded_artifacts,
    resolve_cargo_target_dir,
)


class SharedCargoTargetTests(unittest.TestCase):
    def test_ensure_worktree_cargo_config_preserves_existing_sections_and_flags_conflicts(self):
        with tempfile.TemporaryDirectory() as tmp:
            wt = Path(tmp) / "worktree"
            wt.mkdir()
            shared = Path(tmp) / "shared-target"
            other_shared = Path(tmp) / "other-target"
            cfg = ensure_worktree_cargo_config(wt, shared)
            self.assertTrue(cfg.is_file())
            text = cfg.read_text(encoding="utf-8")
            self.assertIn(f'target-dir = "{shared.resolve()}"', text)
            self.assertIn("incremental = false", text)
            self.assertEqual(ensure_worktree_cargo_config(wt, shared), cfg)

            # Existing custom sections and flags under [build] must survive intact.
            cfg.write_text(
                '[build]\n'
                'rustflags = ["-C", "target-cpu=native"]\n'
                'jobs = 8\n\n'
                '[net]\n'
                'offline = true\n\n'
                '[target.x86_64-unknown-linux-gnu]\n'
                'linker = "clang"\n',
                encoding="utf-8",
            )
            ensure_worktree_cargo_config(wt, shared)
            updated = cfg.read_text(encoding="utf-8")
            self.assertIn('rustflags = ["-C", "target-cpu=native"]', updated)
            self.assertIn("jobs = 8", updated)
            self.assertIn("[net]\noffline = true", updated)
            self.assertIn('[target.x86_64-unknown-linux-gnu]\nlinker = "clang"', updated)
            self.assertIn(f'target-dir = "{shared.resolve()}"', updated)
            self.assertIn("incremental = false", updated)

            # Conflicting target-dir is refused unless force=True (including non-ASCII paths).
            non_ascii_shared = Path(tmp) / "東京-target"
            with self.assertRaisesRegex(ValueError, "already sets"):
                ensure_worktree_cargo_config(wt, other_shared)
            ensure_worktree_cargo_config(wt, non_ascii_shared, force=True)
            # Re-running with identical non-ASCII target-dir succeeds idempotently.
            ensure_worktree_cargo_config(wt, non_ascii_shared)
            self.assertIn(
                f'target-dir = "{non_ascii_shared.resolve()}"',
                cfg.read_text(encoding="utf-8"),
            )

            # Malformed TOML fails loudly rather than silently overwriting configuration.
            cfg.write_text("[build\nbroken = [1, 2", encoding="utf-8")
            with self.assertRaises(ValueError):
                ensure_worktree_cargo_config(wt, shared, force=True)

    def test_resolve_honors_env_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            old = os.environ.get("CARGO_TARGET_DIR")
            try:
                os.environ["CARGO_TARGET_DIR"] = tmp
                self.assertEqual(
                    resolve_cargo_target_dir(Path(__file__).resolve().parents[1]),
                    Path(tmp).resolve(),
                )
            finally:
                if old is None:
                    os.environ.pop("CARGO_TARGET_DIR", None)
                else:
                    os.environ["CARGO_TARGET_DIR"] = old

    def test_prune_superseded_artifacts_keeps_newest_hashes_and_unhashed_objects(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "cargo-target"
            deps = target / "debug" / "deps"
            fp = target / "debug" / ".fingerprint"
            inc = target / "debug" / "incremental" / "stale_unit"
            deps.mkdir(parents=True)
            fp.mkdir(parents=True)
            inc.mkdir(parents=True)
            (inc / "query.bin").write_bytes(b"incremental-cache")
            (deps / "chunk.rcgu.o").write_bytes(b"obj")

            hashes = [
                ("0000000000000001", 100.0),
                ("0000000000000002", 200.0),
                ("0000000000000003", 300.0),
            ]
            for h, ts in hashes:
                exe = deps / f"shardloom-{h}"
                dfile = deps / f"shardloom-{h}.d"
                fpdir = fp / f"shardloom-{h}"
                fpdir.mkdir()
                (fpdir / "bin").write_text("ok", encoding="utf-8")
                exe.write_bytes(b"binary-payload")
                dfile.write_text("dep", encoding="utf-8")
                os.utime(exe, (ts, ts))
                os.utime(dfile, (ts, ts))

            for h, ts in hashes:
                rlib = deps / f"libserde-{h}.rlib"
                rlib.write_bytes(b"rlib")
                os.utime(rlib, (ts, ts))

            report = prune_superseded_artifacts(
                target,
                keep_workspace_hashes=1,
                keep_dep_hashes=2,
                check_open_handles=False,
            )
            self.assertEqual(report["profiles_scanned"], ["debug"])
            self.assertFalse((inc / "query.bin").exists())
            # Unhashed intermediate objects are not deleted aggressively.
            self.assertTrue((deps / "chunk.rcgu.o").exists())
            self.assertFalse((deps / "shardloom-0000000000000001").exists())
            self.assertFalse((fp / "shardloom-0000000000000001").exists())
            self.assertFalse((deps / "shardloom-0000000000000002").exists())
            self.assertTrue((deps / "shardloom-0000000000000003").exists())
            self.assertTrue((fp / "shardloom-0000000000000003").exists())
            self.assertFalse((deps / "libserde-0000000000000001.rlib").exists())
            self.assertTrue((deps / "libserde-0000000000000002.rlib").exists())
            self.assertTrue((deps / "libserde-0000000000000003.rlib").exists())

    def test_prune_never_traverses_symlinks_into_external_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            outside = root / "outside"
            outside.mkdir()
            secret = outside / "important.txt"
            secret.write_text("do-not-delete", encoding="utf-8")

            target = root / "cargo-target"
            deps = target / "debug" / "deps"
            fp = target / "debug" / ".fingerprint"
            inc = target / "debug" / "incremental"
            deps.mkdir(parents=True)
            fp.mkdir(parents=True)
            inc.mkdir(parents=True)

            # Directory symlinks placed under incremental and superseded .fingerprint.
            (inc / "linked-inc").symlink_to(outside, target_is_directory=True)
            (fp / "shardloom-0000000000000001").symlink_to(outside, target_is_directory=True)
            for h, ts in [("0000000000000001", 100.0), ("0000000000000002", 200.0)]:
                exe = deps / f"shardloom-{h}"
                exe.write_bytes(b"bin")
                os.utime(exe, (ts, ts))

            report = prune_superseded_artifacts(
                target,
                keep_workspace_hashes=1,
                keep_dep_hashes=1,
                check_open_handles=False,
            )
            self.assertEqual(secret.read_text(encoding="utf-8"), "do-not-delete")
            self.assertTrue((inc / "linked-inc").is_symlink())
            self.assertTrue((fp / "shardloom-0000000000000001").is_symlink())
            self.assertGreaterEqual(len(report["skipped_symlinks"]), 2)

            symlink_target = root / "target-link"
            symlink_target.symlink_to(target, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "must not be a symlink"):
                prune_superseded_artifacts(symlink_target, check_open_handles=False)

    def test_prune_skips_profile_when_cargo_lock_is_held(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "cargo-target"
            deps = target / "debug" / "deps"
            deps.mkdir(parents=True)
            old_exe = deps / "shardloom-0000000000000001"
            new_exe = deps / "shardloom-0000000000000002"
            old_exe.write_bytes(b"old")
            new_exe.write_bytes(b"new")
            os.utime(old_exe, (100.0, 100.0))
            os.utime(new_exe, (200.0, 200.0))
            lock_file = target / "debug" / ".cargo-lock"
            with lock_file.open("a+b") as lock_fd:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                try:
                    report = prune_superseded_artifacts(
                        target,
                        keep_workspace_hashes=1,
                        keep_dep_hashes=1,
                        check_open_handles=False,
                    )
                finally:
                    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            self.assertEqual(report["skipped_locked_profiles"], ["debug"])
            self.assertEqual(report["profiles_scanned"], [])
            self.assertTrue(old_exe.exists())


if __name__ == "__main__":
    unittest.main()
