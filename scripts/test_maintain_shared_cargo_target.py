"""Unit tests for shared Cargo target resolution, config wiring, and superseded-hash pruning."""

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
    def test_ensure_worktree_cargo_config_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            wt = Path(tmp) / "worktree"
            wt.mkdir()
            shared = Path(tmp) / "shared-target"
            cfg = ensure_worktree_cargo_config(wt, shared)
            self.assertTrue(cfg.is_file())
            text = cfg.read_text(encoding="utf-8")
            self.assertIn(f'target-dir = "{shared.resolve()}"', text)
            self.assertIn("incremental = false", text)
            self.assertEqual(ensure_worktree_cargo_config(wt, shared), cfg)

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

    def test_prune_superseded_artifacts_keeps_newest_hashes_and_drops_old(self):
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

            # Workspace binary hashes: 3 hashes, keep_workspace_hashes=1 should drop the older 2
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

            # External dep hashes: keep_dep_hashes=2 should keep newest 2 and drop 1
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
            self.assertFalse((deps / "chunk.rcgu.o").exists())
            self.assertFalse((deps / "shardloom-0000000000000001").exists())
            self.assertFalse((fp / "shardloom-0000000000000001").exists())
            self.assertFalse((deps / "shardloom-0000000000000002").exists())
            self.assertTrue((deps / "shardloom-0000000000000003").exists())
            self.assertTrue((fp / "shardloom-0000000000000003").exists())
            self.assertFalse((deps / "libserde-0000000000000001.rlib").exists())
            self.assertTrue((deps / "libserde-0000000000000002.rlib").exists())
            self.assertTrue((deps / "libserde-0000000000000003.rlib").exists())


if __name__ == "__main__":
    unittest.main()
