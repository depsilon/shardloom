"""Clean-checkout source checks for the frozen native surface parity packet."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
VERIFIER = ROOT / 'docs/benchmarks/evidence/native-surface-parity-2026-10-24/verify_packet.py'


class NativeSurfaceParitySourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not VERIFIER.is_file():
            if (ROOT / '.git').exists():
                raise AssertionError('tracked native surface parity verifier is missing')
            raise unittest.SkipTest('evidence is excluded from exported source archives')
        spec = importlib.util.spec_from_file_location('native_surface_parity_verify', VERIFIER)
        cls.verifier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.verifier)

    def fixture(self, root):
        sources = {'Cargo.lock': b'committed dependency graph',
                   'engine/src/lib.rs': b'committed runtime',
                   'python/src/runtime.py': b'committed wrapper'}
        for path, value in sources.items():
            target = root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(value)
        return {
            'source_after': {**self.verifier.GENERATED_TEST_LOCKFILES,
                             **{p: self.verifier.digest(v) for p, v in sources.items()
                                if not p.startswith('python/')}},
            'runtime_python_sources': {'python/src/runtime.py': self.verifier.digest(
                sources['python/src/runtime.py'])},
        }

    def test_absent_or_regenerated_test_lockfiles_do_not_affect_runtime_check(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            build = self.fixture(root)
            self.verifier.check_current_source(build, root)
            for path in self.verifier.GENERATED_TEST_LOCKFILES:
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(b'a newly generated standalone test graph')
            self.verifier.check_current_source(build, root)

    def test_missing_or_changed_committed_sources_are_rejected(self):
        for path in ('Cargo.lock', 'engine/src/lib.rs', 'python/src/runtime.py'):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                build = self.fixture(root)
                (root / path).write_bytes(b'changed')
                with self.assertRaisesRegex(ValueError, 'current runtime source differs'):
                    self.verifier.check_current_source(build, root)
                (root / path).unlink()
                with self.assertRaisesRegex(ValueError, 'current runtime source is missing'):
                    self.verifier.check_current_source(build, root)

    def test_recorded_generated_file_identity_cannot_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            build = self.fixture(root)
            build['source_after']['vendor/vortex-file/Cargo.lock'] = 'changed'
            with self.assertRaisesRegex(ValueError, 'recorded generated-test lockfile identities'):
                self.verifier.check_current_source(build, root)


if __name__ == '__main__':
    unittest.main()
