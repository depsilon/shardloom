#!/usr/bin/env python3
"""Small runner contracts; no dataset, benchmark or engine execution."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import run_local_readonly_proof as proof


class ReadonlyProofTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.source = self.root / 'source.txt'
        self.source.write_text('original')
        self.args = ['proof', '--uat-root', str(self.root), '--source', str(self.source),
                     '--name', 'test', '--', sys.executable, '-c', 'pass']

    def run_main(self, execute):
        with patch.object(sys, 'argv', self.args), patch.object(proof, 'check_budgets', return_value={}), \
             patch.object(proof, 'run_profiled_command', side_effect=execute), contextlib.redirect_stdout(io.StringIO()):
            return proof.main()

    def result(self, *_args):
        self.assertTrue((self.root / '.ingest-uat.lock').is_dir())
        return {'returncode': 0, 'guard_failures': []}

    def test_success_records_source_and_executable_and_releases_lock(self):
        self.assertEqual(self.run_main(self.result), 0)
        receipt = json.loads((self.root / 'logs/readonly-proof-test/receipt.json').read_text())
        self.assertTrue(receipt['unchanged'])
        self.assertEqual(receipt['inputs_before'], receipt['inputs_after'])
        self.assertIn(str(Path(sys.executable).resolve()), receipt['inputs_before'])
        self.assertFalse((self.root / '.ingest-uat.lock').exists())

    def test_changed_input_fails(self):
        def change(*args):
            self.source.write_text('changed content')
            return self.result(*args)
        self.assertEqual(self.run_main(change), 1)
        self.assertFalse((self.root / '.ingest-uat.lock').exists())

    def test_failed_child_or_guard_fails(self):
        self.assertEqual(self.run_main(lambda *_: {'returncode': 1, 'guard_failures': ['failed']}), 1)
        self.assertFalse((self.root / '.ingest-uat.lock').exists())

    def test_exception_releases_owned_lock(self):
        def fail(*_):
            raise RuntimeError('watchdog failure')
        with self.assertRaisesRegex(RuntimeError, 'watchdog failure'):
            self.run_main(fail)
        self.assertFalse((self.root / '.ingest-uat.lock').exists())

    def test_existing_run_not_overwritten(self):
        existing = self.root / 'logs/readonly-proof-test'
        existing.mkdir(parents=True)
        marker = existing / 'keep.txt'
        marker.write_text('keep')
        with self.assertRaises(FileExistsError):
            self.run_main(self.result)
        self.assertEqual(marker.read_text(), 'keep')
        self.assertFalse((self.root / '.ingest-uat.lock').exists())

    def test_existing_lock_not_removed(self):
        lock = self.root / '.ingest-uat.lock'
        lock.mkdir()
        with self.assertRaises(FileExistsError):
            self.run_main(self.result)
        self.assertTrue(lock.is_dir())

    def test_log_symlink_escape_rejected(self):
        with tempfile.TemporaryDirectory() as outside:
            (self.root / 'logs').symlink_to(outside, target_is_directory=True)
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                self.run_main(self.result)
        self.assertFalse((self.root / '.ingest-uat.lock').exists())


if __name__ == '__main__':
    unittest.main()
