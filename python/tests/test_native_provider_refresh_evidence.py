"""Source and public-transcript regressions for portable provider acceptance."""
from __future__ import annotations

import gzip
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
PACKET = ROOT / 'docs/benchmarks/evidence/native-vortex-provider-refresh-2026-10-24'


class NativeProviderEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = PACKET / 'verify_packet.py'
        if not path.is_file():
            if (ROOT / '.git').exists():
                raise AssertionError('tracked provider verifier is missing')
            raise unittest.SkipTest('evidence is excluded from exported source archives')
        sys.path.insert(0, str(PACKET))
        try:
            spec = importlib.util.spec_from_file_location('native_provider_verify', path)
            cls.verifier = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(cls.verifier)
        finally:
            sys.path.remove(str(PACKET))

    def test_current_source_rejects_changed_and_missing_tracked_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / 'Cargo.lock'
            source.write_bytes(b'accepted dependency graph')
            tracked = {'Cargo.lock': self.verifier.digest(source.read_bytes())}
            self.verifier.check_current_source(tracked, root)
            source.write_bytes(b'changed dependency graph')
            with self.assertRaisesRegex(ValueError, 'current runtime source differs'):
                self.verifier.check_current_source(tracked, root)
            source.unlink()
            with self.assertRaisesRegex(ValueError, 'current runtime source is missing'):
                self.verifier.check_current_source(tracked, root)

    def test_generated_standalone_locks_are_not_committed_runtime_sources(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'Cargo.lock').write_bytes(b'root graph')
            tracked = {'Cargo.lock': self.verifier.digest(b'root graph')}
            self.verifier.check_current_source(tracked, root)
            generated = root / 'vendor/vortex-file/Cargo.lock'
            generated.parent.mkdir(parents=True)
            generated.write_bytes(b'new standalone test graph')
            self.verifier.check_current_source(tracked, root)

    def test_strict_json_rejects_duplicate_keys_and_nonfinite_values(self):
        for raw in ('{"x":1,"x":2}', '{"x":NaN}', '{"x":1e999}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                self.verifier.strict(raw)

    def test_preparation_uses_its_own_report_namespace_and_rejects_bad_evidence(self):
        def sql_only(_envelope):
            raise ValueError('public SQL evidence is absent')
        envelope = dict(status='success', fallback=dict(attempted=False, engine=None),
                        fields=[dict(key='vortex_fallback_attempted', value='false')])
        self.assertEqual(self.verifier.native_fields(envelope, sql_only, preparation=True),
                         {'vortex_fallback_attempted': 'false'})
        envelope['fields'].append(dict(key='vortex_fallback_attempted', value='false'))
        with self.assertRaisesRegex(ValueError, 'duplicate preparation report fields'):
            self.verifier.native_fields(envelope, sql_only, preparation=True)
        envelope['fields'].pop()
        envelope['fields'][0]['value'] = 'true'
        with self.assertRaisesRegex(ValueError, 'native fallback marker'):
            self.verifier.native_fields(envelope, sql_only, preparation=True)

    def test_case_level_values_archive_is_required_and_checked(self):
        root = Path('/portable/public')
        summary_path = root / 'summary.json'
        raw = b'{"value":1}\n{"value":2}\n'
        packed = gzip.compress(raw, mtime=0)
        ref = dict(path='rows.jsonl.gz', bytes=len(packed), sha256=self.verifier.digest(packed))
        summary = dict(status='passed', performance_claim=False, families=['case'],
                       cases=[dict(name='case', status='passed', complete_rows=ref,
                                   rows_verified=2, all_output_rows_sha256=self.verifier.digest(raw))],
                       compressed_json_files=[])
        files = {str(summary_path): json.dumps(summary).encode(), str(root / ref['path']): packed}
        replay = lambda: self.verifier.verify_public(summary_path, lambda path: files[str(path)])
        self.assertEqual(replay()['counts']['case_archive_rows'], 2)
        files.pop(str(root / ref['path']))
        with self.assertRaises(KeyError):
            replay()
        changed = gzip.compress(b'{"value":1}\n{"value":3}\n', mtime=0)
        ref.update(bytes=len(changed), sha256=self.verifier.digest(changed))
        files[str(root / ref['path'])] = changed
        files[str(summary_path)] = json.dumps(summary).encode()
        with self.assertRaises(AssertionError):
            replay()

    def test_complete_values_compare_actual_to_expected(self):
        root = Path('/portable/public')
        summary_path = root / 'summary.json'
        values = dict(actual=[dict(value=1)], expected=[dict(value=2)])
        raw = json.dumps(values).encode()
        packed = gzip.compress(raw, mtime=0)
        item = dict(path='one-values.json.gz', raw_path='one-values.json',
                    bytes=len(packed), sha256=self.verifier.digest(packed),
                    raw_bytes=len(raw), raw_sha256=self.verifier.digest(raw))
        summary = dict(status='passed', performance_claim=False, families=['case'],
                       cases=[dict(name='case', status='passed')], compressed_json_files=[item])
        files = {str(summary_path): json.dumps(summary).encode(), str(root / item['path']): packed}
        with self.assertRaises(AssertionError):
            self.verifier.verify_public(summary_path, lambda path: files[str(path)])
        raw = b'{"actual":[{"value":1e999}],"expected":[{"value":1e999}]}'
        packed = gzip.compress(raw, mtime=0)
        item.update(bytes=len(packed), sha256=self.verifier.digest(packed),
                    raw_bytes=len(raw), raw_sha256=self.verifier.digest(raw))
        files[str(root / item['path'])] = packed
        files[str(summary_path)] = json.dumps(summary).encode()
        with self.assertRaisesRegex(ValueError, 'non-finite JSON value'):
            self.verifier.verify_public(summary_path, lambda path: files[str(path)])


if __name__ == '__main__':
    unittest.main()
