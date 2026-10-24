"""Engine-free replay and bounded trust controls for the portable G1 packet."""
from __future__ import annotations

import copy
import gzip
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / 'docs/benchmarks/evidence/native-grouped-preparation-2026-10-24'
VERIFIER = EVIDENCE / 'verify_packet.py'


class OverlayPacket:
    """Change one decoded record while leaving the archived packet untouched."""

    def __init__(self, original, reference, change):
        self.original = original
        self.manifest = original.manifest
        self.files = original.files
        self.reference = reference
        self.change = change

    def read(self, ref):
        value = self.original.read(ref)
        if ref == self.reference:
            self.change(value)
        return value

    def data(self, ref):
        return self.original.data(ref)


class NativeGroupedPreparationEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not (EVIDENCE / 'evidence.tar.xz').is_file():
            if (ROOT / '.git').exists():
                raise AssertionError('tracked native grouped preparation evidence is missing')
            raise unittest.SkipTest('evidence packet is excluded from exported source archives')
        global VERIFY
        spec = importlib.util.spec_from_file_location('native_grouped_verify_packet', VERIFIER)
        VERIFY = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = VERIFY
        spec.loader.exec_module(VERIFY)

    def relocated(self, destination):
        shutil.copytree(EVIDENCE, destination)
        return destination

    def packet(self, directory):
        return VERIFY.Packet(directory)

    def test_relocated_cli_replays_without_original_binaries_or_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = self.relocated(Path(temporary) / 'evidence')
            result = subprocess.run([sys.executable, '-B', str(directory / 'verify_packet.py'),
                                     '--root', str(directory)], cwd=directory.parent,
                                    text=True, capture_output=True, timeout=180, check=False)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            replay = json.loads(result.stdout)
            self.assertEqual(replay['status'], 'passed')
            self.assertFalse(replay['engine_executed'])
            self.assertEqual(replay['comparison_calls'], 52)
            self.assertEqual(replay['full43_queries'], 43)
            self.assertEqual(replay['full43_rows'], 381)
            self.assertEqual(replay['full43_engine_seconds'], 48.718933086)

    def test_corrupted_archive_byte_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = self.relocated(Path(temporary) / 'evidence')
            archive = directory / 'evidence.tar.xz'
            content = bytearray(archive.read_bytes())
            content[len(content) // 2] ^= 1
            archive.write_bytes(content)
            with self.assertRaises(AssertionError):
                self.packet(directory)

    def test_strict_json_rejects_duplicate_keys_and_nonfinite_values(self):
        for source in [b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}']:
            with self.subTest(source=source), self.assertRaises(ValueError):
                VERIFY.strict(source)

    def test_result_rejects_fallback_clock_values_and_schema_changes(self):
        with tempfile.TemporaryDirectory() as temporary:
            packet = self.packet(self.relocated(Path(temporary) / 'evidence'))
            manifest = packet.manifest
            plan = packet.read(manifest['full43_plan'])
            summary = packet.read(manifest['full43_receipt'])
            receipt = packet.read(summary['summary'])
            reference = packet.read(plan['reference'])
            expected = {row['query_id']: row['values'] for row in reference['records']}
            record = receipt['records'][0]
            archived = record['completed_log_archives'][0]
            packed = packet.data(dict(path=archived['path'], sha256=archived['gzip_sha256'],
                                      bytes=archived['gzip_bytes']))
            raw = gzip.decompress(packed)
            envelope = VERIFY.strict(raw)

            def rejects(change):
                altered = copy.deepcopy(envelope)
                change(altered)
                with self.assertRaises((AssertionError, KeyError)):
                    VERIFY.result(json.dumps(altered).encode(), expected[record['query']], record)

            rejects(lambda value: value['fallback'].update(attempted=True))
            rejects(lambda value: value['fields'].__setitem__(
                next(i for i, field in enumerate(value['fields'])
                     if field['key'] == 'native_query_timing_clock'),
                dict(key='native_query_timing_clock', value='cpu_time')))
            payload_index = next(i for i, field in enumerate(envelope['fields'])
                                 if field['key'] in ('result_values_json', 'result_jsonl'))
            payload_key = envelope['fields'][payload_index]['key']
            rejects(lambda value: value['fields'].__setitem__(payload_index,
                dict(key=payload_key, value='' if payload_key == 'result_jsonl' else '[]')))
            rejects(lambda value: value['fields'].__setitem__(slice(None), [
                field for field in value['fields'] if field['key'] != 'native_query_timing_clock']))
            altered = OverlayPacket(packet, manifest['full43_plan'], lambda value:
                value['expected_schemas'].__setitem__('1', ['changed', []]))
            with self.assertRaises(AssertionError):
                VERIFY.verify(altered)

    def test_frozen_gate_changes_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            packet = self.packet(self.relocated(Path(temporary) / 'evidence'))
            ref = packet.manifest['comparison_plan']
            altered = OverlayPacket(packet, ref, lambda plan: plan['gates'].__setitem__(
                'control_relative_allowance', .06))
            with self.assertRaises(AssertionError):
                VERIFY.verify(altered)

    def test_comparison_call_order_and_full43_run_count_are_enforced(self):
        mutations = [
            lambda summary: summary['records'].pop(),
            lambda summary: summary['records'].reverse(),
            lambda summary: summary['records'][0].update(run=2),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            packet = self.packet(self.relocated(Path(temporary) / 'evidence'))
            outcome = packet.read(packet.manifest['comparison_outcome'])
            target_summary = outcome['operations'][0]['summary']
            for mutate in mutations:
                altered = OverlayPacket(packet, target_summary, mutate)
                with self.subTest(mutate=mutate), self.assertRaises(AssertionError):
                    VERIFY.verify(altered)

            full = packet.read(packet.manifest['full43_receipt'])
            altered = OverlayPacket(packet, full['summary'], lambda value: value['records'].pop())
            with self.assertRaises(AssertionError):
                VERIFY.verify(altered)

    def test_binary_reference_and_source_identities_are_bound(self):
        with tempfile.TemporaryDirectory() as temporary:
            packet = self.packet(self.relocated(Path(temporary) / 'evidence'))
            plan_ref = packet.manifest['comparison_plan']
            changes = [
                lambda plan: plan['binary']['candidate'].__setitem__('sha256', '0' * 64),
                lambda plan: plan['reference'].__setitem__('sha256', '0' * 64),
                lambda plan: plan.__setitem__('source_commit', '0' * 40),
                lambda plan: plan.__setitem__('source_tree', '0' * 40),
                lambda plan: plan['baseline_revision'].__setitem__('commit', '0' * 40),
            ]
            for change in changes:
                with self.subTest(change=change), self.assertRaises((AssertionError, KeyError)):
                    VERIFY.verify(OverlayPacket(packet, plan_ref, change))


if __name__ == '__main__':
    unittest.main()
