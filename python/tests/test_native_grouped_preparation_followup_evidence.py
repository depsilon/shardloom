"""Bounded replay and trust controls for the completed G2/G3 follow-up packet."""
from __future__ import annotations

import copy
import gzip
import importlib.util
import json
import math
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / 'docs/benchmarks/evidence/native-grouped-preparation-2026-10-24'
FOLLOWUP = EVIDENCE / 'followup'


class OverlayPacket:
    """Apply decoded mutations without modifying the cached packet or originals."""

    def __init__(self, original, mutations):
        self.original = original
        self.manifest = copy.deepcopy(original.manifest)
        self.files = original.files
        self.mutations = mutations

    @staticmethod
    def key(ref):
        return ref['path'], ref['sha256']

    def read(self, ref):
        value = copy.deepcopy(self.original.read(ref))
        change = self.mutations.get(self.key(ref))
        if change is not None:
            change(value)
        return value

    def data(self, ref):
        return self.original.data(ref)


class NativeGroupedPreparationFollowupEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not (FOLLOWUP / 'evidence.tar.xz').is_file():
            if (ROOT / '.git').exists():
                raise AssertionError('tracked grouped preparation follow-up evidence is missing')
            raise unittest.SkipTest('evidence packet is excluded from exported source archives')
        global FOLLOWUP_VERIFY
        spec = importlib.util.spec_from_file_location('native_grouped_verify_followup',
                                                       FOLLOWUP / 'verify_packet.py')
        FOLLOWUP_VERIFY = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = FOLLOWUP_VERIFY
        spec.loader.exec_module(FOLLOWUP_VERIFY)
        cls.packet, cls.core = FOLLOWUP_VERIFY.open_packet(FOLLOWUP)
        cls.manifest = cls.packet.manifest

    @classmethod
    def tearDownClass(cls):
        cls.packet = None

    def relocated(self, destination):
        shutil.copytree(EVIDENCE, destination)
        return destination / 'followup'

    def overlay(self, ref, change):
        return OverlayPacket(self.packet, {OverlayPacket.key(ref): change})

    def comparison(self, candidate):
        item, = [item for item in self.manifest['comparisons'] if item['id'] == candidate]
        return item

    def assert_rejected(self, packet):
        with self.assertRaises((AssertionError, KeyError, ValueError)):
            FOLLOWUP_VERIFY.verify(packet, self.core)

    def test_relocated_cli_replays_g2_g3_and_full43(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self.relocated(Path(temporary) / 'evidence')
            result = subprocess.run([sys.executable, '-B', str(root / 'verify_packet.py'),
                                     '--root', str(root)], cwd=root.parent, text=True,
                                    capture_output=True, timeout=180, check=False)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            replay = json.loads(result.stdout)
            self.assertEqual(replay['status'], 'passed')
            self.assertFalse(replay['engine_executed'])
            self.assertEqual([(row['id'], row['calls'], row['status'])
                              for row in replay['comparisons']],
                             [('g2', 52, 'retained'), ('g3', 58, 'dropped')])
            self.assertEqual(replay['full43_queries'], 43)
            self.assertEqual(replay['full43_rows'], 381)
            self.assertIsInstance(replay['full43_engine_seconds'], (int, float))
            self.assertTrue(math.isfinite(replay['full43_engine_seconds']))
            self.assertGreaterEqual(replay['full43_engine_seconds'], 0)

    def test_corrupted_archive_byte_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self.relocated(Path(temporary) / 'evidence')
            archive = root / 'evidence.tar.xz'
            contents = bytearray(archive.read_bytes())
            contents[len(contents) // 2] ^= 1
            archive.write_bytes(contents)
            with self.assertRaises((AssertionError, ValueError)):
                FOLLOWUP_VERIFY.open_packet(root)

    def test_frozen_gates_and_g3_promotion_are_rejected(self):
        g2 = self.comparison('g2')
        self.assert_rejected(self.overlay(g2['plan'], lambda plan:
            plan['gates'].__setitem__('control_relative_allowance', .06)))
        g3 = self.comparison('g3')
        self.assert_rejected(self.overlay(g3['decision'], lambda decision:
            decision.__setitem__('retained', True)))

    def test_binary_reference_source_and_revision_identity_mutations_are_rejected(self):
        plan_ref = self.comparison('g2')['plan']
        mutations = [
            lambda plan: plan['binary']['candidate'].__setitem__('sha256', '0' * 64),
            lambda plan: plan['reference'].__setitem__('sha256', '0' * 64),
            lambda plan: plan.__setitem__('source_commit', '0' * 40),
            lambda plan: plan.__setitem__('source_tree', '0' * 40),
            lambda plan: plan['candidate_revision'].__setitem__('commit', '0' * 40),
        ]
        for change in mutations:
            with self.subTest(change=change):
                self.assert_rejected(self.overlay(plan_ref, change))

    def test_comparison_records_must_be_complete_ordered_and_unique(self):
        outcome = self.packet.read(self.comparison('g2')['outcome'])
        summary_ref = outcome['operations'][0]['summary']
        mutations = [
            lambda summary: summary['records'].pop(),
            lambda summary: summary['records'].reverse(),
            lambda summary: summary['records'][0].update(run=2),
        ]
        for change in mutations:
            with self.subTest(change=change):
                self.assert_rejected(self.overlay(summary_ref, change))

    def test_full43_query_coverage_and_expected_schema_are_enforced(self):
        full = self.packet.read(self.manifest['full43_receipt'])
        summary_ref = full['summary']
        self.assert_rejected(self.overlay(summary_ref, lambda summary: summary['records'].pop()))
        plan_ref = self.manifest['full43_plan']
        self.assert_rejected(self.overlay(plan_ref, lambda plan:
            plan['expected_schemas'].__setitem__('1', ['changed', []])))

    def test_core_result_rejects_fallback_clock_and_value_changes(self):
        full = self.packet.read(self.manifest['full43_receipt'])
        plan = self.packet.read(self.manifest['full43_plan'])
        summary = self.packet.read(full['summary'])
        record = summary['records'][0]
        reference = self.packet.read(plan['reference'])
        expected = {row['query_id']: row['values'] for row in reference['records']}
        archived, = record['completed_log_archives']
        raw = gzip.decompress(self.packet.data(dict(path=archived['path'],
            sha256=archived['gzip_sha256'], bytes=archived['gzip_bytes'])))
        envelope = self.core.strict(raw)
        fields = envelope['fields']

        def rejects(change):
            altered = copy.deepcopy(envelope)
            change(altered)
            with self.assertRaises(AssertionError):
                self.core.result(json.dumps(altered).encode(), expected[record['query']], record)

        rejects(lambda value: value['fallback'].update(attempted=True))
        clock_index = next(i for i, field in enumerate(fields)
                           if field['key'] == 'native_query_timing_clock')
        rejects(lambda value: value['fields'][clock_index].__setitem__('value', 'cpu_time'))
        payload_index = next(i for i, field in enumerate(fields)
                             if field['key'] in ('result_values_json', 'result_jsonl'))
        payload = fields[payload_index]
        changed_payload = '[]' if payload['key'] == 'result_values_json' else ''
        rejects(lambda value: value['fields'][payload_index].__setitem__('value', changed_payload))


if __name__ == '__main__':
    unittest.main()
