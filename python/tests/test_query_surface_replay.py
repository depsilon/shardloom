# SPDX-License-Identifier: Apache-2.0
"""Replay rejects corrupt receipts, changed semantics and unsafe archives."""
from __future__ import annotations

from copy import deepcopy
import io
import json
import lzma
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'benchmarks/traditional_analytics'))
sys.path.insert(0, str(ROOT / 'scripts'))
from query_surface.replay import bounded_bytes, object_files, sha, verify_runs
from query_surface.semantics import ResultMismatch


def encoded(value):
    return json.dumps(value, sort_keys=True, allow_nan=False).encode()


def fixture():
    base = '/local/evidence/'
    schema = [{'name': 'n', 'type': {'name': 'int64', 'nullable': False, 'parameters': []}}]
    case = {'id': 'count', 'relation': {'source': 'fact'},
            'contract': {'schema': schema, 'ordered': True, 'floats': {}, 'max_matching_edges': 100}}
    case_identity = sha(encoded(case))
    result = {'schema': schema, 'rows': [{'n': 3}]}
    records = {}
    for name, native, failed in [('native', True, False), ('failure', True, True), ('polars', False, False)]:
        path = base + name + '/case'
        status = 'engine_error' if failed else 'passed'
        job = {'case_id': 'count', 'case_identity': case_identity, 'reference_path': base + 'reference.json',
               'reference_sha256': '', **({'surface': 'python_sql'} if native else {'kind': 'polars'})}
        observation = {'complete_consumption': True, 'public_declaration': case['relation'],
            'clocks': {'engine_execution_seconds': 0.125 if native else None, 'complete_workflow_seconds': 0.25},
            **({'surface': 'python_sql', 'fields': {'fallback_attempted': False, 'external_engine_invoked': False}}
               if native else {'execution_engine': 'polars'})}
        outcome = {'status': status, 'case_id': 'count', 'case_identity': case_identity,
                   'comparison_engine_invoked': not native, 'observation': observation, 'actual': deepcopy(result)}
        row = {'case_id': 'count', 'status': status, 'evidence': path + '.stdout.json', 'evidence_sha256': '',
               **({'surface': 'python_sql', 'engine_execution_seconds': 0.125} if native else
                  {'comparison': {'comparable': True, 'clock': 'complete_workflow_seconds',
                    'shardloom_seconds': 0.25, 'polars_seconds': 0.25, 'native_over_polars': 1.0,
                    'native_minus_polars_seconds': 0.0}})}
        provenance = {'comparison_engine_invoked': False if native else 'polars_only', 'selected_cases': ['count'],
            **({'surfaces': ['python_sql'], 'runtime_and_inputs': [], 'harness_sources': [], 'python_sources': []}
               if native else {'native_run': base + 'native', 'native_surface': 'python_sql', 'frozen_files': []})}
        summary = {'selected_cases': 1, **({'completed_selected_cohort': True,
            'source_runtime_posthash_verified': True, 'families': {'aggregate': {'case_surface_outcomes': {status: 1}}}}
            if native else {'completed': True, 'posthash_verified': True, 'all_passed': True,
                            'comparisons': [deepcopy(row['comparison'])]})}
        records.update({name + '/corpus.json': [deepcopy(case)], name + '/provenance.json': provenance,
            name + '/summary.json': summary, name + '/observations.jsonl': [row],
            name + '/case.job.json': job, name + '/case.stdout.json': outcome,
            name + '/case.stderr.txt': b'failed' if failed else b'',
            name + '/case.receipt.json': {'returncode': 2 if failed else 0, 'guard_failures': [],
                'stdout': path + '.stdout.json', 'stderr': path + '.stderr.txt'}})
    records['reference.json'] = {'status': 'passed', 'case_identity': case_identity, 'reference': result}
    manifest = {'original_evidence_root': base.rstrip('/'), 'external_artifacts': [],
        'runs': ['native', 'failure', 'polars'], 'incomplete_before_summary': [],
        'baseline_native_runs': ['native'], 'baseline_pairs': [['native', 'polars']],
        'baseline_case_count': 1, 'baseline_case_ids': ['count'], 'baseline_engine_seconds': 0.125}
    return records, manifest


def freeze(records):
    """Refresh envelope hashes so semantic mutations must fail deeper checks."""
    for name in ('native', 'failure', 'polars'):
        job = records[name + '/case.job.json']
        job['reference_sha256'] = sha(encoded(records['reference.json']))
        receipt = records[name + '/case.receipt.json']
        receipt['job_sha256'] = sha(encoded(job))
        receipt['stdout_sha256'] = sha(encoded(records[name + '/case.stdout.json']))
        receipt['stderr_sha256'] = sha(records[name + '/case.stderr.txt'])
        for row in records[name + '/observations.jsonl']:
            row['evidence_sha256'] = receipt['stdout_sha256']
    return {'evidence/' + name: (b'\n'.join(encoded(row) for row in value) if name.endswith('.jsonl')
                                 else value if isinstance(value, bytes) else encoded(value))
            for name, value in records.items()}


class ReplayTests(unittest.TestCase):
    def test_committed_packet_preserves_all_results_and_failed_observations(self):
        directory = ROOT / 'docs/benchmarks/evidence/native-broad-query-2026-10-26'
        if not (directory / 'packet.tar.xz').exists():
            if (ROOT / '.git').exists():
                self.fail('tracked broad-query evidence is missing')
            self.skipTest('historical evidence packets are excluded from exported source archives')
        manifest = json.loads((directory / 'manifest.json').read_text())
        files = object_files(manifest, (directory / 'packet.tar.xz').read_bytes())
        result = verify_runs(files, manifest)
        self.assertEqual((result['checked_complete_results'], result['checked_receipts']), (2331, 2334))
        self.assertEqual(result['baseline_cases'], 347)
        self.assertAlmostEqual(result['baseline_engine_seconds'], 86.828000503, places=10)
        self.assertEqual(result['runs']['full238_tiny_parity_r1']['statuses'], {'passed': 1189, 'engine_error': 1})
        self.assertEqual(result['runs']['tiny_breadth_r1']['statuses'], {'passed': 393, 'harness_error': 2})
        self.assertFalse(result['runs']['tiny_runner_r1']['completed'])
        self.assertEqual(result['runs']['empty_sort_correction_r1']['statuses'], {'passed': 20})
        self.assertEqual(result['executed_queries'], 0)

    def test_complete_results_and_original_failure_are_retained_without_execution(self):
        records, manifest = fixture()
        result = verify_runs(freeze(records), manifest)
        self.assertEqual((result['checked_complete_results'], result['checked_receipts']), (2, 3))
        self.assertEqual(result['runs']['failure']['statuses'], {'engine_error': 1})
        self.assertEqual(result['executed_queries'], 0)
        self.assertEqual(result['proof_gaps'], [])

    def test_rehashed_semantic_and_receipt_corruption_is_rejected(self):
        changes = {
            'value': lambda r, m: r['native/case.stdout.json']['actual']['rows'][0].update(n=4),
            'schema': lambda r, m: r['native/case.stdout.json']['actual']['schema'][0]['type'].update(nullable=True),
            'fallback': lambda r, m: r['native/case.stdout.json']['observation']['fields'].update(fallback_attempted=True),
            'consumption': lambda r, m: r['native/case.stdout.json']['observation'].update(complete_consumption=False),
            'surface': lambda r, m: r['native/case.job.json'].update(surface='cli_sql'),
            'clock': lambda r, m: r['native/observations.jsonl'][0].update(engine_execution_seconds=2),
            'lost': lambda r, m: r['native/observations.jsonl'].clear(),
            'duplicate': lambda r, m: r['native/observations.jsonl'].append(deepcopy(r['native/observations.jsonl'][0])),
            'contract': lambda r, m: r['native/corpus.json'][0]['contract'].update(ordered=False),
            'erased_failure': lambda r, m: r['failure/summary.json']['families']['aggregate']['case_surface_outcomes'].update(engine_error=0),
            'failed_pass': lambda r, m: r['native/case.receipt.json'].update(returncode=1),
            'paired_clock': lambda r, m: r['polars/case.stdout.json']['observation']['clocks'].update(complete_workflow_seconds=0.5),
            'missing_pair': lambda r, m: m['baseline_pairs'].clear(),
            'unretained_source': lambda r, m: r['native/provenance.json']['python_sources'].append({'path': '/missing.py', 'sha256': '0' * 64}),
        }
        for name, change in changes.items():
            records, manifest = fixture()
            change(records, manifest)
            with self.subTest(change=name), self.assertRaises((ValueError, ResultMismatch)):
                verify_runs(freeze(records), manifest)

    def test_original_job_hash_and_missing_receipt_are_not_ignored(self):
        records, manifest = fixture()
        files = freeze(records)
        files['evidence/native/case.job.json'] += b' '
        with self.assertRaisesRegex(ValueError, 'receipt job changed'):
            verify_runs(files, manifest)
        files = freeze(records)
        del files['evidence/failure/case.receipt.json']
        with self.assertRaisesRegex(ValueError, 'missing evidence'):
            verify_runs(files, manifest)


def packet(*, member_name=None, symlink=False, duplicate=False, inventory_change=None):
    data = b'fixture evidence'
    member = member_name or 'objects/sha256/' + sha(data)
    inventory = [{'path': 'evidence/fixture', 'member': member, 'sha256': sha(data), 'bytes': len(data)}]
    if inventory_change == 'path':
        inventory[0]['path'] = '../escape'
    elif inventory_change == 'missing':
        inventory[0]['sha256'] = '0' * 64
    elif inventory_change == 'size':
        inventory[0]['bytes'] += 1
    index = encoded(inventory)
    index_member = 'objects/sha256/' + sha(index)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w') as archive:
        for _ in range(2 if duplicate else 1):
            info = tarfile.TarInfo(member)
            info.size = len(data)
            if symlink:
                info.type, info.linkname, info.size = tarfile.SYMTYPE, '/tmp/unsafe', 0
            archive.addfile(info, None if symlink else io.BytesIO(data))
        info = tarfile.TarInfo(index_member)
        info.size = len(index)
        archive.addfile(info, io.BytesIO(index))
    packed = lzma.compress(buffer.getvalue())
    manifest = {'schema_version': 'shardloom.broad_query.packet.v1',
        'archive': {'path': 'packet.tar.xz', 'bytes': len(packed), 'sha256': sha(packed)},
        'file_inventory': {'member': index_member, 'sha256': sha(index), 'bytes': len(index)},
        'expanded_file_bytes': len(data), 'file_count': 1, 'unique_objects': 2}
    return manifest, packed


class ArchiveTests(unittest.TestCase):
    def test_packet_input_is_bounded_before_archive_decoding(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'packet'
            path.write_bytes(b'12345')
            self.assertEqual(bounded_bytes(path, 5), b'12345')
            with self.assertRaisesRegex(ValueError, 'exceeds bound'):
                bounded_bytes(path, 4)
            with self.assertRaisesRegex(ValueError, 'regular file'):
                bounded_bytes(Path(directory), 5)

    def test_regular_content_addressed_archive_and_corruption_controls(self):
        manifest, packed = packet()
        self.assertEqual(object_files(manifest, packed), {'evidence/fixture': b'fixture evidence'})
        for arguments in ({'member_name': '../escape'}, {'symlink': True}, {'duplicate': True},
                          {'member_name': 'objects/sha256/' + '0' * 64}):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                object_files(*packet(**arguments))
        for mutation in ('path', 'missing', 'size', 'archive'):
            manifest, packed = packet(inventory_change=mutation)
            if mutation == 'archive':
                packed += b'corrupted'
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                object_files(manifest, packed)
        manifest, packed = packet()
        manifest['file_inventory']['bytes'] += 1
        with self.assertRaisesRegex(ValueError, 'inventory size'):
            object_files(manifest, packed)


if __name__ == '__main__':
    unittest.main()
