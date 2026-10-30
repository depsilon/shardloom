# SPDX-License-Identifier: Apache-2.0
"""Bounded transport and semantic tamper controls; no engine execution."""
import copy
import gzip
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'python/src'),
               str(ROOT / 'benchmarks/traditional_analytics')]
import shared_performance_bundle as transport
from shared_performance_replay import complete_gate, frozen_files, full43, public_comparisons, test_totals, tested_source
from query_surface.replay import MAX_FILE, sha
from query_surface.semantics import ResultMismatch


class BundleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.template = Path(cls.directory.name) / 'template'
        # Two full chunks exercise order and part boundaries under the existing
        # per-member ceiling; high compression keeps test artifacts small.
        cls.payload = b'a' * MAX_FILE + b'b' * MAX_FILE
        cls.original, cls.revised = sha(cls.payload), sha(b'new source')
        with patch.object(transport, 'PART_TARGET', MAX_FILE):
            transport.write_bundle(cls.template,
                {cls.original: cls.payload, cls.revised: b'new source', sha(b''): b''},
                {'/original/source': [cls.original, cls.revised], '/original/empty': [sha(b'')]},
                {'purpose': 'synthetic transport control'})

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory()
        self.folder = Path(self.workspace.name) / 'packet'
        shutil.copytree(self.template, self.folder)

    def tearDown(self):
        self.workspace.cleanup()

    def mutate(self, update):
        path = self.folder / 'manifest.json'
        value = json.loads(path.read_text())
        update(value)
        path.write_bytes(transport.json_bytes(value))

    def reject(self):
        with self.assertRaises((ValueError, KeyError, TypeError)):
            transport.Bundle(self.folder)

    def test_roundtrip_versioned_paths_empty_records_and_bounded_parts(self):
        value = transport.Bundle(self.folder)
        self.assertEqual(value.raw('/original/source', self.original), self.payload)
        self.assertEqual(value.raw('/original/source', self.revised), b'new source')
        self.assertEqual(value.raw('/original/empty'), b'')
        self.assertLessEqual(len(value._cache), 2)
        self.assertLessEqual(value._record_cache_bytes, transport.MAX_RECORD_CACHE)
        with self.assertRaisesRegex(ValueError, 'explicit digest'):
            value.raw('/original/source')
        with self.assertRaisesRegex(ValueError, 'unrecorded path version'):
            value.raw('/original/source', sha(b'unknown'))

    def test_reordered_same_size_chunks_are_rejected(self):
        self.mutate(lambda value: value['records'][self.original]['chunks'].reverse())
        self.reject()

    def test_small_repeated_record_does_not_reopen_evicted_part(self):
        value = transport.Bundle(self.folder)
        self.assertEqual(value.raw('/original/empty'), b'')
        value._cache.clear()
        with patch.object(value, '_part', side_effect=AssertionError('unnecessary archive decode')):
            self.assertEqual(value.raw('/original/empty'), b'')

    def test_duplicate_chunks_cannot_replace_a_record(self):
        def change(value):
            chunks = value['records'][self.original]['chunks']
            chunks[1] = copy.deepcopy(chunks[0])
        self.mutate(change)
        self.reject()

    def test_oversized_record_is_rejected_before_reconstruction(self):
        self.mutate(lambda value: value['records'][self.original].update(bytes=transport.MAX_RECORD + 1))
        self.reject()

    def test_aggregate_reconstruction_work_is_bounded(self):
        with patch.object(transport, 'MAX_TOTAL_RECORD', MAX_FILE):
            self.reject()

    def test_chunk_size_and_name_must_match(self):
        self.mutate(lambda value: value['records'][self.original]['chunks'][0].update(name='../escape'))
        self.reject()

    def test_extra_top_level_fields_are_rejected(self):
        self.mutate(lambda value: value.update(skip_validation=True))
        self.reject()

    def test_unreferenced_record_is_rejected(self):
        self.mutate(lambda value: value['path_map'].pop('/original/empty'))
        self.reject()

    def test_duplicate_path_versions_are_rejected(self):
        self.mutate(lambda value: value['path_map']['/original/source'].append(self.original))
        self.reject()

    def test_unsafe_original_path_is_rejected(self):
        self.mutate(lambda value: value['path_map'].update({'/original/../escape': [self.original]}))
        self.reject()

    def test_extra_bundle_entry_is_rejected(self):
        (self.folder / 'unexpected').write_bytes(b'')
        self.reject()

    def test_missing_part_is_rejected(self):
        shutil.rmtree(self.folder / 'part-001')
        self.reject()

    def test_duplicate_or_unsafe_part_directory_is_rejected(self):
        self.mutate(lambda value: value['parts'][1].update(directory='part-000'))
        self.reject()

    def test_symlinked_part_is_rejected(self):
        directory = self.folder / 'part-000'
        moved = Path(self.workspace.name) / 'moved'
        directory.rename(moved)
        directory.symlink_to(moved, target_is_directory=True)
        self.reject()

    def test_symlinked_manifest_is_rejected(self):
        path = self.folder / 'manifest.json'
        moved = Path(self.workspace.name) / 'manifest.json'
        path.rename(moved)
        path.symlink_to(moved)
        self.reject()

    def test_archive_corruption_is_rejected(self):
        path = self.folder / 'part-000/packet.tar.xz'
        value = bytearray(path.read_bytes())
        value[len(value) // 2] ^= 1
        path.write_bytes(value)
        self.reject()

    def test_rehashed_extra_archive_object_is_rejected(self):
        value = transport.Bundle(self.folder)
        files = dict(value._part(0))
        files['chunks/' + sha(b'undeclared')] = b'undeclared'
        header, archive = transport._archive(files)
        (self.folder / 'part-000/manifest.json').write_bytes(header)
        (self.folder / 'part-000/packet.tar.xz').write_bytes(archive)
        self.mutate(lambda index: index['parts'][0].update(manifest_bytes=len(header), manifest_sha256=sha(header)))
        self.reject()


def public_fixture():
    schema = [{'name': 'value', 'type': {'name': 'int64', 'nullable': False, 'parameters': []}}]
    actual = dict(schema=schema, rows=[dict(value=7)])
    comparisons = []
    for form in ('vortex', 'parquet', 'arrow'):
        for surface in ('cli_sql', 'embedded_workflow', 'python_sql', 'dataframe', 'typed_relation'):
            comparisons.append(dict(format=form, surface=surface, case='synthetic', method='control',
                rows=1, actual=copy.deepcopy(actual), expected=copy.deepcopy(actual),
                contract=dict(schema=schema, ordered=True, floats={}, max_matching_edges=100),
                report=dict(surface=surface, complete_consumption=True,
                    fields=dict(fallback_attempted='false', external_engine_invoked='false'))))
    return dict(status='passed', tests=1, failures=0, errors=0, skipped=0, posthash_verified=True,
        comparison_engine_invoked=False, complete_comparisons=comparisons,
        positive_public_executions=15, compared_rows=15)


class SemanticTests(unittest.TestCase):
    def test_required_nonpayload_evidence_cannot_be_excluded_as_external(self):
        item = dict(path='/original/protocol.json', bytes=2, sha256=sha(b'{}'))
        with self.assertRaisesRegex(ValueError, 'cannot be external'):
            frozen_files(None, [item], {(item['path'], item['sha256'], item['bytes'])})

    def test_public_complete_values_across_interfaces_and_formats(self):
        self.assertEqual(public_comparisons(public_fixture()),
                         dict(status='passed', accepted_executions=15, complete_rows=15))

    def test_changed_complete_value_is_rejected(self):
        value = public_fixture()
        value['complete_comparisons'][0]['actual']['rows'][0]['value'] = 8
        with self.assertRaises(ResultMismatch):
            public_comparisons(value)

    def test_public_false_completion_and_external_execution_are_rejected(self):
        for mutation in ('consumption', 'fallback', 'external', 'skip', 'posthash', 'count', 'rows'):
            with self.subTest(mutation=mutation):
                value = public_fixture()
                first = value['complete_comparisons'][0]
                if mutation == 'consumption':
                    first['report']['complete_consumption'] = False
                elif mutation in ('fallback', 'external'):
                    key = 'fallback_attempted' if mutation == 'fallback' else 'external_engine_invoked'
                    first['report']['fields'][key] = 'true'
                elif mutation == 'skip':
                    value['skipped'] = 1
                elif mutation == 'posthash':
                    value['posthash_verified'] = False
                elif mutation == 'count':
                    value['positive_public_executions'] += 1
                else:
                    first['rows'] += 1
                with self.assertRaises(ValueError):
                    public_comparisons(value)

    def test_duplicate_interface_or_format_cannot_replace_coverage(self):
        for key in ('surface', 'format'):
            with self.subTest(key=key):
                value = public_fixture()
                for item in value['complete_comparisons']:
                    item[key] = value['complete_comparisons'][0][key]
                    item['report']['surface'] = item['surface']
                with self.assertRaises(ValueError):
                    public_comparisons(value)

    def test_duplicate_case_cannot_replace_another_comparison(self):
        value = public_fixture()
        value['complete_comparisons'][1] = copy.deepcopy(value['complete_comparisons'][0])
        with self.assertRaisesRegex(ValueError, 'duplicate public comparison'):
            public_comparisons(value)

    def test_failed_public_attempt_is_preserved_without_acceptance_credit(self):
        value = public_fixture()
        value.update(status='failed', failures=1)
        self.assertEqual(public_comparisons(value),
                         dict(status='failed', accepted_executions=0, completed_comparisons_retained=15))

    def test_zero_test_count_does_not_pass(self):
        value = public_fixture()
        value['tests'] = 0
        with self.assertRaises(ValueError):
            public_comparisons(value)

    def test_workspace_counts_and_failure_status_are_derived_from_logs(self):
        log = 'test result: ok. 2 passed; 0 failed; 1 ignored;\n'
        self.assertEqual(test_totals(log, (1, 2, 0, 1)),
                         dict(binaries=1, passed=2, failed=0, ignored=1))
        for changed in ('', log.replace('2 passed', '0 passed'), log.replace('ok.', 'FAILED.')):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                test_totals(changed, (1, 2, 0, 1))

    def test_exit_code_alone_does_not_establish_process_completion(self):
        gate = dict(status='passed', returncode=0, process_group_drained=True,
                    direct_child_reaped=True, timed_out=False, interrupted_signal=None)
        complete_gate(gate)
        for key, value in [('process_group_drained', False), ('direct_child_reaped', False),
                           ('timed_out', True), ('interrupted_signal', 2)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                complete_gate(gate | {key: value})

    def test_only_the_recorded_lint_format_change_is_admitted(self):
        before = (b'        if !declaration.usage.admits_ranked()\n'
                  b'            || (plan.filter.is_some() && predicate.is_none())\n        {\n')
        after = b'        if !declaration.usage.admits_ranked() || (plan.filter.is_some() && predicate.is_none()) {\n'
        data = {sha(before): before, sha(after): after}
        class Sources:
            def identity(self, item):
                return data[item['sha256']]
        source, built = dict(bytes=len(before), sha256=sha(before)), dict(bytes=len(after), sha256=sha(after))
        args = ('p122-review-native-clippy.json', {'cwd': '/repo'},
                'shardloom-vortex/src/local_primitive_relational_ranked.rs', source, built)
        tested_source(Sources(), *args)
        data[built['sha256']] = after.replace(b'8192', b'8193') + b'new behavior'
        with self.assertRaises(ValueError):
            tested_source(Sources(), *args)
        with self.assertRaises(ValueError):
            tested_source(Sources(), 'p123-review-native-workspace.json', *args[1:])


class RehashedFull43Tests(unittest.TestCase):
    """Repair all changed transport links, then require semantic rejection."""
    @classmethod
    def setUpClass(cls):
        cls.bundle = transport.Bundle(ROOT / 'docs/benchmarks/evidence/native-shared-performance-2026-10-30')
        cls.anchors = json.loads((ROOT / 'scripts/shared_performance_anchors.json').read_text())
        cls.root = cls.anchors['root']
        cls.runtime = json.loads(cls.bundle.raw(cls.root + '/review-integration-release-r1/manifest.json'))
        cls.external = {(item['path'], item['sha256'], item['bytes'])
                        for item in cls.bundle.index['evidence']['external_artifacts']}

    def setUp(self):
        self.overrides = {}
        self.accepted_path = self.root + '/full43-review-r1-accepted.json'
        self.accepted = json.loads(self.bundle.raw(self.accepted_path))
        self.driver_path = self.accepted['driver']['path']
        self.summary_path = self.accepted['summary']['path']
        self.driver = json.loads(self.bundle.raw(self.driver_path))
        self.summary = json.loads(self.bundle.raw(self.summary_path))

    def put(self, path, value):
        data = value if isinstance(value, bytes) else transport.json_bytes(value)
        self.overrides[path] = data
        return dict(path=path, bytes=len(data), sha256=sha(data))

    def replay(self):
        summary = self.put(self.summary_path, self.summary)
        self.driver['summaries'] = [summary]
        self.accepted['summary'] = summary
        self.accepted['driver'] = self.put(self.driver_path, self.driver)
        self.put(self.accepted_path, self.accepted)
        outer = self
        class Overlay:
            identity = transport.Bundle.identity

            def raw(self, path, digest=None):
                data = outer.overrides.get(path)
                if data is None:
                    return outer.bundle.raw(path, digest)
                if digest is not None and sha(data) != digest:
                    raise ValueError('test did not repair a changed transport digest')
                return data
        return full43(Overlay(), self.root, self.anchors, self.runtime, self.external)

    def test_rehashed_unchanged_evidence_remains_valid(self):
        self.assertEqual(self.replay()['engine_seconds'], 624.450913663)

    def test_rehashed_incomplete_cohort_is_rejected(self):
        self.summary['records'].pop()
        with self.assertRaisesRegex(ValueError, 'reference changed'):
            self.replay()

    def test_rehashed_duplicate_query_is_rejected(self):
        self.summary['records'][1]['query'] = 1
        with self.assertRaisesRegex(ValueError, 'missing query'):
            self.replay()

    def test_rehashed_clock_without_engine_evidence_is_rejected(self):
        self.summary['records'][0]['engine_execution_seconds'] += 1
        self.accepted['rows'][0]['engine_execution_seconds'] += 1
        with self.assertRaisesRegex(ValueError, 'clock changed'):
            self.replay()

    def test_rehashed_input_format_command_change_is_rejected(self):
        command = self.summary['records'][0]['command']
        command[command.index('--input-format') + 1] = 'arrow'
        with self.assertRaisesRegex(ValueError, 'execution command changed'):
            self.replay()

    def test_rehashed_summary_cannot_erase_ranked_activation(self):
        self.summary['records'][23]['execution_report']['native_source_ranked_sources'] = '0'
        with self.assertRaisesRegex(ValueError, 'summary differs'):
            self.replay()

    def test_rehashed_incorrect_complete_result_is_rejected(self):
        record, row = self.summary['records'][0], self.accepted['rows'][0]
        archive = record['completed_log_archives'][0]
        output = json.loads(gzip.decompress(self.bundle.raw(archive['path'])))
        for item in output['fields']:
            if item['key'] == 'result_jsonl':
                item['value'] = '{"count_all":99997498}\n'
        raw = transport.json_bytes(output)
        packed = gzip.compress(raw, mtime=0)
        row['archive'] = self.put(archive['path'], packed)
        row['raw_output_sha256'] = archive['raw_sha256'] = sha(raw)
        archive['raw_bytes'], archive['gzip_bytes'], archive['gzip_sha256'] = len(raw), len(packed), sha(packed)
        record['result_sha256'] = sha(json.dumps([dict(count_all=99997498)], sort_keys=True).encode())
        with self.assertRaisesRegex(ValueError, 'complete values differ'):
            self.replay()


if __name__ == '__main__':
    unittest.main()
