"""Adversarial checks of retained values, provenance and complete-run claims."""
import copy
import importlib.util
import json
import lzma
import os
from pathlib import Path
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('correction', HERE / 'verify_correctness_packet_950.py')
correction = importlib.util.module_from_spec(spec)
spec.loader.exec_module(correction)
ROOT = Path(os.environ.get('SHARDLOOM_CORRECTNESS_PACKET', str(HERE)))
REPO = Path(os.environ.get('SHARDLOOM_CORRECTNESS_REPOSITORY', str(HERE.parents[3])))


def set_field(key, value):
    return lambda record: record.__setitem__(key, value)


class CorrectnessPacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.review = correction.strict_json((ROOT / 'review.json').read_bytes())
        cls.packet = correction.strict_json(lzma.decompress((ROOT / 'records.json.xz').read_bytes()))

    def verify(self, review=None, packet=None):
        return correction.verify(self.review if review is None else review,
                                 self.packet if packet is None else packet, REPO)

    def reject_read_changes(self, changes, pattern, review=None):
        # Override decoded records after container validation. This isolates
        # semantic checks from the separate byte-integrity tests below.
        original = correction.Evidence.read

        def changed_read(evidence, reference):
            result = original(evidence, reference)
            action = changes.get(reference['path'])
            if action:
                result = copy.deepcopy(result)
                action(result)
            return result

        with patch.object(correction.Evidence, 'read', changed_read):
            with self.assertRaisesRegex(ValueError, pattern):
                self.verify(review=review)

    def test_all_complete_rows_and_both_oracle_versions_replay(self):
        result = self.verify()
        self.assertEqual(result['complete_native_passes'], 3)
        self.assertEqual(result['current_engine_execution_seconds'], 49.953164785)

    def test_duplicate_json_keys_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'duplicate JSON key'):
            correction.strict_json('{"passed":true,"passed":false}')

    def test_nonfinite_json_numbers_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'nonfinite JSON'):
            correction.strict_json('{"seconds":1e999}')

    def test_changed_member_bytes_are_rejected(self):
        packet = copy.deepcopy(self.packet)
        member = next(row for row in packet['files'] if row['encoding'] == 'utf-8')
        member['data'] += ' '
        with self.assertRaisesRegex(ValueError, 'member bytes changed'):
            self.verify(packet=packet)

    def test_duplicate_member_identity_is_rejected(self):
        packet = copy.deepcopy(self.packet)
        packet['files'].append(packet['files'][0])
        with self.assertRaisesRegex(ValueError, 'duplicate evidence identity'):
            self.verify(packet=packet)

    def test_current_program_cannot_replace_the_old_oracle_program(self):
        packet = copy.deepcopy(self.packet)
        evidence = correction.Evidence(packet)
        protocol = evidence.read(self.review['oracles'][0]['protocol'])
        old_hash = '1f726a0b1a01c261ea5d6e85dc22b1fe4162a59e00da4c5396a35058fd3903e7'
        programs = [program for program in protocol['programs'] if program['sha256'] == old_hash]
        self.assertEqual(len(programs), 1)
        # Member paths identify the producing checkout, not the replay checkout.
        path = programs[0]['path']
        versions = [row for row in packet['files'] if row['path'] == path]
        self.assertEqual(len(versions), 2)
        old = next(row for row in versions if row['sha256'] == old_hash)
        packet['files'].remove(old)
        with self.assertRaisesRegex(ValueError, 'missing exact evidence version'):
            self.verify(packet=packet)

    def test_unearned_competitive_claim_is_rejected(self):
        review = copy.deepcopy(self.review)
        review['claims']['performance_superiority'] = True
        with self.assertRaisesRegex(ValueError, 'review claim scope'):
            self.verify(review=review)

    def test_oracle_scope_independence_and_completion_corruptions(self):
        oracle = self.review['oracles'][1]
        cases = [
            (oracle['protocol'], set_field('executions_per_query', 2), 'oracle execution scope'),
            (oracle['protocol'], lambda d: d['independence'].__setitem__('native_values_input', True), 'oracle independence'),
            (oracle['acceptance'], set_field('calls', 42), 'oracle completion proof'),
            (oracle['acceptance'], set_field('program_and_package_unchanged', False), 'oracle completion proof'),
            (oracle['reference'], set_field('reference_kind', 'retained_native_regression'), 'reference kind'),
            (oracle['receipts'][0], set_field('process_group_drained', False), 'oracle process'),
            (oracle['receipts'][0], set_field('outputs', []), 'oracle raw output/cleanup'),
            (oracle['outputs'][3], lambda d: d['values'][0].__setitem__('avg_UserID_0', 0.0), 'oracle frozen values'),
            (oracle['protocol'], lambda d: d['plans'][3].__setitem__('equivalent_sql', 'SELECT 0'), 'oracle SQL binding'),
        ]
        for ref, action, pattern in cases:
            with self.subTest(pattern=pattern):
                self.reject_read_changes({ref['path']: action}, pattern)

    def test_ingestion_lineage_cannot_be_swapped(self):
        lineage = self.review['input_lineage']
        self.reject_read_changes({lineage['acceptance']['path']:
            lambda d: d['datasets'][0].__setitem__('sha256', '0' * 64)}, 'oracle is not the ingested source')

    def test_exact_average_adjudication_cannot_change_its_count(self):
        evidence = correction.Evidence(self.packet)
        record = evidence.read(self.review['adjudication'])
        self.reject_read_changes({record['output']['path']:
            lambda d: d['values'].__setitem__('count', d['values']['count'] - 1)}, 'exact integer adjudication')

    def test_runtime_build_cannot_use_changed_sources(self):
        evidence = correction.Evidence(self.packet)
        runtime = evidence.read(self.review['runtimes'][-1]['runtime'])
        self.reject_read_changes({runtime['build']['path']:
            lambda d: d['source_after'].__setitem__('Cargo.toml', '0' * 64)}, 'build source changed')

    def test_native_execution_clock_and_scope_corruptions(self):
        run = self.review['native_runs'][-1]
        cases = [
            (run['summary'], set_field('repeats', 2), 'native selected scope'),
            (run['summary'], set_field('max_parallelism', 11), 'native resources'),
            (run['summary'], lambda d: d['records'][1].__setitem__('query', 1), 'native query executions'),
            (run['summary'], lambda d: d['records'][1].__setitem__('returncode', 1), 'native query execution'),
            (run['summary'], lambda d: d['records'][1].__setitem__('engine_timing_reported', False), 'native engine clock missing'),
            (run['receipt'], set_field('post_input_hash_verified', False), 'native completion proof'),
            (run['outputs'][0], lambda d: d['fallback'].__setitem__('attempted', True), 'unsafe envelope fallback evidence'),
        ]
        for ref, action, pattern in cases:
            with self.subTest(pattern=pattern):
                self.reject_read_changes({ref['path']: action}, pattern)

    def test_coordinated_total_edits_still_recompute_each_engine_sample(self):
        review = copy.deepcopy(self.review)
        run = review['native_runs'][-1]
        run['engine_execution_seconds'] += 1.0
        self.reject_read_changes({
            run['receipt']['path']: lambda d: d.__setitem__('engine_execution_seconds', d['engine_execution_seconds'] + 1.0),
            run['summary']['path']: lambda d: d['timing_totals']['totals'].__setitem__(
                'engine_execution_seconds', d['timing_totals']['totals']['engine_execution_seconds'] + 1.0),
        }, 'native engine total differs', review=review)

    def test_complete_native_value_changes_are_rejected(self):
        def corrupt_rows(envelope):
            changed = 0
            for field in envelope['fields']:
                key, value = field['key'], field['value']
                if key in ('result_jsonl', 'result_values_json'):
                    rows = ([json.loads(line) for line in value.splitlines()] if key == 'result_jsonl'
                            else json.loads(value))
                    rows[0]['count_all'] += 1
                    field['value'] = ('\n'.join(json.dumps(row) for row in rows) if key == 'result_jsonl'
                                      else json.dumps(rows))
                    changed += 1
            self.assertEqual(changed, 1, 'corruption must alter the consumed complete payload')

        ref = self.review['native_runs'][-1]['outputs'][0]
        self.reject_read_changes({ref['path']: corrupt_rows}, 'native result disposition differs')

    def test_correct_values_do_not_hide_the_wrong_q29_execution_path(self):
        def corrupt_strategy(envelope):
            fields = [field for field in envelope['fields'] if field['key'] ==
                      'local_primitive_aggregate_update_strategy']
            self.assertEqual(len(fields), 1)
            fields[0]['value'] = 'direct_accessor_general_group_update'

        ref = self.review['native_runs'][-1]['outputs'][28]
        self.reject_read_changes({ref['path']: corrupt_strategy}, 'Q29 corrected dictionary path differs')

    def test_stopped_pass_cannot_acquire_a_full43_total(self):
        review = copy.deepcopy(self.review)
        review['native_runs'][0]['engine_execution_seconds'] = 0.5
        with self.assertRaisesRegex(ValueError, 'incomplete run treated as a Full43 result'):
            self.verify(review=review)

    def test_missing_required_validation_is_rejected(self):
        review = copy.deepcopy(self.review)
        review['validations'].pop()
        with self.assertRaisesRegex(ValueError, 'required validation coverage'):
            self.verify(review=review)

    def test_missing_changed_runtime_source_is_rejected(self):
        review = copy.deepcopy(self.review)
        review['current_sources'].pop()
        with self.assertRaisesRegex(ValueError, 'changed runtime source coverage'):
            self.verify(review=review)


if __name__ == '__main__':
    unittest.main(verbosity=2)
