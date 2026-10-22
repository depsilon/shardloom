"""Reject altered packet identities and coherently rebound invalid reports without engine execution."""
import argparse
import copy
import hashlib
import json
import lzma
from pathlib import Path
import sys

from verify_packet_929 import Evidence, check_environment, replay, strict_json

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--repository', type=Path, required=True)
parser.add_argument('--manifest', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
original_manifest = strict_json(args.manifest.read_bytes())
archive = args.manifest.parent / Path(original_manifest['archive']['path']).name
raw = archive.read_bytes()
assert len(raw) == original_manifest['archive']['bytes']
assert hashlib.sha256(raw).hexdigest() == original_manifest['archive']['sha256']
original_packet = strict_json(lzma.decompress(raw))
positive = replay(args.repository, original_manifest, original_packet)
outcomes = []

def check(name, expected, mutate):
    packet, manifest = copy.deepcopy(original_packet), copy.deepcopy(original_manifest)
    members = {row['path']: row for row in packet['members']}
    def read(path):
        return strict_json(members[path]['data'])
    def put(path, document):
        data = json.dumps(document, sort_keys=True, allow_nan=False)
        encoded = data.encode('utf-8')
        row = members[path]
        row.update(data=data, bytes=len(encoded), sha256=hashlib.sha256(encoded).hexdigest())
        return {key: row[key] for key in ('path', 'bytes', 'sha256')}
    def accepted_change(update):
        path = manifest['acceptance']['path']
        document = read(path)
        update(document)
        manifest['acceptance'] = put(path, document)
    def report_change(ordinal, update):
        accepted = read(manifest['acceptance']['path'])
        report_path = accepted['cohorts'][ordinal - 1]['report']['path']
        report = read(report_path)
        update(report)
        report_ref = put(report_path, report)
        accepted['cohorts'][ordinal - 1]['report'] = report_ref
        matrix_path = accepted['original_failed_matrix']['path']
        matrix = read(matrix_path)
        continued = read(manifest['continuation']['path'])
        if ordinal <= 17:
            matrix['cohorts'][ordinal - 1]['report_sha256'] = report_ref['sha256']
        else:
            continued['report_sha256'] = report_ref['sha256']
        matrix_ref = put(matrix_path, matrix)
        plan_path = accepted['continuation_protocol']['path']
        plan = read(plan_path)
        plan['original_failed_matrix'] = matrix_ref
        plan_ref = put(plan_path, plan)
        continued['protocol'] = plan_ref
        continuation_ref = put(manifest['continuation']['path'], continued)
        accepted.update(original_failed_matrix=matrix_ref, continuation_protocol=plan_ref,
            continuation_result=continuation_ref)
        manifest['acceptance'] = put(manifest['acceptance']['path'], accepted)
        manifest['continuation'] = continuation_ref
    mutate(packet, manifest, members, accepted_change, report_change)
    try:
        replay(args.repository, manifest, packet)
    except ValueError as error:
        message = str(error)
        if expected not in message:
            raise AssertionError(f'{name}: expected {expected!r}, received {message!r}') from error
        outcomes.append({'case': name, 'status': 'rejected', 'reason': message})
    else:
        raise AssertionError(name + ': invalid evidence was accepted')

def native(report):
    return next(row for row in report['records'] if row['engine'] == 'shardloom')

def change_call(report, option, value):
    row = native(report)
    calls = strict_json(row['evidence']['benchmark_native_calls'])
    calls[0]['command'][calls[0]['command'].index(option) + 1] = value
    row['evidence']['benchmark_native_calls'] = json.dumps(calls)

check('duplicate stored path', 'duplicate stored member',
    lambda p, m, files, a, r: p['members'].append(copy.deepcopy(p['members'][0])))
check('stored bytes changed', 'changed member:',
    lambda p, m, files, a, r: p['members'][0].update(data=p['members'][0]['data'] + ' '))
check('omitted executable identity changed', 'substituted reference:',
    lambda p, m, files, a, r: next(row for row in p['omissions'] if row['kind'] == 'published_executable').update(sha256='0' * 64))
check('performance claim injected', 'unearned acceptance claim',
    lambda p, m, files, a, r: a(lambda doc: doc['claims'].update(performance=True)))
check('incomplete records counted', 'failure or continuation erased',
    lambda p, m, files, a, r: a(lambda doc: doc.update(incomplete_cohort_records_counted_as_acceptance=1)))
check('interruption erased', 'failure or continuation erased',
    lambda p, m, files, a, r: a(lambda doc: doc.update(uninterrupted_single_run_claimed=True)))
check('missing result record', 'case coverage is missing, duplicated or undeclared',
    lambda p, m, files, a, r: r(1, lambda doc: doc['records'].pop()))
check('different baseline version', 'reference version/order differs',
    lambda p, m, files, a, r: r(1, lambda doc: doc['records'][0].update(engine_version='2.2.3')))
check('different declared memory', 'scale or resource settings differ',
    lambda p, m, files, a, r: r(1, lambda doc: doc['configuration'].update(memory_gb=2)))
check('different native memory request', 'native call resource request differs',
    lambda p, m, files, a, r: r(1, lambda doc: change_call(doc, '--memory-gb', '2')))
check('fallback admitted', 'native case lacks shared-engine and no-fallback evidence',
    lambda p, m, files, a, r: r(1, lambda doc: native(doc)['evidence'].update(public_workflow_fallback_attempted='true')))
check('different native SQL', 'native calls differ from the declared complete workload',
    lambda p, m, files, a, r: r(1, lambda doc: change_call(doc, '--sql', 'SELECT 1')))
check('writer readback missing', 'native writer lacks readback evidence',
    lambda p, m, files, a, r: r(3, lambda doc: native(doc)['evidence'].update(benchmark_output_validation_calls='[]')))
for name, text, expected in [('duplicate JSON key', '{"x":1,"x":2}', 'duplicate JSON key'),
        ('non-finite JSON number', '{"x":NaN}', 'non-finite JSON value')]:
    try:
        strict_json(text)
    except ValueError as error:
        assert expected in str(error)
        outcomes.append({'case': name, 'status': 'rejected', 'reason': str(error)})
    else:
        raise AssertionError(name + ': invalid JSON was accepted')

evidence = Evidence(original_packet)
baseline = evidence.read(original_manifest['protocol']['path'])['baseline']
preflight = evidence.read(baseline['environment_observation']['path'])
postflight = strict_json(Path(__file__).with_name('baseline-environment-postflight.json').read_bytes())
check_environment(baseline, preflight, postflight)

def environment_check(name, expected, mutate):
    before, after = copy.deepcopy(preflight), copy.deepcopy(postflight)
    mutate(before, after)
    try:
        check_environment(baseline, before, after)
    except ValueError as error:
        assert expected in str(error), (name, str(error))
        outcomes.append({'case': name, 'status': 'rejected', 'reason': str(error)})
    else:
        raise AssertionError(name + ': invalid baseline evidence was accepted')

for distribution in preflight['distributions']:
    environment_check('preflight omits ' + distribution, 'installed baseline distribution mapping differs',
        lambda before, after, name=distribution: before['distributions'].pop(name))
for distribution in baseline['distributions']:
    environment_check('postflight omits ' + distribution, 'complete baseline distribution mapping differs',
        lambda before, after, name=distribution: after['distributions'].pop(name))
environment_check('preflight distribution version changed', 'installed baseline distribution mapping differs',
    lambda before, after: before['distributions'].update(pandas='2.2.3'))
environment_check('postflight distribution version changed', 'complete baseline distribution mapping differs',
    lambda before, after: after['distributions'].update(pyarrow='0.0.0'))
environment_check('undeclared postflight distribution', 'complete baseline distribution mapping differs',
    lambda before, after: after['distributions'].update(undeclared='1.0.0'))
environment_check('source distribution origin erased', 'baseline distribution origin mapping differs',
    lambda before, after: after['source_tree_distributions'].clear())
environment_check('supplemental observation relabelled as original', 'baseline observation scope differs',
    lambda before, after: after.update(original_run_observation=True))
environment_check('baseline interpreter changed', 'baseline interpreter differs',
    lambda before, after: after.update(python='3.11.0'))
result = {'status': 'passed', 'positive_replay': positive, 'negative_cases': outcomes,
    'negative_cases_passed': len(outcomes), 'runtime_executed': False,
    'report_mutations_rebind_report_and_ancestor_identities': True,
    'environment_mutations_use_replays_environment_checker': True}
with args.output.open('x') as stream:
    json.dump(result, stream, indent=2)
    stream.write('\n')
print(json.dumps({'status': 'passed', 'negative_cases': len(outcomes)}))
