"""Check that coherent evidence corruption cannot narrow the current-PR proof."""
import argparse
import copy
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path


def identity(path):
    raw = Path(path).read_bytes()
    return dict(path=str(path), bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def replace_member(packet, path, raw):
    """Repair every reference after mutation, reaching semantic validation."""
    pending = [(path, raw)]
    while pending:
        name, data = pending.pop()
        packet['members'][name] = dict(text=data.decode(), bytes=len(data),
                                      sha256=hashlib.sha256(data).hexdigest())
        new = dict(path=name, bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

        def repair(value):
            changed = False
            if isinstance(value, dict):
                if {'path', 'bytes', 'sha256'} <= value.keys() and value['path'] == name:
                    changed = any(value[key] != new[key] for key in new)
                    value.update(new)
                for child in value.values():
                    changed = repair(child) or changed
            elif isinstance(value, list):
                for child in value:
                    changed = repair(child) or changed
            return changed

        for key, value in packet.items():
            if key != 'members':
                repair(value)
        for other, member in packet['members'].items():
            if other.endswith('.json') and other != name:
                value = json.loads(member['text'])
                if repair(value):
                    pending.append((other, encoded(value)))


def read(packet, ref):
    return json.loads(packet['members'][ref['path']]['text'])


def change_json(packet, ref, action):
    value = read(packet, ref)
    action(value)
    replace_member(packet, ref['path'], encoded(value))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--verifier', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_bytes())
    payload_path = args.manifest.parent / manifest['packet']['file']
    payload = payload_path.read_bytes()
    assert len(payload) == manifest['packet']['bytes']
    assert hashlib.sha256(payload).hexdigest() == manifest['packet']['sha256']
    verifier = identity(args.verifier)
    assert all(verifier[key] == manifest['verifier'][key] for key in ('bytes', 'sha256'))
    spec = importlib.util.spec_from_file_location('pr_packet', args.verifier)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    packet = json.loads(lzma.decompress(payload))
    positive = module.verify(packet, args.repository)
    assert positive == manifest['replay']
    cases = []

    def check(name, mutate, reason):
        candidate = copy.deepcopy(packet)
        mutate(candidate)
        try:
            module.verify(candidate, args.repository)
        except ValueError as error:
            assert reason in str(error), (name, str(error), reason)
            cases.append(dict(name=name, rejected=True, reason=str(error),
                              member_reference_hashes_repaired=True))
        else:
            raise AssertionError('corruption accepted: ' + name)

    def phase(packet, name):
        return read(packet, packet['phases'][name])

    def receipt(packet, name):
        return phase(packet, name)['run']['receipt']

    check('required phase removed', lambda p: p['phases'].pop('minimal-feature'),
          'required phase omitted')
    check('historical timing relabelled', lambda p: change_json(p, p['source'],
          lambda v: v.update(comparison_source_relabelled=True)), 'historical measurement scope changed')
    check('source file omitted', lambda p: change_json(p, p['source'],
          lambda v: v['source_hashes'].pop(next(iter(v['source_hashes'])))), 'source closure incomplete')
    check('native test count narrowed', lambda p: change_json(p, p['phases']['native-tests'],
          lambda v: v.update(passed=3106)), 'test coverage changed: native-tests')
    check('native command narrowed', lambda p: change_json(p, receipt(p, 'native-tests'),
          lambda v: v['command'].insert(-2, 'one_selected_test')), 'validation command narrowed: native-tests')
    check('worker drain unproved', lambda p: change_json(p, receipt(p, 'native-tests'),
          lambda v: v.update(process_group_drained=False)), 'guarded check did not finish cleanly')
    check('optional test family omitted', lambda p: change_json(p, p['phases']['optional-tests'],
          lambda v: v['checks'].pop('credit_window::tests')), 'optional check omitted')

    def wheel_report(p):
        return phase(p, 'wheel')['report']

    check('installed package test skipped', lambda p: change_json(p, wheel_report(p),
          lambda v: v['integration'].update(skipped=1)), 'installed wheel proof incomplete')
    check('installed library identity changed', lambda p: change_json(p, wheel_report(p),
          lambda v: v['integrity'].update(native_library_sha256='0' * 64)),
          'installed artifact differs from fresh build')

    def overlay(p):
        def mutate(value):
            step = next(row for row in value['steps'] if row['name'] == 'integration')
            step['command'][4] = '/tmp/replacement-tests'
        change_json(p, wheel_report(p), mutate)
    check('installed tests use an overlay', overlay, 'installed test runner used an overlay')

    def missing_installed_test(p):
        report_ref = wheel_report(p)
        report = read(p, report_ref)
        step = next(row for row in report['steps'] if row['name'] == 'integration')
        name = str(Path(report_ref['path']).parent / step['log'])
        lines = p['members'][name]['text'].splitlines(keepends=True)
        index = next(i for i, line in enumerate(lines) if line.startswith('test_') and line.endswith(' ... ok\n'))
        del lines[index]
        raw = ''.join(lines).encode()
        replace_member(p, name, raw)
        step['log_sha256'] = hashlib.sha256(raw).hexdigest()
        replace_member(p, report_ref['path'], encoded(report))
    check('installed test omitted despite passing summary', missing_installed_test,
          'normal installed test discovery narrowed')

    assert module.verify(packet, args.repository) == positive
    result = dict(schema='shardloom.prepared_sampling_pr_corruption_checks.v1', status='passed',
        checker=identity(__file__), verifier=verifier, manifest=identity(args.manifest),
        payload=identity(payload_path), cases=cases, rejected_cases=len(cases),
        unchanged_positive_replayed=True, native_execution_performed=False)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2, sort_keys=True)
        stream.write('\n')
    print(json.dumps(dict(status='passed', rejected_cases=len(cases),
                          unchanged_positive_replayed=True)))


if __name__ == '__main__':
    main()
