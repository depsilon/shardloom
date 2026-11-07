"""Replay the wheel-test fixture correction without rebuilding or running tests."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile


def identity(raw):
    return dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def verify(packet, repo_root=None):
    report = json.loads((packet / 'hosted-followup.json').read_bytes())
    assert report['status'] == 'local_fixture_correction_passed'
    assert report['original_head'] == '28098a758e81cdeedbb62cd384d4a1bb7e2f5deb'
    assert identity(Path(__file__).read_bytes()) == report['verifier']
    for name, expected in report['original_packet'].items():
        assert identity((packet / name).read_bytes()) == expected, name
    command = [sys.executable, '-B', str(packet / 'verify.py'), '--packet', str(packet)]
    if repo_root is not None:
        command += ['--repo-root', str(repo_root)]
    subprocess.run(command, check=True, timeout=180)
    raw = (packet / 'hosted-followup.tar.xz').read_bytes()
    assert identity(raw) == report['archive']
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:xz') as archive:
        assert archive.getnames() == [row['path'] for row in report['members']]
        values = {}
        for entry, expected in zip(archive.getmembers(), report['members']):
            assert entry.isfile() and entry.size == expected['bytes']
            data = archive.extractfile(entry).read()
            assert identity(data) == {key: expected[key] for key in ('bytes', 'sha256')}
            values[entry.name] = data
    assert set(values) == {'source-before.py', 'source-after.py', 'seal_wheel_harness_followup.py',
        'pr1661-job-128370598824-r1.log', 'pr1661-job-128370598794-r1.log',
        'wheel-harness-regression-r1.json', 'wheel-harness-regression-r1.log',
        'wheel-harness-regression-r2.json', 'wheel-harness-regression-r2.log',
        'tensor-pr1661-r1.json'}
    prior = json.loads(values['tensor-pr1661-r1.json'])
    assert prior['status'] == 'needs_attention' and prior['head'] == report['original_head']
    assert {row['id'] for row in prior['failures']} == {128370598824, 128370598794}
    for job in (128370598824, 128370598794):
        log = values['pr1661-job-' + str(job) + '-r1.log']
        assert b'RuntimeError: required native integration suite is absent: test_public_context.py' in log
        assert b'Ran 19 tests' in log and b'FAILED (failures=1)' in log
        assert report['original_head'].encode() in log
    for revision, status, code in [('r1', 'failed', 1), ('r2', 'passed', 0)]:
        receipt = json.loads(values['wheel-harness-regression-' + revision + '.json'])
        log = values['wheel-harness-regression-' + revision + '.log']
        assert receipt['status'] == status and receipt['returncode'] == code
        assert receipt['process_group_drained'] and receipt['head'] == report['original_head']
        assert receipt['log_sha256'] == identity(log)['sha256']
        assert receipt['command'][2:] == ['-m', 'unittest', 'discover', '-s', 'scripts',
                                        '-p', 'test_*native_python*.py', '-v']
        assert b'Ran 19 tests' in log
        if code:
            assert b'RuntimeError: required native integration suite is absent: test_public_context.py' in log
            assert b'FAILED (failures=1)' in log
        else:
            assert log.rstrip().endswith(b'OK')
            assert receipt['changed_source_inventory'] == {
                report['source_path']: identity(values['source-after.py'])}
    assert identity(values['source-before.py']) == report['source_before']
    assert identity(values['source-after.py']) == report['source_after']
    assert values['source-before.py'] != values['source-after.py']
    if repo_root is not None:
        assert identity((repo_root / report['source_path']).read_bytes()) == report['source_after']
    assert report['runtime_sources_changed'] is False
    assert report['installed_wheel_rebuilt'] is False
    assert report['negative_cases'] == ['missing adapter suite', 'skipped adapter suite',
                                      'missing public-context suite', 'skipped public-context suite']
    return dict(status='passed', original_failed_jobs=2, local_harness_tests=19,
                archived_members=len(values), runtime_sources_changed=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--repo-root', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.packet, args.repo_root)))
