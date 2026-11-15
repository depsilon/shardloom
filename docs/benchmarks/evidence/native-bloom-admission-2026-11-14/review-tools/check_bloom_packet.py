"""Adversarial checks of the saved-evidence verifier; no engine or query execution."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

B = Path(__file__).resolve().parent
W = Path('/Users/dylan/LocalData/shardloom/optimization-investigation-queue-worktree')
PACKET = W / 'docs/benchmarks/evidence/native-bloom-admission-2026-11-14'
spec = importlib.util.spec_from_file_location('saved_bloom_verifier', PACKET / 'verify.py')
V = importlib.util.module_from_spec(spec)
spec.loader.exec_module(V)
manifest, original = V.read_packet(PACKET)
assert V.verify_payload(original)['checks'] == 28
checks = []


def encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()


def reject(name, operation):
    try:
        operation()
    except (AssertionError, KeyError, ValueError, TypeError, OSError, tarfile.TarError) as error:
        checks.append(dict(name=name, passed=True, rejection=type(error).__name__, detail=str(error)))
    else:
        raise AssertionError('corruption accepted: ' + name)


def payload_case(name, path, change, relink=True):
    members = dict(original)
    value = V.parse(members[path])
    change(value)
    members[path] = encoded(value)
    # Rehash the outside report so semantic cases reach the observation checks.
    if relink and path.startswith('evidence/'):
        disposition_path = 'evidence/bloom-admission-disposition-r1.json'
        if path != disposition_path:
            disposition = V.parse(members[disposition_path])
            relative = path.removeprefix('evidence/')
            if relative in disposition['evidence']:
                disposition['evidence'][relative] = V.shape(members[path])
                members[disposition_path] = encoded(disposition)
    reject(name, lambda: V.verify_payload(members))


def result_case(name, change):
    payload_case(name, 'evidence/logs/bloom-probe-observation-r1.log', change)


def observation(record, name):
    return next(row for row in record['checks'] if row['name'] == name)


result_case('omitted_observation', lambda x: x['checks'].pop())
result_case('duplicate_observation', lambda x: x['checks'].__setitem__(-1, copy.deepcopy(x['checks'][0])))
result_case('unknown_observation', lambda x: x['checks'][0].__setitem__('name', 'unverified'))
result_case('numeric_false_alias', lambda x: observation(x, 'integer_absent_control').__setitem__('observed', 0))
result_case('opposite_zero_claims_present', lambda x: observation(x, 'f64_zero_bits_0_require_equality_review')['observed'].__setitem__('bloom_membership', True))
result_case('same_zero_bits', lambda x: observation(x, 'f32_zero_bits_0_require_equality_review')['observed'].__setitem__('needle_bits', 0))
result_case('missing_options_error', lambda x: observation(x, 'zero_blocks_rejected').__setitem__('observed', ''))
result_case('maximum_allocation_claim', lambda x: observation(x, 'maximum_option_acceptance_is_not_memory_admission')['observed'].__setitem__('partial_allocated', True))
result_case('null_is_unknown_claim', lambda x: observation(x, 'null_partial_is_empty_not_missing_statistics_proof')['observed'].__setitem__('inconclusive_marker', True))
result_case('default_registration_claim', lambda x: observation(x, 'default_aggregate_is_not_registered').__setitem__('observed', True))
result_case('public_query_claim', lambda x: x.__setitem__('public_queries_executed', 1))
result_case('performance_claim', lambda x: x.__setitem__('performance_measured', True))
result_case('failed_native_check', lambda x: x['checks'][0].__setitem__('passed', False))
payload_case('wrong_disposition', 'evidence/bloom-admission-disposition-r1.json', lambda x: x.__setitem__('disposition', 'retain'))
payload_case('runtime_change_claim', 'evidence/bloom-admission-disposition-r1.json', lambda x: x.__setitem__('production_runtime_changed', True))
payload_case('build_command_tamper', 'evidence/bloom-probe-build-r1.json', lambda x: x['command'].remove('--offline'))
payload_case('restoration_tamper', 'evidence/bloom-probe-restoration-r1.json', lambda x: x.__setitem__('restored_manifest_sha256', '0' * 64))
payload_case('locked_archive_tamper', 'evidence/bloom-session-source-check-r1.json', lambda x: x['source_files'][0].__setitem__('archive_sha256', '0' * 64))
payload_case('upstream_unmerged_claim', 'evidence/upstream-pr8933-info-r1.json', lambda x: x['source'].__setitem__('merged', True))
for name, member, replacement in (
    ('probe_source_tamper', 'evidence/probe-source-r1/native_bloom_admission_probe.rs', b'fn main() {}'),
    ('allocator_source_tamper', 'sources/vendor/vortex-buffer/src/allocation.rs', b'changed'),
    ('hash_source_tamper', 'dependencies/twox-hash-2.1.2/src/xxhash3_64.rs', b'changed'),
    ('provider_warning_removed', 'sources/vendor/vortex-layout/src/layouts/zoned/aggregates/bloom_filter/mod.rs', b'pub struct BloomFilter;'),
):
    changed = dict(original)
    changed[member] = replacement
    reject(name, lambda: V.verify_payload(changed))

with tempfile.TemporaryDirectory(prefix='bloom-packet-controls-', dir=B) as temporary:
    root = Path(temporary)
    shutil.copyfile(PACKET / 'verify.py', root / 'verify.py')

    def archive_case(name, extra=None, omit=None, expected_change=None, raw_manifest=None):
        current = copy.deepcopy(manifest)
        with tarfile.open(root / 'evidence.tar.xz', 'w:xz', preset=0) as archive:
            for member, data in original.items():
                if member == omit:
                    continue
                entry = tarfile.TarInfo(member)
                entry.size = len(data)
                archive.addfile(entry, io.BytesIO(data))
            if extra is not None:
                entry, data = extra
                archive.addfile(entry, io.BytesIO(data) if data is not None else None)
        current['archive'].update(V.shape((root / 'evidence.tar.xz').read_bytes()))
        if expected_change:
            expected_change(current)
        (root / 'manifest.json').write_bytes(raw_manifest or encoded(current))
        reject(name, lambda: V.read_packet(root))

    first = next(iter(original))
    archive_case('missing_archive_member', omit=first)
    duplicate = tarfile.TarInfo(first)
    duplicate.size = len(original[first])
    archive_case('duplicate_archive_member', extra=(duplicate, original[first]))
    for name in ('../escape', '/absolute', 'evidence/../escape', 'evidence//empty'):
        entry = tarfile.TarInfo(name)
        archive_case('unsafe_path_' + name, extra=(entry, b''), expected_change=lambda x, n=name: x['members'].__setitem__(n, V.shape(b'')))
    entry = tarfile.TarInfo('link')
    entry.type = tarfile.SYMTYPE
    entry.linkname = '../outside'
    archive_case('symlink_member', extra=(entry, None), expected_change=lambda x: x['members'].__setitem__('link', V.shape(b'')))
    archive_case('member_size_tamper', expected_change=lambda x: x['members'][first].__setitem__('bytes', x['members'][first]['bytes'] + 1))
    archive_case('member_digest_tamper', expected_change=lambda x: x['members'][first].__setitem__('sha256', '0' * 64))
    archive_case('archive_digest_tamper', expected_change=lambda x: x['archive'].__setitem__('sha256', '0' * 64))
    archive_case('verifier_digest_tamper', expected_change=lambda x: x['verifier'].__setitem__('sha256', '0' * 64))
    archive_case('member_byte_limit', expected_change=lambda x: x['members'][first].__setitem__('bytes', (16 << 20) + 1))
    archive_case('member_boolean_size', expected_change=lambda x: x['members'][first].__setitem__('bytes', True))
    archive_case('duplicate_manifest_key', raw_manifest=b'{"schema":0,"schema":1}')
    archive_case('archive_path_escape', expected_change=lambda x: x['archive'].__setitem__('path', '../evidence.tar.xz'))

relocated = Path('/Users/dylan/LocalData/shardloom/native-bloom-portable-check-2026-11-14')
relocated.mkdir(exist_ok=True)
for name in ('verify.py', 'manifest.json', 'evidence.tar.xz'):
    target = relocated / name
    assert not target.exists()
    shutil.copyfile(PACKET / name, target)
blocked_roots = [str(W), str(B), '/Users/dylan/Documents/shardloom-local-repo', '/Users/dylan/.cargo',
    '/Users/dylan/.cache/shardloom', '/Users/dylan/LocalData/shardloom/native-pipeline-sql-minute-2026-11-13',
    '/Users/dylan/LocalData/shardloom/release-0.3.3-20260930', '/Users/dylan/LocalData/shardloom/perf-all-20260906',
    '/Users/dylan/.codex/worktrees/native-vector-representations']
child = '''import json, os, runpy, sys
roots = json.loads(sys.argv[1])
target = sys.argv[2]
attempts = []
def audit(event, args):
    if event == 'open' and isinstance(args[0], (str, bytes, os.PathLike)):
        path = os.path.abspath(os.fsdecode(args[0]))
        if any(path == root or path.startswith(root + os.sep) for root in roots):
            attempts.append(path)
            raise PermissionError('historical source read blocked')
    if event in ('subprocess.Popen', 'os.system', 'socket.connect'):
        attempts.append(event)
        raise PermissionError('runtime or network access blocked')
sys.addaudithook(audit)
try:
    open(roots[0] + '/Cargo.toml', 'rb')
except PermissionError:
    assert len(attempts) == 1
else:
    raise AssertionError('read denial did not activate')
attempts.clear()
sys.argv = [target]
runpy.run_path(target, run_name='__main__')
assert not attempts
print(json.dumps(dict(read_denial_control=True, forbidden_access_attempts=attempts, relocated_verification=True)))
'''
process = subprocess.run([sys.executable, '-I', '-S', '-B', '-c', child, json.dumps(blocked_roots), str(relocated / 'verify.py')],
    cwd=relocated, capture_output=True, text=True, timeout=30)
(B / 'bloom-relocated-verification-r1.log').write_text(process.stdout + process.stderr)
assert process.returncode == 0, process.stderr
result = dict(schema='shardloom.bloom_packet_controls.v1', status='passed', negative_controls=len(checks), checks=checks,
    packet_manifest=V.shape((PACKET / 'manifest.json').read_bytes()),
    verifier=V.shape((PACKET / 'verify.py').read_bytes()), relocated=str(relocated),
    relocated_process_returncode=process.returncode, relocated_result=[json.loads(line) for line in process.stdout.splitlines()],
    runtime_executed=False, query_executions=0)
(B / 'bloom-packet-controls-r1.json').write_bytes(encoded(result))
print(json.dumps({key:result[key] for key in ('status','negative_controls','relocated_process_returncode','runtime_executed','query_executions')}))
