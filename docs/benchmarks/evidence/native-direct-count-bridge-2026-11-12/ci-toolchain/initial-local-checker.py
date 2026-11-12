"""Validate the CI-only compiler pin against the immutable COUNT checkpoint."""
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

OUT = Path(__file__).resolve().parents[1]
ROOT = Path('/Users/dylan/.codex/worktrees/native-vector-representations/shardloom-local-repo')
PACKET = ROOT / 'docs/benchmarks/evidence/native-direct-count-bridge-2026-11-12'
TARGET = OUT / 'ci-toolchain-checks-r1'
assert not TARGET.exists()
TARGET.mkdir()


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


verifier = module('original_count_verifier', PACKET / 'verify.py')
helper = OUT / 'hosted-tools/verify_ci_checkpoint.py'
assert verifier.digest(helper.read_bytes()) == 'b7a239eb349142e0a74a7160fb78704cfd16306292ac1a069d2af2aa3fab0a53'
overlay = module('count_ci_overlay', helper)
report, data = verifier.read_archive(PACKET)
packet = verifier.verify_sources(report, data)
base = verifier.integration_sources(packet)
before = data['source-blobs/' + base[overlay.CI_PATH]['sha256']]
after = (ROOT / overlay.CI_PATH).read_bytes()
supplement = dict(path=overlay.CI_PATH, toolchain=overlay.PIN,
    before=verifier.identity(before), after=verifier.identity(after))
target = overlay.target_sources(verifier, packet, supplement, after)
source_fingerprint = overlay.verify_current_sources(verifier, target, ROOT)
rejected = []
for name, changed in (
        ('floating_compiler', before),
        ('different_compiler', after.replace(b'"1.99.0"', b'"1.100.0"')),
        ('disabled_lint', after.replace(b'-- -D warnings', b'-- -A warnings')),
        ('omitted_job', after.replace(b'  rust-baseline:\n', b'  skipped-rust-baseline:\n'))):
    assert changed != after
    try:
        overlay.target_sources(verifier, packet, dict(supplement, after=verifier.identity(changed)), changed)
    except AssertionError:
        rejected.append(name)
    else:
        raise AssertionError('corruption accepted: ' + name)
actual = dict(target)
actual['shardloom-core/src/schema.rs'] = dict(actual['shardloom-core/src/schema.rs'], sha256='0' * 64)
try:
    overlay.check_actual_sources(verifier, target, actual)
except AssertionError:
    rejected.append('changed_runtime_source')
else:
    raise AssertionError('runtime change accepted')
assert set(rejected) == overlay.CONTROL_NAMES
commands = (
    ('matrix', [sys.executable, '-B', 'scripts/check_ci_gate_matrix.py', '--output', str(TARGET / 'ci-gate-matrix.json')]),
    ('pipeline', [sys.executable, '-B', '-m', 'unittest', 'discover', '-s', 'scripts', '-p', 'test_ci_execution_plan.py']),
    ('format', ['cargo', 'fmt', '--all', '--', '--check']),
)
checks = {}
for name, command in commands:
    result = subprocess.run(command, cwd=ROOT, capture_output=True, timeout=600)
    for stream in ('stdout', 'stderr'):
        (TARGET / (name + '.' + stream + '.log')).write_bytes(getattr(result, stream))
    assert result.returncode == 0, (name, result.stderr[-3000:])
    checks[name] = dict(command=command, returncode=0,
        stdout=verifier.identity(result.stdout), stderr=verifier.identity(result.stderr))
pipeline = (TARGET / 'pipeline.stderr.log').read_text()
match = re.search(r'Ran (\d+) tests in [\d.]+s\s+OK\s*$', pipeline)
assert match and int(match.group(1)) > 0
rustc = subprocess.check_output(['/Users/dylan/.rustup/toolchains/1.99.0-aarch64-apple-darwin/bin/rustc', '--version'], text=True).strip()
assert rustc.startswith('rustc 1.99.0 ')
assert after == (ROOT / overlay.CI_PATH).read_bytes()
assert overlay.verify_current_sources(verifier, target, ROOT) == source_fingerprint
result = dict(status='passed', query_executions=0, workflow=verifier.identity(after),
    before=verifier.identity(before), toolchain_inputs_pinned=16, toolchain='1.99.0', rustc=rustc,
    strict_lint_commands_unchanged=True, msrv_lane_unchanged=True,
    pipeline_tests=int(match.group(1)), rejected_corruptions=sorted(rejected),
    source_fingerprint_sha256=source_fingerprint, checks=checks,
    helper=verifier.identity(helper.read_bytes()), verifier=verifier.identity(Path(__file__).read_bytes()))
with (OUT / 'ci-toolchain-local-validation-r1.json').open('x') as stream:
    json.dump(result, stream, indent=2)
    stream.write('\n')
print(json.dumps(result, indent=2))
