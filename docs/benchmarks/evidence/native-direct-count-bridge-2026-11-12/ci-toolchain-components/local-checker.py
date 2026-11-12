"""Verify explicit CI components with frozen failure and action-source evidence."""
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

B = Path(__file__).resolve().parents[1]
W = Path('/Users/dylan/.codex/worktrees/native-vector-representations/shardloom-local-repo')
PACKET = W / 'docs/benchmarks/evidence/native-direct-count-bridge-2026-11-12'
OUT = B / 'ci-component-checks-r1'
assert not OUT.exists()
OUT.mkdir()
def module(name,path):
    spec = importlib.util.spec_from_file_location(name,path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value
verifier = module('count_original_verifier',PACKET/'verify.py')
overlay = module('count_component_verifier',PACKET/'verify_ci.py')
report,data = verifier.read_archive(PACKET)
packet = verifier.verify_sources(report,data)
original,target = overlay._implementation.verify_overlay(verifier,packet,PACKET)
before = (PACKET/'ci-toolchain/workflow.yml').read_bytes()
after = (W/overlay.CI_PATH).read_bytes()
final = overlay.updated_target(verifier,target,before,after)
source = overlay.verify_current_sources(verifier,final,W)
rejected = []
for name,changed in (
    ('missing_components',before),
    ('missing_rustfmt',after.replace(b'components: rustfmt, clippy',b'components: clippy')),
    ('missing_clippy',after.replace(b'components: rustfmt, clippy',b'components: rustfmt')),
    ('disabled_lint',after.replace(b'-- -D warnings',b'-- -A warnings')),
    ('omitted_job',after.replace(b'  rust-baseline:\n',b'  skipped-rust-baseline:\n')),
):
    assert changed != after
    try:
        overlay.updated_target(verifier,target,before,changed)
    except AssertionError:
        rejected.append(name)
    else:
        raise AssertionError('corruption accepted: '+name)
changed = dict(final)
changed['shardloom-core/src/schema.rs'] = dict(final['shardloom-core/src/schema.rs'],sha256='0'*64)
try:
    overlay._implementation.check_actual_sources(verifier,final,changed)
except AssertionError:
    rejected.append('changed_runtime_source')
else:
    raise AssertionError('runtime corruption accepted')
action = json.loads((B/'hosted-failure-r1/action-source-connector.json').read_bytes())['structuredContent']['content']
assert 'components: ${{inputs.components}}' in action
assert 'for c in ${components//,/ }; do echo -n' in action
assert '${{steps.flags.outputs.components}} --profile minimal' in action
for name,command in (
    ('matrix',[sys.executable,'-B','scripts/check_ci_gate_matrix.py','--output',str(OUT/'matrix.json')]),
    ('pipeline',[sys.executable,'-B','-m','unittest','discover','-s','scripts','-p','test_ci_execution_plan.py']),
    ('format',['cargo','fmt','--all','--','--check']),
    ('components',['rustup','component','list','--toolchain','1.99.0','--installed']),
):
    result = subprocess.run(command,cwd=W,capture_output=True,timeout=600)
    for stream in ('stdout','stderr'):
        (OUT/(name+'.'+stream+'.log')).write_bytes(getattr(result,stream))
    assert result.returncode == 0,(name,result.stderr.decode()[-3000:])
pipeline = (OUT/'pipeline.stderr.log').read_text()
match = re.search(r'Ran (\d+) tests in [\d.]+s\s+OK\s*$',pipeline)
assert match and int(match.group(1)) == 14
components = (OUT/'components.stdout.log').read_text()
assert 'clippy-aarch64-apple-darwin' in components and 'rustfmt-aarch64-apple-darwin' in components
assert overlay.verify_current_sources(verifier,final,W) == source
result = dict(status='passed',query_executions=0,component_inputs=16,pipeline_tests=14,
    rejected_corruptions=sorted(rejected),workflow=verifier.identity(after),before=verifier.identity(before),
    entrypoint=verifier.identity((PACKET/'verify_ci.py').read_bytes()),
    checker=verifier.identity(Path(__file__).read_bytes()),source_fingerprint_sha256=source,
    components=['rustfmt','clippy'],toolchain='1.99.0',action_source_verified=True,
    local_component_installation_verified=True,
    observations={path.name:verifier.identity(path.read_bytes()) for path in OUT.iterdir()})
with (B/'ci-components-local-validation-r1.json').open('x') as stream:
    json.dump(result,stream,indent=2)
    stream.write('\n')
print(json.dumps(result,indent=2))
