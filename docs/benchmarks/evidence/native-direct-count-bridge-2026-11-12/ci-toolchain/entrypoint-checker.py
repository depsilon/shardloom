"""Exercise the supplemented packet both in its repository and relocated."""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

W = Path('/Users/dylan/.codex/worktrees/native-vector-representations/shardloom-local-repo')
B = Path(__file__).resolve().parents[1]
P = W / 'docs/benchmarks/evidence/native-direct-count-bridge-2026-11-12'
PY = '/opt/homebrew/opt/python@3.13/libexec/bin/python3'
results = []
rejected = []
for mode in ('-O', '-OO'):
    output = subprocess.run([PY,'-I','-S','-B',mode,str(P/'verify_ci.py')],cwd=W,capture_output=True,timeout=30)
    assert output.returncode != 0 and not output.stdout
    assert b'evidence verification requires assertion checks' in output.stderr
    rejected.append(mode)
    (B/('ci-entrypoint-optimized'+mode+'-r1.stderr.log')).write_bytes(output.stderr)
with tempfile.TemporaryDirectory(prefix='count-ci-portable-', dir=B) as scratch:
    relocated = Path(scratch) / 'packet'
    shutil.copytree(P, relocated)
    for name, root, tail in (('source', P, ['--repo-root',str(W)]), ('relocated', relocated, [])):
        command = [PY,'-I','-S','-B',str(root/'verify_ci.py'),*tail]
        output = subprocess.run(command,cwd=W,capture_output=True,timeout=900)
        for stream in ('stdout','stderr'):
            with (B/('ci-entrypoint-'+name+'-r1.'+stream+'.log')).open('xb') as destination:
                destination.write(getattr(output,stream))
        assert output.returncode == 0, output.stderr.decode()[-4000:]
        value = json.loads(output.stdout)
        assert value['status'] == 'passed_source_bound_replay' and value['query_executions'] == 0
        assert value['candidate_cases'] == 0 and value['engine_execution_seconds'] is None
        assert value['investigation_b_complete'] is False
        results.append(dict(name=name,result=value))
print(json.dumps(dict(status='passed',query_executions=0,positive_controls=results,rejected_optimized_modes=rejected),indent=2))
