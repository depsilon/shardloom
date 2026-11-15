"""Verify the installed closeout with original paths and execution forbidden."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def content(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    return dict(bytes=path.stat().st_size, sha256=digest)


def main():
    out = Path(__file__).resolve().parent
    packet = Path('/Users/dylan/LocalData/shardloom/optimization-investigation-queue-worktree/docs/benchmarks/evidence/native-fsst-consumer-2026-11-15')
    destination = out / 'fsst-closeout-replay-r1.json'
    assert __debug__ and not destination.exists() and shutil.disk_usage(out).free >= 14 << 30
    before = {p.relative_to(packet).as_posix(): content(p) for p in packet.rglob('*') if p.is_file()}
    with tempfile.TemporaryDirectory(prefix='fsst-complete-closeout-') as temporary:
        copied = Path(temporary) / 'evidence'
        shutil.copytree(packet, copied)
        assert {p.relative_to(copied).as_posix(): content(p) for p in copied.rglob('*') if p.is_file()} == before
        runner = '''import os,runpy,sys
blocked=('/Users/dylan/LocalData/shardloom/','/Users/dylan/Documents/shardloom-local-repo/','/Users/dylan/.cache/shardloom/')
def audit(event,args):
 if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)) and os.path.abspath(os.fsdecode(os.fspath(args[0]))).startswith(blocked):
  raise RuntimeError('original source/runtime/evidence read forbidden')
 if event in ('socket.connect','subprocess.Popen','os.system'):
  raise RuntimeError('execution or network forbidden')
sys.addaudithook(audit)
sys.argv=[sys.argv[1]]
runpy.run_path(sys.argv[0],run_name='__main__')
'''
        result = subprocess.run([sys.executable, '-I', '-B', '-c', runner, str(copied / 'verify.py')],
            capture_output=True, text=True, timeout=1800, cwd=temporary)
        record = dict(status='passed' if result.returncode == 0 else 'failed', returncode=result.returncode,
            stdout=result.stdout, stderr=result.stderr, original_paths_forbidden=True,
            network_and_subprocess_forbidden=True, query_executions=0,
            manifest=content(packet / 'manifest.json'), helper=content(Path(__file__)))
        if result.returncode == 0:
            record['result'] = json.loads(result.stdout)
            assert record['result']['status'] == 'passed_complete_closeout_replay'
            assert record['result']['complete_results'] == 18080 and record['result']['lifecycle_operations'] == 1080
            assert record['result']['disposition'] == 'retain' and record['result']['query_executions'] == 0
        assert before == {p.relative_to(packet).as_posix(): content(p) for p in packet.rglob('*') if p.is_file()}
        record['installed_packet_unchanged'] = True
        with destination.open('x') as stream:
            json.dump(record, stream, indent=2); stream.write('\n')
        assert result.returncode == 0, result.stderr[-6000:]
    print(json.dumps(dict(status='passed_relocated_complete_closeout', query_executions=0,
        result=record['result'])))


if __name__ == '__main__':
    main()
