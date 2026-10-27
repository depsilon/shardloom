"""Replay complete evidence from source-only copies with isolated Python."""
from pathlib import Path
import shutil
import subprocess
import sys

REPO = Path('/Users/dylan/.codex/worktrees/public-docs-api/shardloom-local-repo')
ROOT = Path(__file__).resolve().parent.parent / 'packet-replay-clean'
assert not ROOT.exists()
for prefix in ('scripts', 'python/src/shardloom', 'benchmarks/traditional_analytics/query_surface'):
    for path in (REPO / prefix).rglob('*.py'):
        if '__pycache__' in path.parts:
            continue
        assert path.is_file() and not path.is_symlink()
        target = ROOT / path.relative_to(REPO)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
packet = Path('docs/benchmarks/evidence/native-shared-analytical-performance-2026-10-27')
shutil.copytree(REPO / packet, ROOT / packet)
subprocess.run([sys.executable, '-I', '-B', str(ROOT / 'scripts/verify_shared_analytical_performance_packet.py'),
                str(ROOT / packet)], cwd=ROOT, check=True)
subprocess.run([sys.executable, '-I', '-B', '-m', 'unittest', 'discover', '-s', str(ROOT / 'scripts'),
                '-p', 'test_shared_analytical_performance_packet.py', '-v'], cwd=ROOT, check=True)
