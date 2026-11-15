"""Install the verified finite screen and its original tooling failures."""
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil

ROOT = Path('/Users/dylan/LocalData/shardloom/optimization-investigation-queue-worktree')
OUT = Path(__file__).resolve().parent
DEST = ROOT / 'docs/benchmarks/evidence/native-survivor-admission-2026-11-15'


def main():
    assert not DEST.exists()
    assert shutil.disk_usage(OUT).free >= 12 << 30
    shutil.copytree(OUT / 'packet-r1', DEST)
    additions = {
        'review-tools/original-packet-manifest.json': OUT / 'packet-r1/manifest.json',
        'review-tools/control_survivor_packet.py': OUT / 'control_survivor_packet.py',
        'review-tools/install_survivor_packet.py': Path(__file__),
        'review-tools/control_survivor_packet_failure_r1.py':
            OUT / 'controls-failure-r1/control_survivor_packet.py',
        'notices/shardloom-LICENSE': ROOT / 'LICENSE',
        'notices/shardloom-NOTICE': ROOT / 'NOTICE',
        'notices/vortex-APACHE-2.0.txt': ROOT / 'vendor/vortex-array/LICENSE',
    }
    assert (ROOT / 'vendor/vortex-array/LICENSE').read_bytes() == (
        ROOT / 'vendor/vortex-buffer/LICENSE').read_bytes()
    for name in ('survivor-package-r3', 'survivor-packet-controls-r1',
                 'survivor-packet-controls-r2'):
        additions[f'supplement/{name}.json'] = OUT / (name + '.json')
        additions[f'supplement/{name}.log'] = OUT / 'logs' / (name + '.log')
    for name in ('packet-replay-r1.json', 'packet-controls-r2.json'):
        additions['supplement/' + name] = OUT / name
    for name, source in additions.items():
        target = DEST / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    notice = {
        'source_revision': '236dbccd590a301ede8f34cce09715be92f025e5',
        'archive_source_scope': 'Exact cited source subset and test source snapshots; not a complete rebuild bundle.',
        'upstream': 'Vortex 0.93.0; vendor/vortex-array and vendor/vortex-buffer; Apache-2.0.',
        'local': 'ShardLoom source, tests, observations and tools; Apache-2.0.',
        'copied_implementation_changes': False,
    }
    (DEST / 'notices/provenance.json').write_text(json.dumps(notice, indent=2) + '\n')
    manifest = json.loads((DEST / 'manifest.json').read_bytes())
    for path in sorted(DEST.rglob('*')):
        if path.is_file() and path.name != 'manifest.json':
            data = path.read_bytes()
            manifest['files'][str(path.relative_to(DEST))] = {
                'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}
    (DEST / 'manifest.json').write_text(json.dumps(manifest, sort_keys=True, indent=2) + '\n')
    spec = importlib.util.spec_from_file_location('installed_survivor_verifier', DEST / 'verify.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = module.verify(DEST)
    receipt = {'status': 'passed', 'packet': str(DEST), 'result': result,
               'manifest_files': len(manifest['files']),
               'archive_sha256': manifest['files']['observations.tar.gz']['sha256']}
    with (OUT / 'packet-install-r1.json').open('x') as stream:
        json.dump(receipt, stream, sort_keys=True, indent=2)
        stream.write('\n')
    print(json.dumps(receipt, sort_keys=True))


if __name__ == '__main__':
    main()
