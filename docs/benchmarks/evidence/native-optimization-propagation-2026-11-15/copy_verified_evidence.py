"""Copy sealed, verified evidence into the authorized PR without altering its sources."""
import json
from pathlib import Path
import shutil
import sys

OUT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(OUT), str(OUT / 'postprocessing')]
import selection_support
import selection_runtime as runtime
from query_surface.evidence import identity
from local_uat_storage import GIB, MIB, check_budgets, require_local_path


def main():
    destination = runtime.ROOT / 'docs/benchmarks/evidence/native-optimization-propagation-2026-11-15'
    assert not destination.exists()
    decision = runtime.read(OUT / 'integration/integration-disposition-r1.json')
    assert decision['status'] == 'ready_for_pr_after_complete_source_and_portable_verification'
    require_local_path(destination, Path.home(), sys.platform)
    check_budgets(runtime.ROOT, destination, OUT / 'logs', min_free_bytes=12*GIB,
        reserve_bytes=2*GIB, max_workspace_bytes=100*GIB, max_log_bytes=192*MIB)
    records = {}

    def copy(source, target):
        assert source.is_file() and not source.is_symlink() and not target.exists()
        before = identity(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        assert runtime.identity(source) == runtime.identity(target)
        assert identity(source) == before
        records[str(target.relative_to(destination))] = runtime.identity(target)

    for name, source in (('propagation', runtime.PREVIOUS / 'propagation-packet-r4'),
                         ('selection', OUT / 'selection-packet-r2')):
        manifest = runtime.read(source / 'manifest.json')
        assert runtime.identity(source / 'manifest.json') == decision['packets'][name]['manifest']
        for file in sorted(source.iterdir()):
            if file.name != 'manifest.json':
                assert runtime.identity(file) == manifest['files'][file.name]
            copy(file, destination / name / file.name)
    for name, source in {
        'integration-disposition.json':OUT / 'integration/integration-disposition-r1.json',
        'record_integration.py':OUT / 'integration/record_integration.py',
        'portable-result.json':OUT / 'selection-portable-result-r2.json',
        'portable-replay-receipt.json':OUT / 'selection-portable-replay-r2.json',
        'portable-replay.log':OUT / 'logs/selection-portable-replay-r2.log',
        'relocation-proof.json':OUT / 'selection-packet-relocation-r2.json',
        'cost-inventory.json':OUT / 'postprocessing/selection-cost-inventory-r1.json',
        'cost-inventory.md':OUT / 'postprocessing/selection-cost-inventory-r1.md',
        'cost-primary-verification.json':OUT / 'postprocessing/selection-cost-primary-verification-r1.json',
        'copy_verified_evidence.py':Path(__file__),
    }.items():
        copy(source, destination / name)
    receipt = dict(status='passed_exact_packet_and_closeout_copy', query_executions=0,
        files=records, source_commit=decision['source_commit'], packets=decision['packets'],
        original_file_generations_unchanged=True, method='byte-verified independent copies',
        storage_floor_bytes=12*GIB, publication_attempted=False)
    with (destination / 'copy-verification.json').open('x') as stream:
        json.dump(receipt, stream, indent=2); stream.write('\n')
    print(json.dumps(dict(status=receipt['status'], files=len(records), query_executions=0)))


if __name__ == '__main__':
    main()
