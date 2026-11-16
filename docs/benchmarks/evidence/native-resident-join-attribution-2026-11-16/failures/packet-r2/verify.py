"""Replay both fresh cohorts and their checks entirely from the sealed packet."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile


def load(name, path, expected):
    data = path.read_bytes()
    assert dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest()) == expected
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    sys.modules[name] = value
    spec.loader.exec_module(value)
    return value


def forbid_original_reads(store):
    roots = {str(Path(store.manifest['out']).parent), *store.manifest['roots'].values()}
    originals = set(store.origins) | set(store.external)

    def audit(event, args):
        if event.startswith(('socket.', 'subprocess.')) or event in (
                'os.system', 'os.posix_spawn', 'os.fork', 'os.forkpty', 'os.exec'):
            raise RuntimeError('network/process execution forbidden during portable replay: ' + event)
        if event in ('open', 'os.listdir', 'os.scandir') and args and isinstance(args[0], (str, bytes, os.PathLike)):
            name = os.path.abspath(os.fsdecode(args[0]))
            if name in originals or any(name == root or name.startswith(root + os.sep) for root in roots):
                raise RuntimeError('original evidence/source read forbidden during portable replay: ' + name)
    sys.addaudithook(audit)


def verify(packet):
    assert __debug__ and sys.version_info >= (3, 13)
    os.chdir(packet)
    manifest = json.loads((packet / 'manifest.json').read_bytes())
    transport = load('propagation_portable_store', packet / 'propagation_portable_store.py',
                     manifest['files']['propagation_portable_store.py'])
    current = load('variable_copy_portable_store', packet / 'variable_copy_portable_store.py',
                   manifest['files']['variable_copy_portable_store.py'])
    with tempfile.TemporaryDirectory(prefix='shardloom-variable-copy-replay-') as temporary:
        store = current.Store(packet, Path(temporary))
        out = Path(manifest['out'])
        for name, source in [('verify.py', 'portable_variable_copy.py'),
                             ('variable_copy_portable_store.py', 'variable_copy_portable_store.py')]:
            assert transport.content(packet / name) == transport.shape(
                store.identity(out / 'postprocessing' / source))
        assert transport.content(packet / 'propagation_portable_store.py') == transport.shape(
            store.identity(out / 'postprocessing/propagation_portable_store.py'))
        store.verify([manifest[key] for key in ('protocol', 'replay', 'disposition')])
        protocol_path = Path(manifest['protocol']['path'])
        protocol = store.read(protocol_path)
        store.verify(protocol['frozen_files'], allow_external=True)
        sys.meta_path.insert(0, transport.NoEngineImports())
        forbid_original_reads(store)
        validation = transport.module(store, 'variable_copy_portable_validation',
                                      out / 'postprocessing/variable_copy_portable_validation.py')
        fingerprints = validation.validate(store, protocol)
        bindings = transport.module(store, 'variable_copy_portable_bind',
                                    out / 'postprocessing/variable_copy_portable_bind.py')
        support, replay = bindings.bind(store, protocol)
        assert support.load_protocol(protocol_path) == protocol
        observed = {role: replay.fresh(protocol, protocol_path, role) for role in ('baseline', 'candidate')}
        recomputed = replay.evaluate(protocol, observed)
        expected = store.read(manifest['replay']['path'])
        omitted = {'protocol', 'verifier', 'source_fingerprints', 'fresh_packets_replayed',
                   'query_executions_by_verifier'}
        assert {k: v for k, v in expected.items() if k not in omitted} == recomputed
        assert expected['fresh_packets_replayed'] == 806 and expected['query_executions_by_verifier'] == 0
        assert expected['source_fingerprints'] == fingerprints
        store.verify([expected['protocol'], expected['verifier']])
        baseline = store.read(out / 'variable-copy-baseline-verification-r1.json')
        assert baseline['status'] == 'passed_complete_baseline_replay' and baseline['cases'] == 403
        assert baseline['observations'] == observed['baseline'] and baseline['query_executions'] == 0
        store.verify([baseline[k] for k in ('protocol', 'verifier', 'baseline_runtime')])
        disposition = validation.disposition(store, recomputed, fingerprints)
        diagnostic = validation.diagnostics(store)
        controls = transport.module(store, 'variable_copy_portable_controls',
                                    out / 'postprocessing/variable_copy_portable_controls.py')
        negative = controls.verify(store, protocol, support, replay, recomputed)
        assert not any(n in sys.modules for n in ('shardloom._native', 'polars', 'pyarrow', 'duckdb'))
        return dict(status='passed_complete_portable_replay', query_executions=0,
            fresh_observations_replayed=806, reused_baseline_results=0,
            source_fingerprints=fingerprints, fresh=recomputed['fresh'], decision=recomputed['decision'],
            disposition=disposition, diagnostic=diagnostic, negative_controls=negative,
            original_reads_forbidden=True, network_and_process_execution_forbidden=True,
            external_payloads_not_reopened=len(store.external), causal_speedup_claim=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = verify(args.root.resolve())
    if args.output:
        with args.output.open('x') as stream:
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write('\n')
    print(json.dumps(result, allow_nan=False), flush=True)


if __name__ == '__main__':
    main()
