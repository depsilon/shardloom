"""Replay the complete propagation comparison from saved bytes without any engine."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile


def load_transport(packet):
    manifest=json.loads((packet/'manifest.json').read_bytes())
    path=packet/'propagation_portable_store.py'
    data=path.read_bytes()
    assert manifest['files'][path.name]==dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
    spec=importlib.util.spec_from_file_location('propagation_portable_store',path)
    transport=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=transport;spec.loader.exec_module(transport)
    return transport


def forbid_original_reads(store):
    roots={str(Path(store.manifest['out']).parent),*store.manifest['roots'].values()}
    originals=set(store.origins)|set(store.external)

    def audit(event,args):
        if event.startswith(('socket.','subprocess.')) or event in (
            'os.system','os.posix_spawn','os.fork','os.forkpty','os.exec'):
            raise RuntimeError('network/process execution forbidden during portable replay: '+event)
        if event in ('open','os.listdir','os.scandir') and args and isinstance(args[0],(str,bytes,os.PathLike)):
            name=os.path.abspath(os.fsdecode(args[0]))
            if name in originals or any(name==root or name.startswith(root+os.sep) for root in roots):
                raise RuntimeError('original evidence/source read forbidden during portable replay: '+name)
    sys.addaudithook(audit)


def verify(packet):
    assert __debug__ and sys.version_info>=(3,13)
    # Cleanup opens child names relative to directory FDs. Keep their audit
    # display paths outside the prohibited original checkout as well.
    os.chdir(packet)
    transport=load_transport(packet)
    with tempfile.TemporaryDirectory(prefix='shardloom-propagation-replay-') as temporary:
        store=transport.Store(packet,Path(temporary))
        out=Path(store.manifest['out'])
        manifest=store.manifest
        store.verify([manifest[key] for key in ('protocol','replay','disposition')])
        assert transport.content(packet/'verify.py')==transport.shape(store.identity(out/'portable_propagation.py'))
        assert transport.content(packet/'propagation_portable_store.py')==transport.shape(store.identity(out/'propagation_portable_store.py'))
        protocol_path=Path(manifest['protocol']['path']);protocol=store.read(protocol_path)
        store.verify(protocol['frozen_files'],allow_external=True)
        sys.meta_path.insert(0,transport.NoEngineImports())
        forbid_original_reads(store)
        validation=transport.module(store,'propagation_portable_validation',out/'propagation_portable_validation.py')
        fingerprints=validation.validate(store,protocol)
        diagnostic=validation.diagnostics(store)
        bindings=transport.module(store,'propagation_portable_bind',out/'propagation_portable_bind.py')
        support,replay=bindings.bind(store,protocol)
        assert support.load_protocol(protocol_path)==protocol
        inputs=validation.inputs(store,support)
        fresh={role:replay.fresh(protocol,protocol_path,role) for role in ('baseline','candidate')}
        prepared={role:replay.lifecycle(protocol,protocol_path,role) for role in ('baseline','candidate')}
        recomputed=replay.evaluate(protocol,fresh,prepared)
        expected=store.read(manifest['replay']['path'])
        omitted={'protocol','source_fingerprints','verifier','observations_replayed','lifecycle_operations_replayed'}
        assert {key:value for key,value in expected.items() if key not in omitted}==recomputed
        assert expected['source_fingerprints']==fingerprints
        store.verify([expected['protocol'],expected['verifier']])
        assert expected['observations_replayed']==23158 and expected['lifecycle_operations_replayed']==1080
        disposition=store.read(manifest['disposition']['path'])
        assert disposition['disposition']==recomputed['decision']
        assert disposition['original_replay_status']==recomputed['status']
        assert disposition['original_replay_decision']==recomputed['decision']
        assert disposition['fresh']==recomputed['fresh'] and disposition['lifecycle']==recomputed['lifecycle']
        assert not disposition['thresholds_changed'] and disposition['additional_query_executions']==0
        assert disposition['source_fingerprints']==fingerprints
        assert not disposition['whole_propagation_queue_complete'] and not disposition['publication_attempted']
        for name,wanted in disposition['inputs'].items():
            assert transport.shape(store.identity(out/name))==wanted
        assert not any(name in sys.modules for name in ('shardloom._native','polars','pyarrow','duckdb'))
        return dict(status='passed_complete_portable_replay',query_executions=0,
            fresh_observations_replayed=23158,declared_prepared_operations=1080,reused_baseline_results=0,
            source_fingerprints=fingerprints,external_payloads_not_reopened=len(store.external),
            fresh=recomputed['fresh'],lifecycle=recomputed['lifecycle'],decision=recomputed['decision'],
            required_work_complete=recomputed['required_work_complete'],
            independent_inputs=inputs,untimed_source_bound_diagnostics=diagnostic,
            original_reads_forbidden=True,network_and_process_execution_forbidden=True,causal_speedup_claim=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parent)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args();result=verify(args.root.resolve())
    if args.output:
        with args.output.open('x') as stream:json.dump(result,stream,indent=2);stream.write('\n')
    print(json.dumps(result,allow_nan=False))


if __name__=='__main__':main()
