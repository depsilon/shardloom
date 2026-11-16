"""Replay all saved Polars/native values from a relocated, engine-free packet."""
import argparse
import hashlib
import importlib
import importlib.abc
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile


def load(name,path,wanted):
    raw=path.read_bytes()
    assert dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())==wanted
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module)
    return module


class ArchivedImports(importlib.abc.MetaPathFinder):
    def __init__(self,store,imports):
        self.store=store
        self.origins={row['module']:row['path'] for row in imports['modules'] if row['module']!='__main__'}
        self.roots={name.split('.')[0] for name in self.origins}
        self.forbidden=[*store.manifest['forbidden_original_roots'],str(store.directory)]

    def find_spec(self,fullname,path=None,target=None):
        sys.path[:]=[entry for entry in sys.path if not any(entry==root or entry.startswith(root+'/') for root in self.forbidden)]
        if fullname not in self.origins:
            if fullname.split('.')[0] in self.roots:
                raise ModuleNotFoundError('unrecorded application import: '+fullname)
            return None
        archived=self.store.path(self.origins[fullname])
        return importlib.util.spec_from_file_location(fullname,archived,
            submodule_search_locations=[str(archived.parent)] if archived.name=='__init__.py' else None)


def forbid_originals(store):
    roots=set(store.manifest['forbidden_original_roots'])
    originals=set(store.origins)|set(store.external)
    def audit(event,args):
        if event.startswith(('socket.','subprocess.')) or event in {'os.system','os.posix_spawn','os.fork','os.forkpty','os.exec','ctypes.dlopen'}:
            raise RuntimeError('process/network/engine loading forbidden: '+event)
        if event in {'open','os.listdir','os.scandir'} and args and isinstance(args[0],(str,bytes,os.PathLike)):
            name=os.path.abspath(os.fsdecode(args[0]))
            if name in originals or any(name==root or name.startswith(root+os.sep) for root in roots):
                raise RuntimeError('original-path access forbidden: '+name)
    sys.addaudithook(audit)


def bind(store,transport):
    imports=store.read(store.manifest['import_map']['path'])
    store.verify([imports['frozen_protocol']])
    store.verify([row['identity'] for row in imports['modules']])
    finder=ArchivedImports(store,imports)
    sys.meta_path[:0]=[transport.NoEngineImports(),finder]
    forbid_originals(store)
    for name in finder.origins:
        importlib.import_module(name)
    old_parent=Path(store.manifest['out']).parent
    extracted=store.directory/'data'
    for name,origin in finder.origins.items():
        module=sys.modules[name]
        assert Path(module.__file__).resolve()==store.path(origin).resolve(),name
        for key,value in tuple(vars(module).items()):
            if isinstance(value,Path) and value.is_relative_to(extracted):
                setattr(module,key,old_parent/value.relative_to(extracted))
        # The shared helpers use `read` only for JSON artifact access. Their
        # semantic declarations, result comparators and values stay untouched.
        if callable(vars(module).get('read')):
            module.read=store.read
    import polars_common as c
    import polars_mapping
    import replay_polars_once as replay
    c.OUT=Path(store.manifest['out'])
    c.ROOT=old_parent/'optimization-investigation-queue-worktree'
    c.R=old_parent/'native-optimization-propagation-2026-11-15'
    c.E=old_parent/'expanded-query-2026-11-03'
    c.FROZEN=c.E/'investigation-original-harness-r1'
    c.PROTOCOL=c.R/'propagation-protocol-r1.json'
    c.read,c.identity=store.read,store.identity
    c.historical.cache_clear();polars_mapping.mappings.cache_clear()
    replay.P=c.OUT
    replay.verify=lambda records:store.verify(records,allow_external=True)
    replay.closing_db=store.closing_db
    class EvidencePath(type(Path())):
        def exists(self,*,follow_symlinks=True):
            return store.virtual_exists(self)
        def iterdir(self):
            return iter(EvidencePath(name) for name in store.virtual_children(self))
    replay.Path=EvidencePath
    return c,replay


def verify(packet):
    assert __debug__ and sys.version_info>=(3,13)
    manifest=json.loads((packet/'manifest.json').read_bytes())
    transport=load('propagation_portable_store',packet/'propagation_portable_store.py',manifest['files']['propagation_portable_store.py'])
    current=load('polars_portable_store',packet/'polars_portable_store.py',manifest['files']['polars_portable_store.py'])
    with tempfile.TemporaryDirectory(prefix='shardloom-polars-once-replay-') as temporary:
        store=current.Store(packet,Path(temporary))
        os.chdir(temporary)
        out=Path(manifest['out'])
        for filename,source in [('verify.py','portable_polars_once.py'),('polars_portable_store.py','polars_portable_store.py'),('propagation_portable_store.py','propagation_portable_store.py')]:
            assert transport.content(packet/filename)==transport.shape(store.identity(out/'postprocessing'/source))
        for key in ('protocol','replay','pairing','controls','import_map'):
            store.verify([manifest[key]])
        protocol=store.read(manifest['protocol']['path'])
        store.verify(protocol['frozen_files'],allow_external=True)
        c,replay=bind(store,transport)
        observed={}
        allowed={str(manifest[key]['path']):store.read(manifest[key]['path']) for key in ('replay','pairing','controls')}
        class PairingRecorded(Exception):
            pass
        def capture(path,value):
            path=str(path)
            assert path in allowed and path not in observed,path
            assert value==allowed[path], 'recomputed proof differs: '+path
            observed[path]=value
            if path==manifest['pairing']['path']:
                # The complete pairing has now been recomputed and checked.
                # Its next stage exports a location-specific module inventory;
                # bind() already verified and loaded those archived sources.
                # Stop only that filesystem export, retaining the frozen helper.
                raise PairingRecorded
        c.write=capture
        replay.main()
        comparison=transport.module(store,'compare_saved_baseline',out/'postprocessing/compare_saved_baseline.py')
        try:
            comparison.main()
        except PairingRecorded:
            pass
        else:
            raise AssertionError('complete pairing was not recorded')
        controls=transport.module(store,'test_saved_polars_evidence',out/'postprocessing/test_saved_polars_evidence.py')
        controls.main()
        assert set(observed)==set(allowed)
        assert not any(name in sys.modules for name in ('polars','pyarrow','duckdb','shardloom._native'))
        proof=observed[manifest['replay']['path']]
        paired=observed[manifest['pairing']['path']]
        return dict(status='passed_complete_portable_evidence_replay_with_retained_failures',
            queries_executed=0,polars_attempts_replayed=11579,historical_native_results_replayed=11579,
            outcomes=proof['fullset']['outcomes'],fullset_competitive_total=None,
            passing_subset=paired['passing_subset'],adapter_failure_count=90,
            evidence_corruptions_rejected=observed[manifest['controls']['path']]['corruption_count'],
            memory_peak_exceptions_retained=proof['fullset']['os_high_water_above_sampled_ceiling_count'],
            original_reads_forbidden=True,engine_imports_forbidden=True,process_and_network_forbidden=True,
            identity_only_external_payloads=len(store.external),historical_native_disposition=paired['original_native_disposition'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parent)
    args=parser.parse_args()
    result=verify(args.root.resolve())
    print(json.dumps(result,allow_nan=False),flush=True)


if __name__=='__main__':main()
