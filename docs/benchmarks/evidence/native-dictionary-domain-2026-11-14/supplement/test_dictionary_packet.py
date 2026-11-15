"""Verify relocated replay and corruptions without reading original experiment files."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def identity(path):
    with path.open('rb') as stream:
        digest=hashlib.file_digest(stream,'sha256').hexdigest()
    return dict(bytes=path.stat().st_size,sha256=digest)


def main():
    if not __debug__:
        raise RuntimeError('negative controls require assertions')
    out=Path(__file__).resolve().parent
    packet=out/'packet-r1'
    destination=out/'dictionary-packet-controls-r1.json'
    assert not destination.exists()
    original={p.name:identity(p) for p in packet.iterdir() if p.is_file()}
    controls=[]

    def rejected(name,operation,boundary):
        try:
            operation()
        except (AssertionError,KeyError,ValueError,TypeError,RuntimeError) as error:
            controls.append(dict(name=name,status='rejected',boundary=boundary,error=type(error).__name__))
        else:
            raise AssertionError('corrupt evidence accepted: '+name)

    disabled=subprocess.run([sys.executable,'-I','-B','-O',str(packet/'verify.py')],
        capture_output=True,text=True,timeout=30)
    assert disabled.returncode!=0 and 'assertions must remain enabled' in disabled.stderr
    controls.append(dict(name='disabled_assertions',status='rejected',boundary='entrypoint',error='RuntimeError'))
    with tempfile.TemporaryDirectory(prefix='dictionary-relocated-controls-') as temporary:
        relocated=Path(temporary)/'packet'
        shutil.copytree(packet,relocated)
        # Forbid original evidence, source and runtime reads at the interpreter
        # boundary. The archive and temporary extraction are the only task data.
        runner='''import os,runpy,sys
blocked=('/Users/dylan/LocalData/shardloom/','/Users/dylan/Documents/shardloom-local-repo/','/Users/dylan/.cache/shardloom/')
def audit(event,args):
 if event=='open' and isinstance(args[0],(str,bytes,os.PathLike)):
  path=os.fsdecode(os.fspath(args[0]))
  if os.path.abspath(path).startswith(blocked):
   raise RuntimeError('original source/runtime/evidence read forbidden: '+path)
 if event in ('socket.connect','subprocess.Popen','os.system'):
  raise RuntimeError('execution or network forbidden during portable replay')
sys.addaudithook(audit)
sys.argv=[sys.argv[1]]
runpy.run_path(sys.argv[0],run_name='__main__')
'''
        full=subprocess.run([sys.executable,'-I','-B','-c',runner,str(relocated/'verify.py')],
            capture_output=True,text=True,timeout=1800,cwd=temporary)
        with (out/'dictionary-packet-relocated-r1.json').open('x') as stream:
            json.dump(dict(returncode=full.returncode,stdout=full.stdout,stderr=full.stderr,
                original_paths_forbidden=True,working_directory='disposable directory outside original source'),stream,indent=2)
            stream.write('\n')
        assert full.returncode==0,full.stderr[-6000:]
        positive=json.loads(full.stdout)
        assert positive['status']=='passed_complete_portable_replay' and positive['query_executions']==0
        spec=importlib.util.spec_from_file_location('dictionary_portable_verifier',packet/'verify.py')
        verifier=importlib.util.module_from_spec(spec);spec.loader.exec_module(verifier)
        extract=Path(temporary)/'extracted';extract.mkdir()
        store=verifier.Store(packet,extract)
        manifest=store.manifest
        protocol=store.read(manifest['protocol']['path'])
        for name in ('../escape','/escape','a//b','a\\b',''):
            rejected('unsafe_archive_name_'+repr(name),lambda name=name:verifier.safe_name(name),'archive_path')
        rejected('unbundled_read',lambda:store.path(out/'not-bundled'),'archive_identity')
        verifier.transport_parts(manifest)
        for label,mutation in (
            ('missing_part',lambda m:m['archive']['parts'].pop()),
            ('changed_transport_size',lambda m:m['archive'].__setitem__('bytes',m['archive']['bytes']+1)),
            ('missing_archive_digest',lambda m:m['archive'].pop('sha256')),
            ('wrong_archive_format',lambda m:m['archive'].__setitem__('format','unchecked')),
        ):
            changed=copy.deepcopy(manifest);mutation(changed)
            rejected(label,lambda changed=changed:verifier.transport_parts(changed),'archive_transport')
        tiny=Path(temporary)/'transport';tiny.mkdir()
        data=b'complete transport evidence\n'
        names=['evidence.tar.gz.part-001','evidence.tar.gz.part-002']
        for name,value in zip(names,(data[:9],data[9:]),strict=True):
            (tiny/name).write_bytes(value)
        tiny_manifest=dict(files={name:identity(tiny/name) for name in names},
            archive=dict(format='concatenated_gzip_tar',parts=names,bytes=len(data),sha256=hashlib.sha256(data).hexdigest()))
        reordered=copy.deepcopy(tiny_manifest);reordered['archive']['parts'].reverse()
        rejected('reordered_parts',lambda:verifier.transport_parts(reordered),'archive_transport')
        def open_transport(value):
            with verifier.archive_stream(tiny,value,tiny) as stream:
                assert stream.read()==data
        open_transport(tiny_manifest)
        changed=copy.deepcopy(tiny_manifest);changed['archive']['sha256']='0'*64
        rejected('changed_complete_archive_digest',lambda:open_transport(changed),'archive_transport')
        (tiny/names[1]).write_bytes(b'X'+data[10:])
        rejected('changed_archive_bytes',lambda:open_transport(tiny_manifest),'archive_transport')
        external=next(iter(store.external.values()))['identity']
        rejected('external_payload_read',lambda:store.path(external['path']),'external_identity_only')
        rejected('external_identity_not_explicitly_allowed',lambda:store.verify([external]),'external_identity_only')
        changed=copy.deepcopy(external);changed['sha256']='0'*64
        rejected('changed_external_identity',lambda:store.verify([changed],allow_external=True),'external_identity_only')
        source=store.path(out/'dictionary_support.py');before=source.read_bytes()
        source.write_bytes(before+b'\n# corruption\n')
        rejected('changed_source_member',lambda:store.identity(out/'dictionary_support.py'),'source_identity')
        source.write_bytes(before)
        sys.path.insert(0,str(source.parent))
        sys.meta_path.insert(0,verifier.NoEngineImports())
        subprocess.Popen=verifier.no_process
        literal_path=store.path(Path(manifest['worktree'])/'python/tests/native_dictionary_text_cases.py')
        literal_spec=importlib.util.spec_from_file_location('native_dictionary_text_cases',literal_path)
        literal=importlib.util.module_from_spec(literal_spec)
        sys.modules[literal_spec.name]=literal
        literal_spec.loader.exec_module(literal)
        import dictionary_support as support
        assert Path(support.dictionary_cases.literals.__file__).resolve()==literal_path.resolve()
        import dictionary_observations as observations
        from query_surface.expanded_freeze import unpack
        from query_surface.expanded_report import raw_files
        from dictionary_transport import worker_outcome
        verifier.validation(store,protocol)

        class ChangedStore:
            def __init__(self,path,mutation):
                self.path,self.mutation=str(path),mutation
            def __getattr__(self,key):
                return getattr(store,key)
            def read(self,path):
                value=copy.deepcopy(store.read(path))
                if str(path)==self.path:
                    self.mutation(value)
                return value

        for label,path,mutation in (
            ('missing_required_test_receipt',manifest['validation']['path'],lambda x:x['checks'].pop('dictionary-final-r2-native-tests.json')),
            ('filtered_native_tests',out/'dictionary-final-r2-native-tests.json',lambda x:x['command'].insert(2,'one_test')),
            ('unreaped_validation_process',out/'dictionary-final-r2-native-tests.json',lambda x:x.__setitem__('direct_child_reaped',False)),
            ('validation_source_changed',out/'dictionary-final-r2-native-tests.json',lambda x:x.__setitem__('sources_unchanged',False)),
            ('wrong_installed_test_total',out/'dictionary-final-r1-installed-wheel/integration.json',lambda x:x.__setitem__('tests',449)),
            ('wrong_public_test_total',out/'dictionary-public-r1-result.json',lambda x:x.__setitem__('tests',6)),
            ('missing_provider_validation',manifest['validation']['path'],lambda x:x['checks'].pop('dictionary-final-r1-layout-provider.json')),
            ('failed_provider_test',out/'dictionary-layout-provider-r1/report.json',lambda x:x['commands'][0].__setitem__('returncode',1)),
            ('changed_provider_test_configuration',out/'dictionary-layout-provider-r1/report.json',lambda x:x['commands'][0]['command'].remove('--locked')),
            ('changed_runtime_fingerprint',protocol['runtimes']['candidate']['manifest']['path'],lambda x:x.__setitem__('source_fingerprint_sha256','0'*64)),
        ):
            changed=ChangedStore(path,mutation)
            rejected(label,lambda changed=changed:verifier.validation(changed,protocol),'source_bound_validation')
        target=Path(protocol['targets']['candidate'])/'attempts.sqlite'
        selected=next(row['case'] for row in protocol['cases'] if row['kind']=='dictionary_v1'
            and row['case']['profile']=='dictionary_text_work_dictionary'
            and row['case']['requirement']=='normalized_group' and row['case']['surface']=='python_sql')
        with store.closing_db(target,readonly=True) as database:
            blob=database.execute('SELECT packet FROM attempt WHERE id=?',(selected['id'],)).fetchone()[0]
        raw=unpack(blob);files=raw_files(raw)
        job,outcome=json.loads(files['one.job.json']),worker_outcome(raw['supervision'],files['one.stdout.json'])
        report=outcome['observation'];observations.work(report,job)
        def field(value,key,text):
            value['fields'][key]=text
            underlying=value['raw_report']['fields']
            if isinstance(underlying,list):
                entry=next(x for x in underlying if x['key']==key);entry['value']=text
            else:
                underlying[key]=text
        for label,mutation in (
            ('incomplete_consumption',lambda r:r.__setitem__('complete_consumption',False)),
            ('negative_engine_clock',lambda r:r['clocks'].__setitem__('engine_execution_seconds',-1)),
            ('unreported_engine_clock',lambda r:r['clocks'].__setitem__('engine_timing_reported',False)),
            ('external_fallback',lambda r:field(r,'external_engine_invoked','true')),
            ('denied_memory',lambda r:field(r,'execution_resource_observed_native_denied_reservations','1')),
            ('unexplained_spill',lambda r:field(r,'execution_resource_observed_spill_bytes','1')),
            ('memory_exceeds_grant',lambda r:field(r,'execution_resource_observed_native_peak_reserved_bytes',str(job['memory_bytes']+1))),
        ):
            changed=copy.deepcopy(report);mutation(changed)
            rejected(label,lambda changed=changed:observations.work(changed,job),'completed_native_report')
        failed=copy.deepcopy(raw);failed['supervision']['returncode']=1
        rejected('successful_fields_from_failed_process',lambda:observations.successful_packet(failed,files),'process_completion')
        assert not any(name in sys.modules for name in ('shardloom._native','polars','pyarrow','duckdb'))
    assert original=={p.name:identity(p) for p in packet.iterdir() if p.is_file()}
    assert len(controls) == len({row['name'] for row in controls}) == 36
    result=dict(status='passed',controls_rejected=len(controls),controls=controls,
        relocated_replay=positive,original_source_and_runtime_reads_forbidden=True,
        query_executions=0,original_packet_unchanged=True,helper=identity(Path(__file__)),manifest=original['manifest.json'])
    with destination.open('x') as stream:
        json.dump(result,stream,indent=2);stream.write('\n')
    print(json.dumps(dict(status='passed',controls_rejected=len(controls),query_executions=0)))


if __name__=='__main__':
    main()
