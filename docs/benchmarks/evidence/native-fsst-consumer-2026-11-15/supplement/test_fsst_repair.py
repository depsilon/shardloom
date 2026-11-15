"""Reject changes to the repaired helper provenance without native execution."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile


def main():
    assert __debug__
    out = Path(__file__).resolve().parent
    packet = out / 'fsst-packet-r2'
    output = out / 'fsst-repair-controls-r1.json'
    assert not output.exists()
    spec = importlib.util.spec_from_file_location('fsst_repair_controls', packet / 'verify.py')
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    before = {path.name: verifier.content(path) for path in packet.iterdir() if path.is_file()}
    controls = []
    with tempfile.TemporaryDirectory(prefix='fsst-repair-controls-') as temporary:
        store = verifier.Store(packet, Path(temporary))
        positive = verifier.validate_repair(store)
        root = Path(store.manifest['worktree'])
        source = out.parent / 'native-source-domain-facts-2026-11-14'
        dictionary = out.parent / 'native-dictionary-domain-2026-11-14'

        class ChangedStore:
            def __init__(self, path, change):
                self.path_to_change, self.change = str(path), change
            def __getattr__(self, key):
                return getattr(store, key)
            def read(self, path):
                value = store.read(path)
                if str(path) == self.path_to_change:
                    self.change(value)
                return value

        def changed_module(value):
            row = next(row for row in value['archive_members'].values()
                if row['origin'] == str(dictionary / 'dictionary_observations.py'))
            row['identity']['sha256'] = '0' * 64

        for name, path, change in (
            ('unbound_prior_controls', source / 'source-facts-closeout-r1.json',
                lambda value: value['inputs']['source-facts-packet-controls-r1.json'].__setitem__('sha256', '0' * 64)),
            ('failed_prior_controls', source / 'source-facts-packet-controls-r1.json',
                lambda value: value.__setitem__('status', 'failed')),
            ('unbound_prior_manifest', dictionary / 'dictionary-packet-controls-r1.json',
                lambda value: value['manifest'].__setitem__('sha256', '0' * 64)),
            ('changed_prior_shared_module', root / 'docs/benchmarks/evidence/native-dictionary-domain-2026-11-14/packet/manifest.json', changed_module),
            ('hidden_original_replay_failure', out / 'fsst-packet-relocated-r1.json',
                lambda value: value.__setitem__('returncode', 0)),
            ('changed_original_archive', out / 'fsst-packet-r1/manifest.json',
                lambda value: value['archive'].__setitem__('sha256', '0' * 64)),
            ('missing_required_import', out / 'fsst-import-closure-r1.json',
                lambda value: value.__setitem__('loaded', [row for row in value['loaded'] if row['module'] != 'source_facts_observations'])),
            ('changed_loaded_helper', out / 'fsst-import-closure-r1.json',
                lambda value: value['loaded'][0]['identity'].__setitem__('sha256', '0' * 64)),
        ):
            changed = ChangedStore(path, change)
            try:
                verifier.validate_repair(changed)
            except (AssertionError, KeyError, ValueError, TypeError, RuntimeError) as error:
                controls.append(dict(name=name, status='rejected', error=type(error).__name__))
            else:
                raise AssertionError('corrupt repair provenance accepted: ' + name)
    assert before == {path.name: verifier.content(path) for path in packet.iterdir() if path.is_file()}
    result = dict(status='passed_repair_provenance_controls', query_executions=0,
        controls=controls, count=len(controls), positive=positive, original_packet_unchanged=True,
        manifest=verifier.content(packet / 'manifest.json'), helper=verifier.content(Path(__file__)))
    with output.open('x') as stream:
        json.dump(result, stream, indent=2); stream.write('\n')
    print(json.dumps(dict(status=result['status'], controls=len(controls), query_executions=0)))


if __name__ == '__main__':
    main()
