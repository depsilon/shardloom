"""Replay the original and focused observations from two sealed packets, without queries."""
import argparse
from contextlib import ExitStack
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


def original_replay(parent, transport):
    manifest = parent.manifest
    out = Path(manifest['out'])
    parent.verify([manifest[key] for key in ('protocol', 'replay', 'disposition')])
    assert transport.content(parent.packet / 'verify.py') == transport.shape(parent.identity(out / 'portable_propagation.py'))
    protocol_path = Path(manifest['protocol']['path'])
    protocol = parent.read(protocol_path)
    parent.verify(protocol['frozen_files'], allow_external=True)
    validation = transport.module(parent, 'propagation_portable_validation', out / 'propagation_portable_validation.py')
    fingerprints = validation.validate(parent, protocol)
    diagnostic = validation.diagnostics(parent)
    bindings = transport.module(parent, 'propagation_portable_bind', out / 'propagation_portable_bind.py')
    support, replay = bindings.bind(parent, protocol)
    assert support.load_protocol(protocol_path) == protocol
    inputs = validation.inputs(parent, support)
    fresh = {role: replay.fresh(protocol, protocol_path, role) for role in ('baseline', 'candidate')}
    prepared = {role: replay.lifecycle(protocol, protocol_path, role) for role in ('baseline', 'candidate')}
    recomputed = replay.evaluate(protocol, fresh, prepared)
    expected = parent.read(manifest['replay']['path'])
    omitted = {'protocol', 'source_fingerprints', 'verifier', 'observations_replayed', 'lifecycle_operations_replayed'}
    assert {key: value for key, value in expected.items() if key not in omitted} == recomputed
    assert expected['source_fingerprints'] == fingerprints
    parent.verify([expected['protocol'], expected['verifier']])
    assert expected['observations_replayed'] == 23158 and expected['lifecycle_operations_replayed'] == 1080
    disposition = parent.read(manifest['disposition']['path'])
    assert disposition['disposition'] == disposition['original_replay_decision'] == recomputed['decision']
    assert disposition['original_replay_status'] == recomputed['status']
    assert disposition['fresh'] == recomputed['fresh'] and disposition['lifecycle'] == recomputed['lifecycle']
    assert not disposition['thresholds_changed'] and disposition['additional_query_executions'] == 0
    assert disposition['source_fingerprints'] == fingerprints
    assert not disposition['whole_propagation_queue_complete'] and not disposition['publication_attempted']
    for name, wanted in disposition['inputs'].items():
        assert transport.shape(parent.identity(out / name)) == wanted
    return dict(fingerprints=fingerprints, fresh=fresh, prepared=prepared, recomputed=recomputed,
        independent_inputs=inputs, historical_diagnostics=diagnostic)


def verify(packet, baseline_packet):
    assert __debug__ and sys.version_info >= (3, 13)
    os.chdir(packet)
    manifest = json.loads((packet / 'manifest.json').read_bytes())
    parent_manifest = json.loads((baseline_packet / 'manifest.json').read_bytes())
    parent_entry = load('selection_parent_entry', baseline_packet / 'verify.py', parent_manifest['files']['verify.py'])
    transport = parent_entry.load_transport(baseline_packet)
    assert transport.content(baseline_packet / 'manifest.json') == manifest['baseline_packet_manifest']
    selected_transport = load('selection_portable_store', packet / 'selection_portable_store.py',
        manifest['files']['selection_portable_store.py'])
    with ExitStack() as stack:
        parent_directory = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix='shardloom-selection-parent-')))
        current_directory = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix='shardloom-selection-current-')))
        parent = transport.Store(baseline_packet, parent_directory)
        store = selected_transport.Store(packet, current_directory, parent)
        out = Path(manifest['out'])
        assert transport.content(packet / 'verify.py') == transport.shape(store.identity(out / 'postprocessing/portable_selection.py'))
        assert transport.content(packet / 'selection_portable_store.py') == transport.shape(store.identity(out / 'postprocessing/selection_portable_store.py'))
        sys.meta_path.insert(0, transport.NoEngineImports())
        parent_entry.forbid_original_reads(store)
        original = original_replay(parent, transport)
        protocol_path = Path(manifest['protocol']['path'])
        store.verify([manifest['protocol'], manifest['replay'], manifest['disposition']])
        protocol = store.read(protocol_path)
        store.verify(protocol['frozen_files'], allow_external=True)
        transport_proof = transport.module(store, 'selection_transport_proof', out / 'postprocessing/selection_transport_proof.py')
        rebindings = transport_proof.validate(store, protocol)
        validation = transport.module(store, 'selection_portable_validation', out / 'postprocessing/selection_portable_validation.py')
        fingerprint = validation.candidate(store, protocol)
        reused = validation.baseline_reuse(store, parent, protocol, original)
        bindings = transport.module(store, 'selection_portable_bind', out / 'postprocessing/selection_portable_bind.py')
        support, replay = bindings.bind(store, protocol)
        assert support.load_protocol(protocol_path) == protocol
        observed = dict(baseline=reused['baseline'], candidate=replay.fresh(protocol, protocol_path, 'candidate'))
        prepared = dict(baseline=reused['prepared'], candidate=replay.lifecycle(protocol, protocol_path, 'candidate'))
        recomputed = replay.evaluate(protocol, observed, prepared)
        expected = store.read(manifest['replay']['path'])
        omitted = {'protocol', 'source_fingerprints', 'verifier', 'fresh_packets_replayed',
            'lifecycle_operations_replayed', 'baseline_reuse', 'original_gate_unchanged', 'baseline_choice'}
        assert {key:value for key,value in expected.items() if key not in omitted} == recomputed
        fingerprints = dict(baseline=original['fingerprints']['candidate'], candidate=fingerprint)
        assert expected['source_fingerprints'] == fingerprints
        assert expected['fresh_packets_replayed'] == 4380 and expected['lifecycle_operations_replayed'] == 1080
        assert expected['original_gate_unchanged']
        assert expected['baseline_choice'] == 'separate maintainer retention decision'
        assert expected['baseline_reuse'] == protocol['baseline_observations']
        store.verify([expected['protocol'], expected['verifier'], expected['baseline_reuse']])
        disposition = validation.disposition(store, recomputed, fingerprints)
        assert not any(name in sys.modules for name in ('shardloom._native', 'polars', 'pyarrow', 'duckdb'))
        return dict(status='passed_complete_portable_replay', query_executions=0,
            original_fresh_observations_replayed=23158, original_prepared_operations_replayed=1080,
            focused_new_fresh_observations_replayed=2190, focused_new_prepared_operations_replayed=540,
            focused_reused_fresh_results=2190, focused_reused_prepared_operations=540,
            fresh=recomputed['fresh'], lifecycle=recomputed['lifecycle'], decision=recomputed['decision'],
            required_work_complete=recomputed['required_work_complete'], source_fingerprints=fingerprints,
            original_gate_unchanged=True, original_decision=original['recomputed']['decision'],
            recorded_post_query_archive_rebindings=rebindings,
            disposition=disposition, original_reads_forbidden=True,
            network_and_process_execution_forbidden=True, causal_speedup_claim=False,
            external_payloads_not_reopened=len(store.external))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--baseline-packet', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    packet = args.root.resolve()
    baseline = (args.baseline_packet or packet.parent / 'native-optimization-propagation-2026-11-15').resolve()
    result = verify(packet, baseline)
    if args.output:
        with args.output.open('x') as stream:
            json.dump(result, stream, indent=2); stream.write('\n')
    print(json.dumps(result, allow_nan=False))


if __name__ == '__main__':
    main()
