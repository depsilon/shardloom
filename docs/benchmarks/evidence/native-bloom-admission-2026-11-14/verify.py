"""Verify saved Bloom admission observations without a runtime, checkout or queries."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import struct
import tarfile
import tomllib

BASE = '494b41cca98349b21dfabd09a129a205c6664ecb'
TREE = '14d79c67b28d265ebab0f0697d8ff221176f9747'
PREFIX = 'evidence/'
MAX_BYTES = 64 << 20


def shape(data):
    return dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        assert key not in result, 'duplicate JSON key'
        result[key] = value
    return result


def parse(data):
    return json.loads(data, object_pairs_hook=no_duplicate_keys)


def payload_json(members, name):
    return parse(members[PREFIX + name])


def verify_payload(members):
    disposition = payload_json(members, 'bloom-admission-disposition-r1.json')
    assert disposition['status'] == 'complete_finite_provider_admission_review'
    assert disposition['disposition'] == 'decline_current_unversioned_provider_before_performance'
    assert disposition['base'] == BASE and disposition['base_tree'] == TREE
    assert disposition['fresh_admission_checks'] == 28 and disposition['signed_zero_counterexamples'] == 4
    for key in ('production_runtime_changed', 'new_persisted_format', 'dependency_changed',
                'benchmark_executed', 'fallback_attempted', 'external_engine_invoked'):
        assert disposition[key] is False, key
    assert disposition['public_queries_executed'] == 0
    for key in ('performance_result', 'false_positive_rate_claim', 'pruning_or_read_saving_claim'):
        assert disposition[key] is None, key
    for name, expected in disposition['evidence'].items():
        assert shape(members[PREFIX + name]) == expected, name

    source = payload_json(members, 'bloom-probe-source-r1.json')
    executable = payload_json(members, 'bloom-probe-executable-r1.json')
    assert source == disposition['source'] and executable == disposition['executable']
    assert source['base'] == BASE and source['base_tree'] == TREE
    assert executable['source_manifest_sha256'] == shape(members[PREFIX + 'bloom-probe-source-r1.json'])['sha256']
    assert executable['build_receipt_sha256'] == shape(members[PREFIX + 'bloom-probe-build-r1.json'])['sha256']
    assert executable['expected_check_count'] == 28
    assert executable['performance_claim'] is False
    assert executable['public_queries_expected'] == executable['persisted_bloom_files_expected'] == 0
    assert source['production_source_changed'] is False
    assert source['prospective_design_sha256'] == shape(members[PREFIX + 'bloom-admission-design-r1.md'])['sha256']
    for path, expected in source['files'].items():
        archived = 'probe-source-r1/' + PurePosixPath(path).name
        assert shape(members[PREFIX + archived]) == {key: expected[key] for key in ('bytes', 'sha256')}

    for name in ('bloom-probe-build-r1.json', 'bloom-probe-observation-r1.json'):
        report = payload_json(members, name)
        assert report['status'] == 'passed' and report['returncode'] == 0
        assert report['sources_unchanged'] is True and report['process_group_drained'] is True
        assert report['head'] == BASE and report['source_tree'] == TREE
        assert report['guard_sha256'] == shape(members['supervision/guard_checks.py'])['sha256']
        for path, expected in source['files'].items():
            assert report['source_files'][path] == {key: expected[key] for key in ('bytes', 'sha256')}
    build = payload_json(members, 'bloom-probe-build-r1.json')
    assert build['command'] == ['cargo', 'build', '--offline', '--locked', '--release',
        '--features', 'release-user-surfaces', '-p', 'shardloom-vortex', '--example', 'native_bloom_admission_probe']
    observation = payload_json(members, 'bloom-probe-observation-r1.json')
    assert observation['command'] == [executable['path']]

    result = payload_json(members, 'logs/bloom-probe-observation-r1.log')
    assert result['schema'] == 'shardloom.bloom_admission_probe.v1'
    assert result['provider'] == 'vortex-layout 0.93.0'
    assert result['aggregate_id'] == 'vortex.bloom_filter.sbbf'
    assert result['passed'] is True and result['production_registration_changed'] is False
    assert result['persisted_bloom_files_written'] == result['public_queries_executed'] == 0
    assert result['performance_measured'] is False
    checks = result['checks']
    assert len(checks) == 28 and len({row['name'] for row in checks}) == 28
    assert all(row['passed'] is True for row in checks)
    values = {row['name']: row['observed'] for row in checks}
    consumed = set()

    def expect(name, expected):
        assert name not in consumed
        consumed.add(name)
        # Equality alone would allow JSON 0/1 to masquerade as false/true.
        assert json.dumps(values[name], sort_keys=True, ensure_ascii=False) == json.dumps(expected, sort_keys=True, ensure_ascii=False), name

    for name in ('default_aggregate_is_not_registered', 'default_extension_selection_i64?',
                 'default_extension_selection_utf8?', 'integer_absent_control'):
        expect(name, False)
    expect('options_round_trip', dict(block_count=256, metadata_bytes=[0, 1, 0, 0, 0, 0, 0, 0]))
    errors = ('empty_options_rejected', 'short_options_rejected', 'long_options_rejected',
              'zero_blocks_rejected', 'unknown_hash_rejected', 'incompatible_block_count_rejected')
    errors += tuple(f'partial_length_{length}_rejected' for length in (0, 31, 33, 8191, 8193))
    for name in errors:
        assert isinstance(values[name], str) and values[name].strip(), name
        consumed.add(name)
    expect('maximum_option_acceptance_is_not_memory_admission', dict(
        block_count=(1 << 32) - 1, implied_payload_bytes=((1 << 32) - 1) * 32, partial_allocated=False))
    expect('integer_accumulation_has_no_false_negatives', [[value, True] for value in (-(1 << 63), -1, 0, 42, (1 << 63) - 1)])
    for name in ('empty_state_is_empty', 'all_null_state_is_empty'):
        expect(name, dict(any_control_present=False, partial_bytes=8192))
    expect('utf8_accumulation_has_no_false_negatives', [[value, True] for value in ('', 'ID-☃', 'Case', 'case')])
    for bits, code in ((32, 'f'), (64, 'd')):
        sign = 1 << (bits - 1)
        for inserted, needle in ((0, sign), (sign, 0)):
            lhs = struct.unpack('<' + code, inserted.to_bytes(bits // 8, 'little'))[0]
            rhs = struct.unpack('<' + code, needle.to_bytes(bits // 8, 'little'))[0]
            assert lhs == rhs == 0.0 and inserted != needle
            expect(f'f{bits}_zero_bits_{inserted}_require_equality_review', dict(
                inserted_bits=inserted, needle_bits=needle, numeric_equality=True, bloom_membership=False))
    expect('null_partial_is_empty_not_missing_statistics_proof', dict(contains_42=False, inconclusive_marker=False))
    expect('same_options_union_preserves_members', dict(members=6))
    expect('saturated_partial_cannot_exclude', dict(all_controls_present=True))
    assert consumed == values.keys(), 'unverified admission observation'

    restored = payload_json(members, 'bloom-probe-restoration-r1.json')
    assert restored['head'] == BASE and restored['tree'] == TREE
    assert restored['status'] == 'passed_exact_restoration'
    assert restored['working_tree_clean'] is True and restored['probe_removed_from_runtime_source'] is True
    assert restored['probe_source_preserved'] is True and restored['production_runtime_changed'] is False
    restored_manifest = members['sources/shardloom-vortex/Cargo.toml']
    assert shape(restored_manifest)['sha256'] == restored['restored_manifest_sha256']
    added = b'[[example]]\nname = "native_bloom_admission_probe"\nrequired-features = ["vortex-file-io"]\n\n'
    compiled_manifest = members[PREFIX + 'probe-source-r1/Cargo.toml']
    assert compiled_manifest.count(added) == 1 and compiled_manifest.replace(added, b'', 1) == restored_manifest

    bloom_source = members['sources/vendor/vortex-layout/src/layouts/zoned/aggregates/bloom_filter/mod.rs']
    assert b'Do not persist `vortex.bloom_filter.sbbf` partials' in bloom_source
    assert b'change without a file format version bump' in bloom_source
    assert b'pub struct BloomFilter;' in bloom_source
    for name, keys in (('bloom-provider-inventory-r1.json', ('source_sha256', 'context_sha256')),
                       ('bloom-registration-inventory-r1.json', ('source_hashes_sha256',))):
        inventory = payload_json(members, name)
        for key in keys:
            entries = inventory[key]
            entries = entries.items() if isinstance(entries, dict) else ((entry['path'], entry['sha256']) for entry in entries)
            for path, digest in entries:
                assert shape(members['sources/' + path])['sha256'] == digest
    resource = payload_json(members, 'bloom-resource-inventory-r1.json')
    for item in resource['source_files']:
        assert shape(members['sources/' + item['file']])['sha256'] == item['sha256']
    session = payload_json(members, 'bloom-session-source-check-r1.json')
    assert shape(members['sources/Cargo.lock'])['sha256'] == session['cargo_lock_sha256']
    packages = tomllib.loads(members['sources/Cargo.lock'].decode())['package']
    locked = {(row['name'], row['version']): row.get('checksum') for row in packages if row.get('source', '').startswith('registry+')}
    for item in session['source_files']:
        package = item['crate'] + '-' + item['version']
        relative = item['path'].split('/' + package + '/', 1)[1]
        assert shape(members['dependencies/' + package + '/' + relative]) == {key: item[key] for key in ('bytes', 'sha256')}
        assert item['cached_bytes_equal_to_locked_archive'] is True
        assert locked[item['crate'], item['version']] == item['archive_sha256']
    review = payload_json(members, 'bloom-resource-primary-review-r1.json')
    for path, expected in review['additional_sources'].items():
        assert shape(members['sources/' + path]) == expected
    for path, expected in review['hash_dependency']['source_files_verified_against_archive'].items():
        assert shape(members['dependencies/twox-hash-2.1.2/' + path]) == expected
    assert locked['twox-hash', '2.1.2'] == review['hash_dependency']['locked_archive_sha256']
    for number, key, merged in ((9398, 'source', True), (8933, 'source', False), (9413, 'metadata', False)):
        upstream = payload_json(members, f'upstream-pr{number}-info-r1.json')[key]
        assert upstream['number'] == number and upstream['state'] == 'closed' and upstream['merged'] is merged
    return dict(status='passed_saved_admission_verification', disposition=disposition['disposition'],
        checks=28, signed_zero_counterexamples=4, public_queries_executed=0,
        benchmark_executed=False, production_runtime_changed=False,
        scope='saved source-bound correctness observations and admission decision; no runtime reexecution')


def read_packet(root):
    manifest = parse((root / 'manifest.json').read_bytes())
    assert manifest['schema'] == 'shardloom.bloom_admission_packet.v1'
    assert manifest['base'] == BASE and manifest['tree'] == TREE
    assert manifest['verifier'] == shape((root / 'verify.py').read_bytes())
    archive_path = root / manifest['archive']['path']
    assert archive_path.parent == root and not archive_path.is_symlink() and archive_path.is_file()
    assert archive_path.stat().st_size < 48 << 20
    assert shape(archive_path.read_bytes()) == {key: manifest['archive'][key] for key in ('bytes', 'sha256')}
    expected = manifest['members']
    assert 0 < len(expected) <= 256
    assert all(type(row['bytes']) is int and 0 <= row['bytes'] <= 16 << 20 for row in expected.values())
    assert sum(row['bytes'] for row in expected.values()) <= MAX_BYTES
    members = {}
    with tarfile.open(archive_path, 'r:xz') as archive:
        for member in archive:
            assert member.isfile() and member.name not in members and member.name in expected
            assert not member.name.startswith('/') and all(piece not in ('', '.', '..') for piece in member.name.split('/'))
            assert member.size == expected[member.name]['bytes'] and member.size <= 16 << 20
            data = archive.extractfile(member).read()
            assert shape(data) == expected[member.name]
            members[member.name] = data
    assert members.keys() == expected.keys()
    return manifest, members


def main():
    assert __debug__, 'verification requires assertions'
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet', type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    manifest, members = read_packet(args.packet.resolve())
    result = verify_payload(members)
    result.update(members=len(members), archive_sha256=manifest['archive']['sha256'])
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
