# SPDX-License-Identifier: Apache-2.0
"""Verify saved public-suite values and envelopes without running an engine."""
import gzip
import hashlib
import io
import json
import math
from pathlib import Path


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON object key: {key}')
        result[key] = value
    return result


def strict(data):
    def reject(value):
        raise ValueError(f'non-finite JSON value: {value}')
    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            reject(value)
        return number
    return json.loads(data, object_pairs_hook=unique_object, parse_constant=reject, parse_float=finite_float)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def check_rows_archive(root, reference, expected_count, expected_digest, read_bytes):
    relative = Path(reference['path'])
    assert not relative.is_absolute() and '..' not in relative.parts
    packed = read_bytes(root / relative)
    assert len(packed) == reference['bytes'] and sha(packed) == reference['sha256']
    count, digest = 0, hashlib.sha256()
    with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
        for line in stream:
            strict(line)
            digest.update(line)
            count += 1
    assert count == expected_count and digest.hexdigest() == expected_digest, relative
    return dict(path=str(relative), sha256=reference['sha256'],
                raw_sha256=digest.hexdigest(), rows=count,
                verification='retained_values_digest_and_count')


def verify(summary_path, read_bytes):
    summary_raw = read_bytes(summary_path)
    summary = strict(summary_raw)
    assert summary['status'] == 'passed' and summary['performance_claim'] is False
    cases = summary['cases']
    assert cases and len({case['name'] for case in cases}) == len(cases)
    assert all(case['status'] == 'passed' for case in cases)
    assert len(summary['families']) == len(set(summary['families']))
    checked = []
    counts = dict(compressed_json=0, complete_value_files=0, compared_rows=0,
                  retained_value_archives=0, retained_archive_rows=0, fixture_declarations=0,
                  case_value_archives=0, case_archive_rows=0,
                  successful_envelopes=0, refused_envelopes=0, other_json=0)
    # Pressure and spill producers also reference complete JSONL rows directly
    # from cases; those archives are not in compressed_json_files.
    for case in cases:
        if isinstance(case.get('complete_rows'), dict):
            reference = case['complete_rows']
            count = case.get('rows_verified', case.get('complete_output_rows'))
            digest = case['all_output_rows_sha256']
        elif isinstance(case.get('values'), dict):
            reference, count, digest = case['values'], case['complete_rows'], case['expected_sha256']
        else:
            continue
        assert isinstance(count, int) and not isinstance(count, bool) and count >= 0
        observation = check_rows_archive(summary_path.parent, reference, count, digest, read_bytes)
        checked.append(dict(case=case['name'], **observation))
        counts['case_value_archives'] += 1
        counts['case_archive_rows'] += count
    paths = set()
    for item in summary['compressed_json_files']:
        relative = Path(item['path'])
        assert not relative.is_absolute() and '..' not in relative.parts
        assert str(relative) not in paths
        paths.add(str(relative))
        packed = read_bytes(summary_path.parent / relative)
        assert len(packed) == item['bytes'] and sha(packed) == item['sha256'], relative
        raw = gzip.decompress(packed)
        assert len(raw) == item['raw_bytes'] and sha(raw) == item['raw_sha256'], relative
        payload = strict(raw)
        counts['compressed_json'] += 1
        kind = 'other_json'
        if item['raw_path'].endswith('-values.json'):
            if item['raw_path'] == 'logs/unary-predeclared-values.json':
                assert isinstance(payload, list) and payload
                assert all(isinstance(row['expected'], list) and 'schema' in row for row in payload)
                counts['fixture_declarations'] += len(payload)
                kind = 'fixture_declarations'
            elif isinstance(payload, dict) and set(payload) == {'complete_rows', 'sha256', 'values'}:
                reference = payload['values']
                observation = check_rows_archive(summary_path.parent, reference,
                                                 payload['complete_rows'], payload['sha256'], read_bytes)
                counts['retained_value_archives'] += 1
                counts['retained_archive_rows'] += observation['rows']
                kind = 'retained_values_digest_and_count'
                checked.append(observation)
            else:
                assert isinstance(payload['actual'], list) and isinstance(payload['expected'], list)
                assert payload['actual'] == payload['expected'], relative
                counts['complete_value_files'] += 1
                counts['compared_rows'] += len(payload['actual'])
                kind = 'complete_values'
        elif item['raw_path'].endswith('-envelope.json'):
            assert payload['fallback']['attempted'] is False, relative
            fields = payload['fields']
            assert isinstance(fields, list)
            assert all(isinstance(row['key'], str) for row in fields)
            assert len({row['key'] for row in fields}) == len(fields), relative
            by_key = {row['key']: row['value'] for row in fields}
            if payload['status'] == 'success':
                assert by_key['public_workflow_external_engine_invoked'] == 'false', relative
                counts['successful_envelopes'] += 1
                kind = 'successful_envelope'
            else:
                assert payload['diagnostics'] and by_key.get('result_payload_complete') != 'true', relative
                counts['refused_envelopes'] += 1
                kind = 'refused_envelope'
        else:
            counts['other_json'] += 1
        checked.append(dict(path=str(relative), raw_sha256=sha(raw), verification=kind))
    return dict(status='passed', scope='saved public-suite values, report keys, completion and no-fallback evidence; no engine execution',
                summary=dict(path=str(summary_path), bytes=len(summary_raw), sha256=sha(summary_raw)),
                families=len(summary['families']), cases=len(cases), counts=counts,
                verifier=dict(path=str(Path(__file__).resolve()), sha256=sha(Path(__file__).read_bytes())),
                checked=checked)
