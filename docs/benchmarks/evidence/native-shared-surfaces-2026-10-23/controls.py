"""Exercise acceptance replay using deliberately corrupted in-memory evidence."""
from __future__ import annotations

import base64
import argparse
import copy
import gzip
import hashlib
import importlib.util
import json
import lzma
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, default=ROOT.parents[3])
    parser.add_argument('--check-current-source', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    repository = args.repository.resolve()
    spec = importlib.util.spec_from_file_location('retained_surface_replay', ROOT / 'verify.py')
    replay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(replay)
    original = json.loads((ROOT / 'review.json').read_bytes())
    packet_bytes = (ROOT / 'records.json.xz').read_bytes()
    original_packet = json.loads(lzma.decompress(packet_bytes))
    accepted = replay.verify(original, packet_bytes, repository, current=args.check_current_source)
    rejected = []

    def check(name, expected, mutate_review=None, output_change=None, corrupt_packet=False):
        review = copy.deepcopy(original)
        data = packet_bytes
        if mutate_review:
            mutate_review(review)
        if output_change:
            role, query, change = output_change
            packet = copy.deepcopy(original_packet)
            ref = review['final_outputs'][role][query - 1]
            member = next(item for item in packet['files']
                          if (item['path'], item['sha256']) == (ref['path'], ref['sha256']))
            raw = member['data'].encode() if member['encoding'] == 'utf-8' else base64.b64decode(member['data'])
            envelope = json.loads(gzip.decompress(raw))
            change(envelope)
            raw = gzip.compress(json.dumps(envelope, ensure_ascii=False, allow_nan=False).encode(), mtime=0)
            member.update(bytes=len(raw), sha256=sha(raw), encoding='base64',
                          data=base64.b64encode(raw).decode())
            ref.update(bytes=len(raw), sha256=sha(raw))
            data = lzma.compress(json.dumps(packet, ensure_ascii=False, allow_nan=False).encode(), preset=0)
            review['packet'].update(bytes=len(data), sha256=sha(data))
        if corrupt_packet:
            data = data[:-1] + bytes([data[-1] ^ 1])
        try:
            replay.verify(review, data, repository, current=args.check_current_source)
        except ValueError as error:
            assert expected in str(error), (name, str(error))
            rejected.append(dict(control=name, rejected=True, reason=str(error)))
            print(name + ': rejected as expected', flush=True)
        else:
            raise AssertionError('corrupted evidence accepted: ' + name)

    def field(envelope, name, value):
        item = next(item for item in envelope['fields'] if item['key'] == name)
        item['value'] = value

    def wrong_value(envelope):
        item = next(item for item in envelope['fields'] if item['key'] in ('result_jsonl', 'result_values_json'))
        rows = ([json.loads(line) for line in item['value'].splitlines()]
                if item['key'] == 'result_jsonl' else json.loads(item['value']))
        assert len(rows) == 1 and len(rows[0]) == 1
        name = next(iter(rows[0]))
        rows[0][name] += 1
        item['value'] = ('\n'.join(json.dumps(row) for row in rows)
                         if item['key'] == 'result_jsonl' else json.dumps(rows))

    def wrong_schema(envelope):
        item = next(item for item in envelope['fields'] if item['key'] == 'result_schema_json')
        assert '"u64"' in item['value']
        item['value'] = item['value'].replace('"u64"', '"u32"')

    def wrong_clock(envelope):
        item = next(item for item in envelope['fields'] if item['key'] == 'native_query_execution_nanos')
        item['value'] = str(int(item['value']) + 1)

    check('compressed member integrity', 'packet hash', corrupt_packet=True)
    check('omitted required gate', 'validation gate scope', mutate_review=lambda review: review['gates'].pop())
    check('omitted final result', 'result coverage', mutate_review=lambda review: review['final_outputs']['cli_sql'].pop())
    check('changed complete result value', 'result differs from independent reference',
          output_change=('cli_sql', 1, wrong_value))
    check('external execution flag', 'unsafe execution evidence',
          output_change=('cli_sql', 1, lambda envelope: field(envelope, 'external_engine_invoked', 'true')))
    check('changed engine clock', 'engine clock mismatch', output_change=('cli_sql', 1, wrong_clock))
    check('changed logical result schema', 'Full43 cross-surface schemas', output_change=('cli_sql', 1, wrong_schema))
    check('missing shared aggregate provider', 'grouped provider unreachable',
          output_change=('dataframe', 28, lambda envelope: field(envelope, 'local_primitive_report_present', 'false')))
    check('missing typed scan pushdown', 'typed scan filter not pushed',
          output_change=('native_expression', 20, lambda envelope: field(envelope, 'relational_scan_rows_delivered', '5')))
    result = dict(status='passed', baseline=accepted, controls=rejected,
                  engine_executed=False, archived_code_executed=False,
                  verifier_sha256=sha((ROOT / 'verify.py').read_bytes()),
                  review_sha256=sha((ROOT / 'review.json').read_bytes()),
                  control_program_sha256=sha(Path(__file__).read_bytes()))
    if args.output is not None:
        with args.output.open('x') as stream:
            json.dump(result, stream, indent=2)
            stream.write('\n')
    print(json.dumps(dict(status='passed', rejected_corruptions=len(rejected), engine_executed=False)))


if __name__ == '__main__':
    main()
