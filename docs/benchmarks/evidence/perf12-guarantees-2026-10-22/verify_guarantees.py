"""Verify retained guarantee bindings without executing engines or archived code."""
import argparse
import ast
import gzip
import importlib.util
import json
import lzma
from pathlib import Path
import re

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location(
    'perf12_packet', HERE.parent / 'perf12-comparisons-2026-10-22/verify_packet_929.py')
packet_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packet_module)
Evidence, digest, require, strict_json = (
    packet_module.Evidence, packet_module.digest, packet_module.require, packet_module.strict_json)

CLAIMS = ('engine_changes', 'new_runtime_test_execution', 'new_benchmark_run',
          'independent_full43_parity', 'whole_process_rss_ceiling', 'group_atomic_fanout',
          'partial_query_resume', 'windows_native_extension', 'cross_platform_full_matrix',
          'broad_engine_parity', 'performance_superiority', 'production_certification', 'new_release')
CHILDREN = {'sampling_population_recovery_process_child', 'subquery_membership_recovery_process_child'}
DIAGNOSTICS = {'nested_capture_is_rejected_without_corrupting_outer_capture'}
ACTIONS = ('selected_semantics_and_lifecycle', 'selected_resource_ownership',
           'independent_full43_reference', 'selected_installed_support')
TERMINAL = re.compile(r'\btest ([\w:]+) \.\.\. (ok|FAILED|ignored)(?:\s|$)')


def unique(rows, key, reason):
    result = {row[key]: row for row in rows}
    require(len(result) == len(rows), reason)
    return result


def verify(review, packet, repository=None):
    evidence = Evidence(packet)
    require(review['schema'] == 'shardloom.perf12.selected_guarantee_acceptance.v1'
            and review['status'] == 'accepted_finite_followup_review_with_explicit_limits',
            'acceptance scope differs')
    require(review['claims'] == dict.fromkeys(CLAIMS, False), 'unearned guarantee claim')
    require(review['required_children_closed'] == review['optional_rows_promoted']
            == review['competitive_gates_closed'] == [], 'unearned owner closure or scope promotion')
    for reference in review['supporting_records'] + review['retained_published_proofs']:
        evidence.check(reference)

    def record(suffix):
        refs = [r for r in review['supporting_records'] if r['path'].endswith('/' + suffix)]
        require(len(refs) == 1, 'missing or duplicate supporting record: ' + suffix)
        return evidence.read(evidence.check(refs[0]))

    prior = evidence.read(evidence.check(review['prior_comparison_acceptance']))
    require([a['id'] for a in prior['required_actions']] == list(ACTIONS), 'original action set differs')
    expected_actions = [dict(id=a['id'], owners=a['owners'], engine_change_required_by_this_review=False,
        status='accepted_missing_reference_disposition' if a['id'] == 'independent_full43_reference'
        else 'accepted_selected_existing_evidence') for a in prior['required_actions']]
    require(review['completed_actions'] == expected_actions, 'follow-up action disposition differs')
    merge = record('pr1587-merge-acceptance-937.json')
    checks = record('pr1587-checks-before-merge-937.json')
    current = review['source_commit']
    tested = review['tested_source_commit']
    hosted = review['hosted_source_commit']
    published = review['published_runtime_source_commit']
    require(current == merge['merge']['sha'] and tested == merge['expected_head'] == checks['head']
            and review['source_tree'] == merge['merged_tree'] == checks['expected_merge_tree'],
            'source and completed run identities differ')
    require(review['identical_git_trees'] == dict.fromkeys(
        [current, tested, review['test_checkout_commit']], review['source_tree']), 'test checkout tree differs')
    require(len(checks['checks']) == 44 and all(c['head_sha'] == tested and c['status'] == 'completed'
            and c['conclusion'] == 'success' for c in checks['checks']), 'completed checks differ')

    files = unique(review['source_files'], 'repository_path', 'duplicate source file')
    for name, row in files.items():
        raw = evidence.raw(evidence.check(row['file']))
        role = row['role']
        require(role in ('rust_test_or_helper', 'python_test_or_harness', 'direct_embedded_fixture'),
                'unknown source role')
        expected_commits = [current, tested, hosted] + ([] if role == 'python_test_or_harness' else [published])
        require(row['git_source_sha256'] == dict.fromkeys(expected_commits, digest(raw)),
                'whole source file commit binding differs: ' + name)
        require(not Path(name).is_absolute() and '..' not in Path(name).parts, 'unsafe repository source path')
        if repository is not None:
            require((repository / name).read_bytes() == raw, 'current source changed: ' + name)

    logs = {}
    require({row['job']['id'] for row in review['rust_logs']} == {119915545452, 119915545577}
            and len(review['rust_logs']) == 2, 'Rust job set differs')
    for row in review['rust_logs']:
        raw = gzip.decompress(evidence.raw(evidence.check(row['file'])))
        require(digest(raw) == row['raw_sha256'], 'raw Rust log changed')
        lines = raw.decode().splitlines()
        require(len(lines) == row['line_count'], 'Rust log length differs')
        require(row['job'] in checks['checks'] and row['run_id'] == 39804670420,
                'Rust job is not a completed source-bound check')
        require(any(line.endswith(review['test_checkout_commit']) for line in lines),
                'Rust log checkout identity missing')
        logs[row['file']['path']] = lines

    base = record('perf12-required-current-run-bindings-937.json')
    semantics = record('perf12-selected-guarantee-review-936.json')
    writers = record('perf12-writer-fidelity-source-inventory-936-v2.json')
    recovery = record('recovery-source-inventory-938.json')
    shared = record('recovery-shared-families-inventory-938.json')
    expected = {r['test_function']: r['source_path'] for r in base['bindings']}
    expected.update({name: r['path'] for name, r in semantics['definitions'].items()})
    expected.update({r['test_name']: r['source_path'] for r in writers['definitions']})
    expected.update({r['test_function']: r['source_path'] for r in shared['entries']})
    selectors = unique(review['rust_selectors'], 'test_function', 'duplicate Rust selector')
    require({name: row['source_path'] for name, row in selectors.items()} == expected
            and len(selectors) == 183, 'selected Rust inventory differs')
    terminal_observations = {}
    for path, lines in logs.items():
        for number, line in enumerate(lines, 1):
            match = TERMINAL.search(line)
            if match:
                terminal_observations.setdefault(match[1].split('::')[-1], []).append(
                    dict(log=path, line=number, test_id=match[1], text=line, status=match[2]))
    observed_count = 0
    for name, row in selectors.items():
        file = files[row['source_path']]['file']
        raw = evidence.raw(file['path'])
        require(row['source_sha256'] == file['sha256'], 'test source identity differs')
        locations = [i for i, line in enumerate(raw.decode().splitlines(), 1)
                     if re.match(r'\s*fn ' + re.escape(name) + r'\(', line)]
        require(locations == [row['definition_line']], 'test definition location differs')
        wanted_role = 'process_child_support' if name in CHILDREN else 'diagnostic_capture' if name in DIAGNOSTICS else 'regression'
        require(row['role'] == wanted_role, 'test support role promoted to guarantee')
        observed = terminal_observations.get(name, [])
        require(observed and all(o['status'] == 'ok' for o in observed)
                and row['observations'] == observed, 'exact terminal test observations differ')
        observed_count += len(observed)

    # Check the leaf inventories against retained whole files, rather than accepting
    # their self-reported verification status as a substitute for source evidence.
    for inventory, source_key in [(recovery, 'function_source_exact'), (shared, 'exact_function_source')]:
        for row in inventory['entries']:
            raw = evidence.raw(files[row['source_path']]['file']['path'])
            require(digest(raw) == row['source_sha256'], 'recovery inventory source changed')
            lines = raw.decode().splitlines()
            start, end = row['span_lines']
            require('\n'.join(lines[start - 1:end]) == row[source_key].rstrip('\n'),
                    'recovery function span changed')
            for helper in row.get('same_file_local_called_function_definitions', []):
                start, end = helper['span_lines']
                require('\n'.join(lines[start - 1:end]) == helper['exact_source'].rstrip('\n'),
                        'recovery helper span changed')
            observations = row.get('observations', [dict(log_path=shared['log_path'],
                line=row.get('observed_log_line', {}).get('line'),
                exact_line=row.get('observed_log_line', {}).get('exact_text'), status='ok')])
            for observed in observations:
                require(observed['status'] == 'ok' and
                        logs[observed['log_path']][observed['line'] - 1] == observed['exact_line'],
                        'recovery inventory log observation changed')

    python_source = record('perf12-selected-regression-sources-933.json')
    py = unique(review['python_selectors'], 'method', 'duplicate Python selector')
    require(set(py) == {r['method'] for r in python_source['examples']} and len(py) == 20,
            'selected Python inventory differs')
    for name, row in py.items():
        text = evidence.raw(files[row['source_path']]['file']['path']).decode()
        classes = [n for n in ast.parse(text).body if isinstance(n, ast.ClassDef) and n.name == row['class']]
        require(len(classes) == 1, 'Python class missing or duplicated')
        methods = [n for n in classes[0].body if isinstance(n, ast.FunctionDef) and n.name == name]
        require(len(methods) == 1, 'Python method missing or duplicated')
        method = methods[0]
        require([method.lineno, method.end_lineno] == [row['start_line'], row['end_line']]
                and digest(ast.get_source_segment(text, method).encode()) == row['method_sha256'],
                'Python method source differs')

    installed = record('perf12-hosted-selector-bindings-934.json')
    lanes = unique(review['installed_lanes'], 'lane', 'duplicate installed lane')
    require(set(lanes) == {'py310_ubuntu', 'py313_ubuntu', 'py313_macos'}, 'installed lane set differs')
    require(review['installed_lanes'] == installed['lanes'], 'installed source observation changed')
    normal_summary = dict(tests=121, failures=0, errors=0, skipped=0, status='passed')
    for lane in lanes.values():
        require(lane['head_sha'] == hosted and lane['run_id'] == 39793398084, 'installed run identity differs')
        refs = lane['retained_artifact_file_identities']
        for ref in refs.values():
            evidence.check(ref)
        build = evidence.read(refs['build']['path'])
        report = evidence.read(refs['proof_report']['path'])
        receipt = evidence.read(refs['retention_receipt']['path'])
        require(all(build[k] == v for k, v in lane['build_identity'].items())
                and build['published'] is False and build['status'] == 'passed', 'development build differs')
        require(receipt['head_sha'] == hosted and receipt['run_id'] == lane['run_id']
                and receipt['artifact_id'] == lane['artifact_id'] and receipt['lane'] == lane['lane']
                and receipt['archive_persisted'] is False and receipt['binary_members_extracted'] is False,
                'retained artifact origin differs')
        require(evidence.read(refs['integration_summary']['path']) == report['integration'] == normal_summary,
                'installed test counts or skips differ')
        identity = lane['proof_identity']
        require(report['status'] == identity['status'] == 'passed'
                and report['installed_load'] == identity['installed_load']
                and report['platform'] == identity['platform'] and report['python'] == identity['python']
                and report['scope'] == identity['scope']
                and all(report['integrity'][k] == v for k, v in identity['integrity'].items())
                and report['publication_attempted'] is False, 'installed load or package identity differs')
        lines = evidence.raw(refs['integration_log']['path']).decode().splitlines()
        tests = lane['normal_integration']['tests']
        require(len(tests) == 121 and len({t['test_id'] for t in tests}) == 121,
                'installed test names missing or duplicated')
        for test in tests:
            require(lines[test['line_number'] - 1] == test['raw_line']
                    and test['raw_line'] == test['test_id'] + ' ... ok'
                    and test['observed_result'] == 'ok', 'installed test line differs')
        selected = unique(lane['selected_examples'], 'method', 'duplicate installed selector binding')
        require(set(selected) == set(py), 'installed selected-method coverage differs')
        for name, row in selected.items():
            require(row['method_source_sha256'] == py[name]['method_sha256']
                    and row['source_class'] == py[name]['class'] and row['observation'] in tests,
                    'installed selector source or run differs')
        opt_in = lane['opt_in_output_window']
        require(opt_in['has_per_test_ids_or_log'] is False
                and opt_in['separate_from_normal_121_test_integration'] is True
                and evidence.read(refs['opt_in_output_window_summary']['path']) == opt_in['summary']
                == dict(tests=13, failures=0, errors=0, skipped=0, status='passed'),
                'opt-in summary promoted or changed')

    groups = unique(review['guarantees'], 'id', 'duplicate guarantee review')
    expected_ids = {g['id'] for g in semantics['guarantee_reviews']} | {
        'typed_writer_fidelity', 'spill_generation_failure_and_fresh_restart', 'installed_public_workflow_values_and_owners'}
    require(set(groups) == expected_ids, 'guarantee review coverage differs')
    for group in groups.values():
        require(group['status'] == 'accepted_for_selected_workload_review' and group['limit']
                and group['observed_contract'] and group['tests'], 'guarantee disposition missing')
        require(set(group['tests']) <= (set(selectors) | set(py))
                and not (set(group['tests']) & (CHILDREN | DIAGNOSTICS)),
                'unbound or support-only test promoted to a guarantee')
    for group in semantics['guarantee_reviews']:
        require(groups[group['id']] == {**group, 'status': 'accepted_for_selected_workload_review'},
                'reviewed semantic contract changed')
    require(groups['typed_writer_fidelity']['tests'] == [r['test_name'] for r in writers['definitions']],
            'writer review coverage differs')
    require(groups['spill_generation_failure_and_fresh_restart']['tests'] == [r['test_function'] for r in
        recovery['entries'] if r['test_function'] not in CHILDREN | DIAGNOSTICS] +
        [r['test_function'] for r in shared['entries']], 'failure and recovery review coverage differs')
    require(groups['installed_public_workflow_values_and_owners']['tests'] == list(py),
            'installed workflow review coverage differs')

    full = review['full43']
    disposition = record('perf12-full43-reference-disposition-935.json')
    candidates = record('perf12-full43-candidates-primary-937.json')
    require(full['status'] == disposition['status'] == 'missing_independent_reference_for_current_acceptance'
            and full['disposition'] == disposition['disposition']
            and full['reference_kind'] == 'retained_native_regression'
            and full['independent_correctness_oracle'] is False
            and full['historical_payload_parity_claimed'] is False
            and full['exhaustive_historical_search_claimed'] is False,
            'Full43 reference boundary changed')
    by_name = unique(full['reference_members'], 'archive_member', 'duplicate Full43 reference member')
    require(set(by_name) == {r['archive_member'] for r in disposition['bound_members']}, 'Full43 member set differs')
    retained = {}
    for original in disposition['bound_members']:
        ref = by_name[original['archive_member']]['file']
        require(ref == dict(path=original['original_path'], bytes=original['bytes'], sha256=original['sha256']),
                'Full43 source member identity differs')
        retained[Path(ref['path']).name] = evidence.read(evidence.check(ref))
    reference = retained['clickbench-reference-values-2.json']
    accepted = retained['format-strategy-full43-independent-verification-908.json']
    summary = retained['summary.json']
    require(reference['independent_correctness_oracle'] is False
            and reference['reference_kind'] == accepted['reference_kind'] == summary['reference_kind']
            == 'retained_native_regression', 'native Full43 evidence relabelled')
    require(accepted['queries'] == 43 and accepted['repeats'] == 1 and accepted['rows'] == 381
            and summary['complete'] is True and summary['queries_passed'] == 43
            and summary['repeats'] == 1, 'retained Full43 observation changed')
    require(full['historical_candidate_count'] == candidates['candidate_count'] == 6
            and full['historical_indexed_outputs'] == candidates['indexed_query_outputs'] == 258,
            'finite historical audit extent differs')
    historical = unique(full['historical_sources'], 'candidate_id', 'duplicate historical candidate')
    require(set(historical) == {r['candidate_id'] for r in candidates['candidates']}, 'historical candidate set differs')
    for row in candidates['candidates']:
        source = historical[row['candidate_id']]
        for kind in ('document', 'machine_record'):
            require(source[kind]['path'] == row[kind]['path'] and source[kind]['sha256'] == row[kind]['sha256'],
                    'historical source identity differs')
            evidence.check(source[kind])
        lines = evidence.raw(source['document']['path']).decode().splitlines()
        require(source['reference_excerpts'] == row['document']['native_reference_excerpts'], 'historical excerpt set differs')
        for excerpt in source['reference_excerpts']:
            require(lines[excerpt['line'] - 1] == excerpt['text'], 'historical reference excerpt differs')
        require(row['payload_contents_inspected'] is False and row['historical_query_fixture_binding_accepted'] is False,
                'historical indexed outputs promoted to payload parity')
    return dict(status='passed_selected_guarantee_bindings', completed_followup_reviews=4,
                rust_selectors=183, ordinary_rust_selectors=180, rust_terminal_observations=observed_count,
                python_methods=20, installed_lanes=3, selected_installed_observations=60,
                source_files=len(files), packet_members=len(evidence.members),
                independent_full43_reference='missing_explicit_disposition',
                current_source_checked=repository is not None, runtime_executed=False)


def load(directory=HERE):
    manifest = strict_json((directory / 'manifest.json').read_bytes())
    require(manifest['schema'] == 'shardloom.perf12.guarantee_packet.v1'
            and manifest['runtime_executed'] is False and manifest['review_is_primary_decision'] is True,
            'manifest scope differs')
    values = {}
    for key, name in [('packet', 'packet.json.xz'), ('review', 'review.json')]:
        ref = manifest[key]
        require(ref['path'] == name, 'manifest artifact path differs')
        raw = (directory / name).read_bytes()
        require(len(raw) == ref['bytes'] and digest(raw) == ref['sha256'], 'artifact identity differs: ' + name)
        values[key] = strict_json(lzma.decompress(raw) if key == 'packet' else raw)
    evidence = Evidence(values['packet'])
    require(len(evidence.members) == manifest['member_count'], 'manifest member count differs')
    evidence.check(manifest['builder'])
    return values['review'], values['packet']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check-current-source', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    review, packet = load()
    result = verify(review, packet, args.check_current_source)
    result['verifier_sha256'] = digest(Path(__file__).read_bytes())
    result['manifest_sha256'] = digest((HERE / 'manifest.json').read_bytes())
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
