"""Verify the immutable COUNT packet plus its separately bound CI compiler pin."""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile

CI_PATH = '.github/workflows/ci.yml'
ORIGINAL_HEAD = '3181484789a9dc4b2cd66a335da50d91b831c360'
PIN = '1.99.0'
CONTROL_NAMES = {'floating_compiler', 'different_compiler', 'disabled_lint', 'omitted_job', 'changed_runtime_source'}


def pinned_workflow(before):
    assert before.count(b'toolchain: stable') == 16
    assert before.count(b'  CARGO_TERM_COLOR: always\n') == 1
    assert b'SHARDLOOM_RUST_TOOLCHAIN' not in before
    return before.replace(b'  CARGO_TERM_COLOR: always\n', b'  CARGO_TERM_COLOR: always\n'
        b'  # Upgrade deliberately after validating the full matrix with the new compiler.\n'
        b'  SHARDLOOM_RUST_TOOLCHAIN: "1.99.0"\n').replace(
        b'toolchain: stable', b'toolchain: ${{ env.SHARDLOOM_RUST_TOOLCHAIN }}')


def target_sources(verifier, packet, supplement, workflow):
    target = verifier.integration_sources(packet)
    before = packet.data['source-blobs/' + target[CI_PATH]['sha256']]
    assert verifier.identity(before) == target[CI_PATH] == supplement['before']
    assert workflow == pinned_workflow(before)
    assert verifier.identity(workflow) == supplement['after']
    assert supplement['path'] == CI_PATH and supplement['toolchain'] == PIN
    target[CI_PATH] = verifier.identity(workflow)
    return target


def check_actual_sources(verifier, target, actual):
    assert (set(actual) ^ set(target)) <= verifier.STATUS_PROSE
    for name, expected in target.items():
        if name not in verifier.STATUS_PROSE:
            assert actual.get(name) == expected, name


def verify_current_sources(verifier, target, repo):
    paths = set()
    excluded = ('docs/benchmarks/evidence/', 'docs/release/evidence/', 'docs/dependencies/evidence/')
    for command in (['git','ls-files','--cached','-z'], ['git','ls-files','--others','--exclude-standard','-z']):
        paths.update(name for name in subprocess.check_output(command, cwd=repo).decode().split('\0')
            if name and not name.startswith(('.git/', *excluded)) and '.git' not in Path(name).parts)
    actual = {}
    for name in paths:
        path = repo / name
        if path.is_symlink():
            actual[name] = {'symlink':str(path.readlink())}
        elif path.is_file():
            actual[name] = verifier.identity(path.read_bytes())
        else:
            actual[name] = {'absent':True}
    check_actual_sources(verifier, target, actual)
    return verifier.fingerprint(actual)


def verify_overlay(verifier, packet, root):
    supplement = json.loads((root / 'ci-toolchain/supplement.json').read_bytes())
    assert supplement['schema'] == 'shardloom.direct_count_ci_toolchain.v1'
    assert supplement['status'] == 'validated_ci_only_pin'
    assert supplement['original_pr_head'] == ORIGINAL_HEAD and supplement['pr'] == 1682
    assert supplement['original_acceptance'] == verifier.identity((root / 'acceptance.json').read_bytes())
    assert supplement['original_controls'] == verifier.identity((root / 'corruption-controls.json').read_bytes())
    assert supplement['original_verifier'] == verifier.identity((root / 'verify.py').read_bytes())
    assert supplement['current_verifier'] == verifier.identity(Path(__file__).read_bytes())
    assert supplement['query_executions'] == 0 and supplement['candidate_admitted'] is False
    assert supplement['investigation_b_complete'] is False
    for name, expected in supplement['evidence'].items():
        verifier.safe_name(name)
        assert name.startswith('ci-toolchain/')
        assert verifier.identity((root / name).read_bytes()) == expected
    logs = json.loads((root / 'ci-toolchain/lint-logs-connector.json').read_bytes())['structuredContent']['content']
    assert 'rustc 1.100.0' in logs and 'needless_range_loop' in logs
    assert 'shardloom-core/src/decimal_output_integer.rs' in logs and 'shardloom-core/src/schema.rs' in logs
    assert 'Process completed with exit code 101.' in logs
    build = packet.read('current/candidate-release-build-r1.json')
    driver = packet.data['current/tools/guard_direct_count.py']
    assert build['driver_sha256'] == verifier.digest(driver)
    assert b"PATH='/Users/dylan/.rustup/toolchains/1.99.0-aarch64-apple-darwin/bin:" in driver
    workflow = (root / 'ci-toolchain/workflow.yml').read_bytes()
    target = target_sources(verifier, packet, supplement, workflow)
    report = json.loads((root / 'ci-toolchain/local-validation.json').read_bytes())
    assert report['status'] == 'passed' and report['query_executions'] == 0
    assert report['strict_lint_commands_unchanged'] and report['msrv_lane_unchanged']
    assert report['toolchain_inputs_pinned'] == 16 and report['pipeline_tests'] > 0
    assert report['rustc'].startswith('rustc 1.99.0 ')
    assert report['workflow'] == supplement['after']
    assert report['rejected_corruptions'] == sorted(CONTROL_NAMES)
    return supplement, target


def verify(root, repo=None):
    assert __debug__, 'evidence verification requires assertions'
    spec = importlib.util.spec_from_file_location('immutable_count_checkpoint', root / 'verify.py')
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    report, data = verifier.read_archive(root)
    packet = verifier.verify_sources(report, data)
    supplement, target = verify_overlay(verifier, packet, root)
    if repo is not None:
        verify_current_sources(verifier, target, repo)
    diagnostics = verifier.verify_diagnostics(packet)
    with tempfile.TemporaryDirectory(prefix='count-ci-checkpoint-') as directory:
        replay = verifier.replay_saved(report, data, Path(directory))
    controls = verifier.verify_controls(root, report, data)
    return dict(status='passed_source_bound_replay', diagnostics=diagnostics, controls=controls,
        ci_toolchain=dict(version=PIN, path=CI_PATH, before=supplement['before'], after=supplement['after'],
            original_packet_unchanged=True, rejected_corruptions=len(CONTROL_NAMES)), **replay)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(Path(__file__).resolve().parent, args.repo_root), indent=2, sort_keys=True))
