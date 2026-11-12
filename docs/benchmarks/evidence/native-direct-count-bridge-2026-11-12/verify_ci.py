"""Verify the COUNT checkpoint with assertion checks required in every mode."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile

if not __debug__:
    raise RuntimeError('evidence verification requires assertion checks')

ROOT = Path(__file__).resolve().parent
IMPLEMENTATION = ROOT / 'ci-toolchain/verified-overlay-v1.py'
IMPLEMENTATION_SHA256 = '79aeec770544dbdf1879fa16dd2d99b763cc6f21bbc8a8fdae7d35e26b4dd513'
if hashlib.sha256(IMPLEMENTATION.read_bytes()).hexdigest() != IMPLEMENTATION_SHA256:
    raise RuntimeError('CI evidence verifier implementation identity mismatch')
_spec = importlib.util.spec_from_file_location('count_ci_verified_implementation', IMPLEMENTATION)
_implementation = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_implementation)

# Preserve the independently tested overlay and the immutable original packet.
CI_PATH = _implementation.CI_PATH
PIN = _implementation.PIN
verify_current_sources = _implementation.verify_current_sources


def with_components(before):
    needle = b'          toolchain: ${{ env.SHARDLOOM_RUST_TOOLCHAIN }}\n'
    assert before.count(needle) == 16 and b'          components:' not in before
    return before.replace(needle, needle + b'          components: rustfmt, clippy\n')


def updated_target(verifier, target, before, after):
    assert verifier.identity(before) == target[CI_PATH]
    assert after == with_components(before)
    return dict(target, **{CI_PATH: verifier.identity(after)})


def verify_overlay(verifier, packet, root):
    original, target = _implementation.verify_overlay(verifier, packet, root)
    directory = root / 'ci-toolchain-components'
    supplement = json.loads((directory / 'supplement.json').read_bytes())
    assert supplement['schema'] == 'shardloom.direct_count_ci_components.v1'
    assert supplement['status'] == 'validated_ci_setup'
    assert supplement['components'] == ['rustfmt', 'clippy'] and supplement['toolchain'] == PIN
    assert supplement['query_executions'] == 0 and supplement['candidate_admitted'] is False
    assert supplement['investigation_b_complete'] is False
    assert supplement['original_supplement'] == verifier.identity((root / 'ci-toolchain/supplement.json').read_bytes())
    assert supplement['original_entrypoint'] == verifier.identity((root / 'ci-toolchain/checked-entrypoint-v1.py').read_bytes())
    assert supplement['current_entrypoint'] == verifier.identity(Path(__file__).read_bytes())
    required = {'local-validation.json', 'local-checker.py', 'workflow.yml',
        'fmt-failure-connector.json', 'clippy-failure-connector.json', 'action-source-connector.json'}
    assert required <= set(supplement['evidence'])
    for name, expected in supplement['evidence'].items():
        verifier.safe_name(name)
        assert verifier.identity((directory / name).read_bytes()) == expected
    before = (root / 'ci-toolchain/workflow.yml').read_bytes()
    after = (directory / 'workflow.yml').read_bytes()
    target = updated_target(verifier, target, before, after)
    assert supplement['before'] == original['after'] == verifier.identity(before)
    assert supplement['after'] == target[CI_PATH]
    local = json.loads((directory / 'local-validation.json').read_bytes())
    assert local['status'] == 'passed' and local['query_executions'] == 0
    assert local['entrypoint'] == supplement['current_entrypoint'] and local['workflow'] == supplement['after']
    assert local['checker'] == verifier.identity((directory / 'local-checker.py').read_bytes())
    assert local['component_inputs'] == 16 and local['pipeline_tests'] == 14
    assert local['rejected_corruptions'] == ['changed_runtime_source', 'disabled_lint', 'missing_clippy', 'missing_components', 'missing_rustfmt', 'omitted_job']
    return dict(original, after=supplement['after'], components=supplement['components'],
        component_supplement=supplement), target


def verify(root, repo=None):
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
            components=supplement['components'], original_packet_unchanged=True), **replay)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(ROOT, args.repo_root), indent=2, sort_keys=True))
