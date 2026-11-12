"""Verify the COUNT checkpoint with assertion checks required in every mode."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

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
verify_overlay = _implementation.verify_overlay
verify_current_sources = _implementation.verify_current_sources
verify = _implementation.verify

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo-root', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(ROOT, args.repo_root), indent=2, sort_keys=True))
