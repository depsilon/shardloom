#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Replay the saved grouped-state evidence without executing queries."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'python/src'),
               str(ROOT / 'benchmarks/traditional_analytics')]
from native_workflow_protocol import strict_json
from query_surface.replay import MAX_FILE, bounded_bytes, need
from shared_performance_bundle import Bundle
from grouped_state_replay import verify


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    args = parser.parse_args()
    bundle = Bundle(args.packet)
    anchors = strict_json(bounded_bytes(Path(__file__).with_name('grouped_state_anchors.json'), MAX_FILE))
    acceptance = verify(bundle, anchors)
    saved = args.packet / 'acceptance.json'
    if saved.exists():
        need(strict_json(bounded_bytes(saved, MAX_FILE)) == acceptance,
             'saved acceptance differs from replay')
    print(json.dumps(acceptance, sort_keys=True, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
