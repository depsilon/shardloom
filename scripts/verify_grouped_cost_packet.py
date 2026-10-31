#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify the grouped diagnostic without an engine, original data or subprocesses."""
import argparse
import json
from pathlib import Path

from native_grouped_cost_diagnostic import read
from grouped_cost_replay import verify
from shared_performance_bundle import Bundle


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    args = parser.parse_args()
    result = verify(Bundle(args.packet), read(Path(__file__).with_name('grouped_cost_anchors.json')))
    if (args.packet / 'acceptance.json').exists():
        assert result == read(args.packet / 'acceptance.json'), 'saved acceptance differs'
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
