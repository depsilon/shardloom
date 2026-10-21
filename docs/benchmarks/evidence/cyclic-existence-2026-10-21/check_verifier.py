# SPDX-License-Identifier: Apache-2.0
"""Ensure corrupt result/resource/clock/protocol records cannot pass replay."""
import copy
import json
import lzma

import verify


def main():
    packet = json.loads(lzma.decompress((verify.HERE / "intake.json.xz").read_bytes()))
    members = packet["members"]
    protocol = json.loads(members["cyclic-existence-intake-protocol-850-v3.json"]["text"])
    result = json.loads(members["cyclic-existence-native-intake-results-850-a3.json"]["text"])
    owner, expected = result["cells"][0]["outer"], protocol["workload"]["cells"][0]["expected"]
    original = owner["calls"][0]
    verify.verify_call(original, expected, expected["schema"], owner["prepared_credits"])
    rejected = []
    mutations = (
        ("wrong output", lambda call: call["result"].update(rows=0)),
        ("wrong schema", lambda call: call.update(schema={})),
        ("incomplete values", lambda call: call.update(ordered_values_equal=False)),
        ("incomplete delivery", lambda call: call["delivery"].update(complete=False)),
        ("credit leak", lambda call: call["after_result_release"].update(reserved_bytes=1)),
        ("external engine", lambda call: call["report"].update(external_engine_invoked="true")),
        ("wrong clock", lambda call: call["report"].update(native_query_timing_clock="cpu")),
        ("fabricated engine time", lambda call: call.update(engine_execution_seconds=0)),
        ("nonfinite enclosing call", lambda call: call.update(collect_seconds=float("nan"))),
    )
    for name, mutate in mutations:
        changed = copy.deepcopy(original)
        mutate(changed)
        try:
            verify.verify_call(changed, expected, expected["schema"], owner["prepared_credits"])
        except ValueError:
            rejected.append(name)
        else:
            raise AssertionError(f"corrupt result passed: {name}")
    for name, mutate in (
        ("dropped control", lambda value: value["workload"]["cells"].pop()),
        ("relaxed target", lambda value: value["comparison"].update(target_engine_ratio_max=1.0)),
        ("dropped cohort", lambda value: value["comparison"]["schedule"].pop()),
    ):
        changed = copy.deepcopy(protocol)
        mutate(changed)
        try:
            verify.verify_comparison(changed)
        except ValueError:
            rejected.append(name)
        else:
            raise AssertionError(f"corrupt protocol passed: {name}")
    print(json.dumps({"status": "passed", "corruptions_rejected": rejected}, sort_keys=True))


if __name__ == "__main__":
    main()
