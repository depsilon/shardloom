# SPDX-License-Identifier: Apache-2.0
"""Reuse completed data after its context closes."""

import shardloom as sl


def reuse_completed_result(transport=None):
    # Illustrative caller choices, not automatic engine defaults.
    resources = {"memory_bytes": 256 << 20, "max_parallelism": 2}
    with sl.context(transport=transport, **resources) as ctx:
        result = ctx.range(1, 4, column="id").filter(sl.col("id") != 2).collect()

    # The completed result owns its data independently of the closed context.
    with sl.context(transport=transport, **resources) as receiver:
        return receiver.from_result(result).sort("id").collect()


if __name__ == "__main__":
    print(reuse_completed_result().to_rows())
