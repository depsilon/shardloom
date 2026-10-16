# SPDX-License-Identifier: Apache-2.0
"""Predeclare independent sampling and rolling values; no engine execution.

Sampling globally ranks all rows
instead of using the engine's bounded heap. Rolling evaluates each complete
frame independently instead of using its incremental scheduler. The small
fixtures intentionally use exactly representable binary fractions and sums.
"""
import math
import sys

U64 = (1 << 64) - 1


def score(seed, ordinal):
    # The existing public seed contract uses this pinned 64-bit mapping.
    value = (seed ^ ((ordinal * 0x9E3779B97F4A7C15) & U64)) + 0x9E3779B97F4A7C15
    value &= U64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & U64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & U64
    return value ^ (value >> 31)


def unit(seed, ordinal):
    value = (float(score(seed, ordinal)) + 0.5) / float(1 << 64)
    return min(1.0 - sys.float_info.epsilon, max(sys.float_info.min, value))


def sample_ordinals(population, target, seed, *, weights=None, replace=False):
    assert population >= 0 and target >= 0 and 0 <= seed <= U64
    if weights is not None:
        assert len(weights) == population
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) or
               not math.isfinite(value) or value <= 0 for value in weights):
            raise ValueError('positive finite numeric weights required')
    if population == 0 or target == 0:
        return []
    if replace:
        if weights is None:
            return [score(seed, draw) % population for draw in range(target)]
        total = sum(weights)
        if not math.isfinite(total):
            raise ValueError('finite total weight required')
        result = []
        for draw in range(target):
            threshold = unit(seed ^ 0xA0761D6478BD642F, draw) * total
            cumulative = 0.0
            for ordinal, weight in enumerate(weights):
                cumulative += weight
                if threshold <= cumulative:
                    result.append(ordinal)
                    break
            else:
                result.append(population - 1)
        return result
    ranks = [score(seed, ordinal) if weights is None else
             math.log(unit(seed, ordinal)) / weights[ordinal]
             for ordinal in range(population)]
    selected = sorted(range(population), key=lambda ordinal: (-ranks[ordinal], ordinal))[:target]
    return sorted(selected)


def rolling_values(values, window, min_periods, aggregate, *, center=False):
    assert 1 <= min_periods <= window
    assert aggregate in ('sum', 'mean', 'min', 'max', 'count')
    output = []
    for ordinal in range(len(values)):
        left = (window - 1) // 2 if center else window - 1
        right = window - 1 - left if center else 0
        frame = [value for value in values[max(0, ordinal - left):ordinal + right + 1]
                 if value is not None]
        if len(frame) < min_periods:
            continue
        if aggregate == 'count':
            result = len(frame)
        elif aggregate == 'sum':
            result = float(sum(frame))
        elif aggregate == 'mean':
            result = sum(frame) / len(frame)
        elif aggregate == 'min':
            result = float(min(frame))
        else:
            result = float(max(frame))
        output.append(result)
    return output
