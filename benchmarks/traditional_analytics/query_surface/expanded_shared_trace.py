# SPDX-License-Identifier: Apache-2.0
"""Join bounded reads, delivered and consumed slices, and backing releases."""
from __future__ import annotations

from bisect import bisect_left, bisect_right, insort
from collections import defaultdict

SCHEMA = {1: 4, 2: 4, 3: 4, 4: 4, 10: 4, 11: 5, 12: 2, 13: 3, 14: 3}
LIMITS = {0: 65536, 1: 262144}


class TraceError(ValueError):
    pass


def union_bytes(ranges):
    total, end = 0, -1
    for start, stop in sorted(ranges):
        if stop < start:
            raise TraceError("negative interval")
        total += max(0, stop - max(start, end))
        end = max(end, stop)
    return total


def records(text):
    if len(text.encode()) > 128 << 20:
        raise TraceError("diagnostic text exceeds 128 MiB")
    output, pids, last, times, counts = [], set(), {}, {}, defaultdict(int)
    for line in text.splitlines():
        if not line.startswith("IOOT"):
            continue
        parts = line.split("\t")
        if len(line.encode()) > 512 or parts[0] != "IOOT" or len(parts) < 5:
            raise TraceError("malformed trace row")
        try:
            pid, sequence, timestamp, kind, *values = map(int, parts[1:])
        except ValueError as error:
            raise TraceError("nonnumeric trace row") from error
        if min(pid, sequence, timestamp, kind, *values) < 0:
            raise TraceError("negative trace field")
        if max(pid, sequence, timestamp, kind, *values) >= 1 << 128:
            raise TraceError("oversized trace field")
        if kind == 0:
            raise TraceError("explicit trace overflow")
        if kind not in SCHEMA or len(values) != SCHEMA[kind]:
            raise TraceError("unknown event kind or changed field count")
        stream = int(kind >= 10)
        if sequence != last.get(stream, -1) + 1 or timestamp < times.get(stream, 0):
            raise TraceError("missing/nonmonotonic per-crate sequence or timestamp")
        last[stream], times[stream] = sequence, timestamp
        counts[stream] += 1
        if counts[stream] > LIMITS[stream]:
            raise TraceError("per-crate event bound exceeded")
        pids.add(pid)
        output.append((timestamp, stream, sequence, kind, values))
    if len(pids) != 1 or not output:
        raise TraceError("expected one traced process")
    return sorted(output), pids.pop(), dict(counts)


def parse(text):
    events, pid, counts = records(text)
    allocations, latest_address, pending_reads = {}, {}, {}
    completed_reads, pending_returns, extents = [], defaultdict(list), []
    active_addresses, active_extents = [], {}
    members, future_drops, issues = {}, {}, []
    for timestamp, stream, sequence, kind, values in events:
        if kind == 1:
            identifier, address, length, capacity = values
            if identifier in allocations or capacity < length:
                raise TraceError("duplicate allocation or invalid capacity")
            allocation = dict(id=identifier, address=address, length=length, capacity=capacity,
                              allocated_ns=timestamp, released_ns=None)
            allocations[identifier] = allocation
            latest_address[address] = allocation
        elif kind == 2:
            identifier, address, length, capacity = values
            allocation = allocations.get(identifier)
            if (allocation is None or allocation['released_ns'] is not None
                    or (address, length, capacity) != (allocation['address'], allocation['length'], allocation['capacity'])):
                raise TraceError("release lacks unique matching allocation")
            allocation['released_ns'] = timestamp
            if latest_address.get(address) is allocation:
                del latest_address[address]
            active = active_extents.get(address)
            if active is not None and active['allocation_id'] == identifier:
                del active_extents[address]
                active_addresses.pop(bisect_left(active_addresses, address))
        elif kind == 3:
            source, offset, length, address = values
            allocation = latest_address.get(address)
            if allocation is None or length > allocation['length']:
                raise TraceError("read attempt lacks its live native allocation")
            key = tuple(values)
            if key in pending_reads:
                raise TraceError("ambiguous concurrent read attempt")
            pending_reads[key] = dict(source_identity=source, offset=offset, length=length,
                                      address=address, allocation_id=allocation['id'], attempted_ns=timestamp)
        elif kind == 4:
            read = pending_reads.pop(tuple(values), None)
            if read is None:
                raise TraceError("completed read lacks its attempted extent")
            read['completed_ns'] = timestamp
            completed_reads.append(read)
            pending_returns[(read['offset'], read['length'], read['address'])].append(read)
        elif kind == 10:
            offset, length, address, host_length = values
            candidates = pending_returns.get((offset, length, address), [])
            if not candidates or host_length != length:
                issues.append(dict(kind="unmatched_returned_extent", sequence=sequence, values=values))
                continue
            read = candidates.pop()
            extent = dict(read, returned_ns=timestamp, members=[])
            extents.append(extent)
            if address not in active_extents:
                insort(active_addresses, address)
            active_extents[address] = extent
        elif kind == 11:
            identifier, offset, length, address, host_length = values
            if identifier in members or length != host_length:
                raise TraceError("duplicate member ID or changed slice length")
            member = dict(id=identifier, offset=offset, length=length, address=address,
                          delivered_ns=timestamp, callback=None, consumed_ns=None, future_drop=None)
            members[identifier] = member
            position = bisect_right(active_addresses, address) - 1
            extent = active_extents[active_addresses[position]] if position >= 0 else None
            if (extent is None or address + length > extent['address'] + extent['length']
                    or not extent['offset'] <= offset <= offset + length <= extent['offset'] + extent['length']
                    or address - extent['address'] != offset - extent['offset']):
                issues.append(dict(kind="unmatched_member_or_alignment_copy", id=identifier))
                continue
            member['allocation_id'] = extent['allocation_id']
            extent['members'].append(member)
        elif kind == 12:
            identifier, delivered = values
            member = members.get(identifier)
            if delivered not in (0, 1) or member is None or member['callback'] is not None:
                issues.append(dict(kind="unmatched_or_duplicate_callback", id=identifier))
            else:
                member['callback'] = dict(accepted=bool(delivered), timestamp_ns=timestamp)
        elif kind == 13:
            identifier, address, length = values
            member = members.get(identifier)
            if (member is None or member['consumed_ns'] is not None
                    or (address, length) != (member['address'], member['length'])):
                issues.append(dict(kind="unmatched_or_duplicate_consumption", id=identifier))
            else:
                member['consumed_ns'] = timestamp
        else:
            identifier, finished, polled = values
            if identifier in future_drops or finished not in (0, 1) or polled not in (0, 1):
                raise TraceError("invalid future-drop event")
            future_drops[identifier] = dict(finished=bool(finished), polled=bool(polled), timestamp_ns=timestamp)

    for member in members.values():
        member['future_drop'] = future_drops.get(member['id'])
        if member['callback'] is None or member['future_drop'] is None:
            issues.append(dict(kind="missing_member_terminal_endpoint", id=member['id']))
        if member['consumed_ns'] is not None:
            callback, dropped = member['callback'], member['future_drop']
            if callback is None or not callback['accepted'] or dropped is None or not dropped['finished']:
                issues.append(dict(kind="consumption_without_accepted_finished_future", id=member['id']))
            allocation = allocations.get(member.get('allocation_id'))
            if allocation and allocation['released_ns'] is not None and allocation['released_ns'] < member['consumed_ns']:
                issues.append(dict(kind="backing_released_before_consumption", id=member['id']))

    for extent in extents:
        allocation = allocations[extent['allocation_id']]
        extent['charged_capacity_bytes'] = allocation['capacity']
        extent['last_owner_release_ns'] = allocation['released_ns']
        extent['registered_member_union_bytes'] = union_bytes(
            (m['offset'], m['offset'] + m['length']) for m in extent['members'])
        extent['consumed_member_union_bytes'] = union_bytes(
            (m['offset'], m['offset'] + m['length']) for m in extent['members'] if m['consumed_ns'] is not None)
        consumed = [m['consumed_ns'] for m in extent['members'] if m['consumed_ns'] is not None]
        extent['last_release_after_last_consumption_ns'] = (
            allocation['released_ns'] - max(consumed)
            if allocation['released_ns'] is not None and consumed else None)
    missing = [a['id'] for a in allocations.values() if a['released_ns'] is None]
    issued = sum(read['length'] for read in completed_reads)
    returned = sum(extent['length'] for extent in extents)
    consumed = sum(extent['consumed_member_union_bytes'] for extent in extents)
    return dict(
        complete_chain=not issues and not missing and not pending_reads,
        pid=pid, events_by_crate=counts, issues=issues,
        missing_release_allocation_ids=missing, incomplete_reads=list(pending_reads.values()),
        totals=dict(completed_filesystem_read_requests=len(completed_reads),
                    completed_filesystem_read_bytes=issued, returned_data_extent_bytes=returned,
                    metadata_or_other_completed_read_bytes=issued-returned,
                    registered_member_union_bytes=sum(x['registered_member_union_bytes'] for x in extents),
                    consumed_member_union_bytes=consumed, unconsumed_data_extent_bytes=returned-consumed,
                    returned_data_backing_capacity_bytes=sum(x['charged_capacity_bytes'] for x in extents),
                    consumed_segments=sum(m['consumed_ns'] is not None for m in members.values()),
                    delivered_segments=len(members), allocations=len(allocations)),
        extents=extents,
        interpretation="Instrumented filesystem reads, fulfilled segment futures and final native backing release. Durations are diagnostic. No device-I/O, RSS or exact reservation-peak inference.",
    )


def self_test():
    rows = [(1,[1,1000,100,164]), (3,[7,0,100,1000]), (4,[7,0,100,1000]),
            (10,[0,100,1000,100]), (11,[1,0,20,1000,20]), (12,[1,1]),
            (11,[2,10,30,1010,30]), (12,[2,1]), (13,[1,1000,20]),
            (14,[1,1,0]), (14,[2,0,0]), (2,[1,1000,100,164])]
    def encode(items):
        sequence = defaultdict(int)
        lines = []
        for timestamp, (kind, values) in enumerate(items, 1):
            stream = int(kind >= 10)
            lines.append('\t'.join(map(str, ['IOOT',42,sequence[stream],timestamp,kind,*values])))
            sequence[stream] += 1
        return '\n'.join(lines)
    result = parse(encode(rows))
    assert result['complete_chain'] and result['totals']['registered_member_union_bytes'] == 40
    assert result['totals']['consumed_member_union_bytes'] == 20
    assert not parse(encode(rows[:-1]))['complete_chain']
    assert not parse(encode([x for x in rows if x[0] != 12]))['complete_chain']
    reuse = rows[:-1] + [(1,[2,1000,100,164]), (2,[1,1000,100,164]), (2,[2,1000,100,164])]
    assert parse(encode(reuse))['complete_chain']
    for invalid in ['IOOT\tbad', encode(rows)+'\nIOOT\t42\t0\t100\t0\t65536',
                    encode(rows).replace('IOOT\t42\t1\t2', 'IOOT\t42\t0\t2')]:
        try:
            parse(invalid)
        except TraceError:
            continue
        raise AssertionError('invalid trace admitted')
    return dict(status='passed', controls=['two_independent_crate_sequences','actual_consumption_vs_prefetch',
        'overlapping_member_union','missing_release','missing_callback','pointer_reuse_before_release_log',
        'malformed','overflow','duplicate_sequence'])
