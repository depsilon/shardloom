"""Small bounded controls for trusted replay entrypoints and archive extraction."""
from __future__ import annotations
import hashlib
import importlib.util
import io
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
from unittest import mock


def check(verifier_path):
    verifier_path = Path(verifier_path).resolve()
    spec = importlib.util.spec_from_file_location('trusted_archive_control', verifier_path)
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    assert (verifier.MAX_MEMBER_BYTES, verifier.MAX_ARCHIVE_BYTES,
            verifier.MAX_ARCHIVE_MEMBERS) == (32 << 20, 128 << 20, 1024)
    records = []
    for name, flags, optimize in [('optimized_flag', ['-O'], None),
                                   ('optimized_environment', [], '1')]:
        env = dict(os.environ)
        env.pop('PYTHONOPTIMIZE', None)
        if optimize:
            env['PYTHONOPTIMIZE'] = optimize
        result = subprocess.run([sys.executable, *flags, '-B', str(verifier_path), '--help'],
            env=env, capture_output=True, text=True, timeout=30, check=False)
        assert result.returncode != 0, name
        assert 'optimized Python is unsupported' in result.stderr, result.stderr
        records.append(dict(case=name, rejected=True, returncode=result.returncode))

    # Exercise real tar/gzip streams at small limits, with the exact production
    # values checked above. The reader must never request an unbounded index.
    cases = [
        ('member_limit', [('a', b'12345')], 'archive member size exceeds limit'),
        ('total_limit', [('a', b'123'), ('b', b'456'), ('c', b'789')], 'archive total size exceeds limit'),
        ('count_limit', [('a', b'1'), ('b', b'2'), ('c', b'3'), ('d', b'4')], 'archive member count exceeds limit'),
        ('duplicate', [('a', b'1'), ('./a', b'2')], 'duplicate archive member'),
        ('parent_path', [('../escaped', b'1')], 'unsafe archive member'),
        ('absolute_path', [('/escaped', b'1')], 'unsafe archive member'),
        ('backslash_path', [('a' + chr(92) + 'escaped', b'1')], 'unsafe archive member'),
        ('drive_path', [('C:/escaped', b'1')], 'unsafe archive member'),
        ('long_path', [('a' * 4097, b'1')], 'unsafe archive member'),
        ('symlink', [('link', None)], 'unsafe archive member'),
    ]

    def packet(path, entries):
        with tarfile.open(path, 'w:gz') as archive:
            for name, raw in entries:
                member = tarfile.TarInfo(name)
                if raw is None:
                    member.type = tarfile.SYMTYPE
                    member.linkname = '../escaped'
                    archive.addfile(member)
                else:
                    member.size = len(raw)
                    archive.addfile(member, io.BytesIO(raw))

    with tempfile.TemporaryDirectory(prefix='archive-trust-controls-') as temporary:
        root = Path(temporary)
        with mock.patch.object(verifier, 'MAX_MEMBER_BYTES', 4), \
                mock.patch.object(verifier, 'MAX_ARCHIVE_BYTES', 8), \
                mock.patch.object(verifier, 'MAX_ARCHIVE_MEMBERS', 3), \
                mock.patch.object(tarfile.TarFile, 'getmembers',
                                  side_effect=AssertionError('unbounded member index')):
            valid = root / 'valid.tar.gz'
            packet(valid, [('nested/a', b'1234'), ('b', b'5678'), ('empty', b'')])
            destination = root / 'valid'
            verifier.unpack(valid, destination)
            assert {str(p.relative_to(destination)): p.read_bytes()
                    for p in destination.rglob('*') if p.is_file()} == {
                        'nested/a': b'1234', 'b': b'5678', 'empty': b''}
            for name, entries, diagnostic in cases:
                archive = root / (name + '.tar.gz')
                packet(archive, entries)
                destination = root / name
                try:
                    verifier.unpack(archive, destination)
                except ValueError as error:
                    assert str(error) == diagnostic, (name, str(error))
                else:
                    raise AssertionError('unsafe archive accepted: ' + name)
                files = [p for p in destination.rglob('*') if p.is_file()]
                assert len(files) <= 3 and sum(p.stat().st_size for p in files) <= 8
                assert all(p.stat().st_size <= 4 for p in files)
                assert not (root / 'escaped').exists()
                records.append(dict(case=name, rejected=True, diagnostic=diagnostic,
                                    bounded_partial_extraction=True))
            extended = root / 'extended-header.tar.gz'
            with tarfile.open(extended, 'w:gz', format=tarfile.PAX_FORMAT) as archive:
                member = tarfile.TarInfo('small-file')
                member.pax_headers = {'comment': 'x' * 65536}
                archive.addfile(member, io.BytesIO(b''))
            try:
                verifier.unpack(extended, root / 'extended-header')
            except ValueError as error:
                assert str(error) == 'archive decompressed stream exceeds limit'
            else:
                raise AssertionError('oversized extended header accepted')
            assert not (root / 'extended-header').exists()
            records.append(dict(case='extended_header_limit', rejected=True,
                                rejected_before_file_write=True))
    return dict(status='passed', verifier_sha256=hashlib.sha256(verifier_path.read_bytes()).hexdigest(),
                negative_controls=records, exact_limit_positive_passed=True,
                production_limits=dict(member_bytes=32 << 20, total_bytes=128 << 20, members=1024),
                exercised_limits=dict(member_bytes=4, total_bytes=8, members=3),
                streaming_without_member_index=True, native_queries_executed=0)
