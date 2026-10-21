#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Generate short-lived, local TLS certificates for native exchange tests.

The generated private keys are test fixtures and must never be used outside
the isolated test environment. CA private keys are intentionally not written.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import os
from pathlib import Path
import sys

from cryptography import __version__ as cryptography_version
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def repository_root() -> Path:
    return Path(__file__).resolve().parents[4]


def write_new_file(path: Path, contents: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(contents)


def pem_certificate(certificate: x509.Certificate) -> bytes:
    return certificate.public_bytes(serialization.Encoding.PEM)


def pem_private_key(private_key: ec.EllipticCurvePrivateKey) -> bytes:
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def common_name(value: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, value)])


def key_usage(*, ca: bool) -> x509.KeyUsage:
    return x509.KeyUsage(
        digital_signature=not ca,
        content_commitment=False,
        key_encipherment=False,
        data_encipherment=False,
        key_agreement=False,
        key_cert_sign=ca,
        crl_sign=ca,
        encipher_only=False,
        decipher_only=False,
    )


def issue_certificate(
    *,
    subject: x509.Name,
    public_key: ec.EllipticCurvePublicKey,
    issuer: x509.Name,
    signer: ec.EllipticCurvePrivateKey,
    not_before: datetime,
    not_after: datetime,
    is_ca: bool,
    extended_usage: x509.ObjectIdentifier | None = None,
    server_names: bool = False,
) -> x509.Certificate:
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(public_key)
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(
            x509.BasicConstraints(ca=is_ca, path_length=0 if is_ca else None),
            critical=True,
        )
        .add_extension(key_usage(ca=is_ca), critical=True)
    )
    if extended_usage is not None:
        builder = builder.add_extension(
            x509.ExtendedKeyUsage([extended_usage]), critical=False
        )
    if server_names:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
    return builder.sign(private_key=signer, algorithm=hashes.SHA256())


def ca_certificate(
    name: str, key: ec.EllipticCurvePrivateKey, not_before: datetime, not_after: datetime
) -> x509.Certificate:
    return issue_certificate(
        subject=common_name(name),
        public_key=key.public_key(),
        issuer=common_name(name),
        signer=key,
        not_before=not_before,
        not_after=not_after,
        is_ca=True,
    )


def manifest_certificate(name: str, filename: str, cert: x509.Certificate) -> dict[str, str]:
    return {
        "name": name,
        "filename": filename,
        "der_sha256": cert.fingerprint(hashes.SHA256()).hex(),
        "not_valid_before_utc": cert.not_valid_before_utc.isoformat(),
        "not_valid_after_utc": cert.not_valid_after_utc.isoformat(),
    }


def generate(output: Path) -> Path:
    root = repository_root()
    sys.path.insert(0, str(root / "scripts"))
    from local_uat_storage import require_local_path  # noqa: PLC0415

    if not output.is_absolute():
        raise ValueError("--output must be an absolute path")
    output = require_local_path(output, Path.home(), sys.platform)
    parent = output.parent
    if not parent.is_dir():
        raise ValueError("--output parent must already exist")
    if output.exists():
        raise FileExistsError("--output must name a new directory")

    # mkdir is exclusive and applies mode 0700 only to this new directory.
    os.mkdir(output, 0o700)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    before = (now - timedelta(minutes=5)).replace(tzinfo=None)
    after = (now + timedelta(days=1)).replace(tzinfo=None)
    main_ca_key = ec.generate_private_key(ec.SECP256R1())
    main_ca = ca_certificate("ShardLoom Native Exchange Test CA", main_ca_key, before, after)
    write_new_file(output / "ca.cert.pem", pem_certificate(main_ca))

    certificates = [manifest_certificate("ca", "ca.cert.pem", main_ca)]

    server_key = ec.generate_private_key(ec.SECP256R1())
    server_cert = issue_certificate(
        subject=common_name("localhost"),
        public_key=server_key.public_key(),
        issuer=main_ca.subject,
        signer=main_ca_key,
        not_before=before,
        not_after=after,
        is_ca=False,
        extended_usage=ExtendedKeyUsageOID.SERVER_AUTH,
        server_names=True,
    )
    write_new_file(output / "server.cert.pem", pem_certificate(server_cert))
    write_new_file(output / "server.key.pem", pem_private_key(server_key))
    certificates.append(manifest_certificate("server", "server.cert.pem", server_cert))

    roles = ("reader", "reader2", "denied", "administrator")
    for role in roles:
        key = ec.generate_private_key(ec.SECP256R1())
        cert = issue_certificate(
            subject=common_name(role),
            public_key=key.public_key(),
            issuer=main_ca.subject,
            signer=main_ca_key,
            not_before=before,
            not_after=after,
            is_ca=False,
            extended_usage=ExtendedKeyUsageOID.CLIENT_AUTH,
        )
        write_new_file(output / f"{role}.cert.pem", pem_certificate(cert))
        write_new_file(output / f"{role}.key.pem", pem_private_key(key))
        certificates.append(manifest_certificate(role, f"{role}.cert.pem", cert))

    unlisted_key = ec.generate_private_key(ec.SECP256R1())
    unlisted_cert = issue_certificate(
        subject=common_name("unlisted"),
        public_key=unlisted_key.public_key(),
        issuer=main_ca.subject,
        signer=main_ca_key,
        not_before=before,
        not_after=after,
        is_ca=False,
        extended_usage=ExtendedKeyUsageOID.CLIENT_AUTH,
    )
    write_new_file(output / "unlisted.cert.pem", pem_certificate(unlisted_cert))
    write_new_file(output / "unlisted.key.pem", pem_private_key(unlisted_key))
    certificates.append(manifest_certificate("unlisted", "unlisted.cert.pem", unlisted_cert))

    expired_key = ec.generate_private_key(ec.SECP256R1())
    expired_cert = issue_certificate(
        subject=common_name("expired"),
        public_key=expired_key.public_key(),
        issuer=main_ca.subject,
        signer=main_ca_key,
        not_before=(now - timedelta(days=2)).replace(tzinfo=None),
        not_after=(now - timedelta(days=1)).replace(tzinfo=None),
        is_ca=False,
        extended_usage=ExtendedKeyUsageOID.CLIENT_AUTH,
    )
    write_new_file(output / "expired.cert.pem", pem_certificate(expired_cert))
    write_new_file(output / "expired.key.pem", pem_private_key(expired_key))
    certificates.append(manifest_certificate("expired", "expired.cert.pem", expired_cert))

    unknown_ca_key = ec.generate_private_key(ec.SECP256R1())
    unknown_ca = ca_certificate("Unknown Native Exchange Test CA", unknown_ca_key, before, after)
    unknown_key = ec.generate_private_key(ec.SECP256R1())
    unknown_cert = issue_certificate(
        subject=common_name("unknown"),
        public_key=unknown_key.public_key(),
        issuer=unknown_ca.subject,
        signer=unknown_ca_key,
        not_before=before,
        not_after=after,
        is_ca=False,
        extended_usage=ExtendedKeyUsageOID.CLIENT_AUTH,
    )
    write_new_file(output / "unknown.cert.pem", pem_certificate(unknown_cert))
    write_new_file(output / "unknown.key.pem", pem_private_key(unknown_key))
    certificates.append(manifest_certificate("unknown", "unknown.cert.pem", unknown_cert))

    manifest = {
        "schema_version": 1,
        "cryptography_version": cryptography_version,
        "generated_at_utc": now.isoformat(),
        "main_ca_der_sha256": main_ca.fingerprint(hashes.SHA256()).hex(),
        "certificates": certificates,
    }
    manifest_path = output / "public-manifest.json"
    write_new_file(
        manifest_path,
        (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )
    return manifest_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="absolute path to a new local directory for ephemeral test certificates",
    )
    args = parser.parse_args()
    try:
        manifest_path = generate(args.output)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
