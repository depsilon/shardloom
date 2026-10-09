#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Shared selected-channel contract for ShardLoom technical-preview publication.

This file owns the already-published package-channel proof version. The current
source/package-prep version remains rooted in Cargo.toml and may be ahead of
this contract while a patch release is being prepared.
"""

from __future__ import annotations

from typing import Any


SELECTED_V0_1_0_RELEASE_CHANNEL_IDS = [
    "github_prerelease",
    "testpypi",
    "pypi",
    "homebrew_tap",
]

# The JSON field names still include v0_1_0 for schema compatibility. The
# selected release value itself is the current proof-backed package version.
SELECTED_PACKAGE_RELEASE_VERSION = "0.5.1"
SELECTED_PACKAGE_RELEASE_TAG = f"v{SELECTED_PACKAGE_RELEASE_VERSION}"
# Approved identities observed during the publication train. Keep these keyed
# by release so advancing the selected version cannot reuse a prior build's
# source/run binding. PyPI's source adds only the prerequisite proof documents.
PUBLISHED_REGISTRY_BUILD_IDENTITIES = {
    "0.5.1": {'testpypi': {'source_commit': '764cd97df35918cc34ade1dc41c20264be80927c',
                  'workflow_run_id': 37966086734},
     'pypi': {'source_commit': 'ce4ece33f986a237bc90293db797b6be93b40fb5',
              'workflow_run_id': 37985832079}},
    "0.4.0": {'testpypi': {'source_commit': 'd9ccd11d069f16fd57bc84b944c54672f09351bc',
                  'workflow_run_id': 37385011382},
     'pypi': {'source_commit': '9508eacc904e60e88c4f03975162316ff4d5c527',
              'workflow_run_id': 37387378748}},
    "0.3.3": {'testpypi': {'source_commit': 'e15f2e66faf6d359bba944e9d295fc58ce3bf7d4',
                  'workflow_run_id': 36707733970},
     'pypi': {'source_commit': '16869b635a64f4c12dba06051724118a1d8f28d4',
              'workflow_run_id': 36710669483}},
    "0.3.2": {'testpypi': {'source_commit': 'b06a77d9a994684ee483d43d65a8bc254dd998a6',
                  'workflow_run_id': 36357744771},
     'pypi': {'source_commit': 'c983aad22360c17f1a1c17e6651730f4a9e6d831',
              'workflow_run_id': 36359375442}},
    "0.3.1": {
        "testpypi": {
            "source_commit": "52a2228512341f158dfa6649b6c0985a33034460",
            "workflow_run_id": 36319933854,
        },
        "pypi": {
            "source_commit": "758b7808d0d55ae682e03bbbf3ff1811631f67e7",
            "workflow_run_id": 36321945919,
        },
    },
    "0.3.0": {
        "testpypi": {
            "source_commit": "751126027d3abb438952c2fe157dab87c44e3347",
            "workflow_run_id": 36244227580,
        },
        "pypi": {
            "source_commit": "c03b9c9afba8062fb706a8502775416ac76321a3",
            "workflow_run_id": 36245638911,
        },
    },
    "0.2.4": {
        "testpypi": {
            "source_commit": "8759b16e3421153302c9034e5a00c9d80b61d3d9",
            "workflow_run_id": 34747808607,
        },
        "pypi": {
            "source_commit": "1f180c47419b420509ff59831e416db618ce5ce7",
            "workflow_run_id": 34748638941,
        },
    },
}
PUBLISHED_REGISTRY_DISTRIBUTIONS = {
    "0.5.1": ('shardloom-0.5.1-cp313-cp313-macosx_26_0_arm64.whl',
     'shardloom-0.5.1-cp313-cp313-manylinux_2_39_x86_64.whl',
     'shardloom-0.5.1-cp313-cp313-win_amd64.whl',
     'shardloom-0.5.1.tar.gz'),
    "0.4.0": ('shardloom-0.4.0-cp313-cp313-macosx_26_0_arm64.whl',
     'shardloom-0.4.0-cp313-cp313-manylinux_2_39_x86_64.whl',
     'shardloom-0.4.0-cp313-cp313-win_amd64.whl',
     'shardloom-0.4.0.tar.gz'),
    "0.3.3": ('shardloom-0.3.3-cp313-cp313-macosx_26_0_arm64.whl',
     'shardloom-0.3.3-cp313-cp313-manylinux_2_39_x86_64.whl',
     'shardloom-0.3.3-cp313-cp313-win_amd64.whl',
     'shardloom-0.3.3.tar.gz'),
    "0.3.2": ('shardloom-0.3.2-cp313-cp313-macosx_26_0_arm64.whl',
     'shardloom-0.3.2-cp313-cp313-manylinux_2_39_x86_64.whl',
     'shardloom-0.3.2-cp313-cp313-win_amd64.whl',
     'shardloom-0.3.2.tar.gz'),
    "0.3.1": (
        "shardloom-0.3.1-cp313-cp313-macosx_26_0_arm64.whl",
        "shardloom-0.3.1-cp313-cp313-manylinux_2_39_x86_64.whl",
        "shardloom-0.3.1-cp313-cp313-win_amd64.whl",
        "shardloom-0.3.1.tar.gz",
    ),
    "0.3.0": (
        "shardloom-0.3.0-cp313-cp313-macosx_26_0_arm64.whl",
        "shardloom-0.3.0-cp313-cp313-manylinux_2_39_x86_64.whl",
        "shardloom-0.3.0-cp313-cp313-win_amd64.whl",
        "shardloom-0.3.0.tar.gz",
    ),
    "0.2.4": (
        "shardloom-0.2.4-cp313-cp313-macosx_26_0_arm64.whl",
        "shardloom-0.2.4-cp313-cp313-manylinux_2_39_x86_64.whl",
        "shardloom-0.2.4-cp313-cp313-win_amd64.whl",
        "shardloom-0.2.4.tar.gz",
    ),
}
# Audited -c program embedded in both immutable bundled-wheel transcripts.
# It executes smoke_check, a DataFrame and two SQL calls, asserts complete typed
# results/no fallback, and prints the captured JSON result. A new release must
# approve its own program; arbitrary isolated Python is not execution evidence.
PUBLISHED_REGISTRY_BUNDLED_SMOKE_SHA256 = {
    "0.5.1": 'e9ea9fb690a947f8fcce7f272038ea3dc88b123282e84d0ad2407204e8b3f798',
    "0.4.0": '2f3c6c821689e19bd640a04ca650bf46c74e0920b8beee5b764af5c1f9769534',
    "0.3.3": 'cee1d3fbef2f857028f3694e5ecbc314c8a1ffe497ae971e3a1f9c70a5d35acc',
    "0.3.2": '535d75bc9b620ac5dd4559c850409c2041dd239385797514d2604604dc671516',
    "0.3.1": "ee49b52a55770327d783e177dd6768b670d15b1bb3b8144ad1037f477900b146",
    "0.3.0": "77404a954315c308e2dbacbadd69d6a754e99ca234c82fc3a0d6c056bd074af8",
    "0.2.4": "d8f5c017d800ec0191dd058fd4b986b733c5f0379082f8e64450c7aa7353a6fb",
}
# Immutable, reviewed post-publication records. Pins bind every nested asset,
# command, output, recovery note and lifecycle result, including non-registry
# channels. Updating a record requires explicit review of a new approved pin.
PUBLISHED_CHANNEL_TRANSCRIPTS = {
    "0.5.1": {'github_prerelease': ('github-prerelease',
                           'shardloom.github_prerelease_channel_proof.v1',
                           '6787af4bf457b07e21261202036e56a7a06c4f2f97a59f34dfc7322a836e94bd'),
     'testpypi': ('testpypi',
                  'shardloom.python_registry_package_proof.v1',
                  '01a0a01c9c7321c0e334e22a924344aa2547257ae9c6d6233c8aa4e54860e12d'),
     'pypi': ('pypi',
              'shardloom.python_registry_package_proof.v1',
              '05b0acdb0ca2ec5f6c59c415aed325e9276c76f8288800e729988b863ecf8027'),
     'homebrew_tap': ('homebrew',
                      'shardloom.homebrew_channel_proof.v1',
                      '036b04bd1310affcf41098fdcf62e7c5dbc9ffc44dca7089e484e57657a464f3')},
    "0.4.0": {'github_prerelease': ('github-prerelease',
                           'shardloom.github_prerelease_channel_proof.v1',
                           '38fdf8c22cc6ff86dd30cce5115b5adc60d78c31f8efc1440509d6222a0522d5'),
     'testpypi': ('testpypi',
                  'shardloom.python_registry_package_proof.v1',
                  '6ee38150222523376c1c708422069c042e312a7efa7913c8b687bf5589819ad2'),
     'pypi': ('pypi',
              'shardloom.python_registry_package_proof.v1',
              'b8b390e28eb9c9bef5b641e4f1cf1c58ee160679a45c344f2608218c1964bb03'),
     'homebrew_tap': ('homebrew',
                      'shardloom.homebrew_channel_proof.v1',
                      'd3a718a70bd392c513b92d349901d45215576d1320f326bab391888d859ce6ee')},
    "0.3.3": {'github_prerelease': ('github-prerelease',
                           'shardloom.github_prerelease_channel_proof.v1',
                           'f0f3db4c3cdddd5d063aeccd097f378c8e8d93da6220afac8a14af92f24c6b77'),
     'testpypi': ('testpypi',
                  'shardloom.python_registry_package_proof.v1',
                  '5b882712178cafe237a7600d65d63d8816da350aed9e6a91bd2acfa7752cc560'),
     'pypi': ('pypi',
              'shardloom.python_registry_package_proof.v1',
              '8ebf62d569ff4d4d66175461f5c96f6336cd20e1f97fabe0dc0933fd9ec27389'),
     'homebrew_tap': ('homebrew',
                      'shardloom.homebrew_channel_proof.v1',
                      'e10e8ffbbecf7b7f6c809708ff59f854c7db87d0f6447bb215c0a7113fb1aa6a')},
    "0.3.2": {'github_prerelease': ('github-prerelease',
                           'shardloom.github_prerelease_channel_proof.v1',
                           '6147f1748013726b713a4fe423e02fa3f86352ffd916d07ebb4821e937da9741'),
     'testpypi': ('testpypi',
                  'shardloom.python_registry_package_proof.v1',
                  '28da148cb02574ec56847319b9cfd01502c1c2f69446ba28fc541d2ea0d0d365'),
     'pypi': ('pypi',
              'shardloom.python_registry_package_proof.v1',
              '1ccfccfcd41f81d597dbc60f0c07b1c0b5873d21e081f517b5349cca3699e0ac'),
     'homebrew_tap': ('homebrew',
                      'shardloom.homebrew_channel_proof.v1',
                      'fddeb426c32967eb92d3ec2c890c6fc48b0ee8bc66fdd9a303022b80183cb2db')},
    "0.3.1": {
        "github_prerelease": ("github-prerelease", "shardloom.github_prerelease_channel_proof.v1",
            "f09959f30735623180ea839a262dc589d2ba596257b02cfb23fd1bc145b6d64c"),
        "testpypi": ("testpypi", "shardloom.python_registry_package_proof.v1",
            "d6005d8c9c2b055e7014bc88651cb4987c8aea75f53ccd2889d373fd1d7f38b0"),
        "pypi": ("pypi", "shardloom.python_registry_package_proof.v1",
            "2caf0dedaa54742afc2272809c374a1dcac5fb3b256d32c15525f509d45e767d"),
        "homebrew_tap": ("homebrew", "shardloom.homebrew_channel_proof.v1",
            "c61c68ba6a6275a9500a512c1501f032dfd5d2a36187751d43533b29e0858619"),
    },
    "0.3.0": {
        "github_prerelease": ("github-prerelease", "shardloom.github_prerelease_channel_proof.v1",
            "ba4eca5fe4675cbf48fcc54380902e93da351b72cf2ff7178f2c2474f18faa43"),
        "testpypi": ("testpypi", "shardloom.python_registry_package_proof.v1",
            "210550a1ade7b6c84319aa7afdad4736a2ae0fbcfc71d058a833e497678fc4bf"),
        "pypi": ("pypi", "shardloom.python_registry_package_proof.v1",
            "65e3d697980abae449b285832afbd6452793f7042841b18244b082b87b76757c"),
        "homebrew_tap": ("homebrew", "shardloom.homebrew_channel_proof.v1",
            "bb2cb345d1097cd0df3256c5b6e97661be61e07bed7269077a115d8980537adf"),
    },
    "0.2.4": {
        "github_prerelease": ("github-prerelease", "shardloom.github_prerelease_channel_proof.v1",
            "33ddeaae56d49a2942ea7fde303dc57902ad286722e0b0a4c04274381919ef43"),
        "testpypi": ("testpypi", "shardloom.python_registry_package_proof.v1",
            "1503562681588e8e1fb4b7c8195f68958b3c22d2047aa65cbee78cefd56854e8"),
        "pypi": ("pypi", "shardloom.python_registry_package_proof.v1",
            "2ba6c818a6fe78ed5b9954d41edf12d688125271cbe64f86620fedc7d8a1d895"),
        "homebrew_tap": ("homebrew", "shardloom.homebrew_channel_proof.v1",
            "bf4d86205eabab40727bb000dc82c9a1fb6d0ff1b0e9f3cb8fa1af0aabd05e94"),
    },
}
# These observations were produced by inspecting the eight downloaded workflow
# distributions against the published wheel hashes. Their immutable records bind
# the extracted CLI hashes/sizes, source inputs, and checksum/SBOM hashes.
PUBLISHED_REGISTRY_PROVENANCE_SHA256 = {
    "0.5.1": {'testpypi': '2dd96b0bbf62f99b433ddd1c4c130923b886123e7902a2c2c9547e851f445269',
     'pypi': '09a054ac3fdf9cbee6f1cc138dd9a80dbf5b42ca6bdccc7c6654fd916e7c3713'},
    "0.4.0": {'testpypi': 'b636290d7005337bc9894932b145f468a97563f39f5a8b00229c22141c39eb09',
     'pypi': '80ecf90be526f606ae4729d30c7f6b10e3da2ecd79265bab83b7d638a5fd834d'},
    "0.3.3": {'testpypi': '0e8df9bc8d1498a0f98d414503f82636fac5b5b65a88a9e3a646067ea1f0a79e',
     'pypi': 'd013801ade4732cda9286897236a6b12347d2b560903b6074aaadfed16c9be1c'},
    "0.3.2": {'testpypi': 'f68b130f05b77389bf480b13533a9eb56594e8a06496bd4fe90660b280a2d2dc',
     'pypi': 'e6c4935074ac3cf8bd466cf04e48de12cb928785bdafcb6a02d815aaeb4bf272'},
    "0.3.1": {
        "testpypi": "dba35b1b4fdde2651fed14798c0e376eda2d986c6f941243c1e5a89479dca98b",
        "pypi": "f651ae27d2d5d528d6486a36f2d04227f0c323e7dd4def87fad1afd093e1e341",
    },
    "0.3.0": {
        "testpypi": "b3ef2dc0587117a9cfd3799b3ebcc4e9c235a9b3084b1c9bdd3b60d325bc7273",
        "pypi": "0b8bc004896c98224063410057f8bcfd971d56de5f3d9a53f1ffcab478dfe424",
    },
    "0.2.4": {
        "testpypi": "0e8af91e69e09c005be2e1a455959d50fef637ef7a92738ae15a85ba9f102d43",
        "pypi": "e288722b5b34a5e639deb7c47c78a8b56b2bcad638ce5824132d879091b0d3e7",
    },
}
SELECTED_PACKAGE_CHANNEL_STATUS_MARKER = (
    f"published_v{SELECTED_PACKAGE_RELEASE_VERSION}_selected_channels"
)
SELECTED_PACKAGE_INSTALL_SPEC = f"shardloom=={SELECTED_PACKAGE_RELEASE_VERSION}"
SELECTED_PACKAGE_GITHUB_DOWNLOAD_COMMAND_MARKER = (
    f"gh release download {SELECTED_PACKAGE_RELEASE_TAG}"
)

SELECTED_V0_1_0_FEASIBILITY_STATUS = "included_channel_proof_passed"
SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS = "approved_channel_proof_passed"
SELECTED_V0_1_0_INSTALL_ACCESS_BOUNDARY = (
    f"selected {SELECTED_PACKAGE_RELEASE_TAG} GitHub/TestPyPI/PyPI/Homebrew install access"
)


def selected_channel_ids(matrix: dict[str, Any] | None) -> list[str]:
    """Return the selected release-channel ids from a matrix, or the canonical ids."""
    if isinstance(matrix, dict):
        ids = matrix.get("selected_v0_1_0_release_channel_ids")
        if ids == SELECTED_V0_1_0_RELEASE_CHANNEL_IDS:
            return list(ids)
    return list(SELECTED_V0_1_0_RELEASE_CHANNEL_IDS)


def channel_rows(matrix: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(matrix, dict):
        return []
    rows = matrix.get("channels", [])
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def selected_channel_rows(matrix: dict[str, Any] | None) -> list[dict[str, Any]]:
    selected = set(selected_channel_ids(matrix))
    return [row for row in channel_rows(matrix) if row.get("channel_id") in selected]


def selected_channels_ready(matrix: dict[str, Any] | None) -> bool:
    rows = selected_channel_rows(matrix)
    return len(rows) == len(SELECTED_V0_1_0_RELEASE_CHANNEL_IDS) and all(
        row.get("ready") is True for row in rows
    )


def selected_ready_channel_count(matrix: dict[str, Any] | None) -> int:
    return sum(1 for row in selected_channel_rows(matrix) if row.get("ready") is True)
