#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Regressions for the actual broken-link classes found in the public guide."""
import tempfile
import unittest
from pathlib import Path

from website_links import check_site_links


class WebsiteLinkTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def write(self, path, text):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def blockers(self):
        return check_site_links(self.root)["blockers"]

    def test_missing_same_page_fragment_is_not_ignored(self):
        self.write("index.html", '<h2 id="evidence--certificates">Evidence + Certificates</h2><a href="#evidence-certificates">Evidence</a>')
        self.assertIn("missing fragment #evidence-certificates", self.blockers()[0])

    def test_valid_relative_and_encoded_fragments(self):
        self.write("guide/index.html", '<link rel="canonical" href="https://shardloom.io/guide/"><a href="../target#caf%C3%A9">Target</a>')
        self.write("target/index.html", '<h2 id="café">Title</h2>')
        self.assertEqual(self.blockers(), [])

    def test_redirect_resolves_to_real_section(self):
        self.write("_redirects", "/old /guide#prepare-once 301\n")
        self.write("index.html", '<a href="/old">Old bookmark</a>')
        self.write("guide/index.html", '<h2 id="prepare-once">Prepare once</h2>')
        self.assertEqual(self.blockers(), [])

    def test_unlinked_redirect_must_have_valid_fragment(self):
        self.write("_redirects", "/old /guide#missing 301\n")
        self.write("guide/index.html", '<h2 id="prepare-once">Prepare once</h2>')
        self.assertTrue(any("missing fragment #missing" in value for value in self.blockers()))

    def test_redirect_substring_does_not_admit_missing_page(self):
        self.write("_redirects", "/missing-longer / 301\n")
        self.write("index.html", '<a href="/missing">Missing</a>')
        self.assertTrue(any("missing target: /missing" in value for value in self.blockers()))

    def test_redirect_cycle_fails(self):
        self.write("_redirects", "/one /two 301\n/two /one 301\n")
        self.write("index.html", '<a href="/one">Loop</a>')
        self.assertTrue(any("redirect cycle" in value for value in self.blockers()))

    def test_missing_compiled_asset_fails(self):
        self.write("index.html", '<script src="/_astro/missing.js"></script>')
        self.assertTrue(any("missing target: /_astro/missing.js" in value for value in self.blockers()))

    def test_external_link_is_reported_without_network_request(self):
        self.write("index.html", '<a href="https://example.com/docs">Docs</a>')
        report = check_site_links(self.root)
        self.assertEqual(report["blockers"], [])
        self.assertEqual(report["external_links"], ["https://example.com/docs"])


if __name__ == "__main__":
    unittest.main()
