"""Regression tests for the checksum selected by the release verifier."""
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/release.yml"
SELECTOR_MARKER = (
    'python3 - "$archive" release-assets/published/checksum.toml <<\'PY\'\n'
)


class ReleaseChecksumTests(unittest.TestCase):
    def test_selects_archive_digest_not_embedded_library_digest(self):
        workflow = WORKFLOW.read_text()
        ci = (ROOT / ".github/workflows/ci.yml").read_text()
        self.assertEqual(
            ci.count("run: python3 .github/scripts/test_release_checksum.py"), 1
        )
        self.assertIn(
            'gh release download "$RELEASE_TAG" --repo "$REPOSITORY" '
            '--pattern checksum.toml --dir release-assets/published',
            workflow,
        )
        self.assertIn(SELECTOR_MARKER, workflow)
        source_start = workflow.index(SELECTOR_MARKER) + len(SELECTOR_MARKER)
        source_end = workflow.index("\n          PY", source_start)
        selector = textwrap.dedent(workflow[source_start:source_end])

        archive = "tinyeventqueues-0.1.3-ubuntu-24.04-x86_64.tar.gz"
        archive_digest = "4daf1ad06fdeb8fb5410ff48572aead4db4ce5d9446736bb5637e3083d6a3072"
        library_digest = "718a226caac47b8fd3a570138c6e485dd89367e64a393231afa6428796586494"
        (ROOT / "target").mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="release-checksum-selection-", dir=ROOT / "target"
        ) as scratch:
            checksum = Path(scratch) / "checksum.toml"
            checksum.write_text(
                f'[sha256]\n"{archive}" = "{archive_digest}"\n\n'
                f'[library_sha256]\n"{archive}" = "{library_digest}"\n'
            )
            result = subprocess.run(
                [sys.executable, "-c", selector, archive, str(checksum)],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, f"{archive_digest}\n")
            self.assertNotEqual(result.stdout.strip(), library_digest)

            checksum.write_text(
                f'[library_sha256]\n"{archive}" = "{library_digest}"\n'
            )
            missing_archive = subprocess.run(
                [sys.executable, "-c", selector, archive, str(checksum)],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(missing_archive.returncode, 0)
            self.assertEqual(missing_archive.stdout, "")


if __name__ == "__main__":
    unittest.main()
