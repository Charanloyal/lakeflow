"""CI outcome reporting (scripts/ci_annotate.py): skips and expected failures must be visible, not folded into green."""

import contextlib
import importlib.util
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("ci_annotate", ROOT / "scripts" / "ci_annotate.py")
ci_annotate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci_annotate)

# Shape of pytest's --junitxml output for each outcome.
REPORT = """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest" errors="1" failures="1" skipped="2" tests="6">
  <testcase classname="t" name="passes"/>
  <testcase classname="t" name="fails"><failure message="assert 1 == 2">E assert 1 == 2</failure></testcase>
  <testcase classname="t" name="errors"><error message="fixture failed">boom</error></testcase>
  <testcase classname="t" name="skipped"><skipped type="pytest.skip" message="profile not running"/></testcase>
  <testcase classname="t" name="xfail"><skipped type="pytest.xfail" message="not yet proven in CI"/></testcase>
  <testcase classname="t" name="xpass_non_strict"/>
</testsuite></testsuites>
"""


class JunitOutcomeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.report = Path(self.tmp.name) / "report.xml"
        self.report.write_text(REPORT, encoding="utf-8")

    def test_counts_every_outcome_separately(self):
        self.assertEqual(
            ci_annotate.junit_outcomes(str(self.report)),
            {"tests": 6, "passed": 2, "failed": 1, "errors": 1, "skipped": 1, "xfailed": 1},
        )

    def test_summary_writes_a_table_row_per_report(self):
        target = Path(self.tmp.name) / "summary.md"
        with mock.patch.dict(os.environ, {"GITHUB_STEP_SUMMARY": str(target)}):
            with contextlib.redirect_stdout(io.StringIO()) as out:
                ci_annotate.summary(str(self.report), "integration")
                ci_annotate.summary(str(Path(self.tmp.name) / "absent.xml"), "e2e")
        rows = target.read_text(encoding="utf-8").splitlines()
        self.assertEqual(rows[0], "| Suite | Passed | Failed/errors | Skipped | Expected failures | Total |")
        self.assertEqual(rows[2], "| integration | 2 | 2 | 1 | 1 | 6 |")
        self.assertIn("report not produced", rows[3])
        self.assertIn("::warning", out.getvalue(), "a missing report must be loud, not silently green")


if __name__ == "__main__":
    unittest.main()
