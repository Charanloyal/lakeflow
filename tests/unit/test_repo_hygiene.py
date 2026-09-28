"""Repository guardrails: no committed secrets and no unsupported claims (the failure modes of v1)."""

import re
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TEXT_SUFFIXES = {
    ".py",
    ".md",
    ".json",
    ".yml",
    ".yaml",
    ".ts",
    ".tsx",
    ".js",
    ".mjs",
    ".sql",
    ".sh",
    ".properties",
    ".conf",
    ".toml",
    ".txt",
    ".example",
    ".cfg",
    ".css",
    "",
}
SELF = Path(__file__).resolve()
AUDIT = ROOT / "docs" / "audit"

SECRET_PATTERNS = {
    "private key": re.compile(r"-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "AWS access key": re.compile(r"AKIA[0-9A-Z]{16}"),
    "GitHub token": re.compile(r"gh[pousr]_[A-Za-z0-9]{36}"),
    "v1 MinIO password": re.compile(r"password123"),
    "v1 Fernet key": re.compile(r"46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqJZPo="),
}
CLAIM_PATTERNS = {
    "v1 fabricated throughput": re.compile(r"423[,.]?062"),
    "v1 fabricated speedup": re.compile(r"75\.4\s*x", re.IGNORECASE),
    "unqualified production-ready": re.compile(r"production[- ]ready", re.IGNORECASE),
    "100% exactly-once": re.compile(r"100\s*%\s*exactly", re.IGNORECASE),
}
EXACTLY_ONCE = re.compile(r"exactly[- ]once", re.IGNORECASE)
QUALIFIERS = re.compile(
    r"\b(not|no|never|without|effectively|qualif|instead|rather|isn't|is not|does not)\b", re.IGNORECASE
)


def tracked_files():
    try:
        out = subprocess.run(
            ["git", "ls-files", "-co", "--exclude-standard"], cwd=ROOT, capture_output=True, text=True, check=True
        ).stdout
        paths = [ROOT / line for line in out.splitlines() if line]
    except (OSError, subprocess.CalledProcessError):
        paths = [p for p in ROOT.rglob("*") if p.is_file() and ".git" not in p.parts]
    return [p for p in paths if p.is_file() and p.suffix in TEXT_SUFFIXES and "node_modules" not in p.parts]


class HygieneTests(unittest.TestCase):
    def test_no_secrets_committed(self):
        for path in tracked_files():
            if path.resolve() == SELF:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for label, pattern in SECRET_PATTERNS.items():
                self.assertIsNone(pattern.search(text), f"{label} found in {path.relative_to(ROOT)}")

    def test_no_unsupported_claims(self):
        for path in tracked_files():
            if path.resolve() == SELF or AUDIT in path.resolve().parents:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for label, pattern in CLAIM_PATTERNS.items():
                self.assertIsNone(pattern.search(text), f"{label} in {path.relative_to(ROOT)}")
            for line in text.splitlines():
                if EXACTLY_ONCE.search(line):
                    self.assertRegex(line, QUALIFIERS, f"unqualified exactly-once claim in {path.relative_to(ROOT)}")

    def test_env_example_has_no_real_secrets(self):
        example = ROOT / ".env.example"
        for line in example.read_text(encoding="utf-8").splitlines():
            if "=" not in line or line.lstrip().startswith("#"):
                continue
            name, value = line.split("=", 1)
            if re.search(r"(PASSWORD|SECRET|_KEY)$", name) and value:
                self.assertTrue(
                    value.startswith("local-only-") or value == "__GENERATE__",
                    f"{name} must be a local-only placeholder or __GENERATE__",
                )


if __name__ == "__main__":
    unittest.main()
