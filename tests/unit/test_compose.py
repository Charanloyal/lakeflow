"""Static checks of the compose start-up order (stdlib only; the real start-up runs in CI's data-plane job)."""

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
CTL = (ROOT / "scripts" / "lakeflowctl.py").read_text(encoding="utf-8")


def services() -> dict[str, str]:
    """Top-level service name -> its block of text."""
    body = COMPOSE.split("\nservices:\n", 1)[1].split("\nvolumes:\n", 1)[0]
    blocks: dict[str, str] = {}
    name = None
    for line in body.splitlines():
        match = re.match(r"^  ([a-z0-9-]+):\s*$", line)
        if match:
            name = match.group(1)
            blocks[name] = ""
        elif name:
            blocks[name] += line + "\n"
    return blocks


class ComposeStartupTests(unittest.TestCase):
    def setUp(self):
        self.services = services()

    def test_one_shot_services_exist_and_never_restart(self):
        one_shot = re.search(r"^ONE_SHOT = \[(.*)\]$", CTL, re.MULTILINE).group(1)
        names = re.findall(r'"([a-z0-9-]+)"', one_shot)
        self.assertIn("minio-init", names)
        for name in names:
            with self.subTest(name):
                self.assertIn(name, self.services)
                self.assertIn('restart: "no"', self.services[name])

    def test_catalog_waits_for_the_warehouse_bucket(self):
        # Regression: MinIO reported healthy before the bucket existed and Spark crash-looped on NoSuchBucket.
        self.assertRegex(
            self.services["iceberg-rest"], r"minio-init: \{ condition: service_completed_successfully \}"
        )
        self.assertIn("minio: { condition: service_healthy }", self.services["minio-init"])

    def test_bucket_created_by_init_is_the_catalog_warehouse(self):
        bucket = re.search(r"WAREHOUSE_BUCKET: (\S+)", self.services["minio-init"]).group(1)
        warehouse = re.search(r"CATALOG_WAREHOUSE: s3://([^/\s]+)/", self.services["iceberg-rest"]).group(1)
        self.assertEqual(bucket, warehouse)
        script = (ROOT / "platform" / "minio" / "create-bucket.sh").read_text(encoding="utf-8")
        self.assertIn("mb --ignore-existing", script, "bucket creation must be idempotent")
        self.assertIn("exit 1", script, "a bucket that cannot be created must fail the start-up loudly")


if __name__ == "__main__":
    unittest.main()
