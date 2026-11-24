"""End-to-end checks for the public CLI."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent


class PipelineTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / "lake"
        self.event_path = Path(temporary.name) / "event.json"
        self.event = json.loads((HERE / "sample-event.json").read_text())
        self.data = HERE / "sample-stream.csv"

    def cli(self, command, *args, exit_code=0):
        result = subprocess.run(
            [sys.executable, str(HERE / "pipeline.py"), command,
             "--root", str(self.root), *map(str, args)],
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, exit_code, result.stderr)
        return json.loads(result.stdout) if result.stdout else None

    def ingest(self, exit_code=0):
        self.event_path.write_text(json.dumps(self.event))
        return self.cli("ingest", "--event", self.event_path, "--data", self.data,
                        exit_code=exit_code)

    def test_duplicate_delivery_and_malformed_timestamp(self):
        first = self.ingest()
        self.assertFalse(first["duplicate"])
        self.assertEqual(self.ingest(), dict(first, duplicate=True))
        self.assertEqual(self.cli("work"), {"succeeded": 1, "failed": 0})
        self.assertEqual(self.cli("work"), {"succeeded": 0, "failed": 0})
        job, = self.cli("status")
        self.assertEqual(job["state"], "succeeded")
        raw = (self.root / job["raw_path"]).read_bytes()
        self.assertEqual(raw, self.data.read_bytes())
        output = self.root / job["output_path"]
        summary = json.loads(output.with_name("summary.json").read_text())
        self.assertEqual(summary["source_hash"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(summary["malformed_rows"], 1)
        self.assertEqual(summary["measurements"], 8)
        self.assertEqual(len(output.read_text().splitlines()), 9)

    def test_path_traversal_is_rejected(self):
        self.event["participant"] = "../escape"
        self.ingest(exit_code=1)
        self.assertEqual(self.cli("status"), [])
        self.assertFalse((self.root / "raw").exists())

    def test_conflicting_duplicate_is_rejected(self):
        self.ingest()
        self.event["session"] = "followup"
        self.ingest(exit_code=1)
        job, = self.cli("status")
        self.assertEqual(job["envelope"]["session"], "baseline")

    def test_concurrent_duplicate_delivery_creates_one_job_and_raw_object(self):
        self.event_path.write_text(json.dumps(self.event))
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = [
                pool.submit(self.cli, "ingest", "--event", self.event_path,
                            "--data", self.data)
                for _ in range(6)
            ]
            results = [future.result() for future in futures]
        self.assertEqual(sum(not result["duplicate"] for result in results), 1)
        self.assertEqual(len({result["job_id"] for result in results}), 1)
        self.assertEqual(len(list((self.root / "raw").rglob("*.csv"))), 1)
        self.assertEqual(len(self.cli("status")), 1)

    def test_worker_recovers_a_job_left_running(self):
        self.ingest()
        with sqlite3.connect(self.root / "catalog.sqlite3") as db:
            db.execute("UPDATE jobs SET state = 'running', attempts = 1")
        self.assertEqual(self.cli("work"), {"succeeded": 1, "failed": 0})
        job, = self.cli("status")
        self.assertEqual(job["state"], "succeeded")
        self.assertEqual(job["attempts"], 2)
        self.assertTrue((self.root / job["output_path"]).is_file())

    def test_worker_counts_bad_measurements_without_losing_good_rows(self):
        self.data = self.event_path.with_name("edge-cases.csv")
        self.data.write_text(
            "timestamp,ecg_mv\n"
            "2026-01-01T01:00:00+01:00,0.12\n"
            "2026-01-01T01:00:00,0.13\n"
            "2026-01-01T00:00:00Z,nan\n"
            "2026-01-01T00:00:00Z,inf\n"
            "2026-01-01T00:00:00Z\n"
            "2026-01-01T00:00:00Z,0.14,extra\n"
        )
        self.ingest()
        self.cli("work")
        job, = self.cli("status")
        output = self.root / job["output_path"]
        summary = json.loads(output.with_name("summary.json").read_text())
        self.assertEqual(summary["valid_rows"], 1)
        self.assertEqual(summary["malformed_rows"], 5)
        self.assertEqual(output.read_text().splitlines(), [
            "timestamp,channel,value",
            "2026-01-01T01:00:00+01:00,ecg_mv,0.12",
        ])

    def test_failed_job_can_be_requeued(self):
        self.data = self.event_path.with_name("invalid.csv")
        self.data.write_text("unexpected,header\n")
        job_id = self.ingest()["job_id"]
        self.assertEqual(self.cli("work", exit_code=1), {"succeeded": 0, "failed": 1})
        job, = self.cli("status")
        self.assertEqual(job["state"], "failed")
        self.assertIn("timestamp", job["error"])
        self.assertEqual(self.cli("retry", "--job-id", job_id)["state"], "queued")
        self.cli("work", exit_code=1)
        job, = self.cli("status")
        self.assertEqual(job["attempts"], 2)


if __name__ == "__main__":
    unittest.main()
