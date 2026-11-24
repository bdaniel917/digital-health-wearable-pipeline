"""Local wearable ingestion and processing CLI."""
import argparse
from contextlib import closing, contextmanager
import csv
from datetime import datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import tempfile

PARTITIONS = ("tenant", "study", "participant", "session", "stream")
EVENT_FIELDS = (*PARTITIONS, "provider", "event_id")
PROCESSOR_VERSION = "1"
SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    provider TEXT NOT NULL,
    event_id TEXT NOT NULL,
    envelope TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    raw_path TEXT NOT NULL,
    received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    state TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    processor_version TEXT,
    output_path TEXT,
    error TEXT,
    UNIQUE (provider, event_id)
)
"""


@contextmanager
def atomic_file(path, *, binary=False, immutable=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    options = {} if binary else {"encoding": "utf-8", "newline": ""}
    with tempfile.NamedTemporaryFile(
        mode="wb" if binary else "w", dir=path.parent, delete=False, **options
    ) as temporary:
        staged = Path(temporary.name)
        try:
            yield temporary
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary.close()
            if immutable:
                try:
                    path.hardlink_to(staged)  # Publish the file without replacing an existing object.
                except FileExistsError:
                    pass
            else:
                staged.replace(path)
        finally:
            staged.unlink(missing_ok=True)


def read_event(path):
    event = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(event, dict):
        raise ValueError("Event envelope must be a JSON object")
    for field in EVENT_FIELDS:
        value = event.get(field)
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
            raise ValueError(f"Invalid or missing event field: {field}")
    return event


def ingest(db, root, event_file, data_file):
    event = read_event(event_file)
    payload = data_file.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    raw_path = Path("raw", *(event[key] for key in PARTITIONS), f"{digest}.csv")
    envelope = json.dumps(event, sort_keys=True, allow_nan=False)
    with db:
        db.execute("BEGIN IMMEDIATE")
        existing = db.execute(
            "SELECT * FROM jobs WHERE provider = ? AND event_id = ?",
            (event["provider"], event["event_id"]),
        ).fetchone()
        if existing:
            if (existing["source_hash"], existing["envelope"]) != (digest, envelope):
                raise ValueError("Event key already exists with a different payload or envelope")
            return {"job_id": existing["id"], "duplicate": True}
        with atomic_file(root / raw_path, binary=True, immutable=True) as raw:
            raw.write(payload)
        job = db.execute(
            """INSERT INTO jobs (provider, event_id, envelope, source_hash, raw_path)
               VALUES (?, ?, ?, ?, ?)""",
            (event["provider"], event["event_id"], envelope, digest, raw_path.as_posix()),
        )
        return {"job_id": job.lastrowid, "duplicate": False}


def parse_measurements(row):
    timestamp, *measurements = row
    parsed = datetime.fromisoformat(timestamp)
    if parsed.utcoffset() is None:
        raise ValueError("Timestamp requires a UTC offset")
    values = [float(value) for value in measurements]
    if not all(map(math.isfinite, values)):
        raise ValueError("Measurement must be finite")
    return timestamp, values


def process(root, job):
    event = json.loads(job["envelope"])
    directory = Path("derived", *(event[key] for key in PARTITIONS),
                     f"job-{job['id']}", f"v{PROCESSOR_VERSION}")
    output = directory / "measurements.csv"
    valid = malformed = 0
    with (root / job["raw_path"]).open(encoding="utf-8-sig", newline="") as source:
        rows = csv.reader(source)
        header = next(rows, [])
        if (len(header) < 2 or header[0] != "timestamp"
                or not all(name.strip() for name in header)
                or len(set(header)) != len(header)):
            raise ValueError("Expected timestamp followed by unique measurement column names")
        channels = header[1:]
        with atomic_file(root / output) as target:
            writer = csv.writer(target)
            writer.writerow(("timestamp", "channel", "value"))
            for row in rows:
                if len(row) != len(header):
                    malformed += 1
                    continue
                try:
                    timestamp, values = parse_measurements(row)
                except ValueError:
                    malformed += 1
                    continue
                writer.writerows((timestamp, channel, value)
                                 for channel, value in zip(channels, values))
                valid += 1
    with atomic_file(root / directory / "summary.json") as summary:
        json.dump({
            "event": event,
            "raw_path": job["raw_path"],
            "source_hash": job["source_hash"],
            "processor_version": PROCESSOR_VERSION,
            "valid_rows": valid,
            "malformed_rows": malformed,
            "measurements": valid * len(channels),
        }, summary, indent=2)
        summary.write("\n")
    return output.as_posix()


def work(db, root):
    counts = {"succeeded": 0, "failed": 0}
    # The local worker lock is released by the OS if the process exits.
    with (root / ".worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with db:
            db.execute("UPDATE jobs SET state = 'queued' WHERE state = 'running'")
        while True:
            with db:
                job = db.execute("SELECT * FROM jobs WHERE state = 'queued' ORDER BY id LIMIT 1").fetchone()
                if job is None:
                    return counts
                db.execute(
                    """UPDATE jobs SET state = 'running', attempts = attempts + 1,
                       processor_version = ?, error = NULL WHERE id = ?""",
                    (PROCESSOR_VERSION, job["id"]),
                )
            try:
                output = process(root, job)
            except (OSError, ValueError, csv.Error) as error:
                with db:
                    db.execute("UPDATE jobs SET state = 'failed', error = ? WHERE id = ?",
                               (str(error), job["id"]))
                counts["failed"] += 1
            else:
                with db:
                    db.execute("UPDATE jobs SET state = 'succeeded', output_path = ? WHERE id = ?",
                               (output, job["id"]))
                counts["succeeded"] += 1


def retry(db, job_id):
    with db:
        result = db.execute(
            "UPDATE jobs SET state = 'queued', error = NULL WHERE id = ? AND state = 'failed'",
            (job_id,),
        )
        if result.rowcount != 1:
            raise ValueError(f"Job {job_id} must exist and be failed")
    return {"job_id": job_id, "state": "queued"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("ingest", "work", "status", "retry"):
        command = commands.add_parser(name)
        command.add_argument("--root", required=True, type=Path)
        if name == "ingest":
            command.add_argument("--event", required=True, type=Path)
            command.add_argument("--data", required=True, type=Path)
        if name == "retry":
            command.add_argument("--job-id", required=True, type=int)
    args = parser.parse_args()
    try:
        args.root.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(args.root / "catalog.sqlite3", timeout=30)) as db:
            db.row_factory = sqlite3.Row
            db.execute(SCHEMA)
            if args.command == "ingest":
                result = ingest(db, args.root, args.event, args.data)
            elif args.command == "work":
                result = work(db, args.root)
            elif args.command == "retry":
                result = retry(db, args.job_id)
            else:
                jobs = db.execute("SELECT * FROM jobs ORDER BY id")
                result = [dict(job, envelope=json.loads(job["envelope"])) for job in jobs]
    except (OSError, ValueError, sqlite3.Error) as error:
        parser.exit(1, f"error: {error}\n")
    print(json.dumps(result, indent=2))
    return int(args.command == "work" and result["failed"] > 0)


if __name__ == "__main__":
    raise SystemExit(main())
