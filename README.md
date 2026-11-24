# Digital Health Wearable Pipeline

This local Python prototype ingests timestamped wearable CSVs using a JSON event envelope. It preserves the original bytes under their SHA-256 hash and records queued work in SQLite. A worker writes long-form measurements and a quality-control summary with source lineage.

Requires Python 3.11+ on macOS or Linux. There are no third-party dependencies.

```sh
python3 pipeline.py ingest --root ./demo-lake --event sample-event.json --data sample-stream.csv
python3 pipeline.py work --root ./demo-lake
python3 pipeline.py status --root ./demo-lake
```

The synthetic example produces eight measurements from two valid rows and counts one malformed row. The first CSV column must be `timestamp`, with ISO 8601 timestamps that include an offset. Remaining columns identify numerical measurement channels. The worker preserves timestamp strings and channel names, and rejects rows with missing, invalid, or nonfinite measurements.

An envelope supplies tenant, study, pseudonymous participant, session, stream, provider, and event ID. Provider and event ID jointly identify an event. Repeating the same envelope and bytes returns the existing job. Reusing its key with different input returns an error. A stopped worker leaves its running job available for recovery on the next worker run.

After correcting a parser issue, requeue the failed job and rerun the worker.

```sh
python3 pipeline.py retry --root ./demo-lake --job-id 1
python3 pipeline.py work --root ./demo-lake
```

Use synthetic data in this prototype. The portfolio's cloud deployment, provider integrations, access and consent enforcement, and clinical validation remain future work.

Run the CLI integration tests with `python3 -m unittest -v`.
