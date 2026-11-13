# Solution: Assignment A - Interaction Ledger & Reporting Pipeline

Python 3.11+, SQLite (stdlib), one third-party dependency (`cryptography`,
for PII encryption at rest).

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# generate the sample data (not committed)
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data

# ingest both drops, then report
python -m solution.pipeline ingest data/drop_1
python -m solution.pipeline ingest data/drop_2
python -m solution.pipeline report --out reports/

# tests
python -m pytest solution/tests
```

## Interface

```
python -m solution.pipeline [--db pipeline.db] ingest <path>
python -m solution.pipeline [--db pipeline.db] report --out <dir>
```

- `<path>` may be a drop directory, a directory containing `drop_*`
  directories (ingested in sorted order), or a single CSV/JSONL file.
- All state lives in one SQLite database (default `./pipeline.db`).
  Re-ingesting an already-ingested file changes nothing.
- Reports are recomputed from the ledger on every `report` run, so they
  always reflect everything ingested so far.

## Layout

```
solution/
├── pipeline/
│   ├── cli.py        # argument parsing / entry point
│   ├── schema.py     # workflow schemas + disposition catalogue loading
│   ├── normalize.py  # header / phone / date / decimal / enum / timestamp rules
│   ├── crypto.py     # PII: HMAC matching keys + Fernet encryption
│   ├── db.py         # SQLite schema + counters
│   ├── ingest.py     # lead & event ingestion, dedupe, orphan relinking
│   ├── derive.py     # pure functions: effective disposition, lead bucket
│   └── report.py     # the five output files
└── tests/            # pytest: unit rules + hand-computed end-to-end run
```

Set `AU_PII_KEY` to any strong secret for real data; without it a fixed
development key is used (with a warning) so the demo runs out of the box.
