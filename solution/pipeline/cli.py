"""Command-line interface.

    python -m solution.pipeline ingest data/drop_1
    python -m solution.pipeline ingest data/drop_2
    python -m solution.pipeline report --out reports/

``ingest`` accepts a drop directory, a directory containing drops (each
``drop_*`` sub-directory is ingested in sorted order), or a single CSV/JSONL
file. All state lives in one SQLite database (default ``./pipeline.db``), so
re-running a command picks up exactly where the last one left off.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from . import db
from .crypto import PiiProtector
from .ingest import Pipeline
from .report import generate_reports
from .schema import load_dispositions, load_schemas


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="pipeline", description="Interaction ledger & reporting pipeline"
    )
    parser.add_argument("--db", default="pipeline.db", help="SQLite database path")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="ingest a drop (or a tree of drops)")
    p_ingest.add_argument("path", help="drop dir, parent of drops, or one CSV/JSONL file")

    p_report = sub.add_parser("report", help="write the five report files")
    p_report.add_argument("--out", required=True, help="output directory")

    args = parser.parse_args(argv)

    schemas = load_schemas()
    catalogues = load_dispositions()
    conn = db.connect(args.db)
    pii = PiiProtector.from_env()

    if args.command == "ingest":
        started = time.monotonic()
        Pipeline(conn, pii, schemas).ingest_path(Path(args.path))
        print(f"ingested {args.path} in {time.monotonic() - started:.1f}s")
        return 0

    if args.command == "report":
        started = time.monotonic()
        quality = generate_reports(conn, catalogues, Path(args.out))
        print(f"reports written to {args.out} in {time.monotonic() - started:.1f}s")
        print(f"leads: {quality['lead_rows_total']} rows, "
              f"{quality['lead_rows_rejected']} rejected, "
              f"{quality['duplicate_lead_rows_merged']} duplicates merged | "
              f"events: {quality['events_total']} read, "
              f"{quality['orphan_events']} orphans, "
              f"{quality['duplicate_events_dropped']} duplicates")
        return 0

    return 2  # unreachable, argparse enforces the subcommand


if __name__ == "__main__":
    sys.exit(main())
