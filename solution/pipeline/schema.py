"""Loading of workflow lead-schemas and the disposition catalogue."""
from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path

from .normalize import normalize_header

# Repo layout: <root>/solution/pipeline/schema.py -> parents[2] is the root.
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMAS_DIR = REPO_ROOT / "assignment-a-data" / "schemas"


@dataclass
class Field:
    """One field of a workflow's lead schema."""

    name: str
    type: str
    required: bool = False
    pii: bool = False
    reference: bool = False
    values: list[str] | None = None
    default: str | None = None
    aliases: list[str] = field(default_factory=list)

    def matches(self, normalized_header: str) -> bool:
        """True when an already-normalised CSV header names this field,
        either by the field's own name or by one of its aliases."""
        if normalize_header(self.name) == normalized_header:
            return True
        return any(normalize_header(alias) == normalized_header for alias in self.aliases)


@dataclass
class WorkflowSchema:
    workflow_id: str
    fields: list[Field]

    @property
    def reference_field(self) -> Field:
        return next(f for f in self.fields if f.reference)


@dataclass
class Disposition:
    code: str
    label: str
    is_terminal: bool
    is_success: bool


def load_schemas(schemas_dir: Path = DEFAULT_SCHEMAS_DIR) -> dict[str, WorkflowSchema]:
    """Read every ``*.json`` lead schema, keyed by workflow_id."""
    schemas: dict[str, WorkflowSchema] = {}
    for path in sorted(schemas_dir.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        fields = [Field(**f) for f in raw["fields"]]
        schemas[raw["workflow_id"]] = WorkflowSchema(raw["workflow_id"], fields)
    return schemas


def load_dispositions(
    path: Path = DEFAULT_SCHEMAS_DIR / "dispositions.csv",
) -> dict[str, dict[str, Disposition]]:
    """Read the disposition catalogue: workflow_id -> code -> Disposition."""
    catalogue: dict[str, dict[str, Disposition]] = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            disp = Disposition(
                code=row["code"],
                label=row["label"],
                is_terminal=row["is_terminal"].strip().lower() == "true",
                is_success=row["is_success"].strip().lower() == "true",
            )
            catalogue.setdefault(row["workflow_id"], {})[disp.code] = disp
    return catalogue
