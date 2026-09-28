"""Parse ADR markdown files (docs/adr/NNNN-*.md) into structured records for the UI."""

from __future__ import annotations

import re
from pathlib import Path

_TITLE = re.compile(r"^#\s+ADR[- ](\d{4}):\s*(.+?)\s*$")
_FIELD = re.compile(r"^\*\*(Status|Date|Deciders)\*\*:\s*(.+?)\s*$", re.IGNORECASE)
_SECTION = re.compile(r"^##\s+(.+?)\s*$")


def parse_adr(text: str, filename: str = "") -> dict:
    record: dict = {"id": None, "title": None, "status": None, "date": None, "file": filename, "sections": {}}
    current: str | None = None
    lines: list[str] = []
    for line in text.splitlines():
        title = _TITLE.match(line)
        if title and record["id"] is None:
            record["id"], record["title"] = f"ADR-{title.group(1)}", title.group(2)
            continue
        meta = _FIELD.match(line)
        if meta and current is None:
            record[meta.group(1).lower()] = meta.group(2)
            continue
        section = _SECTION.match(line)
        if section:
            if current is not None:
                record["sections"][current] = "\n".join(lines).strip()
            current, lines = section.group(1), []
            continue
        if current is not None:
            lines.append(line)
    if current is not None:
        record["sections"][current] = "\n".join(lines).strip()
    if record["id"] is None:
        raise ValueError(f"{filename or 'ADR'}: first heading must look like '# ADR-0001: Title'")
    return record


def load_adrs(directory: str | Path) -> list[dict]:
    paths = sorted(Path(directory).glob("[0-9][0-9][0-9][0-9]-*.md"))
    return [parse_adr(p.read_text(encoding="utf-8"), p.name) for p in paths]
