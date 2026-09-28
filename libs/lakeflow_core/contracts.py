"""Versioned data-contract registry and validator.

Contracts are JSON Schema (draft 2020-12) documents restricted to the subset below, plus `x-lakeflow` metadata.
Anything outside the subset raises ContractError at load time, so a rule can never be silently ignored.

Supported field keywords: type (incl. unions with "null"), enum, pattern, format (uuid, date-time, email),
minLength, maxLength, description, x-decimal, x-pii, x-pii-handling.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

SCHEMA_KEYWORDS = {"$schema", "$id", "title", "description", "type", "x-lakeflow", "required", "properties"}
FIELD_KEYWORDS = {
    "type",
    "enum",
    "pattern",
    "format",
    "minLength",
    "maxLength",
    "description",
    "x-decimal",
    "x-pii",
    "x-pii-handling",
}
META_KEYS = {
    "contract",
    "version",
    "status",
    "owner",
    "source",
    "primary_key",
    "compatibility",
    "classification",
    "freshness_sla",
    "target_table",
    "references",
}
JSON_TYPES = {"string", "integer", "number", "boolean", "null"}
FORMATS = {
    "uuid": re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"),
    "date-time": re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$"),
    "email": re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
}
PII_LEVELS = {"none", "pseudonymous", "direct"}
PII_HANDLING = {"keep", "hash", "drop"}
STATUSES = {"current", "superseded", "proposed", "retired"}
ACTIVE_STATUSES = {"current", "superseded"}
COMPATIBILITY_MODES = {"BACKWARD", "BACKWARD_TRANSITIVE"}
IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


class ContractError(ValueError):
    """Raised when a contract file is invalid or an evolution breaks the compatibility rules."""


@dataclass(frozen=True)
class FieldSpec:
    name: str
    types: tuple[str, ...]
    enum: tuple[Any, ...] | None = None
    pattern: str | None = None
    fmt: str | None = None
    min_length: int | None = None
    max_length: int | None = None
    decimal: tuple[int, int] | None = None
    pii: str = "none"
    pii_handling: str = "keep"

    @property
    def nullable(self) -> bool:
        return "null" in self.types

    @property
    def logical_type(self) -> str:
        """Lakehouse column type derived from the JSON representation."""
        if self.decimal:
            return f"decimal({self.decimal[0]},{self.decimal[1]})"
        if self.fmt == "date-time":
            return "timestamp"
        primary = next(t for t in self.types if t != "null")
        return {"string": "string", "integer": "bigint", "number": "double", "boolean": "boolean"}[primary]

    @property
    def target_column(self) -> str | None:
        """Column name in the lakehouse after PII handling (None when the field is dropped)."""
        if self.pii_handling == "drop":
            return None
        if self.pii_handling == "hash":
            return f"{self.name}_hmac"
        return self.name


@dataclass(frozen=True)
class Violation:
    code: str
    message: str


@dataclass(frozen=True)
class Classification:
    valid: bool
    version: int | None
    violations: tuple[Violation, ...]
    drift_fields: tuple[str, ...]


@dataclass(frozen=True)
class Contract:
    name: str
    version: int
    status: str
    owner: dict
    source: dict
    primary_key: tuple[str, ...]
    compatibility: str
    classification: str
    freshness_sla: dict
    target_table: str
    required: tuple[str, ...]
    fields: dict[str, FieldSpec]
    references: tuple[dict, ...] = ()
    path: str | None = None

    @property
    def topic(self) -> str:
        return self.source["topic"]

    @property
    def source_table(self) -> str:
        return self.source["table"]

    def validate(self, record: Any) -> list[Violation]:
        if not isinstance(record, dict):
            return [Violation("$.type", "row image must be a JSON object")]
        out: list[Violation] = []
        for name in self.required:
            if name not in record:
                out.append(Violation(f"{name}.required", f"required field '{name}' is missing"))
        for name, spec in self.fields.items():
            if name in record:
                out.extend(_check_value(spec, record[name]))
        return out

    def unknown_fields(self, record: dict) -> list[str]:
        return sorted(k for k in record if k not in self.fields)


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return type(value).__name__


def _valid_datetime(value: str) -> bool:
    match = FORMATS["date-time"].match(value)
    if not match:
        return False
    try:
        datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return False
    return True


def _check_value(spec: FieldSpec, value: Any) -> list[Violation]:
    name = spec.name
    jtype = _json_type(value)
    if jtype == "null":
        return [] if spec.nullable else [Violation(f"{name}.null", f"'{name}' must not be null")]
    allowed = set(spec.types)
    if "number" in allowed:
        allowed.add("integer")
    if jtype not in allowed:
        return [Violation(f"{name}.type", f"'{name}' must be {'/'.join(spec.types)}, got {jtype}")]
    out: list[Violation] = []
    if spec.enum is not None and value not in spec.enum:
        out.append(Violation(f"{name}.enum", f"'{name}' value {value!r} is not in {list(spec.enum)}"))
    if isinstance(value, str):
        if spec.pattern is not None and not re.search(spec.pattern, value):
            out.append(Violation(f"{name}.pattern", f"'{name}' does not match {spec.pattern}"))
        if spec.fmt == "date-time":
            if not _valid_datetime(value):
                out.append(Violation(f"{name}.format", f"'{name}' is not an RFC 3339 date-time"))
        elif spec.fmt is not None and not FORMATS[spec.fmt].match(value):
            out.append(Violation(f"{name}.format", f"'{name}' is not a valid {spec.fmt}"))
        if spec.min_length is not None and len(value) < spec.min_length:
            out.append(Violation(f"{name}.min_length", f"'{name}' is shorter than {spec.min_length}"))
        if spec.max_length is not None and len(value) > spec.max_length:
            out.append(Violation(f"{name}.max_length", f"'{name}' is longer than {spec.max_length}"))
        if spec.decimal is not None and not _fits_decimal(value, *spec.decimal):
            out.append(Violation(f"{name}.decimal", f"'{name}' does not fit decimal{spec.decimal}"))
    return out


def _fits_decimal(value: str, precision: int, scale: int) -> bool:
    try:
        number = Decimal(value)
    except InvalidOperation:
        return False
    if not number.is_finite():
        return False
    exponent = number.as_tuple().exponent
    digits = len(number.as_tuple().digits)
    fraction_digits = -exponent if isinstance(exponent, int) and exponent < 0 else 0
    integer_digits = max(digits - fraction_digits, 0)
    return fraction_digits <= scale and integer_digits <= precision - scale


def _parse_field(name: str, raw: dict, where: str) -> FieldSpec:
    unknown = set(raw) - FIELD_KEYWORDS
    if unknown:
        raise ContractError(f"{where}: field '{name}' uses unsupported keywords {sorted(unknown)}")
    types = raw.get("type")
    if isinstance(types, str):
        types = [types]
    if not types or not isinstance(types, list) or not set(types) <= JSON_TYPES or set(types) == {"null"}:
        raise ContractError(f"{where}: field '{name}' needs a supported non-null type, got {raw.get('type')!r}")
    fmt = raw.get("format")
    if fmt is not None and fmt not in FORMATS:
        raise ContractError(f"{where}: field '{name}' uses unsupported format {fmt!r}")
    pattern = raw.get("pattern")
    if pattern is not None:
        re.compile(pattern)
    decimal = raw.get("x-decimal")
    if decimal is not None:
        decimal = (int(decimal["precision"]), int(decimal["scale"]))
        if not 0 <= decimal[1] <= decimal[0] <= 38:
            raise ContractError(f"{where}: field '{name}' has invalid x-decimal {decimal}")
    pii = raw.get("x-pii", "none")
    handling = raw.get("x-pii-handling", "keep")
    if pii not in PII_LEVELS or handling not in PII_HANDLING:
        raise ContractError(f"{where}: field '{name}' has invalid PII metadata {pii!r}/{handling!r}")
    if pii == "direct" and handling == "keep":
        raise ContractError(f"{where}: direct PII field '{name}' must be hashed or dropped")
    if not IDENTIFIER.match(name):
        raise ContractError(f"{where}: field name '{name}' is not a safe identifier")
    enum = raw.get("enum")
    return FieldSpec(
        name=name,
        types=tuple(types),
        enum=tuple(enum) if enum is not None else None,
        pattern=pattern,
        fmt=fmt,
        min_length=raw.get("minLength"),
        max_length=raw.get("maxLength"),
        decimal=decimal,
        pii=pii,
        pii_handling=handling,
    )


def parse_contract(doc: dict, path: str | None = None) -> Contract:
    where = path or doc.get("$id", "<contract>")
    unknown = set(doc) - SCHEMA_KEYWORDS
    if unknown:
        raise ContractError(f"{where}: unsupported top-level keywords {sorted(unknown)}")
    if doc.get("type") != "object":
        raise ContractError(f"{where}: contract root must be type=object")
    meta = doc.get("x-lakeflow")
    if not isinstance(meta, dict):
        raise ContractError(f"{where}: missing x-lakeflow metadata")
    missing = (META_KEYS - {"references"}) - set(meta)
    if missing:
        raise ContractError(f"{where}: x-lakeflow is missing {sorted(missing)}")
    if set(meta) - META_KEYS:
        raise ContractError(f"{where}: x-lakeflow has unknown keys {sorted(set(meta) - META_KEYS)}")
    if meta["status"] not in STATUSES or meta["compatibility"] not in COMPATIBILITY_MODES:
        raise ContractError(f"{where}: invalid status or compatibility")
    for key in ("table", "topic"):
        if not meta["source"].get(key):
            raise ContractError(f"{where}: x-lakeflow.source.{key} is required")
    fields = {name: _parse_field(name, spec, where) for name, spec in doc.get("properties", {}).items()}
    required = tuple(doc.get("required", []))
    for name in required:
        if name not in fields:
            raise ContractError(f"{where}: required field '{name}' has no property definition")
    primary_key = tuple(meta["primary_key"])
    for name in primary_key:
        if name not in required or fields[name].nullable:
            raise ContractError(f"{where}: primary key field '{name}' must be required and non-null")
    target = meta["target_table"]
    if not all(IDENTIFIER.match(part) for part in target.split(".")):
        raise ContractError(f"{where}: target_table '{target}' is not a safe identifier")
    if not all(IDENTIFIER.match(part) for part in meta["source"]["table"].split(".")):
        raise ContractError(f"{where}: source table '{meta['source']['table']}' is not a safe identifier")
    return Contract(
        name=meta["contract"],
        version=int(meta["version"]),
        status=meta["status"],
        owner=dict(meta["owner"]),
        source=dict(meta["source"]),
        primary_key=primary_key,
        compatibility=meta["compatibility"],
        classification=meta["classification"],
        freshness_sla=dict(meta["freshness_sla"]),
        target_table=target,
        required=required,
        fields=fields,
        references=tuple(meta.get("references", [])),
        path=path,
    )


def check_backward_compatible(old: Contract, new: Contract) -> list[str]:
    """Return reasons why `new` cannot read every record that was valid under `old` (empty list = compatible)."""
    problems: list[str] = []
    if old.primary_key != new.primary_key:
        problems.append(f"primary key changed {old.primary_key} -> {new.primary_key}")
    if old.source_table != new.source_table or old.topic != new.topic:
        problems.append("source table/topic changed")
    for name in new.required:
        if name not in old.required:
            problems.append(f"'{name}' became required")
    for name, new_spec in new.fields.items():
        old_spec = old.fields.get(name)
        if old_spec is None:
            continue
        widened = set(new_spec.types) | ({"integer"} if "number" in new_spec.types else set())
        if not set(old_spec.types) <= widened:
            problems.append(f"'{name}' type narrowed {old_spec.types} -> {new_spec.types}")
        if new_spec.enum is not None and (old_spec.enum is None or not set(old_spec.enum) <= set(new_spec.enum)):
            problems.append(f"'{name}' enum narrowed")
        if new_spec.pattern is not None and new_spec.pattern != old_spec.pattern:
            problems.append(f"'{name}' pattern changed")
        if new_spec.fmt is not None and new_spec.fmt != old_spec.fmt:
            problems.append(f"'{name}' format changed")
        if new_spec.max_length is not None and (
            old_spec.max_length is None or new_spec.max_length < old_spec.max_length
        ):
            problems.append(f"'{name}' maxLength reduced")
        if new_spec.min_length is not None and (
            old_spec.min_length is None or new_spec.min_length > old_spec.min_length
        ):
            problems.append(f"'{name}' minLength increased")
        if old_spec.decimal and new_spec.decimal:
            if new_spec.decimal[1] != old_spec.decimal[1] or new_spec.decimal[0] < old_spec.decimal[0]:
                problems.append(f"'{name}' decimal precision/scale narrowed")
        elif bool(old_spec.decimal) != bool(new_spec.decimal):
            problems.append(f"'{name}' decimal annotation added or removed")
        if old_spec.pii_handling != new_spec.pii_handling:
            problems.append(f"'{name}' PII handling changed (requires governance review)")
    return problems


class ContractRegistry:
    """All active contract versions, indexed by contract name and source topic."""

    def __init__(self, contracts: list[Contract], fingerprint: str = ""):
        self._by_name: dict[str, list[Contract]] = {}
        for contract in sorted(contracts, key=lambda c: (c.name, c.version)):
            self._by_name.setdefault(contract.name, []).append(contract)
        self.fingerprint = fingerprint
        self._by_topic: dict[str, str] = {}
        for name, versions in self._by_name.items():
            self._validate_versions(name, versions)
            self._by_topic[versions[0].topic] = name

    @staticmethod
    def _validate_versions(name: str, versions: list[Contract]) -> None:
        numbers = [c.version for c in versions]
        if len(set(numbers)) != len(numbers):
            raise ContractError(f"contract '{name}' has duplicate versions {numbers}")
        current = [c for c in versions if c.status == "current"]
        if len(current) != 1:
            raise ContractError(f"contract '{name}' must have exactly one current version, found {len(current)}")
        if current[0] is not versions[-1]:
            raise ContractError(f"contract '{name}': the current version must be the highest active version")
        for index, newer in enumerate(versions[1:], start=1):
            previous = versions[:index] if newer.compatibility == "BACKWARD_TRANSITIVE" else [versions[index - 1]]
            for older in previous:
                problems = check_backward_compatible(older, newer)
                if problems:
                    raise ContractError(
                        f"{name} v{newer.version} is not backward compatible with v{older.version}: {problems}"
                    )

    @property
    def names(self) -> list[str]:
        return sorted(self._by_name)

    def versions(self, name: str) -> list[Contract]:
        return list(self._by_name[name])

    def current(self, name: str) -> Contract:
        return self._by_name[name][-1]

    def name_for_topic(self, topic: str) -> str | None:
        return self._by_topic.get(topic)

    def all_fields(self, name: str) -> dict[str, FieldSpec]:
        """Union of fields across active versions, in first-seen order (later versions win on conflicts)."""
        merged: dict[str, FieldSpec] = {}
        for contract in self._by_name[name]:
            merged.update(contract.fields)
        return merged

    def classify(self, name: str, record: Any) -> Classification:
        """Earliest active version that validates the record.

        A version is only a candidate when it defines every field of the record that some active version
        defines. A v2-only field therefore cannot slip through as "drift" under v1. Drift fields are those no
        active version defines. They are tolerated (additive change) and reported.
        """
        versions = self._by_name[name]
        current = versions[-1]
        if not isinstance(record, dict):
            return Classification(False, None, tuple(current.validate(record)), ())
        known = self.all_fields(name)
        drift = tuple(sorted(k for k in record if k not in known))
        used = {k for k in record if k in known}
        candidates = [c for c in versions if used <= set(c.fields)]
        for contract in candidates:
            if not contract.validate(record):
                return Classification(True, contract.version, (), drift)
        return Classification(False, None, tuple(current.validate(record)), drift)


def load_registry(root: str | Path, include_proposed: bool = False) -> ContractRegistry:
    """Load `<root>/<contract>/v<N>.schema.json`; `proposed/` subfolders are skipped unless requested."""
    root = Path(root)
    files = sorted(root.glob("*/v*.schema.json"))
    if include_proposed:
        files += sorted(root.glob("*/proposed/v*.schema.json"))
    digest = hashlib.sha256()
    contracts: list[Contract] = []
    for path in files:
        data = path.read_bytes()
        if path.parent.name != "proposed":
            digest.update(path.relative_to(root).as_posix().encode())
            digest.update(data)
        contract = parse_contract(json.loads(data.decode("utf-8")), path=path.relative_to(root).as_posix())
        if include_proposed and contract.status == "proposed":
            contract = _with_status(contract, "current")
        if contract.status in ACTIVE_STATUSES:
            contracts.append(contract)
    if include_proposed:
        contracts = _demote_older_current(contracts)
    if not contracts:
        raise ContractError(f"no active contracts found under {root}")
    return ContractRegistry(contracts, fingerprint=digest.hexdigest())


def contracts_fingerprint(root: str | Path) -> str:
    """Cheap change detector for hot reload (same digest as load_registry, without parsing)."""
    root = Path(root)
    digest = hashlib.sha256()
    for path in sorted(root.glob("*/v*.schema.json")):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _with_status(contract: Contract, status: str) -> Contract:
    return dataclasses.replace(contract, status=status)


def _demote_older_current(contracts: list[Contract]) -> list[Contract]:
    latest: dict[str, int] = {}
    for contract in contracts:
        latest[contract.name] = max(latest.get(contract.name, 0), contract.version)
    return [
        _with_status(c, "superseded") if c.status == "current" and c.version != latest[c.name] else c for c in contracts
    ]
