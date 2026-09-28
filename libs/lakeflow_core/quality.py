"""Warehouse-level data quality checks, generated from the contracts and executed through Trino.

Each check returns one number: offending rows, or a measured value such as p95 freshness in seconds. It passes
when that number is <= threshold. Identifiers come from validated contracts only, never from user input.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from .contracts import ContractRegistry
from .tables import BRONZE_TABLE, CATALOG, DLQ_TABLE, QUALITY_TABLE


@dataclass(frozen=True)
class QualityCheck:
    check_id: str
    dataset: str
    category: str
    severity: str
    description: str
    sql: str
    threshold: float
    unit: str = "rows"

    def to_dict(self) -> dict:
        return asdict(self)


def _quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_checks(
    registry: ContractRegistry,
    catalog: str = CATALOG,
    source_catalog: str = "postgres",
    reconciliation_grace_seconds: int = 120,
    freshness_window_minutes: int = 60,
) -> list[QualityCheck]:
    checks: list[QualityCheck] = []
    for name in registry.names:
        current = registry.current(name)
        earliest = registry.versions(name)[0]
        fields = registry.all_fields(name)
        table = f"{catalog}.{current.target_table}"
        pk = current.primary_key[0]
        live = "NOT is_deleted"

        checks.append(
            QualityCheck(
                f"{name}.primary_key_unique",
                current.target_table,
                "uniqueness",
                "critical",
                f"Exactly one silver row per {pk} (tombstones included)",
                f"SELECT count(*) - count(DISTINCT {pk}) FROM {table}",  # noqa: S608 - contract identifiers
                0,
            )
        )
        required_columns = [fields[f].target_column for f in current.required if fields[f].target_column]
        null_predicate = " OR ".join(f"{c} IS NULL" for c in required_columns)
        checks.append(
            QualityCheck(
                f"{name}.required_not_null",
                current.target_table,
                "completeness",
                "critical",
                "Required contract fields are populated on live rows",
                f"SELECT count(*) FROM {table} WHERE {live} AND ({null_predicate})",  # noqa: S608
                0,
            )
        )
        for spec in fields.values():
            if spec.enum is None or spec.target_column is None:
                continue
            allowed = ", ".join(_quote(v) for v in spec.enum if v is not None)
            checks.append(
                QualityCheck(
                    f"{name}.{spec.name}_enum",
                    current.target_table,
                    "validity",
                    "critical",
                    f"{spec.name} only contains contract enum values",
                    f"SELECT count(*) FROM {table} WHERE {live} AND {spec.name} IS NOT NULL "  # noqa: S608
                    f"AND {spec.name} NOT IN ({allowed})",
                    0,
                )
            )
        for ref in current.references:
            parent = registry.current(ref["contract"])
            parent_table = f"{catalog}.{parent.target_table}"
            grace = int(ref.get("grace_seconds", 300))
            checks.append(
                QualityCheck(
                    f"{name}.{ref['field']}_references_{ref['contract']}",
                    current.target_table,
                    "referential_integrity",
                    "warning",
                    f"Live {name} rows reference a live {ref['contract']} row (after a {grace}s grace period, "
                    "because cross-topic ordering is not guaranteed)",
                    f"SELECT count(*) FROM {table} c LEFT JOIN {parent_table} p "  # noqa: S608
                    f"ON c.{ref['field']} = p.{ref['target_field']} AND NOT p.is_deleted "
                    f"WHERE NOT c.is_deleted AND p.{ref['target_field']} IS NULL "
                    f"AND c._ingested_at < current_timestamp - INTERVAL '{grace}' SECOND",
                    0,
                )
            )
        compare = [
            spec.name
            for spec in earliest.fields.values()
            if spec.pii_handling == "keep" and spec.name != pk and spec.name in fields
        ]
        mismatch = " OR ".join(f"{_source_expr(fields[c])} IS DISTINCT FROM l.{c}" for c in compare)
        source_table = f"{source_catalog}.{current.source_table}"
        grace = reconciliation_grace_seconds
        checks.append(
            QualityCheck(
                f"{name}.source_reconciliation",
                current.target_table,
                "reconciliation",
                "critical",
                f"Silver matches PostgreSQL row-for-row on {', '.join(compare)} "
                f"(rows changed in the last {grace}s are in flight and excluded)",
                "SELECT count(*) FROM ("  # noqa: S608
                f"SELECT s.updated_at AS src_ts, l._source_ts AS lake_ts FROM {source_table} s "
                f"FULL OUTER JOIN (SELECT * FROM {table} WHERE {live}) l ON CAST(s.{pk} AS varchar) = l.{pk} "
                f"WHERE s.{pk} IS NULL OR l.{pk} IS NULL OR {mismatch}"
                f") d WHERE coalesce(d.src_ts, d.lake_ts) < current_timestamp - INTERVAL '{grace}' SECOND",
                0,
            )
        )
        sla = float(current.freshness_sla["p95_seconds"])
        checks.append(
            QualityCheck(
                f"{name}.freshness_p95",
                current.target_table,
                "freshness",
                "warning",
                f"p95 of (silver commit - source commit) over the last {freshness_window_minutes} min is within "
                f"the {sla:.0f}s SLA",
                "SELECT approx_percentile(to_unixtime(committed_at) - to_unixtime(source_ts), 0.95) "  # noqa: S608
                f"FROM {catalog}.{BRONZE_TABLE} WHERE source_table = {_quote(current.source_table)} "
                f"AND apply_outcome = 'applied' AND committed_at > current_timestamp - "
                f"INTERVAL '{freshness_window_minutes}' MINUTE",
                sla,
                unit="seconds",
            )
        )
    checks.append(
        QualityCheck(
            "bronze.event_id_unique",
            BRONZE_TABLE,
            "uniqueness",
            "critical",
            "Every change event is stored once in bronze (deduplication across redelivery and replays)",
            f"SELECT count(*) - count(DISTINCT event_id) FROM {catalog}.{BRONZE_TABLE}",  # noqa: S608
            0,
        )
    )
    checks.append(
        QualityCheck(
            "dlq.open_records",
            DLQ_TABLE,
            "dlq",
            "warning",
            "Rejected records waiting for triage or replay",
            f"SELECT count(*) FROM {catalog}.{DLQ_TABLE} WHERE status = 'open'",  # noqa: S608
            0,
        )
    )
    return checks


def _source_expr(spec) -> str:
    """PostgreSQL uuid/char(n)/text all compare as varchar against the lakehouse string columns."""
    column = f"s.{spec.name}"
    return f"CAST({column} AS varchar)" if spec.logical_type == "string" else column


def evaluate(check: QualityCheck, value: float | None, error: str | None = None) -> dict:
    if error is not None:
        status = "error"
    elif value is None:
        status = "no_data"
    else:
        status = "pass" if float(value) <= check.threshold else "fail"
    return {
        "check_id": check.check_id,
        "dataset": check.dataset,
        "category": check.category,
        "severity": check.severity,
        "description": check.description,
        "value": None if value is None else float(value),
        "threshold": check.threshold,
        "unit": check.unit,
        "status": status,
        "error": error,
    }


def run_checks(cursor, checks: list[QualityCheck]) -> list[dict]:
    """Execute checks on a DB-API cursor (Trino). One failing query does not stop the others."""
    results = []
    for check in checks:
        try:
            cursor.execute(check.sql)
            row = cursor.fetchone()
            results.append(evaluate(check, None if row is None else row[0]))
        except Exception as exc:  # noqa: BLE001 - report per-check errors
            results.append(evaluate(check, None, error=f"{type(exc).__name__}: {str(exc)[:300]}"))
    return results


RESULTS_DDL = (
    f"CREATE TABLE IF NOT EXISTS {CATALOG}.{QUALITY_TABLE} (run_id varchar, run_at timestamp(6) with time zone, "
    "check_id varchar, dataset varchar, category varchar, severity varchar, status varchar, value double, "
    "threshold double, unit varchar, error varchar, runner varchar) WITH (partitioning = ARRAY['day(run_at)'])"
)
_RESULT_FIELDS = ("check_id", "dataset", "category", "severity", "status", "value", "threshold", "unit", "error")


def results_insert(run_id: str, run_at, runner: str, results: list[dict]) -> tuple[str, list]:
    """One parameterised INSERT for a whole run (Trino `?` placeholders)."""
    if not results:
        raise ValueError("no results to insert")
    row = "(" + ", ".join(["?"] * (len(_RESULT_FIELDS) + 3)) + ")"
    params: list = []
    for result in results:
        params += [run_id, run_at, *(result[f] for f in _RESULT_FIELDS), runner]
    return f"INSERT INTO {CATALOG}.{QUALITY_TABLE} VALUES {', '.join([row] * len(results))}", params  # noqa: S608
