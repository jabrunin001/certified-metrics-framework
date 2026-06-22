from __future__ import annotations
import json
import subprocess
import duckdb

MF_GRAIN = {"dau": "day", "wau": "week"}  # everything else is monthly


def parse_mf_table(text: str) -> float | None:
    """Sum the value column of MetricFlow's whitespace table output.

    For each line, the metric value is the last whitespace-delimited token.
    Lines whose last token is not a float (header row, 'None' rows, mf log
    lines) are skipped. Returns None when no numeric row is found.
    """
    total = 0.0
    seen = False
    for line in text.splitlines():
        parts = line.split()
        if not parts:
            continue
        try:
            total += float(parts[-1])
            seen = True
        except ValueError:
            continue
    return total if seen else None


def dbt_parse_with_vars(inject_break: bool, runner=subprocess.run) -> bool:
    """Compile the manifest MetricFlow reads, baking in inject_break.

    metricflow 0.206.0 has no --dbt-vars on `mf query`; the var must be set
    at parse time so the compiled measure expr in target/ reflects it.
    """
    cmd = ["dbt", "parse", "--profiles-dir", ".",
           "--vars", json.dumps({"inject_break": bool(inject_break)})]
    result = runner(cmd, capture_output=True, text=True)
    return getattr(result, "returncode", 1) == 0


def semantic_metric_total(metric: str, runner=subprocess.run) -> float | None:
    """Query one metric via MetricFlow at its reconciliation grain and sum it.

    Caller must have run dbt_parse_with_vars(...) first to set the manifest
    state (clean or broken).
    """
    grain = MF_GRAIN.get(metric, "month")
    group_by = f"metric_time__{grain}"
    cmd = ["mf", "query", "--metrics", metric,
           "--group-by", group_by, "--order", group_by]
    result = runner(cmd, capture_output=True, text=True)
    if getattr(result, "returncode", 1) != 0:
        return None
    return parse_mf_table(result.stdout)


def read_reference_totals(db_path: str) -> dict[str, float]:
    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = con.execute(
            "select metric, sum(reference_value) from ref_metric_values group by metric"
        ).fetchall()
    finally:
        con.close()
    return {m: float(v) for m, v in rows}


def read_freshness(db_path: str) -> list[dict]:
    con = duckdb.connect(db_path, read_only=True)
    try:
        cur = con.execute(
            "select source_name, lag_days, is_fresh from cert_freshness"
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        con.close()


def read_registry(db_path: str) -> list[dict]:
    con = duckdb.connect(db_path, read_only=True)
    try:
        cur = con.execute(
            "select metric, owner, grain, kind, definition_file from metric_registry"
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        con.close()
