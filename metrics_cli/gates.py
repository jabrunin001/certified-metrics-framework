from __future__ import annotations
from .models import GateResult

REVENUE_TOLERANCE = 1e-4
MONTHLY_LAG_TOLERANCE = 31


def variance_pct(semantic: float, reference: float) -> float | None:
    if reference == 0:
        return None
    return (semantic - reference) / reference * 100.0


def governance_gate(registry: list[dict], semantic_metric_names: set[str]) -> GateResult:
    reg_names = {row["metric"] for row in registry}
    missing_def = reg_names - semantic_metric_names      # registered but no definition
    unregistered = semantic_metric_names - reg_names      # defined but not governed
    if not missing_def and not unregistered:
        return GateResult(name="governed", status="PASS",
                          detail="registry and semantic layer agree")
    parts = []
    if missing_def:
        parts.append(f"registered without definition: {sorted(missing_def)}")
    if unregistered:
        parts.append(f"defined but unregistered: {sorted(unregistered)}")
    return GateResult(name="governed", status="FAIL", detail="; ".join(parts))


def reconciliation_gate(metric: str, kind: str, semantic_value: float | None,
                        reference_value: float | None) -> GateResult:
    if semantic_value is None or reference_value is None:
        return GateResult(name="reconciled", status="SKIP",
                          detail="missing semantic or reference value")
    if kind == "count":
        ok = float(semantic_value) == float(reference_value)
    else:
        denom = abs(reference_value) if reference_value != 0 else 1.0
        ok = abs(semantic_value - reference_value) / denom <= REVENUE_TOLERANCE
    vp = variance_pct(semantic_value, reference_value)
    vp_txt = "n/a" if vp is None else f"{vp:+.4f}%"
    detail = (f"semantic={semantic_value:g} reference={reference_value:g} "
              f"variance={vp_txt}")
    return GateResult(name="reconciled", status="PASS" if ok else "FAIL", detail=detail)


def freshness_gate(metric: str, grain: str, rows: list[dict]) -> GateResult:
    by_source = {r["source_name"]: r for r in rows}
    if grain == "day":
        src = by_source.get("fct_usage_daily")
        if src is None:
            return GateResult(name="fresh", status="SKIP", detail="no usage freshness row")
        ok = bool(src["is_fresh"])
        return GateResult(name="fresh", status="PASS" if ok else "FAIL",
                          detail=f"fct_usage_daily lag_days={src['lag_days']}")
    src = by_source.get("fct_subscription_revenue")
    if src is None:
        return GateResult(name="fresh", status="SKIP", detail="no revenue freshness row")
    ok = int(src["lag_days"]) <= MONTHLY_LAG_TOLERANCE
    return GateResult(name="fresh", status="PASS" if ok else "FAIL",
                      detail=f"fct_subscription_revenue lag_days={src['lag_days']}")
