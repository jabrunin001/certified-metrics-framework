from metrics_cli.gates import (
    governance_gate, reconciliation_gate, freshness_gate, variance_pct,
)

def test_governance_pass_when_sets_match():
    reg = [{"metric": "net_mrr"}, {"metric": "dau"}]
    r = governance_gate(reg, {"net_mrr", "dau"})
    assert r.status == "PASS"

def test_governance_fail_on_unregistered_semantic_metric():
    reg = [{"metric": "dau"}]
    r = governance_gate(reg, {"dau", "rogue_metric"})
    assert r.status == "FAIL"
    assert "rogue_metric" in r.detail

def test_reconciliation_exact_for_counts():
    assert reconciliation_gate("dau", "count", 100.0, 100.0).status == "PASS"
    assert reconciliation_gate("dau", "count", 101.0, 100.0).status == "FAIL"

def test_reconciliation_tolerance_for_revenue():
    assert reconciliation_gate("net_mrr", "revenue", 1000.00001, 1000.0).status == "PASS"
    bad = reconciliation_gate("net_mrr", "revenue", 1100.0, 1000.0)
    assert bad.status == "FAIL"
    assert "10" in bad.detail  # ~10% variance reported

def test_reconciliation_skip_on_missing_value():
    assert reconciliation_gate("net_mrr", "revenue", None, 1000.0).status == "SKIP"

def test_freshness_daily_uses_usage_source():
    rows = [
        {"source_name": "fct_usage_daily", "lag_days": 0, "is_fresh": True},
        {"source_name": "fct_subscription_revenue", "lag_days": 30, "is_fresh": False},
    ]
    assert freshness_gate("dau", "day", rows).status == "PASS"
    assert freshness_gate("net_mrr", "month", rows).status == "PASS"  # 30 <= 31

def test_variance_pct_sign():
    assert round(variance_pct(110.0, 100.0), 4) == 10.0
    assert variance_pct(100.0, 0.0) is None
