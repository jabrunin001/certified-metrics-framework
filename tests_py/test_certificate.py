import json
from metrics_cli.certificate import build_certificate, render_registry_md

FRESH = [{"source_name": "fct_usage_daily", "lag_days": 0, "is_fresh": True},
         {"source_name": "fct_subscription_revenue", "lag_days": 30, "is_fresh": False}]
REG = [{"metric": "net_mrr", "owner": "finance-analytics", "grain": "month",
        "kind": "revenue", "definition_file": "_semantic_revenue.yml"}]

def test_certificate_certified_when_all_gates_pass():
    cert = build_certificate(
        REG[0], semantic_value=1000.0, reference_value=1000.0,
        freshness_rows=FRESH, semantic_metric_names={"net_mrr"},
        registry=REG, as_of="2026-03-31")
    assert cert.certified is True
    assert cert.variance_pct == 0.0
    assert len(cert.checksum) == 64  # sha256 hex

def test_certificate_fails_reconciliation_when_values_diverge():
    cert = build_certificate(
        REG[0], semantic_value=1100.0, reference_value=1000.0,
        freshness_rows=FRESH, semantic_metric_names={"net_mrr"},
        registry=REG, as_of="2026-03-31")
    assert cert.certified is False
    recon = [g for g in cert.gates if g.name == "reconciled"][0]
    assert recon.status == "FAIL"

def test_checksum_is_stable_and_excludes_itself():
    kwargs = dict(reg_row=REG[0], semantic_value=1000.0, reference_value=1000.0,
                  freshness_rows=FRESH, semantic_metric_names={"net_mrr"},
                  registry=REG, as_of="2026-03-31")
    assert build_certificate(**kwargs).checksum == build_certificate(**kwargs).checksum

def test_registry_md_lists_status():
    cert = build_certificate(
        REG[0], semantic_value=1000.0, reference_value=1000.0,
        freshness_rows=FRESH, semantic_metric_names={"net_mrr"},
        registry=REG, as_of="2026-03-31")
    md = render_registry_md([cert])
    assert "net_mrr" in md and "CERTIFIED" in md
