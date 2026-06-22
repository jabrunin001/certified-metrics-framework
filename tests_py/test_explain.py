from metrics_cli.explain.heuristic import classify
from metrics_cli.explain.ollama import rewrite

def test_classify_refund_offset():
    r = classify("net_mrr", "revenue", semantic_value=11000.0,
                 reference_value=10000.0, refund_total=1000.0)
    assert r["cause"] == "refunds_not_subtracted"
    assert r["confidence"] == "high"

def test_classify_wrong_grain_multiple():
    r = classify("wau", "count", semantic_value=300.0, reference_value=100.0)
    assert r["cause"] == "wrong_grain_or_missing_distinct"

def test_classify_unclassified():
    r = classify("dau", "count", semantic_value=103.0, reference_value=100.0)
    assert r["cause"] == "unclassified"

def test_ollama_falls_back_when_unavailable():
    def failing_runner(*a, **k):
        raise FileNotFoundError("ollama not installed")
    assert rewrite("base explanation", runner=failing_runner) == "base explanation"
