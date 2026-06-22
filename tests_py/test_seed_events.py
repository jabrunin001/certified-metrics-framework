import csv
from pathlib import Path
from scripts.seed_events import generate

def test_generate_is_deterministic(tmp_path):
    a = tmp_path / "a"; b = tmp_path / "b"
    counts_a = generate(str(a), seed=42)
    counts_b = generate(str(b), seed=42)
    assert counts_a == counts_b
    for name in ["users.csv", "usage_events.csv", "subscriptions.csv", "refunds.csv"]:
        assert (a / name).read_text() == (b / name).read_text()

def test_generate_volumes_and_schema(tmp_path):
    counts = generate(str(tmp_path), seed=42)
    assert 800 <= counts["users.csv"] <= 1200
    assert counts["usage_events.csv"] > 10000
    assert counts["refunds.csv"] > 0
    with open(tmp_path / "subscriptions.csv") as f:
        header = next(csv.reader(f))
    assert header == ["subscription_id", "user_id", "plan_id", "period_month", "mrr_amount"]
