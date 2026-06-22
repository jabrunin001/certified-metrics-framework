from metrics_cli.mf_runner import (
    parse_mf_table, semantic_metric_total, dbt_parse_with_vars,
)

def test_parse_mf_table_sums_value_column():
    text = ("metric_time__month  net_mrr\n"
            "2026-01-01T00:00:00  100.5\n"
            "2026-02-01T00:00:00  200.0\n")
    assert parse_mf_table(text) == 300.5

def test_parse_mf_table_skips_none_and_header():
    text = ("metric_time__month  gross_retention\n"
            "2026-01-01T00:00:00  None\n"
            "2026-02-01T00:00:00  1.98\n")
    assert parse_mf_table(text) == 1.98

def test_parse_mf_table_empty_returns_none():
    assert parse_mf_table("metric_time__month  net_mrr\n") is None

def test_semantic_metric_total_uses_correct_grain_and_parses():
    captured = {}
    class FakeCompleted:
        returncode = 0
        stdout = "metric_time__week  wau\n2026-01-05T00:00:00  42.0\n"
        stderr = ""
    def fake_runner(cmd, **kwargs):
        captured["cmd"] = cmd
        return FakeCompleted()
    total = semantic_metric_total("wau", runner=fake_runner)
    assert total == 42.0
    assert "metric_time__week" in captured["cmd"]
    assert "wau" in captured["cmd"]

def test_dbt_parse_with_vars_passes_inject_break_true():
    captured = {}
    class FakeCompleted:
        returncode = 0
    def fake_runner(cmd, **kwargs):
        captured["cmd"] = cmd
        return FakeCompleted()
    assert dbt_parse_with_vars(True, runner=fake_runner) is True
    assert any('"inject_break": true' in str(c) for c in captured["cmd"])
