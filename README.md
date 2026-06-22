# Certified Metrics Framework

A small, runnable SaaS-analytics metrics platform in dbt, MetricFlow, and DuckDB.
A metric is **certified** only when it is *governed* (one MetricFlow definition),
*fresh*, and *reconciled* against an independently re-derived golden value. The
point: **a query that returns a number is not a correct metric**. A definition
bug passes every schema test and is caught only by reconciliation.

**Live demo:** https://jabrunin001.github.io/certified-metrics-framework/ — flip
the *inject definition bug* toggle and watch only the reconciliation gate go red
while every schema and freshness test stays green.

## 60-second quickstart

Needs Python 3.11+ (see `.python-version`).

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
dbt deps --profiles-dir .
python scripts/seed_events.py
dbt build --profiles-dir .
python -m metrics_cli.cli certify
cat evidence/certification_registry.md
```

No warehouse, no credentials, no network. A clean run reports `8/8 metrics certified`.

## The control in action

A schema test can't catch a right-shape, wrong-logic metric. Reconciliation can:

```bash
# Inject a realistic bug: net_mrr forgets to subtract refunds.
dbt build --profiles-dir . --vars 'inject_break: true' --exclude resource_type:unit_test
dbt test  --profiles-dir . --vars 'inject_break: true' --select assert_freshness_ok   # stays GREEN
python -m metrics_cli.cli certify --inject-break                                       # net_mrr FAILS
python -m metrics_cli.cli explain net_mrr                                              # names the cause
dbt build --profiles-dir .                                                             # restore
```

The schema/freshness tests stay green. Only the reconciliation gate goes red,
naming `net_mrr` (and the derived `net_revenue_mom`) with the exact variance.
The explainer reports `refunds_not_subtracted` because the overage equals the
refund total. CI's `cert-proof` job asserts exactly this.

## Capability map

| Requirement (Dropbox Staff DE, Analytics DE) | Where it lives |
| --- | --- |
| Shared/reusable models, conformed dims, shared fact tables | `models/marts/` |
| Translate metric definitions into certified pipelines | `models/semantic/` + `metrics_cli/` certify |
| dbt MetricFlow | `models/semantic/` |
| Databricks Metric Views, Unity Catalog, Delta | `databricks/` |
| Shift-left data governance | governance gate (`metrics_cli/gates.py`) + `seeds/metric_registry.csv` |
| Observability / alerting | freshness gate + `certification_registry.md` + CI `cert-proof` |
| Airflow, failure recovery | `airflow/dags/certify_metrics_dag.py`, `docs/orchestration.md` |
| AI-native tooling | `metrics_cli/explain/` (deterministic + optional local Ollama) |
| dbt unit tests | `models/intermediate/int_unit_tests.yml` |
| Spark SQL | `databricks/` Metric View + Delta SQL |
| CI | `.github/workflows/ci.yml` |

## Running on Databricks

DuckDB is the default target. To run on Databricks, set the `DATABRICKS_*` env
vars (see `profiles.yml`), apply `databricks/unity_catalog.sql` +
`databricks/delta_tables.sql`, then `dbt build --profiles-dir . --target databricks`.
See `databricks/parity_notes.md` for how one governed definition compiles to both
MetricFlow and Databricks Metric Views.

## Optional: richer explanations with a local LLM

```bash
ollama pull llama3.1:8b
python -m metrics_cli.cli explain net_mrr --backend ollama
```

The deterministic classification never changes; Ollama only rewrites the prose,
and the prompt never leaves your machine. Falls back to the heuristic if Ollama
isn't running.
