# Certified Metrics Framework — Design

**Date:** 2026-06-22
**Status:** Approved (design phase)
**Target role:** Dropbox — Staff Data Engineer, Analytics Data Engineering ([builtin.com/job/8530705](https://builtin.com/job/staff-data-engineer-analytics-data-engineering/8530705))

## Purpose

A small, runnable SaaS-analytics metrics platform in dbt + MetricFlow + DuckDB, where a metric is **certified** only when it passes three independent gates. It exists to prove, in code, the job's central ask: *"translate metric definitions into reliable, certified data pipelines"* using dbt MetricFlow and Databricks Metric Views.

The memorable insight: **a query that returns a number is not a correct metric.** A definition bug (wrong grain, missing `DISTINCT`, refunds not subtracted) passes dbt schema tests — the number is non-null and positive — yet is wrong. Only an independent reconciliation catches it. This is the `subledger-as-code` reconciliation control ("a trial balance can't catch a right-amount/wrong-account error") moved into the metrics domain.

It runs free, locally, on DuckDB — no warehouse, no credentials, no network. Databricks is an optional target.

## The certification control (the spine)

Every certified metric must pass three gates, all green:

1. **Governed** — exactly one definition exists, in MetricFlow YAML (single source of truth). A governance check fails if (a) a metric is computed off-semantic-layer inside marts, (b) a metric in the registry lacks a MetricFlow definition, or (c) a MetricFlow metric is absent from the registry. This is the shift-left governance angle.
2. **Fresh** — the input fact tables pass freshness + quality assertions (dbt-native, Great-Expectations-style: row counts in range, no future dates, non-null keys).
3. **Reconciled** — the MetricFlow-computed value equals an **independently re-derived golden reference**, within tolerance. The reference is hand-written SQL straight off raw events and never touches the semantic layer. It is the reconciliation counterparty that catches logic bugs.

`certified === governed && fresh && reconciled`.

## Proof in action

A dbt var injects a realistic definition bug into a metric — e.g. `weekly_active_users` counts event rows instead of `count(distinct user_id)`, or `net_mrr` omits refund subtraction. With the bug present:

- `dbt build` succeeds; MetricFlow returns a number.
- dbt schema/data tests stay **green** (non-null, positive, in-range).
- The **reconciliation gate goes red**, naming the offending metric and the variance.

CI's `cert-proof` job asserts exactly this: inject → certification FAILs naming the right metric → restore → certification PASSes. This is the headline demonstration.

## Architecture / data flow

```
seeds (synthetic SaaS events) → staging → intermediate (sessionization, subscription-period spine)
  → marts:  conformed dims (dim_user, dim_plan, dim_date)
            + shared facts (fct_usage_daily, fct_subscription_revenue)
  → semantic/    MetricFlow semantic_models + metrics   ← the ONE governed definition
  → reference/   independent golden re-derivation of each metric (plain SQL off events)
  → certification/  cert_metric_parity (semantic vs reference + variance) + cert_freshness
```

The discipline that makes the project credible: the **semantic layer** computes each metric one way (MetricFlow); a **reference model** computes the same metric a completely independent way (SQL off the events). Certification is the assertion that they agree.

### Metrics modeled

`dau`, `wau`, `mau`, `paid_conversion`, `net_mrr`, `gross_retention`, `storage_gb_active`.

Conformed dimensions (`dim_user`, `dim_plan`, `dim_date`) and shared fact tables (`fct_usage_daily`, `fct_subscription_revenue`) directly satisfy the job's dimensional-modeling requirement ("shared, reusable data models, conformed dimensions, shared fact tables").

## Components (each an isolated, testable unit)

| Unit | Purpose | Depends on |
| --- | --- | --- |
| `seeds/` + `scripts/seed_events.py` | Deterministic synthetic SaaS event generator (signups, sessions, subscriptions, refunds, storage) | — |
| `models/staging/` | Typed, cleaned source events | seeds |
| `models/intermediate/` | Sessionization + subscription-period spine | staging |
| `models/marts/` | Conformed dims + shared fact tables | intermediate |
| `models/semantic/` | MetricFlow `semantic_models` + `metrics` — the governed definitions | marts |
| `models/reference/` | Independent golden value per metric (plain SQL off events) | marts/staging |
| `models/certification/` | `cert_metric_parity` (semantic vs reference, variance) + `cert_freshness` | semantic, reference |
| `databricks/` | Unity Catalog + Delta DDL; **Databricks Metric Views** mirroring each MetricFlow metric; `parity_notes.md` | — (optional target) |
| `airflow/dags/certify_metrics_dag.py` | Real, lint-clean DAG: `build → test → certify → pack` with fail-routing | — (illustrative) |
| `metrics_cli/` | Typer + Pydantic CLI: `certify`, `explain`, `pack` | certification models |
| `tests/` + `unit_tests/` | dbt unit tests (sessionization/subscription), data tests, `assert_certification_*` controls; pytest for CLI | all |
| `.github/workflows/ci.yml` | `build+test`, `cert-proof`, `cli-tests` | all |
| `README.md` + `docs/` | Quickstart, capability map, orchestration + Databricks notes | all |

## Artifacts the CLI emits

Per metric, a checksummed **metric certificate** (`metric_certificate_<name>.json` + human-readable `.md`):

```json
{"metric":"weekly_active_users","owner":"growth-analytics",
 "definition_source":"models/semantic/_growth.yml",
 "gates":{"governed":"PASS","freshness":"PASS","reconciled":"PASS"},
 "semantic_value":18342,"reference_value":18342,"variance_pct":0.0,
 "as_of":"2026-06-22","checksum":"sha256:…"}
```

Plus a rolled-up `certification_registry.md` scorecard (certified / failed / stale per metric, with owners). `metrics_cli pack` bundles certificates + registry into a checksummed, tamper-evident evidence pack. CI asserts the registry contents.

## Databricks parity story

`databricks/metric_views/` holds Databricks Metric Views (the measure/dimension YAML spec) mirroring each MetricFlow metric, plus Unity Catalog / Delta DDL. `databricks/parity_notes.md` explains how one governed definition compiles to both dbt MetricFlow (DuckDB/warehouse) and Databricks Metric Views (Unity Catalog), so certification holds across engines. DuckDB is the default dbt target; Databricks is optional via `dbt build --target databricks` with `DATABRICKS_*` env vars (same shape as subledger's Snowflake target). An optional script runs the Metric-View query on a live workspace and feeds the result in as a third reconciliation counterparty.

## AI-native angle (deterministic + optional local LLM)

`metrics_cli explain` — on a failed gate, a deterministic classifier names the likely cause from the variance signature:
- semantic value ≈ N× reference → missing `DISTINCT` / wrong grain
- semantic exceeds reference by exactly the refund total → refunds not subtracted
- offsetting period shift → late-arriving data / window boundary

`--backend ollama` rewrites the explanation in plain English using a local model; the deterministic classification never changes and the prompt never leaves the machine. Falls back to heuristic if Ollama isn't running. Mirrors the proven `subledger-as-code` triage pattern.

## Tech stack

Python 3.11+, dbt-core + dbt-duckdb (default target) + dbt-databricks (optional), MetricFlow, DuckDB, Typer + Pydantic, optional Ollama. CI on GitHub Actions.

## Capability map (job requirement → where it lives)

| Job requirement | Where |
| --- | --- |
| Shared/reusable data models, conformed dims, shared fact tables | `models/marts/` |
| Translate metric definitions into reliable, certified pipelines | `models/semantic/` + `models/certification/` + `metrics_cli certify` |
| dbt MetricFlow | `models/semantic/` |
| Databricks Metric Views, Unity Catalog, Delta | `databricks/` |
| Shift-left data governance | governance gate + registry seed + off-semantic-layer lint |
| Observability / alerting standards | freshness gate + CI `cert-proof` + `certification_registry.md` |
| Airflow / orchestration, failure recovery | `airflow/dags/certify_metrics_dag.py` + `docs/orchestration.md` |
| AI-native tooling in the dev lifecycle | `metrics_cli/explain/` (deterministic + optional local Ollama) |
| Spark SQL | `databricks/` Metric View + Delta SQL |
| dbt unit tests | `unit_tests/` |
| CI | `.github/workflows/ci.yml` |

## Out of scope (YAGNI)

- No live Databricks cluster required to run the demo (DuckDB is the source of truth for the runnable proof).
- No Airflow scheduler execution in CI (the DAG is real and lint-clean, but CI runs the steps directly).
- No cloud LLM, no API keys, no network calls anywhere in the default path.
- No BI/dashboard front-end; the registry scorecard is the user-facing surface.
