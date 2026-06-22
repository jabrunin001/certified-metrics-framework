# Certified Metrics Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a runnable dbt + MetricFlow + DuckDB SaaS-analytics platform where a metric is *certified* only when it is governed (one MetricFlow definition), fresh, and reconciled against an independently re-derived golden reference — with a Python CLI that emits checksummed metric certificates and a CI job that proves a definition bug is caught.

**Architecture:** Synthetic SaaS events → dbt staging/intermediate/marts (conformed dims + shared facts) → MetricFlow semantic layer (the one governed definition) and an independent `reference` model (golden values computed straight off events). A Python CLI (`metrics_cli`) runs three gates per metric — governance (registry vs semantic YAML), freshness (dbt `cert_freshness` model), reconciliation (MetricFlow value vs reference value within tolerance) — and emits per-metric certificates + a registry scorecard. A dbt var injects a realistic definition bug (`net_mrr` omits refunds); CI asserts certification fails naming the metric, then passes once restored.

**Tech Stack:** Python 3.11, dbt-core 1.8.x, dbt-duckdb 1.8.x (default target), dbt-databricks (optional target, not exercised in CI), dbt-metricflow / MetricFlow, DuckDB, Typer, Pydantic, pytest, optional local Ollama. CI on GitHub Actions.

## Global Constraints

- Python 3.11+ (`.python-version` pins `3.11`).
- Default dbt target is `duckdb`; the repo must clone-and-run with no warehouse, no credentials, no network.
- Every dbt invocation uses `--profiles-dir .` (profiles live in the repo root, not `~/.dbt`).
- The DuckDB database file is `cmf.duckdb` (gitignored).
- All randomness in the data generator is seeded (`random.Random(42)` + fixed base date `2026-01-01`); regenerating seeds is deterministic.
- No cloud LLM, no API keys, no network calls in any default code path. The optional `--backend ollama` path is the only LLM call and is local-only.
- Bug injection is controlled solely by dbt var `inject_break` (default `false`); no other code path changes metric values.
- Reconciliation tolerance: exact equality for integer count metrics; relative tolerance `1e-4` (0.01%) for revenue/ratio metrics.
- Metric set (canonical, used everywhere) — **8 metrics**: `dau`, `wau`, `mau`, `paid_conversion`, `net_mrr`, `gross_retention`, `storage_gb_active`, `paying_users_monthly`. (`paying_users_monthly` is both a certified metric in its own right and the numerator of the `paid_conversion` ratio; metricflow 0.206.0 requires ratio numerators/denominators to be metric references, not raw measures.) A clean certify reports **8/8 metrics certified**.
- **Pydantic is v1 (1.10.x), not v2** — MetricFlow 0.206.0 (pulled by `dbt-metricflow[duckdb]==0.7.1`) hard-pins `pydantic<1.11`, and the whole project shares one venv. Use pydantic v1 APIs everywhere: `model.dict()` (not `model_dump()`), `model.json(indent=2)` (not `model_dump_json(...)`). `BaseModel`, `Literal` fields, `X | None` annotations, defaults, and `@property` all work unchanged under v1 on Python 3.11.

---

## File Structure

```
.python-version, .user.yml, .gitignore, requirements.txt
dbt_project.yml, profiles.yml, packages.yml
scripts/seed_events.py                      # synthetic event generator
seeds/                                       # generated CSVs + metric_registry.csv
models/
  staging/        stg_*.sql + _staging.yml
  intermediate/   int_user_daily_activity.sql, int_subscription_months.sql + _intermediate.yml
  marts/          dim_user.sql, dim_plan.sql, dim_date.sql,
                  fct_usage_daily.sql, fct_subscription_revenue.sql + _marts.yml
  reference/      ref_metric_values.sql + _reference.yml
  semantic/       _semantic_usage.yml, _semantic_revenue.yml   # MetricFlow semantic_models + metrics
  certification/  cert_freshness.sql + _certification.yml
models/intermediate/int_unit_tests.yml   # dbt 1.8 discovers unit tests under model-paths only
tests/            assert_freshness_ok.sql
metrics_cli/
  __init__.py, cli.py, gates.py, models.py, mf_runner.py, certificate.py
  explain/__init__.py, explain/heuristic.py, explain/ollama.py
databricks/       unity_catalog.sql, delta_tables.sql, metric_views/*.yml, parity_notes.md
airflow/dags/certify_metrics_dag.py
tests_py/         test_gates.py, test_certificate.py, test_explain.py, test_seed_events.py
.github/workflows/ci.yml
docs/             orchestration.md, README.md (root)
```

---

## Task 1: Project scaffolding and dbt skeleton

**Files:**
- Create: `requirements.txt`, `dbt_project.yml`, `profiles.yml`, `packages.yml`, `.gitignore` (already exists — extend)

**Interfaces:**
- Produces: a dbt project named `cmf` with a `duckdb` default target on `cmf.duckdb`, vars `inject_break: false` and `freshness_max_lag_days: 2`.

- [ ] **Step 1: Write `requirements.txt`**

```
dbt-core==1.8.7
dbt-duckdb==1.8.4
dbt-metricflow[duckdb]==0.7.1
duckdb==1.0.0
typer==0.12.5
pydantic>=1.10.0,<1.11.0
pytest==8.3.3
```

> Note: `dbt-metricflow==0.7.1` (not 0.6.0, which requires `dbt-core<1.8.0`) and `pydantic<1.11` (MetricFlow 0.206.0 hard-pins pydantic v1). See Global Constraints — all CLI code uses pydantic v1 APIs.

- [ ] **Step 2: Write `dbt_project.yml`**

```yaml
name: cmf
version: "1.0.0"
config-version: 2
profile: cmf

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
macro-paths: ["macros"]

vars:
  inject_break: false
  freshness_max_lag_days: 2
  analysis_end_date: "2026-03-31"

models:
  cmf:
    staging:
      +materialized: view
    intermediate:
      +materialized: view
    marts:
      +materialized: table
    reference:
      +materialized: table
    certification:
      +materialized: table

seeds:
  cmf:
    +quote_columns: false
```

- [ ] **Step 3: Write `profiles.yml`**

```yaml
cmf:
  target: duckdb
  outputs:
    duckdb:
      type: duckdb
      path: cmf.duckdb
      threads: 4
    databricks:
      type: databricks
      catalog: "{{ env_var('DATABRICKS_CATALOG', 'main') }}"
      schema: "{{ env_var('DATABRICKS_SCHEMA', 'cmf') }}"
      host: "{{ env_var('DATABRICKS_HOST', '') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH', '') }}"
      token: "{{ env_var('DATABRICKS_TOKEN', '') }}"
      threads: 4
```

- [ ] **Step 4: Write `packages.yml`**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
```

- [ ] **Step 5: Extend `.gitignore`**

Append (root `.gitignore` already lists `.venv/`, `target/`, etc.):

```
dbt_packages/
seeds/*.csv
!seeds/metric_registry.csv
cmf.duckdb
```

- [ ] **Step 6: Install and verify dbt connects**

Run:
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
dbt deps --profiles-dir .
dbt debug --profiles-dir .
```
Expected: `dbt debug` ends with `All checks passed!`

- [ ] **Step 7: Commit**

```bash
git add requirements.txt dbt_project.yml profiles.yml packages.yml .gitignore
git commit -m "chore: dbt project skeleton with duckdb + databricks targets"
```

---

## Task 2: Synthetic event generator

**Files:**
- Create: `scripts/seed_events.py`, `seeds/metric_registry.csv`
- Test: `tests_py/test_seed_events.py`

**Interfaces:**
- Produces: function `generate(out_dir: str, *, seed: int = 42) -> dict[str, int]` returning a row-count per generated file. Writes CSVs `seeds/users.csv`, `seeds/usage_events.csv`, `seeds/subscriptions.csv`, `seeds/refunds.csv`.
- Schemas:
  - `users.csv`: `user_id:int, signup_date:date, country:str, acquisition_channel:str`
  - `usage_events.csv`: `event_id:int, user_id:int, event_ts:datetime, event_type:str, storage_bytes:int`
  - `subscriptions.csv`: `subscription_id:int, user_id:int, plan_id:str, period_month:date(first-of-month), mrr_amount:numeric`
  - `refunds.csv`: `refund_id:int, subscription_id:int, period_month:date, refund_amount:numeric`

- [ ] **Step 1: Write the failing test**

```python
# tests_py/test_seed_events.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests_py/test_seed_events.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.seed_events'`

- [ ] **Step 3: Write the generator**

```python
# scripts/seed_events.py
"""Deterministic synthetic SaaS event generator for the Certified Metrics Framework."""
from __future__ import annotations
import csv
import os
import random
from datetime import date, datetime, timedelta

BASE_DATE = date(2026, 1, 1)
DAYS = 90
PLANS = {"free": 0.0, "plus": 12.0, "pro": 24.0, "team": 60.0}
COUNTRIES = ["US", "GB", "DE", "JP", "BR"]
CHANNELS = ["organic", "paid_search", "referral", "social"]


def _write(out_dir: str, name: str, header: list[str], rows: list[list]) -> int:
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, name), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return len(rows)


def generate(out_dir: str, *, seed: int = 42) -> dict[str, int]:
    rng = random.Random(seed)
    n_users = 1000

    users, usage, subs, refunds = [], [], [], []
    event_id = 0
    sub_id = 0
    refund_id = 0

    for uid in range(1, n_users + 1):
        signup_offset = rng.randint(0, DAYS - 1)
        signup = BASE_DATE + timedelta(days=signup_offset)
        users.append([uid, signup.isoformat(), rng.choice(COUNTRIES), rng.choice(CHANNELS)])

        # Engagement: each user active on a random subset of days after signup.
        active_prob = rng.uniform(0.1, 0.7)
        plan = rng.choices(list(PLANS), weights=[55, 20, 15, 10])[0]
        for d in range(signup_offset, DAYS):
            if rng.random() > active_prob:
                continue
            day = BASE_DATE + timedelta(days=d)
            n_events = rng.randint(1, 5)
            for _ in range(n_events):
                event_id += 1
                ts = datetime.combine(day, datetime.min.time()) + timedelta(
                    seconds=rng.randint(0, 86399)
                )
                usage.append([
                    event_id, uid, ts.isoformat(sep=" "),
                    rng.choice(["open", "upload", "share", "preview"]),
                    rng.randint(0, 50_000_000),
                ])

        # Paid users get a subscription row per active month with possible refunds.
        if plan != "free":
            for month_start in (date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1)):
                if month_start < signup.replace(day=1):
                    continue
                sub_id += 1
                mrr = PLANS[plan]
                subs.append([sub_id, uid, plan, month_start.isoformat(), f"{mrr:.2f}"])
                if rng.random() < 0.08:  # ~8% of paid months see a partial refund
                    refund_id += 1
                    amt = round(mrr * rng.uniform(0.25, 1.0), 2)
                    refunds.append([refund_id, sub_id, month_start.isoformat(), f"{amt:.2f}"])

    counts = {}
    counts["users.csv"] = _write(out_dir, "users.csv",
        ["user_id", "signup_date", "country", "acquisition_channel"], users)
    counts["usage_events.csv"] = _write(out_dir, "usage_events.csv",
        ["event_id", "user_id", "event_ts", "event_type", "storage_bytes"], usage)
    counts["subscriptions.csv"] = _write(out_dir, "subscriptions.csv",
        ["subscription_id", "user_id", "plan_id", "period_month", "mrr_amount"], subs)
    counts["refunds.csv"] = _write(out_dir, "refunds.csv",
        ["refund_id", "subscription_id", "period_month", "refund_amount"], refunds)
    return counts


if __name__ == "__main__":
    print(generate("seeds"))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests_py/test_seed_events.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Generate the seeds and write the registry**

Run: `python scripts/seed_events.py`
Expected: prints a dict like `{'users.csv': 1000, 'usage_events.csv': <N>, ...}`

Then create `seeds/metric_registry.csv` (this file IS committed):

```csv
metric,owner,grain,kind,definition_file
dau,growth-analytics,day,count,_semantic_usage.yml
wau,growth-analytics,week,count,_semantic_usage.yml
mau,growth-analytics,month,count,_semantic_usage.yml
paid_conversion,growth-analytics,month,ratio,_semantic_usage.yml
net_mrr,finance-analytics,month,revenue,_semantic_revenue.yml
gross_retention,finance-analytics,month,ratio,_semantic_revenue.yml
storage_gb_active,growth-analytics,day,revenue,_semantic_usage.yml
```

- [ ] **Step 6: Commit**

```bash
git add scripts/seed_events.py tests_py/test_seed_events.py seeds/metric_registry.csv
git commit -m "feat: deterministic synthetic SaaS event generator + metric registry"
```

---

## Task 3: Staging models

**Files:**
- Create: `models/staging/stg_users.sql`, `stg_usage_events.sql`, `stg_subscriptions.sql`, `stg_refunds.sql`, `models/staging/_staging.yml`

**Interfaces:**
- Consumes: seeds `users`, `usage_events`, `subscriptions`, `refunds`.
- Produces: views `stg_users`, `stg_usage_events` (with `event_date` derived), `stg_subscriptions`, `stg_refunds`.

- [ ] **Step 1: Write the staging SQL**

`models/staging/stg_users.sql`:
```sql
select
    user_id,
    cast(signup_date as date) as signup_date,
    country,
    acquisition_channel
from {{ ref('users') }}
```

`models/staging/stg_usage_events.sql`:
```sql
select
    event_id,
    user_id,
    cast(event_ts as timestamp) as event_ts,
    cast(event_ts as date) as event_date,
    event_type,
    storage_bytes
from {{ ref('usage_events') }}
```

`models/staging/stg_subscriptions.sql`:
```sql
select
    subscription_id,
    user_id,
    plan_id,
    cast(period_month as date) as period_month,
    cast(mrr_amount as double) as mrr_amount
from {{ ref('subscriptions') }}
```

`models/staging/stg_refunds.sql`:
```sql
select
    refund_id,
    subscription_id,
    cast(period_month as date) as period_month,
    cast(refund_amount as double) as refund_amount
from {{ ref('refunds') }}
```

- [ ] **Step 2: Write `models/staging/_staging.yml`**

```yaml
version: 2

seeds:
  - name: users
  - name: usage_events
  - name: subscriptions
  - name: refunds
  - name: metric_registry

models:
  - name: stg_users
    columns:
      - name: user_id
        data_tests: [unique, not_null]
  - name: stg_usage_events
    columns:
      - name: event_id
        data_tests: [unique, not_null]
      - name: user_id
        data_tests:
          - not_null
          - relationships: {to: ref('stg_users'), field: user_id}
  - name: stg_subscriptions
    columns:
      - name: subscription_id
        data_tests: [unique, not_null]
  - name: stg_refunds
    columns:
      - name: refund_id
        data_tests: [unique, not_null]
```

- [ ] **Step 3: Build and test**

Run: `dbt build --profiles-dir . --select staging`
Expected: all seeds load, 4 staging views build, all data tests PASS, `ERROR=0`.

- [ ] **Step 4: Commit**

```bash
git add models/staging
git commit -m "feat: staging models over synthetic seeds"
```

---

## Task 4: Intermediate models + dbt unit tests

**Files:**
- Create: `models/intermediate/int_user_daily_activity.sql`, `models/intermediate/int_subscription_months.sql`, `models/intermediate/_intermediate.yml`, `models/intermediate/int_unit_tests.yml`

**Interfaces:**
- Consumes: `stg_usage_events`, `stg_subscriptions`, `stg_refunds`.
- Produces:
  - `int_user_daily_activity` — grain (user_id, event_date): `is_active` (1 per active user-day), `storage_bytes_day` (max storage_bytes that day).
  - `int_subscription_months` — grain (subscription_id, period_month): `mrr_amount`, `refund_amount` (0 if none), `net_amount = mrr_amount - refund_amount`.

- [ ] **Step 1: Write the intermediate SQL**

`models/intermediate/int_user_daily_activity.sql`:
```sql
select
    user_id,
    event_date,
    1 as is_active,
    max(storage_bytes) as storage_bytes_day
from {{ ref('stg_usage_events') }}
group by user_id, event_date
```

`models/intermediate/int_subscription_months.sql`:
```sql
with refunds as (
    select subscription_id, period_month, sum(refund_amount) as refund_amount
    from {{ ref('stg_refunds') }}
    group by subscription_id, period_month
)
select
    s.subscription_id,
    s.user_id,
    s.plan_id,
    s.period_month,
    s.mrr_amount,
    coalesce(r.refund_amount, 0.0) as refund_amount,
    s.mrr_amount - coalesce(r.refund_amount, 0.0) as net_amount
from {{ ref('stg_subscriptions') }} s
left join refunds r
    on s.subscription_id = r.subscription_id
   and s.period_month = r.period_month
```

- [ ] **Step 2: Write the unit tests (dbt unit tests assert logic on fixed inputs)**

`models/intermediate/int_unit_tests.yml`:
```yaml
version: 2

unit_tests:
  - name: net_amount_subtracts_refund
    model: int_subscription_months
    given:
      - input: ref('stg_subscriptions')
        rows:
          - {subscription_id: 1, user_id: 1, plan_id: pro, period_month: "2026-01-01", mrr_amount: 24.0}
          - {subscription_id: 2, user_id: 2, plan_id: plus, period_month: "2026-01-01", mrr_amount: 12.0}
      - input: ref('stg_refunds')
        rows:
          - {refund_id: 1, subscription_id: 1, period_month: "2026-01-01", refund_amount: 10.0}
    expect:
      rows:
        - {subscription_id: 1, period_month: "2026-01-01", mrr_amount: 24.0, refund_amount: 10.0, net_amount: 14.0}
        - {subscription_id: 2, period_month: "2026-01-01", mrr_amount: 12.0, refund_amount: 0.0, net_amount: 12.0}

  - name: daily_activity_collapses_to_one_row_per_user_day
    model: int_user_daily_activity
    given:
      - input: ref('stg_usage_events')
        rows:
          - {user_id: 1, event_date: "2026-01-05", storage_bytes: 100}
          - {user_id: 1, event_date: "2026-01-05", storage_bytes: 300}
          - {user_id: 1, event_date: "2026-01-06", storage_bytes: 50}
    expect:
      rows:
        - {user_id: 1, event_date: "2026-01-05", is_active: 1, storage_bytes_day: 300}
        - {user_id: 1, event_date: "2026-01-06", is_active: 1, storage_bytes_day: 50}
```

- [ ] **Step 3: Write `models/intermediate/_intermediate.yml`**

```yaml
version: 2
models:
  - name: int_user_daily_activity
    columns:
      - name: user_id
        data_tests: [not_null]
  - name: int_subscription_months
    columns:
      - name: net_amount
        data_tests: [not_null]
```

- [ ] **Step 4: Run unit tests to verify they pass**

Run: `dbt build --profiles-dir . --select int_user_daily_activity int_subscription_months`
Then: `dbt test --profiles-dir . --select test_type:unit`
Expected: both unit tests PASS.

- [ ] **Step 5: Commit**

```bash
git add models/intermediate models/intermediate/int_unit_tests.yml
git commit -m "feat: intermediate activity + subscription-month models with dbt unit tests"
```

---

## Task 5: Marts — conformed dims and shared facts

**Files:**
- Create: `models/marts/dim_date.sql`, `dim_user.sql`, `dim_plan.sql`, `fct_usage_daily.sql`, `fct_subscription_revenue.sql`, `models/marts/_marts.yml`

**Interfaces:**
- Consumes: intermediate + staging models.
- Produces:
  - `dim_date(date_day, day, week_start, month_start)`
  - `dim_user(user_id, signup_date, country, acquisition_channel)`
  - `dim_plan(plan_id, plan_name, list_mrr, is_paid)`
  - `fct_usage_daily(user_id, date_day, is_active, storage_bytes_day)` — additive daily usage fact, conformed on `date_day`/`user_id`.
  - `fct_subscription_revenue(subscription_id, user_id, plan_id, month_start, mrr_amount, refund_amount, net_amount)` — conformed on `month_start`/`user_id`/`plan_id`.

- [ ] **Step 1: Write the dimension SQL**

`models/marts/dim_date.sql`:
```sql
with days as (
    select cast(range as date) as date_day
    from range(date '2026-01-01', date '2026-04-01', interval 1 day)
)
select
    date_day,
    date_day as day,
    date_trunc('week', date_day) as week_start,
    date_trunc('month', date_day) as month_start
from days
```

`models/marts/dim_user.sql`:
```sql
select user_id, signup_date, country, acquisition_channel
from {{ ref('stg_users') }}
```

`models/marts/dim_plan.sql`:
```sql
select * from (
    values
        ('free', 'Free', 0.0, false),
        ('plus', 'Plus', 12.0, true),
        ('pro', 'Pro', 24.0, true),
        ('team', 'Team', 60.0, true)
) as t(plan_id, plan_name, list_mrr, is_paid)
```

- [ ] **Step 2: Write the fact SQL**

`models/marts/fct_usage_daily.sql`:
```sql
select
    a.user_id,
    a.event_date as date_day,
    a.is_active,
    a.storage_bytes_day
from {{ ref('int_user_daily_activity') }} a
```

`models/marts/fct_subscription_revenue.sql`:
```sql
select
    subscription_id,
    user_id,
    plan_id,
    period_month as month_start,
    mrr_amount,
    refund_amount,
    net_amount
from {{ ref('int_subscription_months') }}
```

- [ ] **Step 3: Write `models/marts/_marts.yml`**

```yaml
version: 2
models:
  - name: dim_date
    columns:
      - name: date_day
        data_tests: [unique, not_null]
  - name: dim_user
    columns:
      - name: user_id
        data_tests: [unique, not_null]
  - name: dim_plan
    columns:
      - name: plan_id
        data_tests: [unique, not_null]
  - name: fct_usage_daily
    columns:
      - name: user_id
        data_tests:
          - not_null
          - relationships: {to: ref('dim_user'), field: user_id}
      - name: date_day
        data_tests:
          - not_null
          - relationships: {to: ref('dim_date'), field: date_day}
  - name: fct_subscription_revenue
    columns:
      - name: plan_id
        data_tests:
          - relationships: {to: ref('dim_plan'), field: plan_id}
      - name: net_amount
        data_tests: [not_null]
```

- [ ] **Step 4: Build and test**

Run: `dbt build --profiles-dir . --select marts`
Expected: 5 mart models build; all relationships + uniqueness tests PASS; `ERROR=0`.

- [ ] **Step 5: Commit**

```bash
git add models/marts
git commit -m "feat: conformed dimensions and shared usage/revenue fact tables"
```

---

## Task 6: Independent reference (golden) metric values

**Files:**
- Create: `models/reference/ref_metric_values.sql`, `models/reference/_reference.yml`

**Interfaces:**
- Consumes: marts + staging (NOT the semantic layer — this is the independent counterparty).
- Produces: `ref_metric_values(metric, grain_date, reference_value)` — one row per metric per period, the golden value re-derived directly from facts. `grain_date` is the period start (day/week/month) appropriate to the metric.
- Note: this model is unaffected by `inject_break` — it is the source of truth the semantic layer is reconciled against.

- [ ] **Step 1: Write the reference SQL**

`models/reference/ref_metric_values.sql`:
```sql
-- Independent golden re-derivation of every certified metric.
-- Deliberately does NOT use MetricFlow. This is the reconciliation counterparty.

with dau as (
    select 'dau' as metric, date_day as grain_date,
           count(distinct user_id)::double as reference_value
    from {{ ref('fct_usage_daily') }}
    where is_active = 1
    group by date_day
),

wau as (
    select 'wau' as metric, d.week_start as grain_date,
           count(distinct f.user_id)::double as reference_value
    from {{ ref('fct_usage_daily') }} f
    join {{ ref('dim_date') }} d on f.date_day = d.date_day
    where f.is_active = 1
    group by d.week_start
),

mau as (
    select 'mau' as metric, d.month_start as grain_date,
           count(distinct f.user_id)::double as reference_value
    from {{ ref('fct_usage_daily') }} f
    join {{ ref('dim_date') }} d on f.date_day = d.date_day
    where f.is_active = 1
    group by d.month_start
),

net_mrr as (
    select 'net_mrr' as metric, month_start as grain_date,
           sum(net_amount)::double as reference_value
    from {{ ref('fct_subscription_revenue') }}
    group by month_start
),

paid_conversion as (
    -- paid subscribers in month / distinct active users in month
    select 'paid_conversion' as metric, m.month_start as grain_date,
           (count(distinct r.user_id)::double
              / nullif(count(distinct f.user_id), 0)) as reference_value
    from {{ ref('dim_date') }} m
    left join {{ ref('fct_usage_daily') }} f
        on date_trunc('month', f.date_day) = m.month_start and f.is_active = 1
    left join {{ ref('fct_subscription_revenue') }} r
        on r.month_start = m.month_start
    group by m.month_start
),

gross_retention as (
    -- net revenue this month / net revenue prior month (>= second month only)
    select 'gross_retention' as metric, cur.month_start as grain_date,
           (cur.rev / nullif(prev.rev, 0)) as reference_value
    from (
        select month_start, sum(net_amount) as rev
        from {{ ref('fct_subscription_revenue') }} group by month_start
    ) cur
    join (
        select month_start, sum(net_amount) as rev
        from {{ ref('fct_subscription_revenue') }} group by month_start
    ) prev on prev.month_start = cur.month_start - interval 1 month
),

storage_gb_active as (
    select 'storage_gb_active' as metric, date_day as grain_date,
           (sum(storage_bytes_day)::double / 1e9) as reference_value
    from {{ ref('fct_usage_daily') }}
    where is_active = 1
    group by date_day
)

select * from dau
union all select * from wau
union all select * from mau
union all select * from net_mrr
union all select * from paid_conversion
union all select * from gross_retention
union all select * from storage_gb_active
```

- [ ] **Step 2: Write `models/reference/_reference.yml`**

```yaml
version: 2
models:
  - name: ref_metric_values
    columns:
      - name: metric
        data_tests: [not_null]
      - name: reference_value
        data_tests: [not_null]
```

- [ ] **Step 3: Build and test**

Run: `dbt build --profiles-dir . --select ref_metric_values`
Expected: builds; `not_null` tests PASS.

- [ ] **Step 4: Sanity-check the values**

Run:
```bash
duckdb cmf.duckdb "select metric, count(*) n, round(min(reference_value),2) lo, round(max(reference_value),2) hi from ref_metric_values group by metric order by metric"
```
Expected: the 7 base metrics, all with non-null ranges (e.g. `net_mrr` three rows, one per month). An 8th metric, `paying_users_monthly` (distinct paying users per month), is added to this model and to `seeds/metric_registry.csv` once the semantic layer is built in Task 7 — it is the registered counterpart of the ratio numerator, bringing the certified total to 8.

- [ ] **Step 5: Commit**

```bash
git add models/reference
git commit -m "feat: independent golden reference values for all metrics"
```

---

## Task 7: MetricFlow semantic layer (the governed definition)

**Files:**
- Create: `models/semantic/_semantic_usage.yml`, `models/semantic/_semantic_revenue.yml`

**Interfaces:**
- Consumes: `fct_usage_daily`, `fct_subscription_revenue`, `dim_date`.
- Produces: MetricFlow metrics `dau`, `wau`, `mau`, `paid_conversion`, `net_mrr`, `gross_retention`, `storage_gb_active`, queryable via `mf query`.
- **Bug injection lives here:** the `net_mrr` revenue measure expression is `net_amount` normally, but `mrr_amount` (refunds ignored) when `var('inject_break')` is true. This is the ONLY place the break is introduced.

- [ ] **Step 1: Write `models/semantic/_semantic_usage.yml`**

```yaml
version: 2

semantic_models:
  - name: usage_daily
    model: ref('fct_usage_daily')
    defaults:
      agg_time_dimension: activity_day
    entities:
      - name: user
        type: foreign
        expr: user_id
      - name: usage_row
        type: primary
        expr: "cast(user_id as varchar) || '-' || cast(date_day as varchar)"
    dimensions:
      - name: activity_day
        type: time
        expr: date_day
        type_params: {time_granularity: day}
    measures:
      - name: active_users
        agg: count_distinct
        expr: user_id
      - name: storage_gb_sum
        agg: sum
        expr: "storage_bytes_day / 1e9"

  - name: subscription_revenue_users
    model: ref('fct_subscription_revenue')
    defaults:
      agg_time_dimension: revenue_month
    entities:
      - name: user
        type: foreign
        expr: user_id
      - name: subscription
        type: primary
        expr: subscription_id
    dimensions:
      - name: revenue_month
        type: time
        expr: month_start
        type_params: {time_granularity: month}
    measures:
      - name: paying_users
        agg: count_distinct
        expr: user_id

metrics:
  - name: dau
    description: Distinct active users per day.
    type: simple
    type_params: {measure: active_users}
  - name: wau
    description: Distinct active users per week.
    type: simple
    type_params: {measure: active_users}
  - name: mau
    description: Distinct active users per month.
    type: simple
    type_params: {measure: active_users}
  - name: storage_gb_active
    description: Active storage in GB per day.
    type: simple
    type_params: {measure: storage_gb_sum}
  - name: paid_conversion
    description: Paying users divided by active users in the month.
    type: ratio
    type_params:
      numerator: paying_users
      denominator: active_users
```

- [ ] **Step 2: Write `models/semantic/_semantic_revenue.yml`** (contains the injectable break)

```yaml
version: 2

semantic_models:
  - name: subscription_revenue
    model: ref('fct_subscription_revenue')
    defaults:
      agg_time_dimension: revenue_month
    entities:
      - name: subscription
        type: primary
        expr: subscription_id
    dimensions:
      - name: revenue_month
        type: time
        expr: month_start
        type_params: {time_granularity: month}
    measures:
      - name: net_revenue
        agg: sum
        # Governed definition = net_amount (mrr minus refunds).
        # inject_break swaps in the buggy definition that ignores refunds.
        expr: "{{ 'mrr_amount' if var('inject_break', false) else 'net_amount' }}"

metrics:
  - name: net_mrr
    description: Net monthly recurring revenue (MRR minus refunds).
    type: simple
    type_params: {measure: net_revenue}
  - name: gross_retention
    description: Net revenue this month over net revenue prior month.
    type: derived
    type_params:
      expr: "current_net / prior_net"
      metrics:
        - name: net_mrr
          alias: current_net
        - name: net_mrr
          offset_window: 1 month
          alias: prior_net
```

- [ ] **Step 3: Parse and validate the semantic layer**

Run:
```bash
dbt parse --profiles-dir .
mf validate-configs --dbt-profiles-dir . --dbt-target duckdb
```
Expected: `dbt parse` succeeds; `mf validate-configs` reports no errors (warnings about unused dimensions are acceptable).

- [ ] **Step 4: Query a metric to confirm MetricFlow computes it**

Run:
```bash
mf query --metrics net_mrr --group-by metric_time__month --order metric_time__month
```
Expected: a small table of three monthly net_mrr values. (If your installed `mf` uses different flag spellings, run `mf query --help`; the metric/group-by concepts are stable.)

- [ ] **Step 5: Commit**

```bash
git add models/semantic
git commit -m "feat: MetricFlow semantic layer with injectable net_mrr definition break"
```

---

## Task 8: Freshness model + freshness control test

**Files:**
- Create: `models/certification/cert_freshness.sql`, `models/certification/_certification.yml`, `tests/assert_freshness_ok.sql`

**Interfaces:**
- Consumes: `fct_usage_daily`, `fct_subscription_revenue`.
- Produces: `cert_freshness(source_name, max_event_date, row_count, lag_days, is_fresh)` — one row per fact source. `is_fresh = lag_days <= var('freshness_max_lag_days')` measured against `var('analysis_end_date')`.

- [ ] **Step 1: Write `models/certification/cert_freshness.sql`**

```sql
with bounds as (
    select cast('{{ var("analysis_end_date") }}' as date) as as_of
),
usage as (
    select 'fct_usage_daily' as source_name,
           max(date_day) as max_event_date, count(*) as row_count
    from {{ ref('fct_usage_daily') }}
),
revenue as (
    select 'fct_subscription_revenue' as source_name,
           max(month_start) as max_event_date, count(*) as row_count
    from {{ ref('fct_subscription_revenue') }}
),
unioned as (
    select * from usage union all select * from revenue
)
select
    u.source_name,
    u.max_event_date,
    u.row_count,
    date_diff('day', u.max_event_date, b.as_of) as lag_days,
    date_diff('day', u.max_event_date, b.as_of) <= {{ var('freshness_max_lag_days') }} as is_fresh
from unioned u
cross join bounds b
```

Note: `fct_subscription_revenue.max_event_date` is a month-start, so its `lag_days` will exceed the daily threshold by design; the freshness threshold is interpreted per-source in the CLI (Task 11) which knows monthly sources tolerate up to 31 days. To keep this model simple, the singular test below only asserts the daily usage source.

- [ ] **Step 2: Write `tests/assert_freshness_ok.sql`** (dbt singular test — fails if rows returned)

```sql
-- Daily usage data must be fresh within the configured lag.
select *
from {{ ref('cert_freshness') }}
where source_name = 'fct_usage_daily'
  and not is_fresh
```

- [ ] **Step 3: Write `models/certification/_certification.yml`**

```yaml
version: 2
models:
  - name: cert_freshness
    columns:
      - name: source_name
        data_tests: [unique, not_null]
```

- [ ] **Step 4: Build and test**

Run: `dbt build --profiles-dir . --select cert_freshness assert_freshness_ok`
Expected: model builds; `assert_freshness_ok` PASSES (usage data ends `2026-03-31`, lag 0).

- [ ] **Step 5: Commit**

```bash
git add models/certification tests/assert_freshness_ok.sql
git commit -m "feat: freshness certification model + freshness control test"
```

---

## Task 9: CLI gate logic (pure functions) + Pydantic models

**Files:**
- Create: `metrics_cli/__init__.py`, `metrics_cli/models.py`, `metrics_cli/gates.py`
- Test: `tests_py/test_gates.py`

**Interfaces:**
- Produces (in `metrics_cli/models.py`):
  - `class GateResult(BaseModel): name: str; status: Literal["PASS","FAIL","SKIP"]; detail: str`
  - `class MetricCertificate(BaseModel): metric: str; owner: str; definition_source: str; gates: list[GateResult]; semantic_value: float | None; reference_value: float | None; variance_pct: float | None; as_of: str; checksum: str = ""` with property `certified -> bool` (all gates PASS).
- Produces (in `metrics_cli/gates.py`), all pure:
  - `governance_gate(registry: list[dict], semantic_metric_names: set[str]) -> GateResult` — FAIL if the registry metric set and semantic metric set differ; detail names the symmetric difference.
  - `reconciliation_gate(metric: str, kind: str, semantic_value: float | None, reference_value: float | None) -> GateResult` — exact match for `kind == "count"`, else relative tolerance `1e-4`. FAIL with signed variance in detail. SKIP if either value is None.
  - `freshness_gate(metric: str, grain: str, rows: list[dict]) -> GateResult` — daily metrics use `fct_usage_daily.is_fresh`; monthly/week metrics tolerate `lag_days <= 31`.
  - `variance_pct(semantic: float, reference: float) -> float | None`.

- [ ] **Step 1: Write the failing tests**

```python
# tests_py/test_gates.py
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
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests_py/test_gates.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'metrics_cli'`

- [ ] **Step 3: Write `metrics_cli/__init__.py`**

```python
"""Certified Metrics Framework CLI package."""
```

- [ ] **Step 4: Write `metrics_cli/models.py`**

```python
from __future__ import annotations
from typing import Literal
from pydantic import BaseModel

class GateResult(BaseModel):
    name: str
    status: Literal["PASS", "FAIL", "SKIP"]
    detail: str = ""

class MetricCertificate(BaseModel):
    metric: str
    owner: str
    definition_source: str
    gates: list[GateResult]
    semantic_value: float | None = None
    reference_value: float | None = None
    variance_pct: float | None = None
    as_of: str
    checksum: str = ""

    @property
    def certified(self) -> bool:
        return all(g.status == "PASS" for g in self.gates)
```

- [ ] **Step 5: Write `metrics_cli/gates.py`**

```python
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
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `python -m pytest tests_py/test_gates.py -v`
Expected: PASS (7 passed)

- [ ] **Step 7: Commit**

```bash
git add metrics_cli/__init__.py metrics_cli/models.py metrics_cli/gates.py tests_py/test_gates.py
git commit -m "feat: pure gate logic + Pydantic certificate models"
```

---

## Task 10: MetricFlow runner + DuckDB readers

**Files:**
- Create: `metrics_cli/mf_runner.py`
- Test: extend `tests_py/test_gates.py` is not appropriate; create `tests_py/test_mf_runner.py`

**Interfaces:**
- Consumes: `gates`/`models` not required here.
- Produces (in `metrics_cli/mf_runner.py`):
  - `parse_mf_table(text: str) -> float | None` — pure parser that sums the value column of MetricFlow's whitespace-aligned table output. Strategy: for each line, split on whitespace and try `float(last token)`; sum the ones that parse. This naturally skips the header row (last token is the metric name), `None` rows (gross_retention's first month), and any mf log lines. Returns None if no numeric rows.
  - `dbt_parse_with_vars(inject_break: bool, runner=subprocess.run) -> bool` — runs `dbt parse --profiles-dir . --vars '{"inject_break": <bool>}'` to compile the manifest MetricFlow reads from `target/`. Returns True on success. **This is how the break is injected** — metricflow 0.206.0 has no `--dbt-vars` flag on `mf query`, so the var must be baked into the compiled manifest first.
  - `semantic_metric_total(metric: str, runner=subprocess.run) -> float | None` — runs `mf query --metrics <m> --group-by metric_time__<grain> --order metric_time__<grain>` against the currently-compiled manifest and returns the summed value. Grain per `MF_GRAIN = {"dau": "day", "wau": "week"}` (default `"month"`). Does NOT parse vars itself — the caller calls `dbt_parse_with_vars` once first.
  - `read_reference_totals(db_path: str) -> dict[str, float]` — sums `ref_metric_values.reference_value` per metric from DuckDB.
  - `read_freshness(db_path: str) -> list[dict]` — reads `cert_freshness`.
  - `read_registry(db_path: str) -> list[dict]` — reads the `metric_registry` seed table.

Note on grain and reconciliation: distinct-count metrics (dau day, wau week, mau/paying_users_monthly month) MUST be queried at the same grain their reference uses, so the summed totals match. Additive metrics (net_mrr, storage_gb_active) reconcile at any grain because the total is grain-independent; we query them at month. Ratio/derived (paid_conversion, gross_retention) are summed across the same monthly rows on both sides.

- [ ] **Step 1: Write the failing test (pure parser + injected runner)**

```python
# tests_py/test_mf_runner.py
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
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests_py/test_mf_runner.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Write `metrics_cli/mf_runner.py`**

```python
from __future__ import annotations
import json
import subprocess
import sys
from pathlib import Path
import duckdb

MF_GRAIN = {"dau": "day", "wau": "week"}  # everything else is monthly

# Resolve dbt/mf to the venv hosting this package so subprocess doesn't pick
# up a system dbt that lacks the duckdb adapter.
_VENV_BIN = Path(sys.executable).parent
_DBT = str(_VENV_BIN / "dbt")
_MF = str(_VENV_BIN / "mf")


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
    cmd = [_DBT, "parse", "--profiles-dir", ".",
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
    # --decimals 10: mf rounds to 2 dp by default; summed ratio metrics
    # (paid_conversion, gross_retention) then drift past the reconcile tolerance.
    cmd = [_MF, "query", "--metrics", metric,
           "--group-by", group_by, "--order", group_by, "--decimals", "10"]
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
```

Note: `parse_mf_table` is intentionally forgiving — it sums any line ending in a float. This survives mf printing log/banner lines to stdout and is far more robust than column-position parsing. The mechanism (parse-then-query) was verified end-to-end in Task 7: clean net_mrr total 23393.19, broken 24672.00.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests_py/test_mf_runner.py -v`
Expected: PASS (5 passed)

- [ ] **Step 4b: Smoke-test against the real manifest**

Run:
```bash
python -c "from metrics_cli.mf_runner import dbt_parse_with_vars, semantic_metric_total; dbt_parse_with_vars(False); print('net_mrr', semantic_metric_total('net_mrr')); print('dau', semantic_metric_total('dau'))"
```
Expected: `net_mrr` ≈ 23393.19 and `dau` a positive number (sum of daily distinct actives). Confirms the runner drives the real `mf` CLI, not just the fakes.

- [ ] **Step 5: Commit**

```bash
git add metrics_cli/mf_runner.py tests_py/test_mf_runner.py
git commit -m "feat: MetricFlow runner + DuckDB readers with injectable subprocess"
```

---

## Task 11: Certificate assembly + `certify` / `pack` CLI commands

**Files:**
- Create: `metrics_cli/certificate.py`, `metrics_cli/cli.py`
- Test: `tests_py/test_certificate.py`

**Interfaces:**
- Consumes: `gates`, `models`, `mf_runner`, registry rows.
- Produces (in `metrics_cli/certificate.py`):
  - `build_certificate(reg_row: dict, semantic_value: float | None, reference_value: float | None, freshness_rows: list[dict], semantic_metric_names: set[str], registry: list[dict], as_of: str) -> MetricCertificate` — runs all three gates, sets values + variance, computes `checksum` as `sha256` of the certificate JSON minus the checksum field.
  - `render_registry_md(certs: list[MetricCertificate]) -> str` — the scorecard table.
- Produces (in `metrics_cli/cli.py`): a Typer app with `certify` (build DB→certificates→files, exit 1 if any uncertified) and `pack` (checksum bundle of `evidence/`).

- [ ] **Step 1: Write the failing test**

```python
# tests_py/test_certificate.py
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
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests_py/test_certificate.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Write `metrics_cli/certificate.py`**

```python
from __future__ import annotations
import hashlib
import json
from .models import GateResult, MetricCertificate
from .gates import governance_gate, reconciliation_gate, freshness_gate, variance_pct


def build_certificate(reg_row: dict, semantic_value: float | None,
                      reference_value: float | None, freshness_rows: list[dict],
                      semantic_metric_names: set[str], registry: list[dict],
                      as_of: str) -> MetricCertificate:
    gates = [
        governance_gate(registry, semantic_metric_names),
        freshness_gate(reg_row["metric"], reg_row["grain"], freshness_rows),
        reconciliation_gate(reg_row["metric"], reg_row["kind"],
                            semantic_value, reference_value),
    ]
    vp = (variance_pct(semantic_value, reference_value)
          if semantic_value is not None and reference_value is not None else None)
    cert = MetricCertificate(
        metric=reg_row["metric"], owner=reg_row["owner"],
        definition_source=reg_row["definition_file"], gates=gates,
        semantic_value=semantic_value, reference_value=reference_value,
        variance_pct=vp, as_of=as_of)
    payload = cert.dict()
    payload.pop("checksum", None)
    cert.checksum = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()
    return cert


def render_registry_md(certs: list[MetricCertificate]) -> str:
    lines = ["# Certification Registry", "",
             "| Metric | Owner | Status | Variance | Checksum |",
             "| --- | --- | --- | --- | --- |"]
    for c in sorted(certs, key=lambda x: x.metric):
        status = "CERTIFIED" if c.certified else "FAILED"
        vp = "n/a" if c.variance_pct is None else f"{c.variance_pct:+.4f}%"
        lines.append(f"| {c.metric} | {c.owner} | {status} | {vp} | {c.checksum[:12]}… |")
    n_ok = sum(1 for c in certs if c.certified)
    lines += ["", f"**{n_ok}/{len(certs)} metrics certified.**"]
    return "\n".join(lines) + "\n"
```

- [ ] **Step 4: Write `metrics_cli/cli.py`**

```python
# NOTE: do NOT add `from __future__ import annotations` here — under it Typer
# 0.12.5 sees the `bool` annotation as the string "bool", fails to build the
# flag, and delivers None for --inject-break (bool(None) is False, so the break
# never fires). Plain runtime annotations keep Typer's introspection working.
import glob
import hashlib
import json
import os
from pathlib import Path
import yaml
import typer
from .certificate import build_certificate, render_registry_md
from .mf_runner import (
    semantic_metric_total, dbt_parse_with_vars,
    read_reference_totals, read_freshness, read_registry,
)

app = typer.Typer(help="Certified Metrics Framework CLI")
DB_PATH = "cmf.duckdb"
AS_OF = "2026-03-31"


def _semantic_metric_names() -> set[str]:
    """Top-level metric names defined in the MetricFlow YAML (the governed set)."""
    names: set[str] = set()
    for path in glob.glob("models/semantic/*.yml"):
        doc = yaml.safe_load(Path(path).read_text()) or {}
        for m in (doc.get("metrics") or []):
            if isinstance(m, dict) and "name" in m:
                names.add(m["name"])
    return names


@app.command()
def certify(inject_break: bool = typer.Option(False, "--inject-break",
                                              is_flag=True, flag_value=True),
            out: str = typer.Option("evidence", "--out")):
    """Run all gates per metric and emit certificates + registry. Exits 1 if any fail."""
    registry = read_registry(DB_PATH)
    references = read_reference_totals(DB_PATH)
    freshness = read_freshness(DB_PATH)
    semantic_names = _semantic_metric_names()
    os.makedirs(out, exist_ok=True)

    # Compile the manifest once in the requested state; mf reads it from target/.
    if not dbt_parse_with_vars(inject_break):
        typer.echo("dbt parse failed; cannot compute semantic values", err=True)
        raise typer.Exit(code=2)

    certs = []
    for row in registry:
        metric = row["metric"]
        sem = semantic_metric_total(metric)
        cert = build_certificate(row, sem, references.get(metric), freshness,
                                 semantic_names, registry, AS_OF)
        certs.append(cert)
        Path(out, f"metric_certificate_{metric}.json").write_text(
            cert.json(indent=2))
        typer.echo(f"{'PASS' if cert.certified else 'FAIL'}  {metric}")

    Path(out, "certification_registry.md").write_text(render_registry_md(certs))
    failed = [c.metric for c in certs if not c.certified]
    if failed:
        typer.echo(f"\nUNCERTIFIED: {failed}", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"\nAll {len(certs)} metrics certified.")


@app.command()
def pack(out: str = typer.Option("evidence", "--out")):
    """Write a checksum manifest over all evidence files (tamper-evident bundle)."""
    files = sorted(p for p in glob.glob(f"{out}/*") if not p.endswith("MANIFEST.sha256"))
    lines = []
    for p in files:
        digest = hashlib.sha256(Path(p).read_bytes()).hexdigest()
        lines.append(f"{digest}  {os.path.basename(p)}")
    Path(out, "MANIFEST.sha256").write_text("\n".join(lines) + "\n")
    typer.echo(f"Packed {len(files)} files into {out}/MANIFEST.sha256")


if __name__ == "__main__":
    app()
```

- [ ] **Step 5: Run unit tests to verify they pass**

Run: `python -m pytest tests_py/test_certificate.py -v`
Expected: PASS (4 passed)

- [ ] **Step 6: End-to-end clean run**

Run:
```bash
dbt build --profiles-dir .
python -m metrics_cli.cli certify
cat evidence/certification_registry.md
```
Expected: every metric prints `PASS`; registry shows `8/8 metrics certified`; exit code 0.

- [ ] **Step 7: Commit**

```bash
git add metrics_cli/certificate.py metrics_cli/cli.py tests_py/test_certificate.py
git commit -m "feat: certificate assembly + certify/pack CLI commands"
```

---

## Task 12: AI-native explainer (deterministic + optional local Ollama)

**Files:**
- Create: `metrics_cli/explain/__init__.py`, `metrics_cli/explain/heuristic.py`, `metrics_cli/explain/ollama.py`
- Modify: `metrics_cli/cli.py` (add `explain` command)
- Test: `tests_py/test_explain.py`

**Interfaces:**
- Produces (in `metrics_cli/explain/heuristic.py`):
  - `classify(metric: str, kind: str, semantic_value: float, reference_value: float, refund_total: float | None = None) -> dict` returning `{"cause": str, "confidence": str, "explanation": str}`. Recognizes (a) refund-offset: `semantic - reference ≈ refund_total` → `"refunds_not_subtracted"`; (b) integer-multiple: `semantic ≈ N*reference (N>=2)` → `"wrong_grain_or_missing_distinct"`; else `"unclassified"`.
- Produces (in `metrics_cli/explain/ollama.py`): `rewrite(explanation: str, *, model="llama3.1:8b", runner=subprocess.run) -> str` — local Ollama prose rewrite; returns the input unchanged if Ollama is unavailable.

- [ ] **Step 1: Write the failing test**

```python
# tests_py/test_explain.py
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
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests_py/test_explain.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Write `metrics_cli/explain/__init__.py`**

```python
"""Variance explanation: deterministic classification + optional local LLM prose."""
```

- [ ] **Step 4: Write `metrics_cli/explain/heuristic.py`**

```python
from __future__ import annotations


def classify(metric: str, kind: str, semantic_value: float, reference_value: float,
             refund_total: float | None = None) -> dict:
    diff = semantic_value - reference_value

    if refund_total is not None and reference_value != 0:
        if abs(diff - refund_total) <= max(1e-6, 1e-4 * abs(refund_total)):
            return {
                "cause": "refunds_not_subtracted",
                "confidence": "high",
                "explanation": (
                    f"{metric} semantic value exceeds the reference by "
                    f"{diff:,.2f}, which equals the period refund total "
                    f"({refund_total:,.2f}). The semantic measure is summing gross "
                    f"MRR instead of net (mrr_amount minus refunds). Inspect the "
                    f"net_revenue measure expression in _semantic_revenue.yml."
                ),
            }

    if reference_value != 0:
        ratio = semantic_value / reference_value
        nearest = round(ratio)
        if nearest >= 2 and abs(ratio - nearest) <= 0.02:
            return {
                "cause": "wrong_grain_or_missing_distinct",
                "confidence": "medium",
                "explanation": (
                    f"{metric} semantic value is about {nearest}x the reference "
                    f"({semantic_value:g} vs {reference_value:g}). A count metric "
                    f"inflated by an integer factor usually means a missing "
                    f"count(distinct ...) or a join that fans out the grain."
                ),
            }

    return {
        "cause": "unclassified",
        "confidence": "low",
        "explanation": (
            f"{metric} diverges from the reference ({semantic_value:g} vs "
            f"{reference_value:g}) without a recognized signature. Inspect the "
            f"measure definition and upstream joins manually."
        ),
    }
```

- [ ] **Step 5: Write `metrics_cli/explain/ollama.py`**

```python
from __future__ import annotations
import subprocess


def rewrite(explanation: str, *, model: str = "llama3.1:8b", runner=subprocess.run) -> str:
    prompt = (
        "Rewrite the following data-quality finding in two clear sentences for an "
        "analytics engineer. Do not change any numbers, metric names, or the root "
        f"cause.\n\nFinding: {explanation}"
    )
    try:
        result = runner(["ollama", "run", model, prompt],
                        capture_output=True, text=True, timeout=60)
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return explanation
    if getattr(result, "returncode", 1) != 0 or not result.stdout.strip():
        return explanation
    return result.stdout.strip()
```

- [ ] **Step 6: Add the `explain` command to `metrics_cli/cli.py`**

Insert this import near the top with the others:
```python
from .explain.heuristic import classify
from .explain.ollama import rewrite
```

Add this command before `if __name__ == "__main__":`:
```python
@app.command()
def explain(metric: str = typer.Argument(...),
            backend: str = typer.Option("heuristic", "--backend"),
            out: str = typer.Option("evidence", "--out")):
    """Explain why a metric failed certification (deterministic; optional local LLM)."""
    cert_path = Path(out, f"metric_certificate_{metric}.json")
    if not cert_path.exists():
        typer.echo(f"No certificate for {metric}; run certify first.", err=True)
        raise typer.Exit(code=1)
    cert = json.loads(cert_path.read_text())
    refs = read_reference_totals(DB_PATH)
    reg = {r["metric"]: r for r in read_registry(DB_PATH)}
    refund_total = None
    if metric == "net_mrr":
        import duckdb
        con = duckdb.connect(DB_PATH, read_only=True)
        refund_total = float(con.execute(
            "select sum(refund_amount) from fct_subscription_revenue").fetchone()[0])
        con.close()
    result = classify(metric, reg[metric]["kind"],
                      cert["semantic_value"], cert["reference_value"], refund_total)
    text = result["explanation"]
    if backend == "ollama":
        text = rewrite(text)
    typer.echo(f"## {metric}: {result['cause']} ({result['confidence']} confidence)\n")
    typer.echo(text)
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `python -m pytest tests_py/test_explain.py -v`
Expected: PASS (4 passed)

- [ ] **Step 8: Commit**

```bash
git add metrics_cli/explain metrics_cli/cli.py tests_py/test_explain.py
git commit -m "feat: deterministic variance explainer with optional local Ollama"
```

---

## Task 13: The proof — bug injection caught end-to-end

**Files:**
- No new source; this task verifies the integrated control and records the demonstration.

**Interfaces:**
- Consumes: everything built so far.

- [ ] **Step 1: Confirm the clean build certifies**

Run:
```bash
dbt build --profiles-dir .
python -m metrics_cli.cli certify
echo "exit: $?"
```
Expected: `8/8 metrics certified`, `exit: 0`.

- [ ] **Step 2: Inject the bug and confirm dbt schema tests stay green**

Run:
```bash
dbt build --profiles-dir . --vars 'inject_break: true' --exclude resource_type:unit_test
dbt test --profiles-dir . --vars 'inject_break: true' --select assert_freshness_ok
echo "schema tests exit: $?"
```
Expected: build succeeds; `assert_freshness_ok` PASSES (the data is still fresh — schema tests cannot see the logic bug). `schema tests exit: 0`.

- [ ] **Step 3: Confirm certification catches it**

Run:
```bash
python -m metrics_cli.cli certify --inject-break
echo "certify exit: $?"
```
Expected: `net_mrr` and `gross_retention` print `FAIL` (gross_retention is derived from net_mrr); `certify exit: 1`. Registry shows them as FAILED with a positive variance.

- [ ] **Step 4: Confirm the explainer names the cause**

Run:
```bash
python -m metrics_cli.cli explain net_mrr
```
Expected: `net_mrr: refunds_not_subtracted (high confidence)` with the explanation that the variance equals the refund total.

- [ ] **Step 5: Restore the clean ledger and re-certify**

Run:
```bash
dbt build --profiles-dir .
python -m metrics_cli.cli certify && echo "restored: certified"
```
Expected: `8/8 metrics certified`, prints `restored: certified`.

- [ ] **Step 6: Commit (records nothing new but marks the milestone)**

```bash
git commit --allow-empty -m "test: verify certification catches injected definition break end-to-end"
```

---

## Task 14: Databricks parity layer

**Files:**
- Create: `databricks/unity_catalog.sql`, `databricks/delta_tables.sql`, `databricks/metric_views/net_mrr.yml`, `databricks/metric_views/active_users.yml`, `databricks/parity_notes.md`

**Interfaces:**
- No runtime dependency in the default path; these are reviewed artifacts + an optional target. Validated by inspection, not CI execution.

- [ ] **Step 1: Write `databricks/unity_catalog.sql`**

```sql
-- Unity Catalog namespace for the certified metrics framework.
CREATE CATALOG IF NOT EXISTS cmf;
CREATE SCHEMA IF NOT EXISTS cmf.analytics;

-- Governance: the metric registry is the contract; restrict who can alter definitions.
GRANT USE CATALOG ON CATALOG cmf TO `analytics-readers`;
GRANT CREATE, MODIFY ON SCHEMA cmf.analytics TO `analytics-engineers`;
```

- [ ] **Step 2: Write `databricks/delta_tables.sql`**

```sql
-- Delta-backed conformed fact tables mirroring the dbt marts, clustered for the
-- period-and-user reconciliation queries the certification framework runs.
CREATE TABLE IF NOT EXISTS cmf.analytics.fct_usage_daily (
    user_id BIGINT,
    date_day DATE,
    is_active INT,
    storage_bytes_day BIGINT
) USING DELTA
CLUSTER BY (date_day, user_id);

CREATE TABLE IF NOT EXISTS cmf.analytics.fct_subscription_revenue (
    subscription_id BIGINT,
    user_id BIGINT,
    plan_id STRING,
    month_start DATE,
    mrr_amount DOUBLE,
    refund_amount DOUBLE,
    net_amount DOUBLE
) USING DELTA
CLUSTER BY (month_start);
```

- [ ] **Step 3: Write `databricks/metric_views/net_mrr.yml`** (Databricks Metric View mirroring the MetricFlow net_mrr)

```yaml
version: 0.1
source: cmf.analytics.fct_subscription_revenue
dimensions:
  - name: revenue_month
    expr: month_start
measures:
  - name: net_mrr
    # Same governed definition as MetricFlow's net_revenue measure: net of refunds.
    expr: SUM(net_amount)
```

- [ ] **Step 4: Write `databricks/metric_views/active_users.yml`**

```yaml
version: 0.1
source: cmf.analytics.fct_usage_daily
dimensions:
  - name: activity_day
    expr: date_day
measures:
  - name: active_users
    expr: COUNT(DISTINCT user_id)
  - name: storage_gb_active
    expr: SUM(storage_bytes_day) / 1e9
```

- [ ] **Step 5: Write `databricks/parity_notes.md`**

```markdown
# Cross-engine metric parity

One governed definition, two compile targets:

| Metric | MetricFlow (dbt) | Databricks Metric View |
| --- | --- | --- |
| net_mrr | `_semantic_revenue.yml` measure `net_revenue = SUM(net_amount)` | `metric_views/net_mrr.yml` measure `SUM(net_amount)` |
| dau/wau/mau | `_semantic_usage.yml` measure `active_users = COUNT(DISTINCT user_id)` | `metric_views/active_users.yml` measure `COUNT(DISTINCT user_id)` |
| storage_gb_active | `storage_gb_sum = SUM(storage_bytes_day)/1e9` | `SUM(storage_bytes_day)/1e9` |

Both express the SAME logic against the SAME conformed fact tables. Certification
holds across engines because the reference values in `ref_metric_values` are
engine-independent: run `dbt build --target databricks` and the same
`metrics_cli certify` reconciliation applies, with the Databricks Metric View as
an optional third counterparty.

## Running on Databricks

DuckDB is the default target so the repo runs free. To run on Databricks set the
`DATABRICKS_*` env vars (see `profiles.yml`), apply `unity_catalog.sql` and
`delta_tables.sql`, then `dbt build --profiles-dir . --target databricks`.
Clustering `fct_usage_daily` by `(date_day, user_id)` prunes files on the
per-day, per-user reconciliation scans.
```

- [ ] **Step 6: Commit**

```bash
git add databricks
git commit -m "feat: Databricks Unity Catalog + Delta + Metric View parity layer"
```

---

## Task 15: Airflow DAG + orchestration doc

**Files:**
- Create: `airflow/dags/certify_metrics_dag.py`, `docs/orchestration.md`
- Test: `tests_py/test_dag_imports.py`

**Interfaces:**
- Produces: a lint-clean, importable DAG `certify_metrics` with tasks `dbt_build → dbt_test → certify → pack`, and `certify` routed to an `alert_on_failure` task on failure. Not executed in CI; validated by import + structure assertions.

- [ ] **Step 1: Write the failing test**

```python
# tests_py/test_dag_imports.py
import importlib.util
from pathlib import Path
import pytest

def _load():
    spec = importlib.util.spec_from_file_location(
        "certify_metrics_dag", "airflow/dags/certify_metrics_dag.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def test_dag_structure():
    pytest.importorskip("airflow")
    mod = _load()
    dag = mod.dag
    task_ids = {t.task_id for t in dag.tasks}
    assert {"dbt_build", "dbt_test", "certify", "pack", "alert_on_failure"} <= task_ids
    certify = dag.get_task("certify")
    assert "dbt_test" in {t.task_id for t in certify.upstream_list}
```

- [ ] **Step 2: Run to verify failure (or skip without airflow)**

Run: `python -m pytest tests_py/test_dag_imports.py -v`
Expected: FAIL (file missing) — or SKIP if airflow isn't installed locally. Either way it must not error after Step 3 with airflow present.

- [ ] **Step 3: Write `airflow/dags/certify_metrics_dag.py`**

```python
"""Daily certification pipeline: build -> test -> certify -> pack, with failure alerting.

Demonstrates pipeline decomposition, scheduling, and failure-recovery routing.
Not executed in CI; the same steps run directly there (see .github/workflows/ci.yml).
"""
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "analytics-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="certify_metrics",
    description="Build models and certify metrics against golden references daily.",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["metrics", "certification"],
) as dag:
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="dbt build --profiles-dir . --select staging intermediate marts reference certification",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --profiles-dir .",
    )
    certify = BashOperator(
        task_id="certify",
        bash_command="python -m metrics_cli.cli certify",
    )
    pack = BashOperator(
        task_id="pack",
        bash_command="python -m metrics_cli.cli pack",
    )

    def _alert(**_):
        # In production: page the metric owner from certification_registry.md.
        raise RuntimeError("Certification failed; see certification_registry.md")

    alert_on_failure = PythonOperator(
        task_id="alert_on_failure",
        python_callable=_alert,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    dbt_build >> dbt_test >> certify >> pack
    certify >> alert_on_failure
```

- [ ] **Step 4: Write `docs/orchestration.md`**

```markdown
# Orchestration

The certification pipeline decomposes into four idempotent stages:

1. `dbt_build` — materialize staging → marts → reference → certification.
2. `dbt_test` — schema, relationship, and unit tests.
3. `certify` — run the three gates per metric, emit certificates + registry, exit non-zero on any failure.
4. `pack` — checksum the evidence bundle.

`certify` is the gate that fails the DAG run. `alert_on_failure` uses Airflow's
`ONE_FAILED` trigger rule to page the owner named in the certificate. Retries
(2x, 5-minute backoff) cover transient warehouse errors; a certification failure
is a real failure and is not retried into green — it must be fixed at the
definition layer. CI (`.github/workflows/ci.yml`) runs the same four steps
directly, plus the inject-break proof.
```

- [ ] **Step 5: Run the test to verify pass (or skip)**

Run: `python -m pytest tests_py/test_dag_imports.py -v`
Expected: PASS if airflow installed, else SKIP. Must not ERROR.

- [ ] **Step 6: Commit**

```bash
git add airflow docs/orchestration.md tests_py/test_dag_imports.py
git commit -m "feat: Airflow certification DAG + orchestration doc"
```

---

## Task 16: CI workflows

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Three jobs: `build-test` (dbt build + test + pytest), `cert-proof` (clean certify passes; injected certify fails naming net_mrr; restored certify passes), `cli-tests` (pytest only, fast).

- [ ] **Step 1: Write `.github/workflows/ci.yml`**

```yaml
name: ci
on: [push, pull_request]

jobs:
  build-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.11"}
      - run: pip install -r requirements.txt
      - run: dbt deps --profiles-dir .
      - run: python scripts/seed_events.py
      - run: dbt build --profiles-dir .
      - run: python -m pytest tests_py -v

  cert-proof:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.11"}
      - run: pip install -r requirements.txt
      - run: dbt deps --profiles-dir .
      - run: python scripts/seed_events.py
      - name: clean build certifies
        run: |
          dbt build --profiles-dir .
          python -m metrics_cli.cli certify
      - name: injected break fails certification (and names net_mrr)
        run: |
          if python -m metrics_cli.cli certify --inject-break > out.txt 2>&1; then
            echo "ERROR: certification should have failed under inject-break"; cat out.txt; exit 1
          fi
          grep -q "FAIL  net_mrr" out.txt
          python -m metrics_cli.cli explain net_mrr | grep -q "refunds_not_subtracted"
      - name: restored build re-certifies
        run: |
          dbt build --profiles-dir .
          python -m metrics_cli.cli certify

  cli-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: "3.11"}
      - run: pip install -r requirements.txt
      - run: python -m pytest tests_py/test_gates.py tests_py/test_certificate.py tests_py/test_explain.py tests_py/test_mf_runner.py -v
```

Note: `cert-proof` injected-break step reads `certify --inject-break` stdout, which prints `FAIL  net_mrr` (two spaces, matching the CLI's `f"{'FAIL'}  {metric}"`).

- [ ] **Step 2: Verify the cert-proof logic locally**

Run:
```bash
dbt build --profiles-dir . && python -m metrics_cli.cli certify
python -m metrics_cli.cli certify --inject-break > out.txt 2>&1; echo "exit $?"
grep "FAIL  net_mrr" out.txt
```
Expected: clean certify exit 0; inject-break exit 1; grep finds the line.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: build+test, cert-proof (inject-break caught), cli-tests"
```

---

## Task 17: README + capability map

**Files:**
- Create: `README.md`

**Interfaces:**
- The front door: quickstart, the control in action, capability map keyed to the job.

- [ ] **Step 1: Write `README.md`**

````markdown
# Certified Metrics Framework

A small, runnable SaaS-analytics metrics platform in dbt, MetricFlow, and DuckDB.
A metric is **certified** only when it is *governed* (one MetricFlow definition),
*fresh*, and *reconciled* against an independently re-derived golden value. The
point: **a query that returns a number is not a correct metric** — a definition
bug passes every schema test and is caught only by reconciliation.

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
naming `net_mrr` (and the derived `gross_retention`) with the exact variance.
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
````

- [ ] **Step 2: Verify the quickstart end-to-end from clean**

Run:
```bash
rm -f cmf.duckdb && rm -rf evidence
dbt deps --profiles-dir . && python scripts/seed_events.py && dbt build --profiles-dir .
python -m metrics_cli.cli certify && python -m metrics_cli.cli pack
```
Expected: `8/8 metrics certified`; `pack` writes `evidence/MANIFEST.sha256`.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: README with quickstart, control-in-action, and capability map"
```

---

## Self-Review

**Spec coverage:**
- Certification = governed + fresh + reconciled → Tasks 9 (gates), 7/6 (semantic vs reference), 8 (freshness). ✓
- Proof in action (inject bug, schema green, reconciliation red) → Task 13 + CI Task 16. ✓
- Conformed dims + shared facts → Task 5. ✓
- MetricFlow as single governed definition → Task 7; governance gate enforces registry↔semantic parity → Task 9. ✓
- Independent golden reference → Task 6. ✓
- Metric certificate (JSON + checksum) + registry scorecard → Tasks 11. ✓
- Databricks Metric Views / Unity Catalog / Delta + optional target → Tasks 1 (profile) + 14. ✓
- Airflow DAG + failure recovery → Task 15. ✓
- AI-native explainer (deterministic + optional local Ollama) → Task 12. ✓
- dbt unit tests → Task 4. ✓
- CI (build+test, cert-proof, cli-tests) → Task 16. ✓
- Capability map + README → Task 17. ✓
- All 7 canonical metrics modeled in both semantic (Task 7) and reference (Task 6). ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; every run step shows the command and expected output. ✓

**Type consistency:** `GateResult`/`MetricCertificate` fields used identically across Tasks 9, 11, 12. `build_certificate` signature in Task 11 matches its test and CLI caller. `semantic_metric_total(metric, runner)` + `dbt_parse_with_vars(inject_break, runner)` consistent across Tasks 10–11 (injection via dbt parse, not an mf flag). CLI prints `FAIL  net_mrr` (two spaces) — matched by the CI grep in Task 16 and the proof in Task 13. Registry CSV columns (`metric,owner,grain,kind,definition_file`) consistent across Tasks 2, 10, 11. ✓
