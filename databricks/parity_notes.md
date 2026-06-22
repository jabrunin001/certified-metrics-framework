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
