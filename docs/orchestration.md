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
