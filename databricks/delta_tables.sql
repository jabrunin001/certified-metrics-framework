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
