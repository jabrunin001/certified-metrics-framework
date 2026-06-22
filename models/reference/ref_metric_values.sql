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

paying_users_monthly as (
    select 'paying_users_monthly' as metric, month_start as grain_date,
           count(distinct user_id)::double as reference_value
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
union all select * from paying_users_monthly
union all select * from paid_conversion
union all select * from gross_retention
union all select * from storage_gb_active
