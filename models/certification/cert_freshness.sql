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
