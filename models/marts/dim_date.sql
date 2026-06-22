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
