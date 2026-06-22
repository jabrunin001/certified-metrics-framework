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
