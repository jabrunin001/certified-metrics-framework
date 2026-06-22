select
    subscription_id,
    user_id,
    plan_id,
    period_month as month_start,
    mrr_amount,
    refund_amount,
    net_amount
from {{ ref('int_subscription_months') }}
