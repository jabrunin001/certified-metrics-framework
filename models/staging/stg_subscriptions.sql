select
    subscription_id,
    user_id,
    plan_id,
    cast(period_month as date) as period_month,
    cast(mrr_amount as double) as mrr_amount
from {{ ref('subscriptions') }}
