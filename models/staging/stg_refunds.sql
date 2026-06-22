select
    refund_id,
    subscription_id,
    cast(period_month as date) as period_month,
    cast(refund_amount as double) as refund_amount
from {{ ref('refunds') }}
