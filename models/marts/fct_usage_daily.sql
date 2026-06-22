select
    a.user_id,
    a.event_date as date_day,
    a.is_active,
    a.storage_bytes_day
from {{ ref('int_user_daily_activity') }} a
