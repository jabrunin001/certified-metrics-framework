select
    user_id,
    event_date,
    1 as is_active,
    max(storage_bytes) as storage_bytes_day
from {{ ref('stg_usage_events') }}
group by user_id, event_date
