select
    event_id,
    user_id,
    cast(event_ts as timestamp) as event_ts,
    cast(event_ts as date) as event_date,
    event_type,
    storage_bytes
from {{ ref('usage_events') }}
