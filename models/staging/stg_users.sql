select
    user_id,
    cast(signup_date as date) as signup_date,
    country,
    acquisition_channel
from {{ ref('users') }}
