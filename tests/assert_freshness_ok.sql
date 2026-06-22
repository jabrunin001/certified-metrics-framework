-- Daily usage data must be fresh within the configured lag.
select *
from {{ ref('cert_freshness') }}
where source_name = 'fct_usage_daily'
  and not is_fresh
