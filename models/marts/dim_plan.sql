select * from (
    values
        ('free', 'Free', 0.0, false),
        ('plus', 'Plus', 12.0, true),
        ('pro', 'Pro', 24.0, true),
        ('team', 'Team', 60.0, true)
) as t(plan_id, plan_name, list_mrr, is_paid)
