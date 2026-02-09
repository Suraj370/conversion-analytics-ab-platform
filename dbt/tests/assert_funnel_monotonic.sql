-- Custom dbt test: funnel steps must be monotonically decreasing.
-- If any step has MORE users than the previous step, something is wrong.

with funnel as (
    select
        step,
        step_order,
        users_reached,
        lag(users_reached) over (order by step_order) as prev_users
    from {{ ref('fct_funnel') }}
)

-- This query should return 0 rows if the funnel is valid.
-- Any rows returned indicate a step with more users than the previous step.
select *
from funnel
where prev_users is not null
  and users_reached > prev_users
