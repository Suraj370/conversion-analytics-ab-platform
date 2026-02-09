-- Funnel metrics: aggregate conversion rates at each stage.
-- One row per funnel step showing volume and drop-off.

with user_journey as (
    select * from {{ ref('fct_user_journey') }}
),

funnel_steps as (
    select
        'page_view' as step,
        1 as step_order,
        count(*) as users_reached,
        count(*) as total_users
    from user_journey
    where reached_page_view

    union all

    select
        'signup' as step,
        2 as step_order,
        count(*) as users_reached,
        (select count(*) from user_journey where reached_page_view) as total_users
    from user_journey
    where reached_signup

    union all

    select
        'purchase' as step,
        3 as step_order,
        count(*) as users_reached,
        (select count(*) from user_journey where reached_page_view) as total_users
    from user_journey
    where reached_purchase
)

select
    step,
    step_order,
    users_reached,
    total_users,
    round(users_reached * 100.0 / nullif(total_users, 0), 2) as conversion_rate_pct,
    -- Drop-off from previous step
    lag(users_reached) over (order by step_order) as prev_step_users,
    round(
        users_reached * 100.0 / nullif(lag(users_reached) over (order by step_order), 0),
        2
    ) as step_conversion_rate_pct
from funnel_steps
order by step_order
