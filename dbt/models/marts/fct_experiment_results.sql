-- Experiment results: per-variant metrics for A/B analysis.
-- Joins experiment assignments with conversion outcomes.
-- This model will be populated once experiment assignment logic is added (Commit 7).

with assignments as (
    select
        event_id,
        user_id,
        event_timestamp as assigned_at,
        json_extract_string(properties, '$.experiment_id') as experiment_id,
        json_extract_string(properties, '$.variant') as variant
    from {{ ref('stg_events') }}
    where event_type = 'experiment_assignment'
),

user_journey as (
    select * from {{ ref('fct_user_journey') }}
),

experiment_users as (
    select
        a.experiment_id,
        a.variant,
        a.user_id,
        a.assigned_at,
        coalesce(uj.is_converted, false) as is_converted,
        uj.first_purchase_amount,
        uj.first_purchase_at
    from assignments a
    left join user_journey uj on a.user_id = uj.user_id
),

variant_metrics as (
    select
        experiment_id,
        variant,
        count(*) as users,
        sum(case when is_converted then 1 else 0 end) as conversions,
        round(
            sum(case when is_converted then 1 else 0 end) * 1.0 / nullif(count(*), 0),
            6
        ) as conversion_rate,
        coalesce(avg(case when is_converted then first_purchase_amount end), 0) as avg_revenue
    from experiment_users
    group by experiment_id, variant
)

select * from variant_metrics
