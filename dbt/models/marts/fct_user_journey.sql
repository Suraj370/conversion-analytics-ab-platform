-- Fact table: one row per user summarizing their funnel progression.
-- Used by both funnel metrics and experiment analysis.

with events as (
    select * from {{ ref('stg_events') }}
),

user_first_events as (
    select
        user_id,

        -- First timestamp per funnel stage
        min(case when event_type = 'page_view' then event_timestamp end) as first_page_view_at,
        min(case when event_type = 'signup' then event_timestamp end) as first_signup_at,
        min(case when event_type = 'purchase' then event_timestamp end) as first_purchase_at,

        -- Counts
        count(case when event_type = 'page_view' then 1 end) as page_view_count,
        count(case when event_type = 'click' then 1 end) as click_count,
        count(case when event_type = 'signup' then 1 end) as signup_count,
        count(case when event_type = 'purchase' then 1 end) as purchase_count,

        -- Purchase details (first purchase)
        min(case when event_type = 'purchase' then purchase_plan end) as first_purchase_plan,
        min(case when event_type = 'purchase' then purchase_amount end) as first_purchase_amount,

        -- Experiment assignment (if any)
        min(case when event_type = 'experiment_assignment' then event_timestamp end) as assigned_at,
        count(*) as total_events

    from events
    group by user_id
),

enriched as (
    select
        *,

        -- Boolean funnel flags
        first_page_view_at is not null as reached_page_view,
        first_signup_at is not null as reached_signup,
        first_purchase_at is not null as reached_purchase,

        -- Converted = completed purchase
        first_purchase_at is not null as is_converted

    from user_first_events
)

select * from enriched
