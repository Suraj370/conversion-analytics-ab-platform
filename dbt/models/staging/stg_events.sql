-- Staging model: clean and type-cast raw events
-- This is the single entry point for all downstream models

with source as (
    select * from {{ source('raw', 'raw_events') }}
),

staged as (
    select
        event_id,
        user_id,
        event_type,
        timestamp as event_timestamp,
        properties,
        ingested_at,

        -- Extract common properties for easy access downstream
        json_extract_string(properties, '$.page') as page,
        json_extract_string(properties, '$.target') as click_target,
        json_extract_string(properties, '$.source') as signup_source,
        json_extract_string(properties, '$.plan') as purchase_plan,
        try_cast(json_extract_string(properties, '$.amount') as double) as purchase_amount

    from source
)

select * from staged
