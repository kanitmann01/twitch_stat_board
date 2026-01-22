with source as (
    
    -- "source" tells dbt to look at the 'twitch' source defined in sources.yml
    select * from {{ source('twitch', 'STREAM_LOGS') }}

),

cleaned as (

    select
        -- Extract fields from the VARIANT column (raw_data)
        -- Syntax: column_name:json_key::target_data_type
        
        raw_data:timestamp::timestamp as collected_at,
        raw_data:stream_id::string as stream_id,
        raw_data:user_name::string as streamer_name,
        raw_data:game_name::string as game_name,
        raw_data:viewer_count::integer as viewer_count,
        raw_data:language::string as language,
        raw_data:started_at::timestamp as stream_started_at,
        
        -- Calculated field: How long has the stream been live? (in minutes)
        datediff(minute, raw_data:started_at::timestamp, raw_data:timestamp::timestamp) as stream_duration_minutes,
        
        ingested_at

    from source

)

select * from cleaned
where viewer_count > 0
