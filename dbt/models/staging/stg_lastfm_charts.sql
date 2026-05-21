with source as (
    select * from {{ source('raw', 'lastfm') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['artist_name', 'chart_week']) }} as chart_key,
        artist_mbid,
        artist_name,
        cast(chart_week as date)        as chart_week,
        cast(rank as integer)           as rank,
        cast(listeners as integer)      as listeners,
        cast(playcount as integer)      as playcount,
        _ingested_at
    from source
    qualify row_number() over (
        partition by artist_name, chart_week
        order by _ingested_at desc
    ) = 1
)

select * from renamed
