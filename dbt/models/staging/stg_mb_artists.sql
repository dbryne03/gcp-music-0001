with source as (
    select * from {{ source('raw', 'mb_dump') }}
),

renamed as (
    select
        -- TODO: map MusicBrainz JSON fields to typed columns
        -- key fields: mbid, name, sort_name, country, begin_date, end_date, type
        cast(null as string)    as artist_mbid,
        cast(null as string)    as artist_name,
        cast(null as string)    as sort_name,
        cast(null as string)    as country,
        cast(null as string)    as artist_type,
        cast(null as date)      as begin_date,
        cast(null as date)      as end_date,
        _ingested_at
    from source
)

select * from renamed
