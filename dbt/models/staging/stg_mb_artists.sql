with source as (
    select * from {{ source('raw', 'mb_dump') }}
),

renamed as (
    select
        id                                          as artist_mbid,
        name                                        as artist_name,
        sort_name,
        country,
        type                                        as artist_type,
        safe.parse_date('%Y-%m-%d', begin_date)     as begin_date,
        safe.parse_date('%Y-%m-%d', end_date)       as end_date,
        ended,
        genres,
        _ingested_at
    from source
)

select * from renamed
