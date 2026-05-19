with source as (
    select * from {{ source('raw', 'spotify') }}
),

renamed as (
    select
        -- TODO: confirm column names from Parquet schema
        track_id,
        track_name,
        artists,
        cast(danceability as float64)   as danceability,
        cast(energy as float64)         as energy,
        cast(tempo as float64)          as tempo,
        cast(valence as float64)        as valence,
        cast(loudness as float64)       as loudness,
        cast(speechiness as float64)    as speechiness,
        cast(acousticness as float64)   as acousticness,
        cast(instrumentalness as float64) as instrumentalness,
        cast(liveness as float64)       as liveness,
        cast(popularity as integer)     as popularity,
        _ingested_at
    from source
)

select * from renamed
