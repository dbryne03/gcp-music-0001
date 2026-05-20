with source as (
    select * from {{ source('raw', 'spotify') }}
),

renamed as (
    select
        track_id,
        track_name,
        artists,
        album_name,
        track_genre,
        cast(popularity          as integer)  as popularity,
        cast(duration_ms         as integer)  as duration_ms,
        cast(explicit            as boolean)  as explicit,
        cast(danceability        as float64)  as danceability,
        cast(energy              as float64)  as energy,
        cast(key                 as integer)  as key,
        cast(loudness            as float64)  as loudness,
        cast(mode                as integer)  as mode,
        cast(speechiness         as float64)  as speechiness,
        cast(acousticness        as float64)  as acousticness,
        cast(instrumentalness    as float64)  as instrumentalness,
        cast(liveness            as float64)  as liveness,
        cast(valence             as float64)  as valence,
        cast(tempo               as float64)  as tempo,
        cast(time_signature      as integer)  as time_signature,
        _ingested_at
    from source
)

select * from renamed
