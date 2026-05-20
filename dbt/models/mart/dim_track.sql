with source as (
    -- Deduplicate: a track can appear once per matched chart artist;
    -- we keep one row per track_id, taking the highest popularity.
    select
        track_id,
        track_name,
        artists,
        album_name,
        track_genre,
        popularity,
        duration_ms,
        explicit,
        danceability,
        energy,
        key,
        loudness,
        mode,
        speechiness,
        acousticness,
        instrumentalness,
        liveness,
        valence,
        tempo,
        time_signature
    from {{ ref('int_track_enriched') }}
    qualify row_number() over (
        partition by track_id
        order by popularity desc, track_id
    ) = 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['track_id']) }} as track_key,
        track_id,
        track_name,
        artists,
        album_name,
        track_genre,
        popularity,
        duration_ms,
        explicit,
        danceability,
        energy,
        key,
        loudness,
        mode,
        speechiness,
        acousticness,
        instrumentalness,
        liveness,
        valence,
        tempo,
        time_signature
    from source
)

select * from final
