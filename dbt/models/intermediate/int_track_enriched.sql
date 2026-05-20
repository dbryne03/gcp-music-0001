-- Joins Spotify track audio features to Last.fm charting artists.
-- Matching is on normalised artist name: the chart artist name is checked
-- against the Spotify 'artists' field using case-insensitive substring
-- search. One row per Spotify track per matching chart artist.
-- Provides the source data for dim_track.

with chart_artists as (
    select distinct
        artist_mbid,
        artist_name,
        lower(regexp_replace(trim(artist_name), r'[^a-z0-9 ]', '')) as name_key
    from {{ ref('stg_lastfm_charts') }}
),

spotify_tracks as (
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
    from {{ ref('stg_spotify_tracks') }}
),

enriched as (
    select
        sp.track_id,
        sp.track_name,
        sp.artists,
        sp.album_name,
        sp.track_genre,
        sp.popularity,
        sp.duration_ms,
        sp.explicit,
        sp.danceability,
        sp.energy,
        sp.key,
        sp.loudness,
        sp.mode,
        sp.speechiness,
        sp.acousticness,
        sp.instrumentalness,
        sp.liveness,
        sp.valence,
        sp.tempo,
        sp.time_signature,
        ca.artist_mbid,
        ca.artist_name                              as chart_artist_name
    from spotify_tracks sp
    inner join chart_artists ca
        on contains_substr(sp.artists, ca.name_key)
)

select * from enriched
