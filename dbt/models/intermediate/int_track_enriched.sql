-- Joins Spotify audio features to Last.fm chart play data.
-- TODO: implement track matching logic (track name + artist name normalisation)
with charts as (
    select artist_mbid, chart_week, rank, listeners, playcount
    from {{ ref('stg_lastfm_charts') }}
),

spotify as (
    select
        track_id, track_name, artists,
        danceability, energy, tempo, valence,
        loudness, speechiness, acousticness,
        instrumentalness, liveness, popularity
    from {{ ref('stg_spotify_tracks') }}
),

-- TODO: join on normalised artist name and track name
enriched as (
    select
        spotify.track_id,
        spotify.track_name,
        spotify.artists,
        spotify.danceability,
        spotify.energy,
        spotify.tempo,
        spotify.valence,
        spotify.popularity,
        charts.artist_mbid,
        charts.chart_week,
        charts.rank,
        charts.listeners,
        charts.playcount
    from spotify
    left join charts
        on true -- TODO: replace with proper join condition
)

select * from enriched
