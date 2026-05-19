-- Resolves artist identities across Last.fm and MusicBrainz.
-- MusicBrainz MBID is the canonical key; name normalisation is the fallback.
with lastfm as (
    select artist_mbid, artist_name from {{ ref('stg_lastfm_charts') }}
    group by 1, 2
),

mb as (
    select artist_mbid, artist_name, sort_name, country, artist_type, begin_date, end_date
    from {{ ref('stg_mb_artists') }}
),

-- TODO: implement name normalisation fallback for records without MBID
resolved as (
    select
        coalesce(mb.artist_mbid, lastfm.artist_mbid)    as artist_mbid,
        coalesce(mb.artist_name, lastfm.artist_name)    as artist_name,
        mb.sort_name,
        mb.country,
        mb.artist_type,
        mb.begin_date,
        mb.end_date,
        mb.artist_mbid is not null                      as is_mb_verified
    from lastfm
    left join mb using (artist_mbid)
)

select * from resolved
