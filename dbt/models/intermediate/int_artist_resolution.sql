-- Resolves artist identities across Last.fm and MusicBrainz.
-- Primary resolution is by MBID. For artists without an MBID, or whose
-- MBID is not in the MusicBrainz dump, normalised name matching is used
-- as a fallback. A qualify window deduplicates the rare case where a
-- single normalised name matches multiple MusicBrainz records.

with lastfm_artists as (
    select distinct
        artist_mbid,
        artist_name,
        lower(regexp_replace(trim(artist_name), r'[^a-z0-9 ]', '')) as name_key
    from {{ ref('stg_lastfm_charts') }}
),

mb_artists as (
    select
        artist_mbid,
        artist_name,
        sort_name,
        country,
        artist_type,
        begin_date,
        end_date,
        lower(regexp_replace(trim(artist_name), r'[^a-z0-9 ]', '')) as name_key
    from {{ ref('stg_mb_artists') }}
),

resolved as (
    select
        coalesce(mb_mbid.artist_mbid, mb_name.artist_mbid, lfm.artist_mbid)
                                                    as artist_mbid,
        coalesce(mb_mbid.artist_name, mb_name.artist_name, lfm.artist_name)
                                                    as artist_name,
        coalesce(mb_mbid.sort_name,   mb_name.sort_name)   as sort_name,
        coalesce(mb_mbid.country,     mb_name.country)     as country,
        coalesce(mb_mbid.artist_type, mb_name.artist_type) as artist_type,
        coalesce(mb_mbid.begin_date,  mb_name.begin_date)  as begin_date,
        coalesce(mb_mbid.end_date,    mb_name.end_date)    as end_date,
        mb_mbid.artist_mbid is not null
            or mb_name.artist_mbid is not null              as is_mb_verified
    from lastfm_artists lfm

    -- Primary: direct MBID match
    left join mb_artists mb_mbid
        on  lfm.artist_mbid is not null
        and lfm.artist_mbid = mb_mbid.artist_mbid

    -- Fallback: normalised name match when MBID join produced nothing
    left join mb_artists mb_name
        on  mb_mbid.artist_mbid is null
        and lfm.name_key = mb_name.name_key
)

select * from resolved
qualify row_number() over (
    partition by artist_name
    order by artist_mbid nulls last
) = 1
