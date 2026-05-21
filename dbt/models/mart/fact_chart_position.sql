-- Grain: one row per artist per chart week.
-- Resolves artist_key by MBID first (precise), name fallback (for
-- Last.fm artists whose name was normalised by MusicBrainz in
-- int_artist_resolution).

with charts as (
    select * from {{ ref('stg_lastfm_charts') }}
),

artists as (
    select artist_key, artist_mbid, artist_name from {{ ref('dim_artist') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['charts.chart_key']) }}
                                    as chart_position_key,
        coalesce(
            by_mbid.artist_key,
            by_name.artist_key
        )                           as artist_key,
        charts.artist_mbid,
        charts.artist_name,
        charts.chart_week,
        charts.rank,
        charts.listeners,
        charts.playcount
    from charts

    -- Primary: join by MBID when the chart record carries one
    left join artists by_mbid
        on  charts.artist_mbid is not null
        and charts.artist_mbid = by_mbid.artist_mbid

    -- Fallback: join by name for artists without an MBID
    left join artists by_name
        on  by_mbid.artist_key is null
        and charts.artist_name = by_name.artist_name
)

select * from final
