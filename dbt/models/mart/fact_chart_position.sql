-- Grain: one row per artist per chart week.
-- Joins to dim_artist for the dimensional artist key.
-- Audio feature analysis is available via dim_artist → dim_track
-- using the artist_name relationship in int_track_enriched.

with charts as (
    select * from {{ ref('stg_lastfm_charts') }}
),

artists as (
    select artist_key, artist_name from {{ ref('dim_artist') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['charts.chart_key']) }}
                                    as chart_position_key,
        artists.artist_key,
        charts.artist_mbid,
        charts.artist_name,
        charts.chart_week,
        charts.rank,
        charts.listeners,
        charts.playcount
    from charts
    left join artists on charts.artist_name = artists.artist_name
)

select * from final
