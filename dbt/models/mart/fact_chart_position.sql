with charts as (
    select * from {{ ref('stg_lastfm_charts') }}
),

artists as (
    select artist_key, artist_mbid from {{ ref('dim_artist') }}
),

tracks as (
    select track_key, track_id from {{ ref('dim_track') }}
),

-- TODO: join tracks once track matching is implemented in int_track_enriched
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['charts.chart_key']) }} as chart_position_key,
        artists.artist_key,
        charts.chart_week,
        charts.rank,
        charts.listeners,
        charts.playcount
    from charts
    left join artists using (artist_mbid)
)

select * from final
