with source as (
    select distinct
        track_id,
        track_name,
        artists,
        danceability,
        energy,
        tempo,
        valence,
        popularity
    from {{ ref('int_track_enriched') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['track_id']) }} as track_key,
        track_id,
        track_name,
        artists,
        danceability,
        energy,
        tempo,
        valence,
        popularity
    from source
)

select * from final
