with source as (
    select * from {{ ref('int_artist_resolution') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['artist_mbid']) }} as artist_key,
        artist_mbid,
        artist_name,
        sort_name,
        country,
        artist_type,
        begin_date,
        end_date,
        is_mb_verified
    from source
)

select * from final
