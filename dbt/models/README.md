# dbt/models

dbt model definitions for the music pipeline transformation layer.

---

## Lineage

```
raw.lastfm ──► stg_lastfm_charts ──┬──► int_artist_resolution ──► dim_artist ──┐
                                   │                                             │
raw.mb_dump ──► stg_mb_artists ────┘                                             ├──► fact_chart_position
                                   ┌──► int_track_enriched ────► dim_track ──────┘
raw.spotify ──► stg_spotify_tracks ┘
```

## Staging (`staging/`)

One model per raw source. Views in BigQuery — no storage cost.

| Model | Source | Key responsibilities |
|:---|:---|:---|
| `stg_lastfm_charts` | `raw.lastfm` | Casts types, generates `chart_key` surrogate on `artist_name + chart_week`, deduplicates on `(artist_name, chart_week)` keeping the most recent ingestion |
| `stg_mb_artists` | `raw.mb_dump` | Maps dump fields, parses partial date strings via `safe.parse_date`, validates `artist_type` |
| `stg_spotify_tracks` | `raw.spotify` | Range tests on all 0–1 audio features, `popularity` 0–100, `key` 0–11 |

## Intermediate (`intermediate/`)

Ephemeral — compiled inline by the models that reference them; no BigQuery objects created.

| Model | Description |
|:---|:---|
| `int_artist_resolution` | Cross-source artist matching. Primary path: MBID join. Fallback: normalised name join (`lower(regexp_replace(trim(name), r'[^a-z0-9 ]', ''))`) for artists without an MBID. `qualify row_number()` deduplicates one-to-many normalised name matches. `is_mb_verified` distinguishes both paths. |
| `int_track_enriched` | Joins Spotify tracks to charting artists via `INSTR(lower(artists), name_key) > 0` — case-insensitive substring match. One row per Spotify track per matched chart artist. |

## Mart (`mart/`)

Dimensional models — tables in BigQuery, consumed by Looker Studio and Google Sheets.

| Model | Grain | Description |
|:---|:---|:---|
| `dim_artist` | One row per artist | MBID as natural key, `artist_key` surrogate, full MusicBrainz metadata, `is_mb_verified` flag |
| `dim_track` | One row per `track_id` | Full Spotify audio feature set; deduplicates `int_track_enriched` on `track_id`, taking the highest-popularity row |
| `fact_chart_position` | One row per artist × chart week | Chart rank, listener and play counts; references `dim_artist` and `dim_track` |

## Testing

All models have schema tests defined in `*.yml` files alongside the SQL. Run with:

```bash
dbt test
```

Key test coverage:
- `not_null` and `unique` on all primary keys
- `accepted_values` on `artist_type` and `mode`
- `dbt_utils.accepted_range` for numeric range checks on audio features (0–1 scores, popularity, key, time signature)
- Source freshness thresholds declared in `sources.yml`
