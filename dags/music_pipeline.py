from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="music_pipeline",
    description="Monthly orchestrator — triggers all source pipelines then transform",
    schedule="0 0 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["music", "gcp", "monthly"],
) as dag:

    trigger_lastfm = TriggerDagRunOperator(
        task_id="trigger_lastfm",
        trigger_dag_id="lastfm_pipeline",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
    )

    trigger_musicbrainz = TriggerDagRunOperator(
        task_id="trigger_musicbrainz",
        trigger_dag_id="musicbrainz_pipeline",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
    )

    trigger_spotify = TriggerDagRunOperator(
        task_id="trigger_spotify",
        trigger_dag_id="spotify_pipeline",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="music_transform",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
    )

    [trigger_lastfm, trigger_musicbrainz, trigger_spotify] >> trigger_transform
