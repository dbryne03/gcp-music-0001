import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import config

with DAG(
    dag_id="music_pipeline",
    description="Monthly orchestrator — triggers all source pipelines then transform",
    schedule=config.SCHEDULE,
    start_date=config.START_DATE,
    catchup=False,
    tags=["music", "gcp", "monthly"],
) as dag:

    trigger_lastfm = TriggerDagRunOperator(
        task_id="trigger_lastfm",
        trigger_dag_id=config.DAG_LASTFM,
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=config.TRIGGER_POKE_INTERVAL,
    )

    trigger_musicbrainz = TriggerDagRunOperator(
        task_id="trigger_musicbrainz",
        trigger_dag_id=config.DAG_MUSICBRAINZ,
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=config.TRIGGER_POKE_INTERVAL,
    )

    trigger_spotify = TriggerDagRunOperator(
        task_id="trigger_spotify",
        trigger_dag_id=config.DAG_SPOTIFY,
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=config.TRIGGER_POKE_INTERVAL,
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id=config.DAG_TRANSFORM,
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=config.TRIGGER_POKE_INTERVAL,
    )

    [trigger_lastfm, trigger_musicbrainz, trigger_spotify] >> trigger_transform
