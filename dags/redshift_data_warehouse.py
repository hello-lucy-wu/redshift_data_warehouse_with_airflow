from datetime import datetime, timedelta
import os
from airflow import DAG

from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)

from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'lucy',
    'start_date': datetime(2020, 3, 8),
    'depends_on_past': False, # The DAG does not have dependencies on past runs
    'email': ['lucy.wu000@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False, # Do not email on retry
    'retries': 3, # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5), # Retries happen every 5 minutes
}

with DAG('redshift_data_warehouse_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False # Catchup is turned off
        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events'
    )
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table'
    )
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table'
    )
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table'
    )
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table'
    )
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks'
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator
    


