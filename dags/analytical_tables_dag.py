from datetime import datetime, timedelta
import os
from airflow import DAG

from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries


default_args = {
    'start_date':datetime.utcnow(),
    'depends_on_past': False, # The DAG does not have dependencies on past runs
    'email': [''],
    'email_on_failure': True,
    'email_on_retry': False, # Do not email on retry
    'retries': 3, # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5), # Retries happen every 5 minutes
    'catchup': False # Catchup is turned off
}

with DAG('analytical_tables_dag',
        default_args=default_args,
        description='Load and transform data in Redshift with Airflow',
        schedule_interval='0 * * * *'
) as dag:
    start_operator = DummyOperator(task_id='Begin_execution')

    create_staging_events_task = PostgresOperator(
        task_id='Create_staging_events',
        sql=SqlQueries.staging_events_table_create,
        postgres_conn_id="redshift"
    )
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_bucket='udacity-dend',
        s3_key='log_data',
        aws_iam_role_arn='arn:aws:iam::850186040772:role/dwhRole',
        s3_json_path='s3://udacity-dend/log_json_path.json',
        region='us-west-2',
        redshift_conn_id='redshift'
    )

    create_staging_songs_task = PostgresOperator(
        task_id='Create_staging_songs',
        sql=SqlQueries.staging_songs_table_create,
        postgres_conn_id="redshift"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song_data',
        aws_iam_role_arn='arn:aws:iam::850186040772:role/dwhRole',
        region='us-west-2',
        redshift_conn_id='redshift'
    )

    create_songplays_task = PostgresOperator(
        task_id='Create_songplays',
        sql=SqlQueries.songplays_table_create,
        postgres_conn_id="redshift"
    )

    load_songplays = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        insert_query=SqlQueries.songplays_table_insert,
        redshift_conn_id='redshift'
    )

    create_users_task = PostgresOperator(
        task_id='Create_users',
        sql=SqlQueries.users_table_create,
        postgres_conn_id="redshift"
    )

    load_users = LoadDimensionOperator(
        task_id='Load_users_dim_table',
        table='users',
        load_mode='truncate-insert',
        select_query=SqlQueries.users_table_select,
        redshift_conn_id='redshift'
    )

    create_songs_task = PostgresOperator(
        task_id='Create_songs',
        sql=SqlQueries.songs_table_create,
        postgres_conn_id="redshift"
    )

    load_songs = LoadDimensionOperator(
        task_id='Load_songs_dim_table',
        table='songs',
        load_mode='truncate-insert',
        select_query=SqlQueries.songs_table_select,
        redshift_conn_id='redshift'
    )

    create_artists_task = PostgresOperator(
        task_id='Create_artists',
        sql=SqlQueries.artists_table_create,
        postgres_conn_id="redshift"
    )

    load_artists = LoadDimensionOperator(
        task_id='Load_artists_dim_table',
        table='artists',
        load_mode='truncate-insert',
        select_query=SqlQueries.artists_table_select,
        redshift_conn_id='redshift'
    )

    create_time_task = PostgresOperator(
        task_id='Create_time',
        sql=SqlQueries.time_table_create,
        postgres_conn_id="redshift"
    )

    load_time = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='time',
        load_mode='truncate-insert',
        select_query=SqlQueries.time_table_select,
        redshift_conn_id='redshift'
    )

    check_data_quality = DataQualityOperator(
        task_id='Run_data_quality_checks',
        tables=['time', 'artists', 'songs', 'users', 'songplays'],
        redshift_conn_id='redshift'
    )

    end_operator = DummyOperator(task_id='End_excution')

    start_operator >> create_staging_songs_task >> stage_songs_to_redshift >> create_songplays_task >> load_songplays
    start_operator >> create_staging_events_task >> stage_events_to_redshift >> create_songplays_task >> load_songplays

    load_songplays >> create_users_task >> load_users >> check_data_quality 
    load_songplays >> create_songs_task >> load_songs >> check_data_quality 
    load_songplays >> create_artists_task >> load_artists >> check_data_quality 
    load_songplays >> create_time_task >> load_time >> check_data_quality 
    check_data_quality >> end_operator