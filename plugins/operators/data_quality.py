from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables=[],
                 redshift_conn_id='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f'Checking if table {table} has rows')
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
 
            if records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table} has no records")

            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            

            