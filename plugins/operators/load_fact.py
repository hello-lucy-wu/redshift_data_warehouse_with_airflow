from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
        insert_query='',
        load_mode='append',
        redshift_conn_id='',
        table='',
        *args, **kwargs
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.insert_query = insert_query
        self.redshift_conn_id = redshift_conn_id
        self.load_mode = load_mode
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.load_mode is 'truncate-insert':
            self.log.info("Clearing data from {self.table} table")
            query = '''DELETE FROM {}'''.format(self.table) 
            redshift.run(query)

        redshift.run(self.insert_query)