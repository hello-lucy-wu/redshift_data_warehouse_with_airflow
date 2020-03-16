from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        copy {} from '{}' 
        iam_role '{}' json '{}' region '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 aws_iam_role_arn='',
                 s3_json_path='auto',
                 region='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_iam_role_arn = aws_iam_role_arn
        self.s3_json_path = s3_json_path
        self.region = region

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from {self.table} table")
        query = '''DELETE FROM {}'''.format(self.table) 
        redshift.run(query)

        self.log.info("Copying data from S3 to Redshift")
        s3_path = '''s3://{}/{}'''.format(self.s3_bucket, self.s3_key)
        
        query = StageToRedshiftOperator.copy_sql.format(
            self.table, 
            s3_path, 
            self.aws_iam_role_arn, 
            self.s3_json_path,
            self.region
        )

        redshift.run(query)
