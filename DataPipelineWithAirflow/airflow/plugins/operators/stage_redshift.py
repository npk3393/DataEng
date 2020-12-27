from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sqlquery = """ copy {} from '{}' access_key_id '{}' secret_access_key '{}' json '{}'; """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="",
                s3_key="",
                json="auto",
                ignore_headers=1
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.ignore_headers = ignore_headers

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        credentials = AwsHook(self.aws_credentials_id).get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('clearing data from redshift table')

        redshift.run("delete from {}".format(self.table))

        self.log.info('copying data from S3 to Redshift')

        s3_path = "s3://{}//{}".format(self.s3_bucket, self.s3_key.format(**context))

        query = StageToRedshiftOperator.copy_sqlquery.format(self.table,s3_path,credentials.access_key,credentials.secret_key,self.json)

        redshift.run(query)

        self.log.info('table {} copied successfully'.format(self.table))





