from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                truncate_table=True,
                query="",
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = 
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_table = truncate_table
        self.query = query

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        redshift.run("delete from {}".format(self.table))
        redshift.run(f"Insert into {self.table} {self.query}")

