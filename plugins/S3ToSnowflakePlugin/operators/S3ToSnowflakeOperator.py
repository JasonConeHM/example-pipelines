from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from ..hooks.SnowflakeHook import SnowflakeHook

class S3ToSnowflakeOperator(BaseOperator):
    base_copy = """
        COPY INTO {snowflake_destination}
        FROM s3://{s3_bucket}/{s3_key} CREDENTIALS=(AWS_KEY_ID='{aws_access_key_id}' AWS_SECRET_KEY='{aws_secret_access_key}')
        FILE_FORMAT=(TYPE='{file_format_name}')
    """

    def __init__(self,
                 s3_bucket,
                 s3_key,
                 database,
                 schema,
                 table,
                 file_format_name,
                 s3_conn_id,
                 snowflake_conn_id,
                 *args, **kwargs):

        super(S3ToSnowflakeOperator, self).__init__(*args, **kwargs)
        self.copy = self.base_copy
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.database = database
        self.schema = schema
        self.table = table
        self.file_format_name = file_format_name or 'JSON'
        self.s3_conn_id = s3_conn_id
        self.snowflake_conn_id = snowflake_conn_id

    def build_copy(self):
        a_key, s_key = S3Hook(s3_conn_id=self.s3_conn_id).get_credentials()
        snowflake_destination = ''

        if self.database:
            snowflake_destination += '{}.'.format(self.database)

        if self.schema:
            snowflake_destination += '{}.'.format(self.schema)

        snowflake_destination += self.table

        fmt_str = {
            'snowflake_destination': snowflake_destination,
            's3_bucket': self.s3_bucket,
            's3_key': self.s3_key,
            'aws_access_key_id': a_key,
            'aws_secret_access_key': s_key,
            'file_format_name': self.file_format_name
        }

        return self.copy.format(**fmt_str)

    def execute(self, context):
        sf_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id).get_conn()
        sql = self.build_copy()
        sf_hook.cursor().execute(sql)