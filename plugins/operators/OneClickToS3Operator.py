from airflow.utils.decorators import apply_defaults
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow.hooks.S3_hook import S3Hook


def getParams(returnable):
    oc_conn = get_conn('astronomer-oneclick')
    if returnable == 'client_uuid':
        return oc_conn.extra_dejson.get('client_uuid')
    elif returnable == 'client_api_key':
        return oc_conn.extra_dejson.get('client_api_key')


@provide_session
def get_conn(conn_id, session=None):
    conn = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .first())
    return conn


class OneClickToS3Operator(BaseOperator):
    """
    One Click Retail to S3 Operator
    :param one_click_conn: The connection string for One Click API.
    :type one_click_conn: string
    :param start: The start date bound to request data for in 'YYYY-MM-DD'
        date format.
    :type start: string
    :param end: The end date bound to request data for in 'YYYY-MM-DD'
        date format.
    :type end: string
    :param weeks_back:The number of weeks back to bound a time parameter.
    :type weeks_back: string
    :param filter_id: The id of a self-defined filter kept in One Click.
    :type filter_id: string
    """

    template_fields = ['start', 'end', 's3_key']

    @apply_defaults
    def __init__(self,
                 one_click_conn,
                 start=None,
                 end=None,
                 weeks_back=None,
                 filter_id=None,
                 s3_conn_id=None,
                 s3_bucket=None,
                 s3_key=None,
                 **kwargs):
        super(OneClickToS3Operator, self).__init__(**kwargs)
        self.one_click_conn = one_click_conn
        self.start = start
        self.end = end
        self.weeks_back = weeks_back
        self.filter_id = filter_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        conn = HttpHook(method='GET', http_conn_id=self.one_click_conn)
        uuid = getParams('client_uuid')
        api_key = getParams('client_api_key')
        endpoint = ''.join([uuid, '/reports/export'])

        payload = {}
        payload['X-API-KEY'] = api_key

        if self.start:
            payload['start_date'] = self.start
        if self.end:
            payload['end_date'] = self.end
        if self.weeks_back:
            payload['weeks_back'] = self.weeks_back
        if self.filter_id:
            payload['filter_id'] = self.filter_id

        r = conn.run(endpoint, data=payload)
        rows = r.text.strip().split('\n\n')
        rows = [i for i in rows if i]

        if len(rows) > 2:
            raise Exception('One dataset accepted. {0} provided.'
                            .format(len(rows)))
        with open('temp.csv', 'w') as f:
            f.write(rows[1])
            output_s3 = S3Hook(s3_conn_id=self.s3_conn_id)
            output_s3.load_file(
                filename='temp.csv',
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            )
            output_s3.connection.close()
