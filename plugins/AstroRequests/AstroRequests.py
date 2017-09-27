"""
An extensible requests plugin for Airflow
"""
__author__ = 'astronomerio'

# TODO Ratelimiting
# TODO XCOM
# TODO XCOM Request chaining
# TODO Streaming
# TODO Basic Login to Retrieve Token
# TODO timeouts http://docs.python-requests.org/en/master/user/quickstart/#timeouts
# TODO Connection Pooling
# TODO Session
#   TODO Will this create a new session for each dag run?
#   TODO Is this another place we can leverage the DB to store session auth information?
import requests
from airflow.hooks import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.http_hook import HttpHook


class AstroRequestsHook(BaseHook):
    """
    """
    conn_type = 'HTTP'

    def __init__(self, http_conn_id='http_default'):
        self.http_conn_id = http_conn_id
    
    # headers is required to make it required
    def get_conn(self, headers):
        """
        Returns http session for use with requests
        """
        conn = self.get_connection(self.http_conn_id)
        session = requests.Session()
        self.base_url = conn.host
        if not self.base_url.startswith('http'):
            self.base_url = 'http://' + self.base_url

        if conn.port:
            self.base_url = self.base_url + ":" + str(conn.port) + "/"
        if conn.login:
            session.auth = (conn.login, conn.password)
        if headers:
            session.headers.update(headers)

        return session

class AstroRequestsToS3Operator(BaseOperator):
    """
    """
    def __init__(self, http_conn_id, reqs, s3_conn_id, s3_bucket, s3_key, *args, **kwargs):
        super(AstroRequestsToS3Operator, self).__init__(*args, **kwargs)

        self.http_conn_id = http_conn_id
        self.s3_conn_id = s3_conn_id
        # TODO reqs array validation
        self.reqs = reqs
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        self.headers = kwargs.pop('headers', {})


    def execute(self, context):
        s3 = S3Hook(self.s3_conn_id).get_conn()
        session = AstroRequestsHook(self.http_conn_id).get_conn(self.headers)

        action = req['type'].lower()
        unpack = req['kwargs']
    
        return getattr(session, action)(**unpack).json()




class AstroRequests(AirflowPlugin):
    name = "AstroRequests"
    hooks = [AstroRequestsHook]
    operators = [AstroRequestsToS3Operator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []