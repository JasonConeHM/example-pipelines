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
from os.path import join

def join_on(str1, str2, on):
    """
    Joins two strings __on__ another string
    Ensuring that there is one and only one __on__ character at join point
    """
    join_point = str1[-1] + str2[0]
    if join_point.count(on) == 2:
        join_point = join_point[1] # remove duplicate join characters that may occur on join
    elif join_point.count(on) == 0:
        # recombine join_point with character inserted
        join_point = join_point[0] + on + join_point[-1]
    return str1[:-1] + join_point + str2[1:]


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
        
        # Use connection extra field as default headers
        # Override with any headers submitted directly to get_conn()
        self.headers = conn.extra_dejson()
        self.headers.update(headers)
        session.headers.update(headers)

        return session

class AstroRequestsToS3Operator(BaseOperator):
    """
    """
    def __init__(self, http_conn_id, request, s3_conn_id, s3_bucket, s3_key,headers=None, transform=None, *args, **kwargs):
        super(AstroRequestsToS3Operator, self).__init__(*args, **kwargs)

        self.http_conn_id = http_conn_id
        self.s3_conn_id = s3_conn_id
        self.request = request
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        
        self.headers = {} if headers is None else headers
        self.transform = lambda x: x if transform is None else transform



    def execute(self, context):
        # s3 = S3Hook(self.s3_conn_id).get_conn()
        astro = AstroRequestsHook(self.http_conn_id)
        session = astro.get_conn(self.headers)

        # Defaults
        passthrough = {
            'timeout': 30.000
        }

        passthrough.update(self.request['kwargs'])
        action = self.request['type'].lower()
       
        # Build URL using connection base_url if a fully qualified url was not provided in request
        if not passthrough['url'].startswith('http'):
            passthrough['url'] = join_on(astro.base_url, passthrough['url'], '/')

        return self.transform(getattr(session, action)(**unpack).json())


class AstroRequests(AirflowPlugin):
    name = "AstroRequests"
    hooks = [AstroRequestsHook]
    operators = [AstroRequestsToS3Operator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []