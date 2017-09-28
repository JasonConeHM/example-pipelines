"""
An extensible requests plugin for Airflow
"""
__author__ = 'astronomerio'

# TODO Ratelimiting
# TODO XCOM
# TODO XCOM Request chaining
# TODO Streaming
# TODO Basic Login to Retrieve Token
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
        self.headers = {}

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

class AstroRequestsBaseOperator(BaseOperator):
    """
    """
    def __init__(self, http_conn_id, request, headers=None, transform=None, *args, **kwargs):
        super(AstroRequestsBaseOperator, self).__init__(*args, **kwargs)

        # Connetion information
        self.http_conn_id = http_conn_id
        self.base_url = AstroRequestsHook(self.http_conn_id).base_url
        # Custom Tranform
        self.transform = lambda x: x if transform is None else transform
        # Headers to passthrough
        self.headers = {} if headers is None else headers
        # Default request parameters
        self.params = {
            'timeout': 30.000
        }
        self.params.update(request['kwargs']) # Override defaults

        self.url = (self.params['url'] if self.params['url'].startswith('http') 
                    else join_on(self.base_url, self.params['url'], '/'))

        self.request_type = request['type'].lower()

class AstroRequestsToXComOperator(AstroRequestsBaseOperator):
    """
    """
    def __init__(self, http_conn_id, request, headers=None, transform=None, *args, **kwargs):
        super(AstroRequestsToXComOperator, self).__init__(http_conn_id, request, headers, transform, *args, **kwargs)

    def execute(self, context):
        http_conn = AstroRequestsHook(self.http_conn_id).get_conn(self.headers)
        return self.transform(getattr(http_conn, self.request_type)(self.url, **self.params))

class AstroRequests(AirflowPlugin):
    name = "AstroRequests"
    hooks = [AstroRequestsHook]
    operators = [AstroRequestsToXComOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []