"""
An extensible requests plugin for Airflow
"""
__author__ = 'astronomerio'

# TODO Ratelimiting

import requests
from json import dumps
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.http_hook import HttpHook

def join_on(str1, str2, char):
    """
    Joins str1 and str2 on a char
    Ensure that there is one and only one char at join point
    """
    try:
        join_point = str1[-1] + str2[0]
    except IndexError:
        return str1 + char + str2
    if join_point.count(char) == 2:
        join_point = join_point[1] # remove duplicate join characters that may occur on join
    elif join_point.count(char) == 0:
        # recombine join_point with character inserted
        join_point = join_point[0] + char + join_point[-1]
    return str1[:-1] + join_point + str2[1:]


class AstroRequestsHook(BaseHook):
    """
    Provides a requests Session()
    """
    # TODO Support All types of Autopilot API Login

    conn_type = 'HTTP'

    def __init__(self, http_conn_id='http_default'):
        self.http_conn_id = http_conn_id
        self.headers = {}
        self.base_url = ''

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
        self.headers = conn.extra_dejson
        self.headers.update(headers)
        session.headers.update(headers)

        return session

class AstroRequestsBaseOperator(BaseOperator):
    """
    Base class that handles configuration and some defaults
    """
    def __init__(self, http_conn_id, request, headers=None, func=lambda x: x, *args, **kwargs):
        super(AstroRequestsBaseOperator, self).__init__(*args, **kwargs)

        # Connetion information
        self.http_conn_id = http_conn_id
        self.base_url = AstroRequestsHook().get_connection(self.http_conn_id).host

        # Custom Tranform
        self.func = func
        # Headers to passthrough
        self.headers = {} if headers is None else headers
        # Default request parameters
        self.params = {
            'timeout': 30.000
        }
        self.params.update(request['kwargs']) # Override defaults

        params_url = self.params.pop('url')
        self.url = (params_url if params_url.startswith('http')
                    else join_on(self.base_url, params_url, '/'))

        self.request_type = request['type'].lower()

class ToXComOperator(AstroRequestsBaseOperator):
    """
    Write a response to XCom
    """
    def execute(self, context):
        http_conn = AstroRequestsHook(self.http_conn_id).get_conn(self.headers)

        action = getattr(http_conn, self.request_type)
        json_response = action(self.url, **self.params).json()
        
        return self.func(json_response)

class ToS3Operator(AstroRequestsBaseOperator):
    """
    Write a response to an S3 bucket
    """
    template_fields = ['s3_key']

    # TODO Take templated parameters from an AstroRequestToS3Operator

    def __init__(self, http_conn_id, s3_conn_id, s3_bucket, s3_key, request, xcom_info=None,
                 headers=None, func=None,
                 *args, **kwargs):
        super(ToS3Operator, self).__init__(
            http_conn_id,
            request,
            headers,
            func,
            *args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        http_conn = AstroRequestsHook(self.http_conn_id).get_conn(self.headers)
        s3_conn = S3Hook(self.s3_conn_id)

        action = getattr(http_conn, self.request_type)
        json_response = action(self.url, **self.params).json()
        funced_str_resp = dumps(self.func(json_response))
        s3_conn.load_string(funced_str_resp,
                            self.s3_key,
                            bucket_name=self.s3_bucket
                           )


class FromXComOperatorToS3(AstroRequestsBaseOperator):
    # TODO Refactor for multiple inhertiance
    def __init__(self, http_conn_id, s3_conn_id, s3_bucket, s3_key, chained_task, param_func, request):
        super(FromXComOperatorToS3, self).__init__(http_conn_id, request, func=param_func,
                                                   *args, **kwargs)
        self.chained_task = chained_task
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        x = context['ti'].xcom_pull(chain_task_id)

        http_conn = AstroRequestsHook(self.http_conn_id).get_conn(self.headers)
        action = getattr(http_conn, self.request_type)
        json_response = action(self.url, **self.params).json()

        if x is None:
            
        if x isinstance(x, dict):
            fetched_params = func(x)
            self.params.update(fetched_params)
            return action(self.url, **self.params).json()


        if x isinstance(x, list):
            

    

class AstroRequests(AirflowPlugin):
    name = "AstroRequests"
    hooks = [AstroRequestsHook]
    operators = [AstroRequestsToXComOperator, AstroRequestsToS3Operator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []