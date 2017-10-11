"""
An extensible requests plugin for Airflow
"""
__author__ = 'astronomerio'

# TODO Ratelimiting
# TODO Pagination
#   TODO Move pagination into a class with each method being a different type of pagination?
#   TODO It's more of an execution class with depagination methods?
# TODO Support various types of auth

from json import dumps
import requests

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.S3_hook import S3Hook

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


def fn_from_dict(dictionary):
    """
    Create a filename consisting of key/values from a dict
    """
    file_name = ''
    for key in dictionary:
        file_name += '{}_{}'.format(str(key), str(dictionary[key]))

    return file_name

def autopilot_depagify(request_method, endpoint, request_kwargs, recs=None):
    '''
    Written as a closure so we can reuse our request Session()
    connection passed by http_conn
    '''
    resp = request_method(endpoint, **request_kwargs).json()
    recs = [] if recs is None else recs
    # Append New Records
    if isinstance(resp['contacts'], list):
        recs += resp['contacts']
    else:
        recs.append(resp['contacts'])
    # Check if we need to de-paginate
    if resp.get('bookmark', False):
        next_endpoint = endpoint + '/' + resp['bookmark']
        autopilot_depagify(request_method, next_endpoint, request_kwargs, recs)
    else:
        return recs

def default_depagify(request_method, endpoint, request_kwargs):
    """
    Default execution method that assumes no pagination
    """
    return request_method(endpoint, **request_kwargs).json()


class AstroRequestHook(BaseHook):
    """
    Provides a requests Session()
    """

    conn_type = 'HTTP'

    def __init__(self, http_conn_id='http_default'):
        super().__init__(http_conn_id)
        self.http_conn_id = http_conn_id
        self.base_url = None

    # headers is required to make it required
    def get_conn(self, headers=None):
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
        connection_headers = conn.extra_dejson
        session.headers.update(connection_headers)

        return session

class AstroRequestsBaseOperator(BaseOperator):
    """
    Another base class that extends BaseOperator while providing
    more functionality for AstroRequest classes
    """
    def __init__(self,
                 http_conn_id,
                 request_definition,
                 headers=None,
                 src_transform=lambda x: x,
                 dst_transform=lambda x: x,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)


        self.http_conn_id = http_conn_id
        self.base_url = AstroRequestHook().get_connection(self.http_conn_id).host
        # Gets passed through to the request
        self.headers = {} if headers is None else headers
        # Pre and Post Request Transforms
        self.src_transform = src_transform
        self.dst_transform = dst_transform

        # ---------------------------
        # Parse out request information from request_definition
        # ---------------------------

        # Default request parameters
        self.request_kwargs = {
            'timeout': 30.000
        }
        self.request_kwargs.update(request_definition['kwargs']) # Override defaults

        # Create endpoint url
        params_url = self.request_kwargs.pop('url')
        self.url = (params_url if params_url.startswith('http')
                    else join_on(self.base_url, params_url, '/'))
        # Grab request type to dynamically build request
        self.request_type = request_definition['type'].lower()

    def execute_request(self, endpoint, request_kwargs):
        """
        Executes a request using the provided Session() method and endpoint.
        For anything more than that it unpacks the request_kwargs in the method
        """
        http_conn = AstroRequestHook(self.http_conn_id).get_conn(self.headers)
        fetched_method = getattr(http_conn, self.request_type)

        default_depagify(fetched_method, endpoint, request_kwargs)


class ToXComOperator(AstroRequestsBaseOperator):
    """
    Extends AstroRequestsBaseOperator and allows writing to XCOM for 
    logging purposes or chaining of another request
    """
    def execute(self, context):
        resp = self.execute_request(self.url, self.request_kwargs)

        return self.dst_transform(resp)

class ToS3Operator(AstroRequestsBaseOperator):
    """
    Extends AstroRequestsBaseOperator and allows for writing to S3 for
    logging purposes or chaining of another request with FromS3ToS3Operator
    """
    template_fields = ['s3_key']

    def __init__(self, http_conn_id, s3_conn_id, s3_bucket, s3_key, request_definition, 
                 xcom_info=None,
                 headers=None,
                 dst_transform=lambda x: x,
                 src_transform=lambda x: x,
                 *args, **kwargs):
        super().__init__(
            http_conn_id,
            request_definition=request_definition,
            headers=headers,
            src_transform=src_transform,
            dst_tranform=dst_transform,
            *args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        s3_conn = S3Hook(self.s3_conn_id)
              
        resp = self.execute_request(self.url, self.request_kwargs)
        # Apply transformation then dump back to string
        transformed_str = dumps(self.dst_transform(resp))
        
        s3_conn.load_string(transformed_str,
                            self.s3_key,
                            bucket_name=self.s3_bucket
                            )

class FromXcomToS3Operator(AstroRequestsBaseOperator):
    """
    Processes a JSON object from XCOM, transforms the object and uses it as the
    params on a request
    """

    template_fields = ['s3_key']

    def __init__(self, http_conn_id, s3_conn_id, s3_bucket, s3_key,
                 xcom_task_id,
                 request_definition,
                 dst_transform,
                 src_transform,
                 *args, **kwargs):

        super().__init__(http_conn_id,
                         request_definition=request_definition,
                         src_transform=src_transform,
                         dst_transform=dst_transform,
                         *args, **kwargs)

        self.xcom_task_id = xcom_task_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        prev_requests = context['ti'].xcom_pull(self.xcom_task_id)
        s3_conn = S3Hook(self.s3_conn_id)

        if isinstance(prev_requests, dict):
            req_params = self.src_transform(prev_requests)
            self.request_kwargs.update({'params': req_params})

            resp = self.execute_request(self.url, self.request_kwargs)
            # Apply transformation then dump back to string
            transformed_str = dumps(self.dst_transform(resp))

            s3_conn.load_string(transformed_str,
                                self.s3_key,
                                bucket_name=self.s3_bucket
                               )

        if isinstance(prev_requests, list):
            # Use Source to Implement New Params
            for result in prev_requests:
                req_params = self.src_transform(result)
                self.request_kwargs.update({'params': req_params})

                resp = self.execute_request(self.url, self.request_kwargs)
                # Apply transformation then dump back to string
                transformed_str = dumps(self.dst_transform(resp))

                new_s3_key = '{}-{}'.format(self.s3_key,
                                            fn_from_dict(self.request_kwargs['params']))

                s3_conn.load_string(transformed_str,
                                    new_s3_key,
                                    bucket_name=self.s3_bucket
                                   )

class AstroRequests(AirflowPlugin):
    name = "AstroRequests"
    hooks = [AstroRequestHook]
    operators = [ToXComOperator, ToS3Operator, FromXcomToS3Operator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
