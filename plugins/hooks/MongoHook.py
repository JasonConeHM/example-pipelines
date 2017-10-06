"""
TEST
"""
from ssl import CERT_NONE
from pymongo import MongoClient

from airflow.hooks.base_hook import BaseHook

class MongoHook(BaseHook):
    """
    PyMongo Wrapper to Interact With Mongo Database

    Mongo Connection Documentation
    https://docs.mongodb.com/manual/reference/connection-string/index.html

    You can specify connection string options in extra field of your connection
    https://docs.mongodb.com/manual/reference/connection-string/index.html#connection-string-options
    ex.
        {replicaSet: test, ssl: True, connectTimeoutMS: 30000}
    """
    conn_type = 'MongoDb'

    def __init__(self, mongo_conn_id='mongo_default'):
        self.mongo_conn_id = mongo_conn_id

    def get_conn(self):
        """
        Fetches PyMongo Client
        """
        conn = self.get_connection(self.mongo_conn_id)

        uri = 'mongodb://{creds}{host}:{port}/{database}?'.format(
            creds='{}:{}@'.format(conn.login, conn.password) if conn.login is not None else '',
            host=conn.host,
            port=conn.port,
            database=conn.schema
        )

        # Mongo Connection Options dict that is unpacked when passed to MongoClient
        options = conn.extra_dejson

        # If we are using SSL disable requiring certs from specific hostname
        if options.get('ssl', False):
            options.update({'ssl_cert_reqs':CERT_NONE})

        return MongoClient(uri, **options)
