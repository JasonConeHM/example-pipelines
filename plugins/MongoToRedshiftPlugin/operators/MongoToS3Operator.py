"""
"""
import json
from bson import json_util

from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from ..hooks.MongoHook import MongoHook


class MongoToS3BaseOperator(BaseOperator):
    """
    Mongo -> S3
	A more specific baseOperator meant to move data from mongo via pymongo to s3 via boto

	things to note
		.execute() is written to depend on .transform()
		.transform() is meant to be extended by child classes to perform transformations unique
		    to those operators needs
    """

    template_fields = ['s3_key', 'mongo_query']

    def __init__(self,
                 mongo_conn_id,
                 s3_conn_id,
                 mongo_collection,
                 mongo_database,
                 mongo_query,
                 s3_bucket,
                 s3_key,
                 *args, **kwargs):
        super(MongoToS3BaseOperator, self).__init__(*args, **kwargs)
        # Conn Ids
        self.mongo_conn_id = mongo_conn_id
        self.s3_conn_id = s3_conn_id
        # Mongo Query Settings
        self.mongo_db = mongo_database
        self.mongo_collection = mongo_collection
        # Grab query and determine if we need to run an aggregate pipeline
        self.mongo_query = mongo_query
        self.is_pipeline = True if isinstance(self.mongo_query, list) else False

        # S3 Settings
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        # KWARGS
        self.replace = kwargs.pop('replace', False)

    def execute(self, context):
        """
        Executed by task_instance at runtime
        """
        mongo_conn = MongoHook(self.mongo_conn_id).get_conn()
        s3_conn = S3Hook(self.s3_conn_id)

        # Grab collection and execute query according to whether or not it is a pipeline
        collection = mongo_conn.get_database(self.mongo_db).get_collection(self.mongo_collection)
        results = collection.aggregate(self.mongo_query) if self.is_pipeline else collection.find(self.mongo_query)
        
        # Performs transform then stringifies the docs results into json format
        docs_str = self._stringify(self.transform(results))
        
        #if len(docs_str) > 0:
        s3_conn.load_string(docs_str, self.s3_key, bucket_name=self.s3_bucket, replace=self.replace)

        return {'rendered_templates': {'mongo_query': self.mongo_query, 's3_key': self.s3_key}}

    def _stringify(self, iter, joinable='\n'):
        """
        Takes an interable (pymongo Cursor or Array) containing dictionaries and
        returns a stringified version using python join
        """
        return joinable.join([json.dumps(doc, default=json_util.default) for doc in iter])

    def transform(self, docs):
        """
        Processes pyMongo cursor and returns single array with each element being
                a JSON serializable dictionary

        MongoToS3BaseOperator.transform() assumes no processing is needed
        ie. docs is a pyMongo cursor of documents and cursor just needs to be
            converted into an array.
        """
        return [doc for doc in docs]
