"""
"""
from airflow.plugins_manager import AirflowPlugin
from MongoToRedshiftPlugin.hooks.MongoHook import MongoHook
from MongoToRedshiftPlugin.operators.MongoToS3Operator import MongoToS3BaseOperator
from MongoToRedshiftPlugin.operators.S3ToRedshiftOperator import S3ToRedshiftOperator

class MongoToRedshiftPlugin(AirflowPlugin):
    name = "MongoToRedshift"
    hooks = [MongoHook]
    operators = [MongoToS3BaseOperator, S3ToRedshiftOperator]
    # Leave in for explicitness
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []