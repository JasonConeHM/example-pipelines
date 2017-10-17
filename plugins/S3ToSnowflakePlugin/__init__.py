from airflow.plugins_manager import AirflowPlugin
from S3ToSnowflakePlugin.hooks.SnowflakeHook import SnowflakeHook
from S3ToSnowflakePlugin.operators.S3ToSnowflakeOperator import S3ToSnowflakeOperator

class S3ToSnowflakePlugin(AirflowPlugin):
    name = "S3ToSnowflake"
    hooks = [SnowflakeHook]
    operators = [S3ToSnowflakeOperator]
    # Leave in for explicitness
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
