from airflow.plugins_manager import AirflowPlugin
from MySQLToRedshiftPlugin.operators.MySQLToS3Operator\
    import MySQLToS3Operator
from MySQLToRedshiftPlugin.operators.S3ToRedshiftOperator\
    import S3ToRedshiftOperator
from MySQLToRedshiftPlugin.hooks.AstroMySqlHook\
    import AstroMySqlHook


class MySQLToRedshiftPlugin(AirflowPlugin):
    name = "MySQLToRedshiftPlugin"
    operators = [MySQLToS3Operator, S3ToRedshiftOperator]
    # Leave in for explicitness
    hooks = [AstroMySqlHook]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
