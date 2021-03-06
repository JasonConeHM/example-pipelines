from airflow.plugins_manager import AirflowPlugin
from OneClickToSpreadsheetPlugin.operators.OneClickToS3Operator\
    import OneClickToS3Operator
from OneClickToSpreadsheetPlugin.operators.S3ToSpreadsheetOperator\
    import S3ToSpreadsheetOperator


class OneClickPlugin(AirflowPlugin):
    name = "OneClickToSpreadsheet"
    operators = [OneClickToS3Operator, S3ToSpreadsheetOperator]
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
