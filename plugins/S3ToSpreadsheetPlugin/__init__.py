from airflow.plugins_manager import AirflowPlugin
from S3ToSpreadsheetPlugin.operators.S3ToSpreadsheetOperator\
    import S3ToSpreadsheetOperator


class S3ToSpreadsheetPlugin(AirflowPlugin):
    name = "S3ToSpreadsheet"
    operators = [S3ToSpreadsheetOperator]
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
