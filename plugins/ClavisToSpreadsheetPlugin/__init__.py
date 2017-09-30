from airflow.plugins_manager import AirflowPlugin
from OneClickToSpreadsheet.hooks.ClavisHook import ClavisHook
from ClavisToSpreadsheet.operators.ClavisToS3Operator\
    import ClavisToS3Operator
from OneClickToSpreadsheet.operators.S3ToSpreadsheetOperator\
    import S3ToSpreadsheetOperator


class ClavisToSpreadsheetPlugin(AirflowPlugin):
    name = "ClavisToSpreadsheet"
    operators = [ClavisToS3Operator, S3ToSpreadsheetOperator]
    hooks = [ClavisHook]
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
