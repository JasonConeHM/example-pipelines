from airflow.plugins_manager import AirflowPlugin
from SalesforceToS3Plugin.operators.SalesforceToS3Operator \
    import SalesforceToS3Operator


class SalesforceToS3Plugin(AirflowPlugin):
    name = "SalesforceToS3Plugin"
    operators = [SalesforceToS3Operator]
    # Leave in for explicitness
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
