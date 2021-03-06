from airflow.hooks.mysql_hook import MySqlHook

class AstroMySqlHook(MySqlHook):
    def get_schema(self, table):
        query = \
            """
            SELECT COLUMN_NAME, COLUMN_TYPE
            FROM COLUMNS
            WHERE TABLE_NAME = '{0}';
            """.format(table)
        self.schema = 'information_schema'
        return super().get_records(query)
