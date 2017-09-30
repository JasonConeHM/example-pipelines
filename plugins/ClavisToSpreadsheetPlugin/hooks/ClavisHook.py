from airflow.hooks.http_hook import HttpHook


class ClavisHook(HttpHook):
    def __init__(self, method='GET', http_conn_id='http_default'):
        super().__init__(method, http_conn_id)

    def get_conn(self, headers=None):
        session = super().get_conn(headers)
        if self.endpoint == 'token':
            return session
        else:
            session.auth = None
            return session

    def run(self,
            endpoint,
            data=None,
            headers=None,
            extra_options=None,
            token=None):
        self.endpoint = endpoint
        if endpoint != 'token':
            headers = {"Authorization": "Token token={0}".format(token)}
        return super().run(endpoint, data, headers, extra_options)
