from airflow.hooks.base_hook import BaseHook
import github3


class GithubHook(BaseHook):

    def __init__(self, github_conn_id):
        self.github_conn_id = self.get_connection(github_conn_id)
        if self.github_conn_id.password:
            self.github_token = self.github_conn_id.password
        else:
            self.github_token = self.github_conn_id.extra_dejson.get('token')

    def get_conn(self):
        """
        Returns the base Github object.
        http://bit.ly/2yltp1R
        """
        return github3.login(token=self.github_token)

    def get_user(self):
        """
        Returns the user object for the authenticated user
        associated with the used token.
        http://bit.ly/2h1Lfzk
        """
        return self.get_conn().me()
