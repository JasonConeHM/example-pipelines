"""

    Github Plugin

    This plugin provides a wrapper around the Github3.py library
    (https://github.com/sigmavirus24/github3.py) to interact with the Github
    API.

    The Github Hook provides methods to return both the base Github
    object as well as the currently authenticated user associated with the used
    token.

    The Github Operator provides support for the following endpoints:
        Comments
        Commits
        Commit Comments
        Issue Comments
        Issues
        Organizations
        Repositories
        Members

    The only required required connection information is the user
    generated token, put into either the password or extras field. If put
    into the extras field, it should follow the following format.

    {"token":"XXXXXXXXXXXXXXXXXXXXX"}

"""

from airflow.plugins_manager import AirflowPlugin
from GithubPlugin.hooks.github_hook import GithubHook
from GithubPlugin.operators.github_to_s3_operator import GithubToS3Operator


class GithubPlugin(AirflowPlugin):
    name = "github_plugin"
    operators = [GithubToS3Operator]
    hooks = [GithubHook]
