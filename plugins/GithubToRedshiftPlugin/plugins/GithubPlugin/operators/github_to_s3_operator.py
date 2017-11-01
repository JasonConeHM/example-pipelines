from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from ..hooks.github_hook import GithubHook
from airflow.hooks import S3Hook
from flatten_json import flatten
import json
import logging


class GithubToS3Operator(BaseOperator):
    """
    Github To S3 Operator
    :param github_conn_id:           The Github connection id.
    :type github_conn_id:            string
    :param github_org:               The Github organization.
    :type github_org:                string
    :param github_repo:              The Github repository. Required for
                                     commits, commit_comments, issue_comments,
                                     and issues objects.
    :type github_repo:               string
    :param github_object:            The desired Github object. The currently
                                     supported values are:
                                        - commits
                                        - commit_comments
                                        - issue_comments
                                        - issues
                                        - organizations
                                        - repositories
                                        - members
    :type github_object:             string
    :param github_params:            The associated parameters to pass into
                                     the object request as keyword arguments.
    :type github_params:             dict
    :param s3_conn_id:               The s3 connection id.
    :type s3_conn_id:                string
    :param s3_bucket:                The S3 bucket to be used to store
                                     the Github data.
    :type s3_bucket:                 string
    :param s3_key:                   The S3 key to be used to store
                                     the Github data.
    :type s3_key:                    string
    """

    template_field = ['s3_key']

    @apply_defaults
    def __init__(self,
                 github_conn_id,
                 github_org,
                 github_object,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 github_repo=None,
                 github_params={},
                 **kwargs):
        super().__init__(**kwargs)
        self.github_conn_id = github_conn_id
        self.github_org = github_org
        self.github_repo = github_repo
        self.github_object = github_object
        self.github_params = github_params
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        g = GithubHook(self.github_conn_id).get_conn()
        s3 = S3Hook(self.s3_conn_id)
        org = g.organization(self.github_org)

        output = self.retrieve_data(g, org)

        s3.load_string(
            string_data=str(output),
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )
        s3.connection.close()

    def process_data(self, parent):
        output = ''
        if parent.size:
            if self.github_object == 'issue_comments':
                """
                Issue comments is unique from the other objects thus far
                as it requires a secondary (sub_child) endpoint to be hit.
                Example: whereas the commits object requires the repo to
                make the request, retrieving issue comments requires the repo
                and each individual issue in that issue.
                """
                for child in getattr(parent, 'issues')(**self.github_params):
                    for sub_child in getattr(child, 'comments')():
                        sub_child = getattr(sub_child, 'as_dict')()
                        sub_child['user_id'] = sub_child['user']['id']
                        del sub_child['user']
                        output += json.dumps(flatten(sub_child))
                        output += '\n'
            else:
                if self.github_object == 'commit_comments':
                    self.github_object = 'comments'
                for child in (getattr(parent, self.github_object)
                              (**self.github_params)):
                    child = getattr(child, 'as_dict')()
                    """
                    This process strips out unnecessary objects (i.e. ones
                    that are duplicated in other core objects).
                    Example: a commit returns all the same user information
                    for each commit as already returned the members endpoint).
                    The id for these striped objects are kept for reference.
                    """
                    if self.github_object == 'commits':
                        if child['author']:
                            child['author_id'] = child['author']['id']
                            del child['author']
                        if child['committer']:
                            child['committer_id'] = child['committer']['id']
                            del child['committer']
                    elif self.github_object == 'commit_comments':
                        child['user_id'] = child['user']['id']
                        del child['user']
                    elif self.github_object == 'repositories':
                        child['owner_id'] = child['owner']['id']
                        del child['owner']
                    elif self.github_object == 'issues':
                        """
                        Labels is currently returned as an array of dicts.
                        When flattened, this can cause an undo amount of
                        columns with the naming convention labels_0_name,
                        labels_1_name, etc. Until a better data model can be
                        determined (possibly putting labels in their own table)
                        these fields are striped out entirely.
                        """
                        del child['labels']

                    output += json.dumps(flatten(child))
                    output += '\n'

            return output
        else:
            logging.info('Resource unavailable')
            return ''

    def retrieve_data(self, g, org):
        output = ''
        if self.github_object in ['commits',
                                  'issues',
                                  'commit_comments',
                                  'issue_comments']:
            """
            The following adds support for single, multiple, and "all"
            repository requests.
            """
            if self.github_repo == 'all':
                for repo in org.repositories():
                    repo = g.repository(self.github_org, repo.name)
                    output += self.retrieve_repo_data(repo)
            elif isinstance(self.github_repo, list):
                for repo in self.github_repo:
                    repo = g.repository(self.github_org, repo)
                    output += self.retrieve_repo_data(repo)
            elif isinstance(self.github_repo, str):
                repo = g.repository(self.github_org, self.github_repo)
                output = self.retrieve_repo_data(repo)

        elif self.github_object in ['repositories', 'members']:
            output = self.retrieve_org_data(org)

        elif self.github_object == 'organizations':
            output = self.retrieve_account_data(g)

        return output

    def retrieve_repo_data(self, repo):
        output = self.process_data(repo)
        return output

    def retrieve_org_data(self, org):
        output = self.process_data(org)
        return output

    def retrieve_account_data(self, g):
        output = self.process_data(g)
        return output
