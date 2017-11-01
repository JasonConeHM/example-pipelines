from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.operators import GithubToS3Operator, S3ToRedshiftOperator

default_args = {
                'owner': 'airflow',
                'start_date': datetime(2017, 10, 19)
}

dag = DAG('github_to_redshift_data',
          default_args=default_args,
          schedule_interval='@once'
          )

endpoints = [{"name": "commits",
             "payload": {},
             "load_type": "rebuild"},
            {"name": "issue_comments",
             "payload": {"state":"all"},
             "load_type": "rebuild"},
            {"name": "issues",
             "payload": {"state":"all"},
             "load_type": "rebuild"},
            {"name": "repositories",
             "payload": {},
             "load_type": "rebuild"},
            {"name": "members",
             "payload": {},
             "load_type": "rebuild"}]


with dag:
    for endpoint in endpoints:
        kick_off_dag = DummyOperator(task_id='kick_off_dag')

        github = GithubToS3Operator(task_id='github_{0}_to_s3'.format(endpoint['name']),
                                    github_conn_id='astronomer-github',
                                    github_org='astronomerio',
                                    github_repo='all',
                                    github_object=endpoint['name'],
                                    github_params=endpoint['payload'],
                                    s3_conn_id='astronomer-s3',
                                    s3_bucket='astronomer-workflows-dev',
                                    s3_key='github-test/services/{0}.json'.format(endpoint['name']))

        redshift = S3ToRedshiftOperator(task_id='github_{0}_to_redshift'.format(endpoint['name']),
                                        s3_conn_id='astronomer-s3',
                                        s3_bucket='astronomer-workflows-dev',
                                        s3_key='github-test/services/{0}.json'.format(endpoint['name']),
                                        origin_schema='github-test/services/{0}_schema.json'.format(endpoint['name']),
                                        load_type='rebuild',
                                        redshift_schema='astronomer_github',
                                        table=endpoint['name'],
                                        redshift_conn_id='astronomer-redshift-dev'
                                       )

        kick_off_dag >> github >> redshift
