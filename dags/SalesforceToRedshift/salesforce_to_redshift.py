from airflow.operators.salesforce_plugin import SalesforceToS3Operator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.db import provide_session
from datetime import datetime, timedelta
from airflow.models import Connection
from airflow import DAG
import boto3

SLACK_CHANNEL = '@channel'
SLACK_USER = 'Bot-Name'
SLACK_ICON = 'https://imagepath.com/image.jpg'


def getS3Conn():
    s3_conn = get_conn('s3')
    aws_key = s3_conn.extra_dejson.get('aws_access_key_id')
    aws_secret = s3_conn.extra_dejson.get('aws_secret_access_key')
    return "aws_access_key_id={0};aws_secret_access_key={1}".format(aws_key,
                                                                    aws_secret)


@provide_session
def get_conn(conn_id, session=None):
    conn = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .first())
    return conn


def getSlackConn():
    slack_conn = get_conn('slack')
    return slack_conn.extra_dejson.get('token')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 8, 29),
    'email': ['example@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

names = ['Account',
         'Campaign',
         'CampaignMember',
         'Contact',
         'Lead',
         'Opportunity',
         'OpportunityContactRole',
         'OpportunityHistory',
         'Task',
         'User']

dag = DAG('salesforce_to_redshift',
          default_args=default_args,
          schedule_interval='0 13 * * *',
          catchup=False)

kick_off_dag = DummyOperator(
    task_id='kick_off_dag',
    dag=dag)

slack_push_sfdc_records = SlackAPIPostOperator(
    task_id='slack_sfdc_records',
    channel=SLACK_CHANNEL,
    token=getSlackConn(),
    username=SLACK_USER,
    text='New SFDC records have been imported into S3.',
    icon_url=SLACK_ICON,
    dag=dag
)

slack_push_raw_import = SlackAPIPostOperator(
    task_id='slack_raw_import',
    channel=SLACK_CHANNEL,
    provide_context=True,
    token=getSlackConn(),
    username=SLACK_USER,
    text='New SFDC records have been imported into Redshift.',
    icon_url=SLACK_ICON,
    dag=dag)

slack_push_transforms_started = SlackAPIPostOperator(
    task_id='slack_transforms_started',
    channel=SLACK_CHANNEL,
    provide_context=True,
    token=getSlackConn(),
    username=SLACK_USER,
    text='The SFDC models have started.',
    icon_url=SLACK_ICON,
    dag=dag
)


def triggerRun(context, dag_run_obj):
    if context['params']['condition_param']:
        return dag_run_obj


trigger_models = TriggerDagRunOperator(
    task_id='trigger_models',
    trigger_dag_id='salesforce_models',
    python_callable=triggerRun,
    params={'condition_param': True},
    dag=dag
)

for name in names:
    TASK_ID_INSERT_RECORDS = '{}_row_count'.format(name)

    salesforce_to_s3 = SalesforceToS3Operator(
        task_id='{}_To_S3'.format(name),
        sf_conn_id='sfdc',
        obj=name,
        output='salesforce/{}.json'.format(name.lower()),
        fmt='ndjson',
        s3_conn_id='s3',
        s3_bucket='bucket-name',
        record_time_added=True,
        coerce_to_timestamp=True,
        dag=dag)

    raw_import_query = \
        """
        TRUNCATE \"{0}\".\"{1}\";
        COPY \"{0}\".\"{1}\"
        FROM 's3://bucket-name/salesforce/{1}.json'
        CREDENTIALS '{2}'
        JSON 'auto'
        TRUNCATECOLUMNS;
        """.format('salesforce_raw', name.lower(), getS3Conn())

    raw_import = PostgresOperator(
        task_id='{}_raw_import'.format(name),
        op_args=[name],
        sql=raw_import_query,
        postgres_conn_id='redshift',
        dag=dag)

    insert_records_query = \
        """
        INSERT INTO "salesforce_stats"."insertion_records"
        (SELECT '{0}' AS "table_name",
        count(1) AS "row_count",
        trunc(cast(\'{1}\' as date)) AS "Date"
        FROM "salesforce_raw".\"{0}\")
        """.format(name.lower(), '{{ ts }}')

    insert_records = PostgresOperator(
        task_id=TASK_ID_INSERT_RECORDS,
        op_args=[name],
        sql=insert_records_query,
        postgres_conn_id='redshift',
        dag=dag)

    (kick_off_dag >>
     salesforce_to_s3 >>
     slack_push_sfdc_records >>
     raw_import >>
     slack_push_raw_import >>
     insert_records >>
     trigger_models)


def remove_files_py():
    s3_conn = get_conn('s3')
    access_key_id = s3_conn.extra_dejson.get('aws_access_key_id')
    secret_access_key = s3_conn.extra_dejson.get('aws_secret_access_key')

    s3 = boto3.client('s3',
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key)

    s3.delete_objects(Bucket='bucket-name', Delete={
                      'Objects': [
                          {'Key': 'salesforce/account.json'},
                          {'Key': 'salesforce/campaign.json'},
                          {'Key': 'salesforce/campaignmember.json'},
                          {'Key': 'salesforce/contact.json'},
                          {'Key': 'salesforce/lead.json'},
                          {'Key': 'salesforce/opportunity.json'},
                          {'Key': 'salesforce/opportunitycontactrole.json'},
                          {'Key': 'salesforce/opportunityhistory.json'},
                          {'Key': 'salesforce/task.json'},
                          {'Key': 'salesforce/user.json'}
                      ]})


remove_files = PythonOperator(
    task_id='remove_files',
    python_callable=remove_files_py,
    dag=dag)

trigger_models >> slack_push_transforms_started
trigger_models >> remove_files
