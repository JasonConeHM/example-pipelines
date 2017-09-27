from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import ClavisToS3Operator
from airflow.operators import S3ToSpreadsheetOperator
from datetime import datetime
from airflow import DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 9, 27)
}

dag = DAG('clavis_to_excel',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

endpoints = ['products', 'search_terms', 'content']

with dag:
    kick_off_dag = DummyOperator(task_id='kick_off_dag')
    for endpoint in endpoints:
        clavis_to_s3 = ClavisToS3Operator(
            task_id=('clavis_output_{0}'.format(endpoint)),
            http_conn_id='clavis-connection-id',
            clavis_endpoint=endpoint,
            payload={'report_date': '{{ yesterday_ds }}'},
            s3_conn_id='s3-connection-id',
            s3_bucket='bucket-name',
            s3_key=('/path/to/file/{0}.json'.format(endpoint)))

        i = '{0}.json'.format(endpoint)
        o = "{0}.xlsx".format(endpoint)

        s3_to_excel = S3ToSpreadsheetOperator(
            task_id=('read_{0}_into_excel'.format(endpoint)),
            input_s3_conn_id='input-s3-connection-id',
            input_s3_bucket='input-bucket-name',
            input_s3_key="/path/to/file/{0}".format(i),
            input_file_type='JSON',
            output_s3_conn_id='output-s3-connection-id',
            output_s3_bucket='output-bucket-name',
            output_s3_key='/path/to/file/{0}'.format(o),
            output_format='excel',
            output_payload={"index": False},
            append_fields={'run_date': '{{ ds }}'}
        )

        kick_off_dag >> clavis_to_s3 >> s3_to_excel
