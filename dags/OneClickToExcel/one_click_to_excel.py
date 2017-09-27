from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import OneClickToS3Operator
from airflow.operators import S3ToSpreadsheetOperator
from datetime import datetime
from airflow import DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 8, 29)
}

dag = DAG('oneclick_to_excel',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

with dag:
    kick_off_dag = DummyOperator(task_id='kick_off_dag')

    one_click_to_s3 = OneClickToS3Operator(
        task_id='one_click',
        one_click_conn='one_click-connection-id',
        start='{{ macros.ds_add(ds, -1) }}',
        end='{{ macros.ds_add(ds, -2) }}',
        s3_conn_id='s3-connection-id',
        s3_bucket='output-bucket-name',
        s3_key='/path/to/file/{{ ds }}.csv')

    s3_to_excel = S3ToSpreadsheetOperator(
        task_id=('read_to_excel'),
        input_s3_conn_id='input-s3-connection-id',
        input_s3_bucket='input-bucket-name',
        input_s3_key='/path/to/file/{{ ds }}.csv',
        input_file_type='csv',
        output_s3_conn_id='output-s3-connection-id',
        output_s3_bucket='output-bucket-name',
        output_s3_key='/path/to/file/{{ ds }}.xlsx',
        output_format='excel',
        output_payload={"index": False},
        append_fields={'run_date': '{{ ds }}'}
    )

    kick_off_dag >> one_click_to_s3 >> s3_to_excel
