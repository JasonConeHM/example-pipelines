from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# Custom Plugin
from airflow.operators import AstroRequestsToS3Operator


# Passed into Operator as a template string and is then picked
# https://pythonhosted.org/airflow/code.html#macro for full list
LOWER_BOUND = "{{ prev_execution_date.isoformat() + 'Z' if prev_execution_date != None else ts + 'Z' }}"
UPPER_BOUND = "{{ ts + 'Z' }}"

reqs = [
    # Test Route
    {
        'type': 'GET',
        'kwargs': {
            'url': 'http://jsonplaceholder.typicode.com/posts/'
        }
    }
]

args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 4, 20, 0, 0),
    'provide_context': True
}

dag = DAG(
    'test_astro_requests',
    schedule_interval="@once",
    default_args=args
)

start = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

request_to_s3 = AstroRequestsToS3Operator(
    task_id='from_requests_to_s3',
    http_conn_id='http_test',
    s3_conn_id='s3_test',
    s3_bucket='astronomer-workflows-dev',
    s3_key='http-test/test-{{ ds_nodash }}',
    reqs=reqs,
    dag=dag
)

end = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

start.set_downstream(request_to_s3)
request_to_s3.set_downstream(end)
