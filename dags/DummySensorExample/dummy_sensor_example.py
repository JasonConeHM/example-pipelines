from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# Custom Plugin
from airflow.operators import DummySensorOperator


# Passed into Operator as a template string and is then picked
# https://pythonhosted.org/airflow/code.html#macro for full list

args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 4, 20, 0, 0),
    'provide_context': True
}

dag = DAG(
    'replicate_skipped_bug',
    schedule_interval="@once",
    default_args=args
)

start = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

dummies = []
for i in range(0, 10):
    dummy = DummySensorOperator(
        task_id='dummy_sensor_{}'.format(i),
        timeout=1,
        poke_interval=2,
        flag=False,
        soft_fail=True,
        dag=dag
    )

    dummies.append(dummy)

end = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

start.set_downstream(dummies)
end.set_upstream(dummies)