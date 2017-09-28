# Pipeline - Salesforce To Redshift

## DAGs
### Salesforce To Redshift
This dag queries salesforce and writes the resulting JSON object to S3 where
it is copied into redshift and then deleted from S3. Use of the
SlackAPIPostOperator, PostgresOperator, and TriggerDagRunOperator is shown to
illustrate alerts, QA measures, and relationships to downstream DAGs.

## Hooks
### S3 Hook
This operator is available within the core Airflow hooks library.

### PostgresHook
This operator is available within the core Airflow hooks library.

## Operators
### Dummy Operators
This operator is available within the core Airflow operators library.

### PostgresOperator
This operator is available within the core Airflow operators library.

### PythonOperator
This operator is available within the core Airflow operators library.

### SalesforceToS3Operator Operator
This operator is available within the SalesforceToS3 Plugin.

### TriggerDagRunOperator
This operator is available within the core Airflow operators library.

### SlackAPIPostOperator
This operator is available within the core Airflow operators library.
