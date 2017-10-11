# S3ToSnowflakePlugin
This plugin works as named, moving data from S3 to Snowflake. It relies on 2 hooks and one operator, detailed below.  It uses the COPY command to move data directly from S3 to Snowflake efficiently.

## Hooks
### SnowflakeHook
This wrapper for [snowflake-connector-python](https://github.com/snowflakedb/snowflake-connector-python) handles the fetching of connetion properties and the instatiation of the connection to the Snowflake database.

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

## Operators
### S3ToSnowflakeOperator
This operator initializes the connections and executes a COPY command to move the data from S3 to Snowflake.  It takes in a number of parameters:  
`s3_bucket`: Name of the S3 Bucket where your file is located  
`s3_key`: Name of the file inside of the bucket  
`database`: Name of the Snowflake database  
`schema`: Name of the Snowflake schema  
`table`: Name of the Snowflake table  
`file_format_name`: File Format Type (default 'JSON', ex. 'AVRO', 'CSV')  
`s3_conn_id`: Name of the s3 connection to use for the S3Hook  
`snowflake_conn_id`: Name of the Snowflake connection to use for the SnowflakeHook