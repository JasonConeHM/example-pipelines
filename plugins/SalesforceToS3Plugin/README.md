# Plugin - Salesforce To S3
This plugin moves data from the Salesforce API to S3 based on the specified object(s).

## Hooks
### SalesforceHook
[Contrib Airflow SaleforceHook](https://github.com/apache/incubator-airflow/blob/master/airflow/contrib/hooks/salesforce_hook.py) with a dependency on [`simple-salesforce`](https://github.com/simple-salesforce/simple-salesforce).

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.


## Operators
### SalesforceToS3Operator
This operator initializes the connections and executes a COPY command to move the data from S3 to Snowflake.  It takes in a number of parameters:  
`conn_id`: The name of the Airflow connection that has your Salesforce
username, password and security_token.  
`obj`: The name of the Salesforce object we are fetching data from.  
`output`: The name of the file where the results should be saved.  
`fields`: *(optional)* The list of fields that you want to get from the object.
If *None*, then this will get all fields for the object.  
`fmt`: *(optional)* The format that the output of the data should be in. *Default: CSV*  
`query`: *(optional)* A specific query to run for the given object.  
This will override default query creation. *Default: None*  
`relationship_object`: *(optional)* Some queries require relationship objects
to work, and these are not the same names as the SF object. Specify that
relationship object here. *Default: None*  
`record_time_added`:   *(optional)* True if you want to add a Unix
timestamp field to the resulting data that marks when the data was
fetched from Salesforce.  *Default: False*.  
`coerce_to_timestamp`: *(optional)* True if you want to convert all fields with
 dates and datetimes into Unix timestamp (UTC). *Default: False*.  
