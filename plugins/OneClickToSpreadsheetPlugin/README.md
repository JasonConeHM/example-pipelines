# Plugin - OneClick To Spreadsheet
This plugin queries the [OneClick](http://oneclickretail.com) API and
transforms the resulting JSON output to an Excel, using S3 as an intermediary.

## Hooks
### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html)
with the standard boto dependency.

## Operators
### OneClickToSpreadsheetOperator

The parameters it can accept include the following.
`one_click_conn`: The connection string for One Click API.  
`start`: The start date bound to request data for in 'YYYY-MM-DD' date format.  
`end`: The end date bound to request data for in 'YYYY-MM-DD' date format.  
`weeks_back`: The number of weeks back to bound a time parameter.  
`filter_id`: The id of a self-defined filter kept in One Click.  

### S3ToSpreadsheetOperator
This operator composes the logic for this operator. In addition to parameters
defining the location of the input/output file, there is also support for
appending new, user-defined field and custom fields. Both of these can be
templatized. The parameters it can accept include the following.
`input_s3_conn_id`: The input s3 connection id.  
`input_s3_bucket`: The input s3 bucket.  
`input_s3_key`: The input s3 key.  
`input_file_type`: The file type of the input file. (JSON/CSV)  
`output_s3_conn_id`: The output s3 connection id.  
`output_s3_bucket`: The output s3 bucket.  
`output_s3_key`: The output s3 key.  
`output_format`: The output file format. Either CSV or Excel.  
`output_payload`: The output payload, a self-defined dictionary of
dataframe parameters to pass into output functions.  
`filters`: Key-Value pairs that filters the pandas dataframe prior
to creating the Excel file.  
`append_fields`: Key-Value pairs that get appended to the pandas
dataframe prior to creating the Excel file.f
