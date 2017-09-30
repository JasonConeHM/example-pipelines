# Plugin - Clavis To Spreadsheet
This plugin queries the [Clavis Insight](https://www.clavisinsight.com) API and
transforms the resulting JSON output to an Excel, using S3 as an intermediary.

## Hooks
### ClavisHook
This hook handles the authentication and request to the Clavis API. If the endpoint
being requested is `\token`, the hook will use the username and password associated
with the `clavis_conn_id`. If the endpoint is something else, the hook looks for the
token and passes it in as part of the header.

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html)
with the standard boto dependency.

## Operators
### ClavisToS3Operator
This operator uses the Clavis Hook to query the Clavis API then handles all
pagination and subsequent requests. At least two requests are made for each task:
1) A request to the `\token` endpoint using the stored username and password.
The resulting token is then stored by the operator for use in subsequent requests.
2) A request to one of the available metric endpoints (see `clavis_endpoint`)
using the previously received token in the header of the request. The resulting
object is then written to S3. The parameters this operator can accept include
the following.
`clavis_conn_id`: The Airflow connection id used to store the Clavis credentials.
`clavis_endpoint`: The endpoint to retrieve data for.
Possible values are: [kpi, products, search_terms, content]
`payload`: Payload variables -- all are optional
    `start_date`: The requested date to return data for.
    (Format: yyyy-mm-dd eg. 2017-04-10). Used only in KPI endpoint.
    `end_date`: The requested date to return data for.
    (Format: yyyy-mm-dd eg. 2017-04-10). Used only in KPI endpoint.
    `report_date`: The requested date to return a single days data for
    (Format: yyyy-mm-dd eg. 2017-04-10). Used for Products, Search Terms,
     and Content endpoints.
    `online_store`: Filter the response data by a comma separated list
     of online stores.
    `brand`: Filter the response data by a comma separated list of
     manufacturer brands.
    `category`: Filter the response data by a comma separated list
     of manufacturer categories.
    `include_competitor_data`: Indicates whether the response should include
     competitor data. This is turned off by default. Available values are: [0,1]
    `manufacturers`: Filter the response data by a comma separated list
     of manufacturers.
    `s3_conn_id`:The Airflow connection id used to store the S3 credentials.
`s3_bucket`: The S3 bucket to be used to store the Clavis data.
`s3_key`: The S3 key to be used to store the Clavis data.

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
dataframe prior to creating the Excel file.
