from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
import pandas as pd


class S3ToSpreadsheetOperator(BaseOperator):
    """
    S3 to Spreadsheet Operator
    :param input_s3_conn_id: The input s3 connection id.
    :type input_s3_conn_id: string
    :param input_s3_bucket: The input s3 bucket.
    :type input_s3_bucket: string
    :param input_s3_key: The input s3 key.
    :type input_s3_key: string
    :param input_file_type: The file type of the input file. (JSON/CSV)
    :type input_file_type: string
    :param output_s3_conn_id: The output s3 connection id.
    :type output_s3_conn_id: string
    :param output_s3_bucket: The output s3 bucket.
    :type output_s3_bucket: string
    :param output_s3_key: The output s3 key.
    :type output_s3_key: string
    :param output_format: The output file format. Either CSV or Excel.
    :type output_format: string
    :param output_payload: The output payload, a self-defined dictionary
        of dataframe parameters to pass into output functions.
    :type output_payload: string
    :param filters: Key-Value pairs that filters the pandas
        dataframe prior to creating the Excel file.
    :type filters: dictionary
    :param append_fields: Key-Value pairs that get appended to the
        pandas dataframe prior to creating the Excel file.
    :type append_fields: dictionary
    """

    template_fields = ['input_s3_key',
                       'output_s3_key',
                       'output_payload',
                       'filters',
                       'append_fields']

    @apply_defaults
    def __init__(self,
                 input_s3_conn_id,
                 input_s3_bucket,
                 input_s3_key,
                 input_file_type,
                 output_s3_conn_id=None,
                 output_s3_bucket=None,
                 output_s3_key=None,
                 output_format=None,
                 output_payload=None,
                 filters=None,
                 append_fields=None,
                 *args,
                 **kwargs):
        super(S3ToSpreadsheetOperator, self).__init__(*args, **kwargs)
        self.input_s3_conn_id = input_s3_conn_id
        self.input_s3_bucket = input_s3_bucket
        self.input_s3_key = input_s3_key
        self.input_file_type = input_file_type
        self.output_s3_conn_id = output_s3_conn_id
        self.output_s3_bucket = output_s3_bucket
        self.output_s3_key = output_s3_key
        self.output_format = output_format
        self.output_payload = output_payload
        self.filters = filters
        self.append_fields = append_fields

    def execute(self, context):
        input_s3 = S3Hook(s3_conn_id=self.input_s3_conn_id)
        output_s3 = S3Hook(s3_conn_id=self.output_s3_conn_id)

        input_key = (input_s3.get_key(self.input_s3_key,
                                      bucket_name=self.input_s3_bucket))
        input_s3.connection.close()

        if self.input_file_type.lower() == 'json':
            df = pd.read_json(input_key
                              .get_contents_as_string(encoding='utf-8'),
                              orient='records')
        elif self.input_file_type.lower() == 'csv':
            df = pd.read_csv(input_key, low_memory=False)
        else:
            raise Exception('Unsupported File Type')
        # Apply a mapping function to escape invalid characters
        df = (df.applymap(lambda x: x.encode('unicode_escape')
                                     .decode('utf-8')
                          if isinstance(x, str) else x))
        # Apply any self-defined filters if they exist
        if self.filters:
            for i in self.filters:
                if i in df.columns.values.tolist():
                    df = df[df[i] == self.filters[i]]

        # Append on any user-defined fields if they exist
        if self.append_fields:
            for i in self.append_fields:
                df[i] = self.append_fields[i]

        if self.output_format.lower() == 'excel':
            w = pd.ExcelWriter('temp.xlsx')
            df.to_excel(w, **self.output_payload)
            w.save()
            output_s3.load_file(
                filename='temp.xlsx',
                key=self.output_s3_key,
                bucket_name=self.output_s3_bucket,
                replace=True)
            output_s3.connection.close()
        elif self.output_format.lower() == 'csv':
            output_string = df.to_csv(**self.output_payload)
            output_s3.load_string(
                string_data=output_string,
                bucket_name=self.output_s3_bucket,
                key=self.output_s3_key,
                replace=True
            )
            output_s3.connection.close()
        else:
            raise Exception('Unsupported File Type.')
