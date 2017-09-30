# Pipeline - Clavis To Excel

## DAGs
### ClavisToExcel
This is the only DAG required. The DAG created task flows for the `products`,
`search_terms`, and `content` endpoints. The `report_date` is set to
one day before the execution date.

## Operators
### ClavisToS3
This operator is available within the ClavisToSpreadsheet Plugin.

### S3ToSpreadsheet
This operator is available within the ClavisToSpreadsheet Plugin.
