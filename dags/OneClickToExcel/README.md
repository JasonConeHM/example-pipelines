# Pipeline - OneClick To Excel

## DAGs
### OneClickToExcel
This is the only DAG required. The DAG creates ones flow to query the OneClick
with the `start` date set to two days before the execution date and the `end`
date to one day before the execution date.

## Operators
### OneClickToS3
This operator is available within the OneClickToSpreadsheet Plugin.

### S3ToSpreadsheet
This operator is available within the OneClickToSpreadsheet Plugin.
