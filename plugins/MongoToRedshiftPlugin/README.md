# MongoToRedshiftPlugin
This plugin works as named, moving data from Mongo to Redshift. It relies on 3 hooks and two operators, detailed below.

## Hooks
### MongoHook
This wrapper for [pymongo](https://api.mongodb.com/python/current/) handles the fetching of connetion properties and the instatiation of the connection/MongoClient for interacting with your Mongo Database.

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

### PostgresHook
[Core Airflow PostgresHook](https://pythonhosted.org/airflow/code.html?highlight=postgreshook#airflow.hooks.PostgresHook) that is capable of interacting with Amazon Redshift with no modification.

## Operators
### MongoToS3BaseOperator
This is the meat of the plugin. Leveraging the MongoHook it takes a mongo_query either in the form of a dictionary which will map to the [pymongo find method](https://api.mongodb.com/python/current/api/pymongo/collection.html?highlight=find#pymongo.collection.Collection.find) - an array will be passed to the [aggregate pipeline method](https://api.mongodb.com/python/current/api/pymongo/collection.html?highlight=aggregate#pymongo.collection.Collection.aggregate).

The .transform() method inside that doesn't appear to do much. This is intentional, if you need a custom transform you can extend this method and perform your custom transform logic here. In general though it is recommended to perform the transforms using an aggregate pipeline transformation on the source system. This is because it will be more obvious to developers who work with your DAG file in the future.

### S3ToRedshiftOperator
This operator is a wrapper around the [Redshift copy command](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html). 
