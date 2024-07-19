# dell-pystarburst-demo
dell-pystarburst-demo is a Python application that demonstrates using PyStarburst and Boto3 with the Dell Data Lakehouse to show how bucket versioning and object locking can be used to prevent unintended data loss:

1. Create a session with the Dell Data Analytics Engine (DDAE) powered by Starburst
2. Create a schema and table into a Hive catalog that references an S3 Bucket in the Dell Lakehouse
3. Load a Parquet table into the S3 Bucket in the Dell Lakehouse
4. Perform a query of the data
5. Delete the Parquet file using S3
   - Since the bucket is version and object lock enabled the object vesrion is "locked" and a delete marker is created  
7. Re-query the data to show the data is no longer available
8. Issue a version specific S3 object delete to remove the delete marker to restore the deleted file
9. Re-issue the query again to show the data is returned
10. Clean up object versions, bucket, table, and schema
