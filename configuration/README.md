# dell-pystarburst-demo configuration
----------------------------------------------------------------------------------------------
dell-pystarburst-demo is a PYTHON based POC that is intended to show the use of PyStarburst
with the Dell DDAE to create schemas, create tables, and query data.  In addition, we will 
show the use of S3 Bucket Versioning and ObjectLock to ensure that data stored in the Dell
Data Lake House is not lost if inadvertently deleted

We've provided a sample configuration file:

- dell_pystarburst_demo.sample: Change file suffix from .sample to .json and configure as needed
  This contains the tool configuration for Flask, S3, and Thumbnail processing.
  
  BASE:
  logging_level - The default is "info" but it can be set to "debug" to generate a LOT of details


  DELL_S3_CONNECTION:

  - protocol - This can be either "https" or "http"
  - host - This is the IP address of FQDN of a Dell ObjectScale ObjectStore S3 Endpoint
  - port - This is the port of the Dell ObjectScale ObjectStore S3 Endpoint
  - s3AccessKey - This is the access key of the ObjectScale IAM Account User
  - s3SecretKey - This is the secret key of the ObjectScale IAM Account User
  - connectTimeout - This is the connection timeout in seconds 
  - readTimeout - This is the read timeout in seconds

  DDAE_SESSION:

  - protocol - This can be either "https" or "http"
  - host - This is the IP address of FQDN of a Dell ObjectScale ObjectStore S3 Endpoint
  - port - This is the port of the Dell ObjectScale ObjectStore S3 Endpoint
  - s3AccessKey - This is the access key of the ObjectScale IAM Account User
  - s3SecretKey - This is the secret key of the ObjectScale IAM Account User
  - connectTimeout - This is the connection timeout in seconds
  - readTimeout - This is the read timeout in seconds

  DDAE_DATA_CONFIG

  - ddae_catalog - DDAE Catalog Name
  - ddae_schema - DDAE Schema Name
  - ddae_table_location - DDAE Table Location
  - ddae_table_name_customer - DDAE Table Name for Customer Data
  - ddae_table_schema_customer - DDAE Table Schema for Customer Data
  - dell_lakehouse_s3_bucket = Dell Lakehouse S3 Bucket Name