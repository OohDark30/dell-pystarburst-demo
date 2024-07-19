"""
DELL Data Lakehouse / DDAE Demo - PyStarburst and Object Lock Demo
"""
import logging
import os
import json

# Constants
BASE_CONFIG = 'BASE'                                          # Base Configuration Section
DELL_S3_CONNECTION = 'DELL_S3_CONNECTION'                     # Dell OBS S3 Configuration Section
DDAE_SESSION = 'DDAE_SESSION'                                 # DDAE Session Configuration Section
DDAE_DATA_CONFIG = 'DDAE_DATA_CONFIG'                         # Iceberg Configuration Section


class InvalidConfigurationException(Exception):
    pass


class DellPyStarburstDemoConfiguration(object):
    def __init__(self, config, tempdir):

        if config is None:
            raise InvalidConfigurationException("No file path to the DELL PyStarburst Demo Module "
                                                "configuration provided")

        if not os.path.exists(config):
            raise InvalidConfigurationException("The DELL PyStarburst Demo configuration "
                                                "file path does not exist: " + config)
        if tempdir is None:
            raise InvalidConfigurationException("No path for temporary file storage provided")

        # Store temp file storage path to the configuration object
        self.tempfilepath = tempdir

        # Attempt to open configuration file
        try:
            with open(config, 'r') as f:
                parser = json.load(f)
        except Exception as e:
            raise InvalidConfigurationException("The following unexpected exception occurred in the "
                                                "DELL PyStarburst Demo Module attempting to parse "
                                                "the configuration file: " + e.message)

        # We parsed the configuration file now lets grab values
        self.dells3connection = parser[DELL_S3_CONNECTION]
        self.ddaesession = parser[DDAE_SESSION]

        # Grab Iceberg Configuration
        self.ddae_catalog = parser[DDAE_DATA_CONFIG]['ddae_catalog']
        self.ddae_schema = parser[DDAE_DATA_CONFIG]['ddae_schema']
        self.ddae_table_location = parser[DDAE_DATA_CONFIG]['ddae_table_location']
        self.ddae_table_name_customer = parser[DDAE_DATA_CONFIG]['ddae_table_name_customer']
        self.ddae_table_schema_customer = parser[DDAE_DATA_CONFIG]['ddae_table_schema_customer']
        self.dell_lakehouse_s3_bucket = parser[DDAE_DATA_CONFIG]['dell_lakehouse_s3_bucket']

        # Set logging level
        logging_level_raw = parser[BASE_CONFIG]['logging_level']
        self.logging_level = logging.getLevelName(logging_level_raw.upper())

        # Validate logging level
        if logging_level_raw not in ['debug', 'info', 'warning', 'error']:
            raise InvalidConfigurationException(
                "Logging level can be only one of ['debug', 'info', 'warning', 'error']")

        # Validate Dell S3 connection details
        if not self.dells3connection['protocol']:
            raise InvalidConfigurationException("The Dell S3 Protocol is not configured in the module configuration")

        protocol = self.dells3connection['protocol']
        if protocol not in ['http', 'https']:
            raise InvalidConfigurationException("The Dell S3 Protocol can only be one of ['http', 'https']")

        if not self.dells3connection['host']:
            raise InvalidConfigurationException("The Dell S3 Host is not configured in the module configuration")

        if not self.dells3connection['port']:
            raise InvalidConfigurationException("The Dell S3 port is not configured in the module configuration")

        if not self.dells3connection['s3AccessKey']:
            raise InvalidConfigurationException("The Dell S3 Access Key is not configured in the module configuration")

        if not self.dells3connection['s3SecretKey']:
            raise InvalidConfigurationException("The Dell S3 Secrete Key is not configured "
                                                "in the module configuration")

        if not self.dells3connection['connectTimeout']:
            raise InvalidConfigurationException("The Dell S3 connection timeout is not configured in the module "
                                                "configuration")

        if not self.dells3connection['readTimeout']:
            raise InvalidConfigurationException("The Dell S3 read timeout is not configured in the module configuration")

        # Validate DDAE Session Details
        ddae_protocol = self.ddaesession['protocol']
        if ddae_protocol not in ['http', 'https']:
            raise InvalidConfigurationException("The DDAE Session Protocol can only be one of ['http', 'https']")

        if not self.ddaesession['host']:
            raise InvalidConfigurationException("The DDAE Session Host is not configured in the module configuration")

        if not self.ddaesession['port']:
            raise InvalidConfigurationException("The DDAE Session port is not configured in the module configuration")

        if not self.ddaesession['user']:
            raise InvalidConfigurationException("The DDAE Session User is not configured in the module configuration")

        if not self.ddaesession['password']:
            raise InvalidConfigurationException("The DDAE Session Password is not configured "
                                                "in the module configuration")
        if not self.ddaesession['catalog']:
            raise InvalidConfigurationException("The DDAE Session Catalog is not configured in the module configuration")

        if not self.ddaesession['schema']:
            raise InvalidConfigurationException("The DDAE Session Schema is not configured in the module configuration")

        if not self.ddaesession['connectTimeout']:
            raise InvalidConfigurationException("The DDAE Session connection timeout is not configured in the module "
                                            "configuration")

        if not self.ddaesession['readTimeout']:
            raise InvalidConfigurationException("The DDAE Session read timeout is not configured in the module configuration")

        # Validate Iceberg Configuration
        if not self.ddae_catalog:
            raise InvalidConfigurationException("The Iceberg Catalog is not configured in the module configuration")
        if not self.ddae_schema:
            raise InvalidConfigurationException("The Iceberg Schema is not configured in the module configuration")
        if not self.ddae_table_location:
            raise InvalidConfigurationException("The Iceberg table location is not configured in the module configuration")
        if not self.ddae_table_name_customer:
            raise InvalidConfigurationException("The Iceberg table name for customer is not configured in the module configuration")
        if not self.ddae_table_schema_customer:
            raise InvalidConfigurationException("The Iceberg table schema for customer data is not configured in the module configuration")
        if not self.dell_lakehouse_s3_bucket:
            raise InvalidConfigurationException("The Dell Lakehouse S3 Bucket is not configured in the module configuration")


