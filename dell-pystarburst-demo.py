"""
DELL Data Lakehouse / DDAE Demo - PyStarburst and Object Lock Demo
"""
import json
import os
import signal
import threading
import time
import traceback
import logging
import urllib3
from botocore.exceptions import ClientError

from configuration.dell_pystarburst_demo_configuration import DellPyStarburstDemoConfiguration
from logger import dell_pystarburst_demo_logger
from ddae.ddae import DDAEAuthentication
from ddae.ddae import DDAEDataProcessor
from s3 import GetConnection

# Constants
MODULE_NAME = "Dell_PyStarburst_Demo_Module"  # Module Name
INTERVAL = 30  # In seconds
CONFIG_FILE = 'dell_pystarburst_demo.json'  # Default Configuration File

urllib3.disable_warnings()

# Globals
_configuration = None
_logger = None
_ddaeDataProcessor = None
_ddaeSession = None


class DellPyStarburstDemoShutdown:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.controlled_shutdown)
        signal.signal(signal.SIGTERM, self.controlled_shutdown)

    def controlled_shutdown(self, signum, frame):
        self.kill_now = True


def dell_pystarburst_demo_config(config, temp_dir):
    global _configuration
    global _logger
    global _ddaeSession

    try:
        # Load and validate module configuration
        _configuration = DellPyStarburstDemoConfiguration(config, temp_dir)

        # Grab loggers and log status
        _logger = dell_pystarburst_demo_logger.get_logger(__name__, _configuration.logging_level)
        _logger.info(MODULE_NAME + '::dell_starburst_demo_config()::We have configured logging level to: '
                     + logging.getLevelName(str(_configuration.logging_level)))
        _logger.info(MODULE_NAME + '::dell_starburst_demo_config()::Configuring Dell Starburst Demo Module complete.')
    except Exception as e:
        _logger.error(MODULE_NAME + '::dell_starburst_demo_config()::The following unexpected '
                                    'exception occurred: ' + str(e) + "\n" + traceback.format_exc())


def dell_ddae_session():
    global _ddaeSession
    global _configuration
    global _logger
    global _ddaeDataProcessor
    connected = True

    try:
        # Wait till configuration is set
        while not _configuration:
            time.sleep(1)

        # Attempt to create Starburst session(s)
        sep = DDAEAuthentication(_configuration.ddaesession['protocol'],
                                 _configuration.ddaesession['host'], _configuration.ddaesession['user'],
                                 _configuration.ddaesession['password'],
                                 _configuration.ddaesession['port'],
                                 _configuration.ddaesession['catalog'],
                                 _configuration.ddaesession['schema'], _logger)

        sep.connect()

        # Check to see if we have a token returned
        if sep.sep_session is None:
            _logger.error(
                MODULE_NAME + '::dell_ddae_session()::Unable to create a DDAE Session as configured.  '
                              'Please validate and try again.')
            connected = False
        else:
            _ddaeSession = sep

            # Instantiate StarburstDataProcessor object
            _ddaeDataProcessor = DDAEDataProcessor(_configuration, sep, _logger)

            if not _ddaeSession:
                _logger.info(MODULE_NAME + '::dell_ddae_session()::DDAE Data Processor '
                                           'Module is not ready.  Please check logs.')
                connected = False
            else:
                _ddaeDataProcessor.get_list_of_catalogs()

        return connected

    except Exception as e:
        _logger.error(MODULE_NAME + '::dell_ddae_session()::Cannot initialize plugin. Cause: '
                      + str(e) + "\n" + traceback.format_exc())
        connected = False
        return connected


"""
Main 
"""
if __name__ == "__main__":

    try:
        # Create object to support controlled shutdown
        controlledShutdown = DellPyStarburstDemoShutdown()

        # Dump out application path
        currentApplicationDirectory = os.getcwd()
        configFilePath = os.path.abspath(os.path.join(currentApplicationDirectory, "configuration", CONFIG_FILE))
        tempFilePath = os.path.abspath(os.path.join(currentApplicationDirectory, "temp"))

        print(MODULE_NAME + "::__main__::Current directory is : " + currentApplicationDirectory)
        print(MODULE_NAME + "::__main__::Configuration file path is: " + configFilePath)

        # Initialize configuration
        dell_pystarburst_demo_config(configFilePath, tempFilePath)

        # Generate dell object log file data path and initialize parser
        dell_object_log_file_path = os.path.abspath(os.path.join(currentApplicationDirectory, "dellobjectlogdata"))

        # Initialize connection(s) to Starburst
        if dell_ddae_session():
            print(MODULE_NAME + "__main__::Successfully connected to the DDAE / Starburst.")

            # Grab S3 connection info and create boto3 S3 client
            s3endpoint = _configuration.dells3connection['protocol'] + "://" + _configuration.dells3connection[
                'host'] + ":" + _configuration.dells3connection['port']
            s3accesskey = _configuration.dells3connection['s3AccessKey']
            s3secretkey = _configuration.dells3connection['s3SecretKey']
            s3 = GetConnection.getConnection(s3endpoint, False, s3accesskey, s3secretkey)

            # Demo Scenario
            # 1. Create version enabled and object lock enabled bucket in our object store
            create_bucket_response = s3.create_bucket(Bucket=_configuration.dell_lakehouse_s3_bucket, ObjectLockEnabledForBucket=True)
            put_versioning_response = s3.put_bucket_versioning(Bucket=_configuration.dell_lakehouse_s3_bucket,
                                                               VersioningConfiguration={'Status': 'Enabled'})

            # Check Object Lock Configuration
            ol_bucket_response = s3.get_object_lock_configuration(
                Bucket='dell-pystarburst-demo'
            )
            print(
                MODULE_NAME + "__main__::Bucket " + _configuration.dell_lakehouse_s3_bucket + " created with versioning and object lock enabled.  Object Lock Configuration before creating Governance rule:")
            print(ol_bucket_response)

            # 2. Create an object lock rule on the bucket
            ol_rule = {
                'DefaultRetention': {
                    'Mode': 'GOVERNANCE',
                    'Days': 1
                }
            }

            pol_bucket_response = s3.put_object_lock_configuration(
                Bucket=_configuration.dell_lakehouse_s3_bucket,
                ObjectLockConfiguration={
                    'ObjectLockEnabled': 'Enabled',
                    'Rule': {
                        'DefaultRetention': {
                            'Mode': 'GOVERNANCE',
                            'Days': 1
                        }
                    }
                }
            )
            print(
                MODULE_NAME + "__main__::Bucket " + _configuration.dell_lakehouse_s3_bucket + " Added object lock rule to bucket.  Object Lock Configuration after creation of Governance rule:")
            ol_bucket_response2 = s3.get_object_lock_configuration(
                Bucket=_configuration.dell_lakehouse_s3_bucket
            )
            print(ol_bucket_response2)

            # 3. Create Schema and Table in Hive with PyStarburst
            print(
                MODULE_NAME + "__main__::Create the following catalog, schema, and table: " + _configuration.ddae_catalog + "." + _configuration.ddae_schema + "." + _configuration.ddae_table_name_customer)
            _ddaeDataProcessor.create_ddae_hive_table(_configuration.ddae_catalog, _configuration.ddae_schema,
                                                      _configuration.ddae_table_name_customer,
                                                      _configuration.ddae_table_location,
                                                      _configuration.ddae_table_schema_customer)

            # 4. Write parquet data to the bucket
            print(MODULE_NAME + "__main__::About to add the following test data to the Data Lakehouse S3 Bucket:")
            _logger.info('__main__::About to add the following test data to the Data Lakehouse S3 Bucket:')
            try:
                test_hive_data = os.path.abspath(os.path.join(currentApplicationDirectory, "testdata",
                                                              "20240716_195545_07788_nxv46_b7038b63-56dc-4c8e-8b2d-595a2e2a4a84"))
                print(MODULE_NAME + "__main__::\t " + test_hive_data)
                _logger.info('__main__::\t' + test_hive_data)
                response = s3.upload_file(test_hive_data, _configuration.dell_lakehouse_s3_bucket,
                                          "hive/customer/20240716_195545_07788_nxv46_b7038b63-56dc-4c8e-8b2d-595a2e2a4a84")
            except ClientError as e:
                logging.error(e)

            # 5. Perform DDAE Query against the bucket via PyStarburst
            print(MODULE_NAME + "__main__::Starting query of customer data in the DDAE using PyStarburst")
            _ddaeDataProcessor.get_customer_data(_configuration.ddae_catalog, _configuration.ddae_schema,
                                                 _configuration.ddae_table_name_customer, MODULE_NAME)

            # 6. Delete the parquet file in the bucket
            delete_object_response = response = s3.delete_object(Bucket=_configuration.dell_lakehouse_s3_bucket,
                                                                 Key='hive/customer/20240716_195545_07788_nxv46_b7038b63-56dc-4c8e-8b2d-595a2e2a4a84')
            print(
                MODULE_NAME + "__main__::Deleting Parquet file: " + 'hive/customer/20240716_195545_07788_nxv46_b7038b63-56dc-4c8e-8b2d-595a2e2a4a84' + ".  Since versioning is enabled we get a delete marker for the object created AND the object is locked with Governance due to the rule we created.")
            _logger.info(
                '__main__::Deleting Parquet file: ' + 'hive/customer/20240716_195545_07788_nxv46_b7038b63-56dc-4c8e-8b2d-595a2e2a4a84' + '.  Since versioning is enabled we get a delete marker for the object created AND the object is locked with Governance due to the rule we created.')

            # 7. Re-run the DDAE Query against the bucket via PyStarburst - WHICH SHOULD FAIL
            print(
                MODULE_NAME + "__main__::Running query of customer data again in the DDAE using PyStarburst.  We shouldn't get any data as we just deleted the data file")
            _ddaeDataProcessor.get_customer_data(_configuration.ddae_catalog, _configuration.ddae_schema,
                                                 _configuration.ddae_table_name_customer, MODULE_NAME)

            # 8. Restore the deleted object version by deleting the delete marker
            print(
                MODULE_NAME + "__main__::Delete the Delete Marker for the Parquet file: " + 'hive/customer/20240716_195545_07788_nxv46_b7038b63-56dc-4c8e-8b2d-595a2e2a4a84 with version id ' +
                delete_object_response['VersionId'] + ".  This will restore the object.")
            _logger.info(
                "__main__::Deleting the Delete Marker for Parquet file: " + 'hive/customer/20240716_195545_07788_nxv46_b7038b63-56dc-4c8e-8b2d-595a2e2a4a84 with version id ' +
                delete_object_response['VersionId'] + ".  This will restore the object.")
            delete_object_version_response = response = s3.delete_object(Bucket=_configuration.dell_lakehouse_s3_bucket,
                                                                         Key='hive/customer/20240716_195545_07788_nxv46_b7038b63-56dc-4c8e-8b2d-595a2e2a4a84',
                                                                         VersionId=delete_object_response['VersionId'],
                                                                         BypassGovernanceRetention=True)

            # 9. Re-run the DDAE Query against the bucket via PyStarburst - WHICH SHOULD SUCCEED
            print(
                MODULE_NAME + "__main__::Running query of customer data one more time in the DDAE using PyStarburst.  We SHOULD get data again as we just restored the deleted data file")
            _ddaeDataProcessor.get_customer_data(_configuration.ddae_catalog, _configuration.ddae_schema,
                                                 _configuration.ddae_table_name_customer, MODULE_NAME)

            # 10. Delete all object versions and delete markers in the bucket
            print(MODULE_NAME + "__main__::Starting to delete all object versions and delete markers in the bucket")
            list_object_versions_response = response = s3.list_object_versions(Bucket=_configuration.dell_lakehouse_s3_bucket)
            if 'Versions' in list_object_versions_response:
                for obj_version in list_object_versions_response['Versions']:
                    obj_key = obj_version['Key']
                    obj_version = obj_version['VersionId']
                    delete_object_version_response = response = s3.delete_object(Bucket=_configuration.dell_lakehouse_s3_bucket,
                                                                                 Key=obj_key, VersionId=obj_version,
                                                                                 BypassGovernanceRetention=True)

            if 'DeleteMarkers' in list_object_versions_response:
                for delete_marker in list_object_versions_response['DeleteMarkers']:
                    marker_key = delete_marker['Key']
                    marker_version = delete_marker['VersionId']
                    delete_marker_version_response = response = s3.delete_object(Bucket=_configuration.dell_lakehouse_s3_bucket,
                                                                                 Key=marker_key,
                                                                                 VersionId=marker_version,
                                                                                 BypassGovernanceRetention=True)
            # 11. Delete bucket
            print(MODULE_NAME + "__main__::Deleting the bucket")
            delete_bucket_response = s3.delete_bucket(Bucket=_configuration.dell_lakehouse_s3_bucket)

            # 12. Drop Table and Schema with PyStarburst
            print(MODULE_NAME + "__main__::Drop the following table: " + _configuration.ddae_catalog + "." + _configuration.ddae_schema + "." + _configuration.ddae_table_name_customer)
            _ddaeDataProcessor.drop_ddae_table(_configuration.ddae_catalog, _configuration.ddae_schema, _configuration.ddae_table_name_customer)

            print(MODULE_NAME + "__main__::Drop the following schema: " + _configuration.ddae_catalog + "." + _configuration.ddae_schema)
            _ddaeDataProcessor.drop_ddae_schema(_configuration.ddae_catalog, _configuration.ddae_schema)

            # 13. Close DDAE session
            print(MODULE_NAME + "__main__::Closing DDAE / Starburst session")
            _logger.info("__main__::Closing DDAE / Starburst session")
            _ddaeSession.sep_session.close()

    except Exception as e:
        print(MODULE_NAME + '__main__::The following unexpected error occurred: '
              + str(e) + "\n" + traceback.format_exc())
