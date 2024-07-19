"""
DELL Data Analytics Engine - Starburst.
"""
import traceback

import trino
from pystarburst import Session
from pystarburst.types import IntegerType, StringType, StructField, StructType, TimestampType, DoubleType, BooleanType, \
    ArrayType

from tabulate import tabulate
import pandas as pd

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET


class DDAEException(Exception):
    pass


class TrinioException(Exception):
    pass


class DDAEAuthentication(object):
    """
    Stores DDAE Session Information
    """

    def __init__(self, protocol, host, username, password, port, catalog, schema, logger):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.logger = logger
        self.catalog = catalog
        self.schema = schema
        self.logger.info('DDAEAuthentication::DDAE Session object instance initialization complete.')
        self.url = "{0}://{1}:{2}".format(self.protocol, self.host, self.port)
        self.sep_session = ''

        # Disable warnings
        # urllib3.disable_warnings()

    def get_url(self):
        """
        Returns a url made from protocol, host, and port.
        """
        return self.url

    def get_token(self):
        """
        Returns a session object
        """
        return self.sep_session

    def connect(self):
        """
        Connect to DDAE and generate a session object
        """
        if self.protocol == 'https':
            db_parameters = {
                "host": self.host,
                "port": self.port,
                "http_scheme": self.protocol,
                "verify": False,
                "roles": {"system": "ROLE{sysadmin}"},
                "auth": trino.auth.BasicAuthentication(self.username, self.password)
            }
        else:
            db_parameters = {
                "host": self.host,
                "port": self.port,
                "http_scheme": self.protocol,
                "roles": {"system": "ROLE{sysadmin}"},
                "user": self.username
            }

        self.logger.info(
            'DDAEAuthentication::connect()::We are about to attempt to build a session to Starburst at the following location : '
            + "{0}://{1}:{2}".format(self.protocol, self.host,
                                     self.port) + ' with user ' + self.username + ' against catalog ' + self.catalog + ' and schema ' + self.schema)

        try:
            session = Session.builder.configs(db_parameters).create()

            if session is None:
                self.logger.info('DDAEAuthentication::connect()::Session object returned is None.')
                self.sep_session = None
            else:
                self.logger.info('DDAEAuthentication::connect()::Session object returned is not None.')
                self.sep_session = session

        except Exception as e:
            self.logger.error('DDAEAuthentication::connect()::The following unexpected '
                              'exception occurred: ' + str(e) + "\n" + traceback.format_exc())

    def disconnect(self):
        """
        Disconnect from DDAE and release the session object
        """

        self.logger.info(
            'DDAEAuthentication::disconnect()::Closing DDAE / Starburst session at the following location : '
            + "{0}://{1}:{2}".format(self.protocol, self.host,
                                     self.port) + ' with user ' + self.username + ' against catalog ' + self.catalog + ' and schema ' + self.schema)

        try:
            self.sep_session.close()


        except Exception as e:
            self.logger.error('DDAEAuthentication::disconnect()::The following unexpected '
                              'exception occurred: ' + str(e) + "\n" + traceback.format_exc())


class DDAEDataProcessor(object):
    """
    Perform Data Operations against DDAE/Starburst
    """

    def __init__(self, configuration, sepsession, logger):
        self.configuration = configuration
        self.sepsession = sepsession
        self.logger = logger
        self.response_xml_file = None

    def create_ddae_hive_table(self, catalog, schema, table_name, table_location, table_columns):

        # Create the schema and table to store the Dell Object Log Data
        self.logger.info(
            'DDAEDataProcessor::create_ddae_hive_table()::Creating the following catalog, schema, and table: ' + catalog + '.' + schema + '.' + table_name)
        try:
            # Create the schema if needed
            sqlString = "CREATE SCHEMA IF NOT EXISTS {0}.{1} WITH (location = '{2}')".format(catalog, schema, table_location)
            self.sepsession.sep_session.sql(sqlString).collect()

            # Create the table if needed
            sqlString2 = "CREATE TABLE IF NOT EXISTS {0}.{1}.{2} ({3}) WITH (external_location = '{4}{5}', format = 'PARQUET')".format(
                catalog, schema, table_name, table_columns, table_location, table_name)
            self.sepsession.sep_session.sql(sqlString2).collect()

        except Exception as e:
            self.logger.error('DDAEDataProcessor::create_ddae_hive_table()::The following unexpected '
                              'exception occurred: ' + str(e) + "\n" + traceback.format_exc())

    def drop_ddae_table(self, catalog, schema, table_name):

        # Log the table to be dropped
        self.logger.info(
            'DDAEDataProcessor::drop_ddae_table()::Dropping the following catalog, schema, and table: ' + catalog + '.' + schema + '.' + table_name)
        try:

            # Drop the table if exists
            sqlString = "DROP TABLE IF EXISTS {0}.{1}.{2}".format(catalog, schema, table_name)
            self.sepsession.sep_session.sql(sqlString).collect()

        except Exception as e:
            self.logger.error('DDAEDataProcessor::drop_ddae_table()::The following unexpected '
                              'exception occurred: ' + str(e) + "\n" + traceback.format_exc())

    def drop_ddae_schema(self, catalog, schema):

        # Log the schema to be dropped
        self.logger.info(
            'DDAEDataProcessor::drop_ddae_schema()::Dropping the following catalog and schema: ' + catalog + '.' + schema)
        try:

            # Drop the table if exists
            sqlString = "DROP SCHEMA IF EXISTS {0}.{1}".format(catalog, schema)
            self.sepsession.sep_session.sql(sqlString).collect()

        except Exception as e:
            self.logger.error('DDAEDataProcessor::drop_ddae_schema()::The following unexpected '
                              'exception occurred: ' + str(e) + "\n" + traceback.format_exc())

    def create_ddae_iceberg_table(self, catalog, schema, table_name, table_location, table_columns):

        # Create the schema and table to store the Dell Object Log Data
        self.logger.info(
            'DDAEDataProcessor::create_table()::Creating the following catalog, schema, and table: ' + catalog + '.' + schema + '.' + table_name)
        try:
            # Create the schema if needed
            sqlString = "CREATE SCHEMA IF NOT EXISTS {0}.{1} WITH (location = '{2}')".format(catalog, schema, table_location)
            self.sepsession.sep_session.sql(sqlString).collect()

            # Create the table if needed
            sqlString2 = "CREATE TABLE IF NOT EXISTS {0}.{1}.{2} ({3}) WITH (location = '{4}{5}', format = 'PARQUET')".format(
                catalog, schema, table_name, table_columns, table_location, table_name)
            self.sepsession.sep_session.sql(sqlString2).collect()

        except Exception as e:
            self.logger.error('DDAEDataProcessor::create_table()::The following unexpected '
                              'exception occurred: ' + str(e) + "\n" + traceback.format_exc())

    def get_list_of_catalogs(self):
        try:
            # Create the schema and table to store the Dell Object Log Data
            self.logger.info(
                'DDAEDataProcessor::get_list_of_catalogs()::Getting a list of catalogs available in the session: ')

            # Get a list of catalogs from the session
            sqlString = "select * from {0}.{1}.catalogs".format("system", "metadata")
            df_catalogs = self.sepsession.sep_session.sql(sqlString).collect()

            # Print catalog names
            s_list_of_catalogs = ''
            for row in df_catalogs:
                s_list_of_catalogs += row['catalog_name'] + ', '

            self.logger.info(
                'DDAEDataProcessor::get_list_of_catalogs()::The following catalogs are available in the session: ' + s_list_of_catalogs)

        except Exception as e:
            self.logger.error('DDAEDataProcessor::get_list_of_catalogs()::The following unexpected '
                              'exception occurred: ' + str(e) + "\n" + traceback.format_exc())

    def get_table_details(self, catalog, schema, table_name):
        try:
            # Create the schema and table to store the Dell Object Log Data
            self.logger.info(
                'DDAEDataProcessor::get_table_details()::Getting details for the following table: ' + catalog + '.' + schema + '.' + table_name)

            # Get a list of attributes for the specified catalog, schema, and table
            df_table = self.sepsession.sep_session.table(catalog + "." + schema + "." + table_name)

            self.logger.info(
                'DDAEDataProcessor::get_table_details()::The following details are available for this table: ')

            # Print columns names
            self.print_table_data(df_table)

        except Exception as e:
            self.logger.error('DDAEDataProcessor::get_table_details()::The following unexpected '
                              'exception occurred: ' + str(e) + "\n" + traceback.format_exc())

    def get_customer_data(self, catalog, schema, table_name, module_name):
        try:
            # Create the schema and table to store the Dell Object Log Data
            self.logger.info(
                'DDAEDataProcessor::get_customer_data()::Getting data from the following table: ' + catalog + '.' + schema + '.' + table_name)

            # Get data for the specified catalog, schema, and table
            sql_statement = "SELECT * FROM {0} LIMIT 10".format(catalog + "." + schema + "." + table_name)
            pydf_customer = self.sepsession.sep_session.sql(sql_statement).collect()
            df_length = len(pydf_customer)

            if df_length > 0:
                print(module_name + "DDAEDataProcessor::get_customer_data()::Retrieved the following customer data: ")
                self.logger.info(
                    'DDAEDataProcessor::get_customer_data()::Retrieved the following customer data: ')
                # Print customer data
                self.print_table_data(pydf_customer)
            else:
                print(module_name + "DDAEDataProcessor::get_customer_data()::No data was returned from the table: " + catalog + '.' + schema + '.' + table_name)
                self.logger.info(
                    'DDAEDataProcessor::get_customer_data()::No data was returned from the table: ' + catalog + '.' + schema + '.' + table_name)

        except Exception as e:
            self.logger.error('DDAEDataProcessor::get_customer_data()::The following unexpected '
                              'exception occurred: ' + str(e) + "\n" + traceback.format_exc())

    def print_table_data(self, pydf_table):
        try:
            # Print the table data
            self.logger.info(
                'DDAEDataProcessor::print_table_data()::Printing the data from the table: ')

            print(tabulate(pydf_table, headers='keys', tablefmt='psql'))
            self.logger.info(
                'DDAEDataProcessor::print_table_data()::\n' + tabulate(pydf_table, headers='keys', tablefmt='psql'))
        except Exception as e:
            self.logger.error('DDAEDataProcessor::print_table_data()::The following unexpected '
                              'exception occurred: ' + str(e) + "\n" + traceback.format_exc())
