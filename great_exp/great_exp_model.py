from typing import List, Optional
import yaml
import json
import os

import great_expectations as gx
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError

from request_models import connection_enum_and_metadata as conn_enum
from utils import find_validation_result
from logging_config import dqt_logger

class GreatExpectationsModel:
    def __init__(self, quality_checks: List[dict]):
        """
        Initializes the quality_checks and creates a new great_expectations context 
        
        :param quality_checks (List[dict]): The list of expectations to validate
        """
        self.ge_context = gx.get_context()
        self.quality_checks = quality_checks


    class GE_SQL_Datasource:
        def __init__(self, datasource_type: str, datasource_name: str, host: str, 
                     port: int, username: str, password: str, database: str, 
                     schema_name: str, table_name: str):
            
            self.datasource_type = datasource_type
            self.datasource_name = datasource_name
            self.host = host
            self.password = password
            self.database = database
            self.schema_name = schema_name
            self.table_name = table_name
            self.username = username
            self.port = port

        def get_database_config(self) -> yaml:
            """
            Returns the correct YAML config based on the datasource type
            """
            if self.datasource_type == conn_enum.ConnectionEnum.MYSQL:
                return self.__get_mysql_datasource_config()
            elif self.datasource_type == conn_enum.ConnectionEnum.POSTGRES:
                return self.__get_postgres_datasource_config()
            elif self.datasource_type == conn_enum.ConnectionEnum.REDSHIFT:
                return self.__get_redshift_datasource_config()
            elif self.datasource_type == conn_enum.ConnectionEnum.SNOWFLAKE:
                return self.__get_snowflake_datasource_config()
            elif self.datasource_type == conn_enum.ConnectionEnum.BIGQUERY:
                return self.__get_bigquery_datasource_config()
            elif self.datasource_type == conn_enum.ConnectionEnum.TRINO:
                return self.__get_trino_database_config()
            elif self.datasource_type == conn_enum.ConnectionEnum.ATHENA:
                return self.__get_athena_database_config()
            elif self.datasource_type == conn_enum.ConnectionEnum.CLICKHOUSE:
                return self.__get_clickhouse_database_config()
            else:
                return self.__get_other_database_config()


        def __get_mysql_datasource_config(self) -> yaml:
            """
            Creates a JSON file with the predefined configurations for great_expectations library for mysql.

            :return datasource_config_for_mysql_yaml (yaml): The config file for mysql converted to YAML
            """
            datasource_config_for_mysql_json = {
                "name": self.datasource_name,
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "SqlAlchemyExecutionEngine",
                    "credentials": {
                    "host": self.host,
                    "port": str(self.port),
                    "username": self.username,
                    "password": self.password,
                    "database": self.database,
                    "drivername": "mysql+pymysql"
                    }
                },
                "data_connectors": {
                    "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                        "batch_identifiers": [
                            "default_identifier_name"
                        ]
                    },
                        "default_inferred_data_connector_name": {
                        "class_name": "InferredAssetSqlDataConnector",
                        "include_schema_name": True,
                        "introspection_directives": {
                            "schema_name": self.schema_name
                        }
                    },
                    "default_configured_data_connector_name": {
                    "class_name": "ConfiguredAssetSqlDataConnector",
                        "assets": {
                            self.table_name: {
                            "class_name": "Asset",
                            "schema_name": self.schema_name
                            }
                        }
                    }
                }
            }
            datasource_config_for_mysql_yaml = yaml.dump(datasource_config_for_mysql_json)
            dqt_logger.info("Created datasource config for mysql")
            return datasource_config_for_mysql_yaml

        def __get_postgres_datasource_config():
            pass

        def __get_redshift_datasource_config():
            pass

        def __get_snowflake_datasource_config():
            pass

        def __get_bigquery_datasource_config():
            pass

        def __get_trino_database_config():
            pass

        def __get_athena_database_config():
            pass

        def __get_clickhouse_database_config():
            pass

        def __get_other_database_config():
            pass


    class GE_File_Datasource:
        def __init__(self, datasource_name: str, dir_name: str, datasource_type: str):
            self.datasource_name = datasource_name
            self.dir_name = dir_name
            self.datasource_type = datasource_type

        def get_file_config(self) -> yaml:
            if self.datasource_type in {conn_enum.ConnectionEnum.CSV, conn_enum.ConnectionEnum.EXCEL,
                                        conn_enum.ConnectionEnum.JSON, conn_enum.ConnectionEnum.PARQUET,
                                        conn_enum.ConnectionEnum.ORC, conn_enum.ConnectionEnum.FILESERVER}:
                return self.__get_pandas_datasource_config()
            elif self.datasource_type == "pyspark": # TODO: Change when pyspark config is added
                return self.__get_pyspark_datasource_config()
            

        def __get_pandas_datasource_config(self) -> yaml:
            """
            Creates a JSON file with the predefined configurations for great_expectations library for pandas.

            :return datasource_config_for_pandas_yaml (yaml): The config file for pandas converted to YAML
            """
            
            datasource_config_for_pandas_json = {
                "name": self.datasource_name,
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "PandasExecutionEngine"
                },
                "data_connectors": {
                    "default_inferred_data_connector_name": {
                        "class_name": "InferredAssetFilesystemDataConnector",
                        "base_directory": self.dir_name,
                        "default_regex": {
                            "group_names": ["data_asset_name"],
                            "pattern": "(.*)"
                        }
                    },
                    "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "assets": {
                            "my_runtime_asset_name": {
                                "batch_identifiers": ["runtime_batch_identifier_name"]
                            }
                        }
                    }
                }
            }

            datasource_config_for_pandas_yaml = yaml.dump(datasource_config_for_pandas_json)
            dqt_logger.info("Created datasource config for pandas")
            return datasource_config_for_pandas_yaml

        def __get_pyspark_datasource_config():
            pass


    def __create_datasource(self, config_yaml: yaml) -> None:
        """
        Tests the provided yaml config file for the great_expectations library

        :param config_yaml (yaml): The configuration YAML file that needs to be added in the great_expectations.yml file

        :return: None
        """
        try:
            self.ge_context.test_yaml_config(yaml_config=config_yaml)
            sanitize_yaml_and_save_datasource(self.ge_context, config_yaml, overwrite_existing=True)
            dqt_logger.info("Tested and saved datasource config")
        except Exception as e:
            error = f"Datasource could not be created\n{str(e)}"
            dqt_logger.error(error)
            raise Exception(error)
        

    def __create_batch_request_json_for_db(datasource_name: str, data_asset_name: str, 
                                           limit: Optional[int] = 0) -> dict:
        """
        Creates a batch request json for database

        :param datasource_name (str): Name of datasource
        :param data_asset_name (str): Name of table
        :param limit (int) [default = 0]: Number of validation batches to be returned

        :return batch_request_json (dict): A dict containing the params required to generate a batch of data
        """
        batch_request_json = {}
        
        if limit == 0:
            batch_request_json = {'datasource_name': datasource_name, 
                            'data_connector_name': 'default_configured_data_connector_name', 
                            'data_asset_name': data_asset_name}
        else:
            batch_request_json = {'datasource_name': datasource_name, 
                            'data_connector_name': 'default_configured_data_connector_name', 
                            'data_asset_name': data_asset_name, 
                            'limit': limit}
            
        dqt_logger.debug(f"Created batch request JSON for database:\n{str(batch_request_json)}")
        dqt_logger.info("Created batch request JSON for database")
        return batch_request_json
    

    def __create_batch_request_json_for_file(datasource_name: str, data_asset_name: str,
                                                limit: Optional[int] = 0) -> dict:
        """
        Creates a batch request json for file

        :param datasource_name (str): Name of datasource
        :param data_asset_name (str): Name of file
        :param limit (int) [default = 0]: Number of validation batches to be returned

        :return batch_request_json (dict): A dict containing the params required to generate a batch of data
        """
        batch_request_json = {}

        if limit == 0:
            batch_request_json = {'datasource_name': datasource_name, 
                            'data_connector_name': 'default_inferred_data_connector_name', 
                            'data_asset_name': data_asset_name}
        else:
            batch_request_json = {'datasource_name': datasource_name, 
                            'data_connector_name': 'default_inferred_data_connector_name', 
                            'data_asset_name': data_asset_name, 
                            'limit': limit}
            
        dqt_logger.debug(f"Created batch request JSON for file:\n{str(batch_request_json)}")
        dqt_logger.info("Created batch request JSON for file")
        return batch_request_json
    

    def __create_or_load_expectation_suite(self, expectation_suite_name: str) -> None:
        """
        This function creates a new expectations suite

        :param expectation_suite_name (str): Name of the newly created expectation suite

        :return: None
        """
        try:
            suite = self.ge_context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
            dqt_logger.info(f"""Loaded ExpectationSuite "{suite.expectation_suite_name}" 
                containing {len(suite.expectations)} expectations.""")
        except DataContextError:
            suite = self.ge_context.add_expectation_suite(expectation_suite_name=expectation_suite_name)
            dqt_logger.info(f'Created ExpectationSuite "{suite.expectation_suite_name}".')


    def __create_validator(self, expectation_suite_name: str, batch_request: json):
        """
        This function creates a validator using a batch request and expectation suite

        :param expectation_suite_name (str): Name of the expectation suite to be used
        :param batch_request (json): The batch request json that is to be processed

        :return validator: A validator used to execute expectations checks
        """
        try:
            validator = self.ge_context.get_validator(
                batch_request=BatchRequest(**batch_request),
                expectation_suite_name=expectation_suite_name
            )

            validator.save_expectation_suite(discard_failed_expectations=False)
            dqt_logger.info("Validator created and expectation suite added")
            return validator
        except Exception as validator_error:
            error = f"An error occured while creating validator:\n{str(validator_error)}"
            dqt_logger.error(error)
            raise Exception(error)

    def __add_expectations_to_validator(validator, expectations: List[dict]) -> None:
        """
        This function adds the provided expectations to the validation suite

        :param validator: Validator object
        :param expectations (List[dict]): List of expectations

        :return: None
        """
        
        if len(expectations) == 0:
            error = "An error occured while adding expectations to expectation_suite: No expectations provided"
            dqt_logger.error(error)
            raise Exception(error)
        
        # adding expectations to the validator
        for expectation in expectations:
            expectation_type = expectation.expectation_type
            kwargs = expectation.kwargs

            expectation_func = getattr(validator, expectation_type)
            expectation_func(**kwargs)
            
        # saving expectation suite
        validator.save_expectation_suite(discard_failed_expectations=False)
        dqt_logger.info("Successfully added expectations")


    def __create_and_execute_checkpoint(self, expectation_suite_name: str, validator, batch_request: json) -> json:
        """
        This function creates a new checkpoint and executes it.
        A great_expectations checkpoint includes a batch of data that needs to be validated,
        a expectations suite which contains the checks that need to be applied and 
        a validator that applies the checks defined in the expectation suite on the 
        batch of data.

        :param expectation_suite_name (str): The name of the expectation suite  
        :param validator: Validator object
        :param batch_request (json): The batch request json to be validated

        :return: JSON containing validation results
        """
        checkpoint_config = {
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": expectation_suite_name
                }
            ]
        }

        try:
            checkpoint = SimpleCheckpoint(
                f"{validator.active_batch_definition.data_asset_name}_{expectation_suite_name}",
                self.ge_context,
                **checkpoint_config
            )
            checkpoint_result = checkpoint.run()
            dqt_logger.info("Successfully created and executed checkpoint, returning validation results")
            return checkpoint_result
        except Exception as checkpoint_error:
            error = f"An error occured while creating or executing checkpoint:\n{str(checkpoint_error)}"
            dqt_logger.error(error)
            raise Exception(error)


def run_quality_checks_for_db(datasource_type: str, hostname: str, password: str, username: str, 
                                port: int, datasource_name: str, schema_name: str, database: str, 
                                table_name: str, quality_checks: List[dict], batch_limit: Optional[int] = 0) -> json:
    """
    Triggers the functions of great_expectations library in the required sequence

    :param datasource_type (str): The type of datasource, e.g.: file, mysql, snowflake, csv, etc.
    :param datasource_name (str): The name of datasource
    :param quality_checks (List[dict]): The list of checks that are to be performed on the file, formatted as required by 
    the great_expectations library
    :param batch_limit (int): The number of batches to be returned after validation
    :param hostname (str): The host IPv4 address to connect to
    :param password (str): The password required to connect to the host server
    :param username (str): The name of the user who wants to connect to the host server
    :param port (int): The port number to be connected on
    :param database (str): The name of the database that needs to be accessed
    :param table_name (str): The name of the table whose data needs to be accessed
    
    :return checkpoint_results (json): The generated validation results
    """
    expectation_suite_name_db = f"{datasource_name}_{username}_{table_name}_{port}_{hostname}" # expectation suite name format for db
    
    ge = GreatExpectationsModel(quality_checks=quality_checks)
    ge_sql = ge.GE_SQL_Datasource(datasource_type=datasource_type, host=hostname, password=password, 
                                    username=username, database=database, schema_name=schema_name, 
                                    datasource_name=datasource_name, port=port, table_name=table_name)
    
    database_config_yaml = ge_sql.get_database_config()
    ge.__create_datasource(config_yaml=database_config_yaml)
    ge.__create_or_load_expectation_suite(expectation_suite_name=expectation_suite_name_db)
    batch_request_json = ge.__create_batch_request_json_for_db(datasource_name=datasource_name,
                                            data_asset_name=table_name,
                                            limit=batch_limit)
    db_validator = ge.__create_validator(expectation_suite_name=expectation_suite_name_db, 
                                        batch_request=batch_request_json)
    ge.__add_expectations_to_validator(validator=db_validator,expectations=ge.quality_checks)
    checkpoint_results = ge.__create_and_execute_checkpoint(expectation_suite_name=expectation_suite_name_db,
                                                            batch_request=batch_request_json,
                                                            validator=db_validator)
    validation_results = find_validation_result(data=checkpoint_results)
    return validation_results


def run_quality_check_for_file(datasource_type: str, datasource_name: str, dir_path: str, quality_checks: List[dict], 
                            file_name: str, batch_limit: Optional[int] = 0) -> json:
    """
    Triggers the functions of great_expectations library in the required sequence

    :param datasource_type (str): The type of datasource, e.g.: file, mysql, snowflake, csv, etc.
    :param datasource_name (str): The name of datasource
    :param dir_path (str): The path where the file is stored
    :param quality_checks (List[dict]): The list of checks that are to be performed on the file, formatted as required by 
    the great_expectations library
    :param file_name (str): The name of the file
    :param batch_limit (int): The number of batches to be returned after validation

    :return checkpoint_results (json): The generated validation results
    """
    dir_name = os.path.basename(dir_path) # extract name of dir from dir_path to create expectation_suite_name
    expectation_suite_name_file = f"{datasource_name}_{dir_name}_{datasource_type}_{file_name}" # expectation suite name format for file
    
    ge = GreatExpectationsModel(quality_checks=quality_checks)
    ge_file = ge.GE_File_Datasource(datasource_name=datasource_name, datasource_type=datasource_type, dir_name=dir_path)
    
    file_config_yaml = ge_file.get_file_config()
    ge.__create_datasource(config_yaml=file_config_yaml)
    ge.__create_or_load_expectation_suite(expectation_suite_name=expectation_suite_name_file)
    batch_request_json = ge.__create_batch_request_json_for_file(datasource_name=datasource_name, 
                                                                data_asset_name=file_name,
                                                                limit=batch_limit)
    file_validator = ge.__create_validator(expectation_suite_name=expectation_suite_name_file)
    ge.__add_expectations_to_validator(validator=file_validator,
                                    expectations=quality_checks)
    checkpoint_results = ge.__create_and_execute_checkpoint(expectation_suite_name=expectation_suite_name_file,
                                                            batch_request=batch_request_json,
                                                            validator=file_validator)
    validation_results = find_validation_result(data=checkpoint_results)
    return validation_results
