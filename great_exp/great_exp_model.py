import json
import os
from typing import Dict, List, Optional, Union
import uuid
import yaml

import great_expectations as gx
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError
from great_expectations.validator.validator import Validator

from database.db_models.job_run_status import JobRunStatusEnum
from job_state_singleton import JobStateSingleton
from logging_config import dqt_logger
from request_models import connection_enum_and_metadata as conn_enum
from utils import find_validation_result


class GreatExpectationsModel:
    def __init__(self):
        """
        Creates a new great_expectations context 
        """
        self.ge_context = gx.get_context()

    class GESQLDatasource:
        def __init__(self, 
                     datasource_type: str, 
                     datasource_name: str, 
                     host: str, 
                     port: int, 
                     username: str, 
                     password: str, 
                     database: str, 
                     schema_name: str, 
                     table_name: str
            ):
            """
            Initializes instance variables

            :param datasource_type (str): The type of the datasource (mysql, postgres, redshift, etc.)
            :param datasource_name (str): The name of the datasource
            :param host (str): The address of the host
            :param port (int): The port number of the host to connect
            :param username (str): The name of the user that wants to connect
            :param password (str): The password of the user that wants to connect
            :param database (str): The name of the database the user wants to connect
            :param schema_name (str): The name of the database schema
            :param table_name (str): The name of the table in the database to connect
            
            :return: None
            """
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
            
            :return: A YAML configuration for the datasource
            """
            if self.datasource_type == conn_enum.Database_Datasource_Enum.MYSQL:
                return self.__get_mysql_datasource_config()
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.POSTGRES:
                return self.__get_postgres_datasource_config()
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.REDSHIFT:
                return self.__get_redshift_datasource_config()
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.SNOWFLAKE:
                return self.__get_snowflake_datasource_config()
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.BIGQUERY:
                return self.__get_bigquery_datasource_config()
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.TRINO:
                return self.__get_trino_database_config()
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.ATHENA:
                return self.__get_athena_database_config()
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.CLICKHOUSE:
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
                        "batch_identifiers": ["default_identifier_name"]
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
            info_msg = "Created datasource config for mysql"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return datasource_config_for_mysql_yaml

        def __get_postgres_datasource_config(self) -> yaml:
            """
            Creates a JSON file with the predefined configurations for great_expectations library for postgres.

            :return datasource_config_for_postgres_yaml (yaml): The config file for postgres converted to YAML
            """
            datasource_config_for_postgres_json = {
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
                    "drivername": "postgresql"
                    }
                },
                "data_connectors": {
                    "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"]
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

            datasource_config_for_postgres_yaml = yaml.dump(datasource_config_for_postgres_json)
            info_msg = "Created datasource config for postgres"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return datasource_config_for_postgres_yaml

        def __get_redshift_datasource_config(self) -> yaml:
            """
            Creates a JSON file with the predefined configurations for great_expectations library for mysql.

            :return datasource_config_for_redshift_yaml (yaml): The config file for Amazon Redshift converted to YAML
            """
            datasource_config_for_redshift_json = {
                "name": self.datasource_name,
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "SqlAlchemyExecutionEngine",
                    "credentials": {
                    "host": self.host,
                    "port": self.port,
                    "username": self.username,
                    "password": self.password,
                    "database": self.database,
                    "query": {
                        "sslmode": "prefer"
                    },
                    "drivername": "postgresql+psycopg2"
                    }
                },
                "data_connectors": {
                        "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                        },
                        "default_inferred_data_connector_name": {
                        "class_name": "InferredAssetSqlDataConnector",
                        "include_schema_name": True,
                            "introspection_directives": {
                                "schema_name": self.schema_name}
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
            
            datasource_config_for_redshift_yaml = yaml.dump(datasource_config_for_redshift_json)
            info_msg = "Created datasource config for postgres"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return datasource_config_for_redshift_yaml 

        def __get_snowflake_datasource_config(self) -> yaml:
            """FUTURE: Make changes in input params to implement for all three mechanisms of snowflake:
            1. User and Password
            2. Single sign-on (SSO)
            3. Key pair authentication
            Also include an if condition
            """
            
            datasource_config_for_snowflake_user_and_password_json = {
                "name": self.datasource_name,
                "class_name": "Datasource",
                    "execution_engine": {
                        "class_name": "SqlAlchemyExecutionEngine",
                        "credentials": {
                        "host": self.host,
                        "username": self.username,
                        "database": self.database,
                        "query": {
                            "schema": self.schema_name,
                            "warehouse": "{warehouse}",
                            "role": "{role}"
                        },
                        "password": self.password,
                        "drivername": "snowflake"
                        }
                    },
                    "data_connectors": {
                        "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
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
        

            datasource_config_for_snowflake_sso_json = {
                "name": self.datasource_name,
                "class_name": "Datasource",
                    "execution_engine": {
                        "class_name": "SqlAlchemyExecutionEngine",
                        "credentials": {
                        "host": self.host,
                        "username": self.username,
                        "database": self.database,
                        "query": {
                            "schema": self.schema_name,
                            "warehouse": "{warehouse}",
                            "role": "{role}"
                        },
                        "connect_args": {
                            "authenticator": "{authenticator_url}"
                        },
                        "drivername": "snowflake"
                        }
                    },
                    "data_connectors": {
                        "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
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
            
            datasource_config_for_snowflake_key_pair_auth_json = {
                "name": self.datasource_name,
                "class_name": "Datasource",
                    "execution_engine": {
                        "class_name": "SqlAlchemyExecutionEngine",
                        "credentials": {
                        "host": self.host,
                        "username": self.username,
                        "database": self.database,
                        "query": {
                            "schema": self.schema_name,
                            "warehouse": "{warehouse}",
                            "role": "{role}"
                        },
                        "private_key_path": "{private_key_path}",
                        "private_key_passphrase": "{private_key_passphrase}",
                        "drivername": "snowflake"
                        }
                    },
                    "data_connectors": {
                        "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
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
            
            raise NotImplementedError("Snowflake datasource configuration is not yet implemented.")

        def __get_bigquery_datasource_config(self) -> yaml:
            raise NotImplementedError("BigQuery datasource configuration is not yet implemented.")

        def __get_trino_database_config(self) -> yaml:
            raise NotImplementedError("Trino datasource configuration is not yet implemented.")

        def __get_athena_database_config(self) -> yaml:
            raise NotImplementedError("Athena datasource configuration is not yet implemented.")

        def __get_clickhouse_database_config(self) -> yaml:
            raise NotImplementedError("Clickhouse datasource configuration is not yet implemented.")

        def __get_other_database_config(self) -> yaml:
            """
            Creates a JSON file with the predefined configurations for great_expectations library for other databases.
            E.g.: MSSQL.
            This connection is created using a connection string.

            :return datasource_config_for_other_yaml (yaml): The config file for other database converted to YAML
            """
            datasource_config_for_other_json = {
                "name": self.datasource_name,
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "SqlAlchemyExecutionEngine",
                    "connection_string": "{connection_string}"
                    },
                    "data_connectors": {
                        "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
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

            datasource_config_for_other_yaml = yaml.dump(datasource_config_for_other_json)
            info_msg = f"Created datasource config for {self.datasource_type}"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return datasource_config_for_other_yaml


    class GEFileDatasource:
        def __init__(self, datasource_name: str, dir_name: str, datasource_type: str):
            self.datasource_name = datasource_name
            self.dir_name = dir_name
            self.datasource_type = datasource_type

        def get_file_config(self) -> yaml:
            if self.datasource_type in [item.value for item in conn_enum.File_Datasource_Enum]:
                return self.__get_pandas_datasource_config()
            elif self.datasource_type == "pyspark": # TODO: Change when pyspark config is added
                return self.__get_pyspark_datasource_config()
            else:
                error_msg = f"Unsupported datasource type: {self.datasource_type}"
                dqt_logger.error(error_msg)
                raise ValueError(error_msg)
            
        def __create_file_datasource_config(self, execution_engine_class: str) -> yaml:
            """
            Creates a JSON file with the predefined configurations for great_expectations library for pandas or pyspark.

            :return datasource_config_for_file_yaml (yaml): The config file for pandas pr pyspark converted to YAML
            """
            
            datasource_config_for_file_json = {
                "name": self.datasource_name,
                "class_name": "Datasource",
                "execution_engine": {"class_name": execution_engine_class},
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
                            "my_runtime_asset_name": {"batch_identifiers": ["runtime_batch_identifier_name"]}
                        }
                    }
                }
            }

            datasource_config_for_file_yaml = yaml.dump(datasource_config_for_file_json)
            info_msg = f"Created datasource config using {execution_engine_class}"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return datasource_config_for_file_yaml

        def __get_pandas_datasource_config(self) -> yaml:
            """
            Creates a JSON file with the predefined configurations for great_expectations library for pandas.

            :return datasource_config_for_pandas_yaml (yaml): The config file for pandas converted to YAML
            """
            
            return self.__create_file_datasource_config(execution_engine_class="PandasExecutionEngine")
            
        def __get_pyspark_datasource_config(self) -> yaml:
            """
            Creates a JSON file with the predefined configurations for great_expectations library for pyspark.

            :return datasource_config_for_pyspark_yaml (yaml): The config file for pyspark converted to YAML
            """
            return self.__create_file_datasource_config(execution_engine_class="SparkDFExecutionEngine")
          
          
          
    def create_datasource(self, config_yaml: yaml) -> None:
        """
        Tests the provided yaml config file for the great_expectations library

        :param config_yaml (yaml): The configuration YAML file that needs to be added in the great_expectations.yml file

        :return: None
        
        :raises Exception: If the datasource could not be created or saved.
        """
        try:
            self.ge_context.test_yaml_config(yaml_config=config_yaml)
            sanitize_yaml_and_save_datasource(self.ge_context, config_yaml, overwrite_existing=True)
            info_msg = "Tested and saved datasource config"
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            dqt_logger.info(info_msg)
        except Exception as e:
            error_msg = f"Failed to create datasource. Error: {str(e)}"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
            raise Exception(error_msg)
     
    def __create_batch_request_json_for_datasource(self, 
                                                   datasource_name: str, 
                                                   data_asset_name: str, 
                                                   data_connector_name: str, 
                                                   limit: Optional[int] = 0
                                                   ) -> dict:
        """
        Creates a batch request json for the given datasource

        :param datasource_name (str): Name of datasource
        :param data_asset_name (str): Name of table
        :param data_connector_name (str): Type of the data connector used
        :param limit (int) [default = 0]: Number of validation batches to be returned

        :return batch_request_json (dict): A dict containing the params required to generate a batch of data
        """
        batch_request_json = {'datasource_name': datasource_name, 
                            'data_connector_name': data_connector_name, 
                            'data_asset_name': data_asset_name}
        
        if limit > 0:
            batch_request_json['limit'] = limit
            
        dqt_logger.debug(f"Created batch request JSON for database:\n{str(batch_request_json)}")
        info_msg = "Created batch request for JSON for database"
        dqt_logger.info(info_msg)
        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
        return batch_request_json

    def create_batch_request_json_for_db(self, 
                                         datasource_name: str, 
                                         data_asset_name: str, 
                                         limit: Optional[int] = 0
                                        ) -> dict:
        """
        Creates a batch request json for database

        :param datasource_name (str): Name of datasource
        :param data_asset_name (str): Name of table
        :param limit (int) [default = 0]: Number of validation batches to be returned

        :return batch_request_json (dict): A dict containing the params required to generate a batch of data
        """
        return self.__create_batch_request_json_for_datasource(datasource_name=datasource_name,
                                                               data_asset_name=data_asset_name,
                                                               data_connector_name="default_configured_data_connector_name",
                                                               limit=limit)
    
    def create_batch_request_json_for_file(self, datasource_name: str, 
                                           data_asset_name: str,
                                           limit: Optional[int] = 0
                                           ) -> dict:
        """
        Creates a batch request json for file

        :param datasource_name (str): Name of datasource
        :param data_asset_name (str): Name of file
        :param limit (int) [default = 0]: Number of validation batches to be returned

        :return batch_request_json (dict): A dict containing the params required to generate a batch of data
        """
        return self.__create_batch_request_json_for_datasource(datasource_name=datasource_name,
                                                               data_asset_name=data_asset_name,
                                                               data_connector_name="default_inferred_data_connector_name",
                                                               limit=limit)

    def create_or_load_expectation_suite(self, expectation_suite_name: str) -> None:
        """
        This function creates a new expectations suite

        :param expectation_suite_name (str): Name of the newly created expectation suite

        :return: None
        """
        try:
            suite = self.ge_context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
            info_msg = f"""Loaded ExpectationSuite "{suite.expectation_suite_name}" 
                containing {len(suite.expectations)} expectations."""
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
        except DataContextError:
            suite = self.ge_context.add_expectation_suite(expectation_suite_name=expectation_suite_name)
            info_msg = f'Created ExpectationSuite "{suite.expectation_suite_name}".'
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)

    def create_validator(self, expectation_suite_name: str, batch_request: json) -> Optional[Validator]:
        """
        This function creates a validator using a batch request and expectation suite

        :param expectation_suite_name (str): Name of the expectation suite to be used
        :param batch_request (json): The batch request json that is to be processed

        :return validator: A validator used to execute expectations checks
        """
        try:
            batch_request_obj = BatchRequest(**batch_request)
            validator = self.ge_context.get_validator(
                batch_request=batch_request_obj,
                expectation_suite_name=expectation_suite_name
            )
            validator.save_expectation_suite(discard_failed_expectations=False)
            info_msg = "Validator created and expectation suite added"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return validator
        except Exception as validator_error:
            error_msg = f"An error occured while creating validator:\n{str(validator_error)}"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, 
                                                     status_message="An error occured while creating validator.")
            raise Exception(error_msg)

    def add_expectations_to_validator(self, validator, expectations: List[Dict]) -> None:
        """
        This function adds the provided expectations to the validation suite.

        :param validator: Validator object.
        :param expectations (List[dict]): List of expectations.

        :return: None
        """
        if not expectations:
            error_msg = "An error occured while adding expectations to expectation_suite: No expectations provided"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
            raise ValueError(error_msg)
        
        # Add expectations to the validator
        for expectation in expectations:
            try:
                expectation_type = expectation.expectation_type
                kwargs = expectation.kwargs
                
                if not expectation_type or not isinstance(kwargs, dict):
                    error_msg = f"Invalid expectation format: {expectation}"
                    JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR,
                                                             status_message=error_msg)
                    raise ValueError(error_msg)
                
                # Dynamically call the appropriate expectation method
                expectation_func = getattr(validator, expectation_type, None)
                if not expectation_func:
                    error_msg = f"Expectation type '{expectation_type}' is not supported."
                    JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, 
                                                             status_message=error_msg)
                    raise AttributeError(error_msg)
                
                expectation_func(**kwargs)
            except Exception as e:
                error_msg = f"Error adding expectation: {expectation}\n{str(e)}"
                JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, 
                                                         status_message=error_msg)
                raise ValueError(error_msg)
            
        # Save the updated expectation suite
        validator.save_expectation_suite(discard_failed_expectations=False)
        info_msg = "Successfully added expectations"
        dqt_logger.info(info_msg)
        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)


    def create_and_execute_checkpoint(self, expectation_suite_name: str, validator: Validator, batch_request: json) -> json:
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
            info_msg = "Successfully created and executed checkpoint, returning validation results"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return checkpoint_result
        except Exception as checkpoint_error:
            error_msg = f"An error occured while creating or executing checkpoint:\n{str(checkpoint_error)}"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
            raise Exception(error_msg)


def __run_quality_checks(
    datasource_type: str,
    datasource_name: str,
    quality_checks: List[dict],
    config: Dict[str, Union[str, int]],
    batch_limit: Optional[int] = 0,
    is_file: bool = True,
) -> Dict:
    """
    Generic function to execute quality checks using Great Expectations.

    :param datasource_type (str): Type of datasource (e.g., file, mysql).
    :param datasource_name (str): Name of the datasource.
    :param quality_checks (List[dict]): List of checks to perform.
    :param config (Dict): Configuration parameters (e.g., hostname, dir_path).
    :param batch_limit (int): Number of batches to validate.
    :param is_file (bool): True for file-based data; False for database.

    :return: Validation results.
    """
    
    # Generate a unique name for the expectation suite
    unique_id = uuid.uuid4().hex
    expectation_suite_name = (
        f"{datasource_name}_{config.get('dir_name', config.get('table_name'))}_{datasource_type}_{unique_id}"
    )
    
    try:
        ge = GreatExpectationsModel()
        
        if is_file:
            datasource = ge.GEFileDatasource(
                datasource_name=datasource_name,
                datasource_type=datasource_type,
                dir_name=config["dir_path"],
            )
            config_yaml = datasource.get_file_config()
        else:
            datasource = ge.GESQLDatasource(
                datasource_type=datasource_type,
                host=config["hostname"],
                password=config["password"],
                username=config["username"],
                database=config["database"],
                schema_name=config["schema_name"],
                datasource_name=datasource_name,
                port=config["port"],
                table_name=config["table_name"],
            )
            config_yaml = datasource.get_database_config()

        ge.create_datasource(config_yaml=config_yaml)
        
        # Create or load the expectation suite
        ge.create_or_load_expectation_suite(expectation_suite_name=expectation_suite_name)
        
        # Generate batch request
        if is_file:
            batch_request = ge.create_batch_request_json_for_file(
                datasource_name=datasource_name,
                data_asset_name=config["file_name"],
                limit=batch_limit,
            )
        else:
            batch_request = ge.create_batch_request_json_for_db(
                datasource_name=datasource_name,
                data_asset_name=config["table_name"],
                limit=batch_limit,
            )
        
        # Create a validator and add expectations
        validator = ge.create_validator(
            expectation_suite_name=expectation_suite_name, batch_request=batch_request
        )
        ge.add_expectations_to_validator(validator=validator, expectations=quality_checks)
        
        # Execute the checkpoint
        checkpoint_results = ge.create_and_execute_checkpoint(
            expectation_suite_name=expectation_suite_name,
            batch_request=batch_request,
            validator=validator,
        )
        
        # Process validation results
        return find_validation_result(data=checkpoint_results)

    except Exception as e:
        error_msg = f"Failed to run quality checks: {str(e)}"
        dqt_logger.error(error_msg)
        raise RuntimeError(error_msg) 

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
    db_config = {
        "hostname": hostname,
        "password": password,
        "username": username,
        "port": port,
        "database": database,
        "schema_name": schema_name,
        "table_name": table_name,
    }
    return __run_quality_checks(
        datasource_type=datasource_type,
        datasource_name=datasource_name,
        quality_checks=quality_checks,
        config=db_config,
        batch_limit=batch_limit,
        is_file=False,
    )

def run_quality_checks_for_file(datasource_type: str, datasource_name: str, dir_path: str, quality_checks: List[dict], 
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
    file_config = {"dir_path": dir_path, "file_name": file_name, "dir_name": os.path.basename(dir_path)}
    return __run_quality_checks(
        datasource_type=datasource_type,
        datasource_name=datasource_name,
        quality_checks=quality_checks,
        config=file_config,
        batch_limit=batch_limit,
        is_file=True,
    )
    