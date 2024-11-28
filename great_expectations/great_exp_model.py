from typing import List, Optional
import yaml
import json

import great_expectations as gx
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError


class GreatExpectationsModel:
    def __init__(self, quality_checks: List[dict]):
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
            if self.datasource_type == "mysql":
                return self.__get_mysql_datasource_config()
            elif self.datasource_type == "postgres":
                return self.__get_postgres_datasource_config()
            elif self.datasource_type == "redshift":
                return self.__get_redshift_datasource_config()
            elif self.datasource_type == "snowflake":
                return self.__get_snowflake_datasource_config()
            elif self.datasource_type == "bigquery":
                return self.__get_bigquery_datasource_config()
            elif self.datasource_type == "trino":
                return self.__get_trino_database_config()
            elif self.datasource_type == "athena":
                return self.__get_athena_database_config()
            elif self.datasource_type == "clickhouse":
                return self.__get_clickhouse_database_config()
            else:
                return self.__get_other_database_config()


        def __get_mysql_datasource_config(self) -> yaml:
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
            if self.datasource_type in {"csv", "json", "parquet", "orc", "file"}:
                return self.__get_pandas_datasource_config()
            elif self.datasource_type == "pyspark":
                return self.__get_pyspark_datasource_config()
            

        def __get_pandas_datasource_config(self) -> yaml:
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
            return datasource_config_for_pandas_yaml

        def __get_pyspark_datasource_config():
            pass


    def __create_file_datasource(self, file_config_yaml: yaml) -> None:
        try:
            self.ge_context.test_yaml_config(yaml_config=file_config_yaml)
            sanitize_yaml_and_save_datasource(self.ge_context, file_config_yaml, overwrite_existing=True)
        except Exception as e:
            raise Exception(f"Datasource for file could not be created\n{str(e)}")

    def __create_sql_datasource(self, database_config_yaml: yaml) -> None:
        try:
            self.ge_context.test_yaml_config(yaml_config=database_config_yaml)
            sanitize_yaml_and_save_datasource(self.ge_context, database_config_yaml, overwrite_existing=True)
        except Exception as e:
            raise Exception(f"Datasource for database could not be created\n{str(e)}")


    def __create_batch_request_json_for_db(datasource_name: str, data_asset_name: str, 
                                           limit: Optional[int] = 0) -> dict:
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
            
        return batch_request_json
    

    def __create_batch_request_json_for_file(datasource_name: str, data_asset_name: str,
                                                limit: Optional[int] = 0) -> json:
        
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
            
        return batch_request_json
    

    def __create_or_load_expectation_suite(self, expectation_suite_name: str) -> None:
        """
        This function creates a new expectations suite

        :param expectation_suite_name (str): Name of the newly created expectation suite

        :return: None
        """
        try:
            suite = self.ge_context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
            print(f"""Loaded ExpectationSuite "{suite.expectation_suite_name}" 
                containing {len(suite.expectations)} expectations.""")
        except DataContextError:
            suite = self.ge_context.add_expectation_suite(expectation_suite_name=expectation_suite_name)
            print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')


    def __create_validator(self, expectation_suite_name: str, batch_request: json):
        """
        This function creates a validator using a batch request and expectation suite

        :param expectation_suite_name (str): Name of the expectation suite to be used
        :param batch_request (json): The batch request json that is to be processed

        :return validator: A validator used to execute expectations checks
        """
        validator = self.ge_context.get_validator(
            batch_request=BatchRequest(**batch_request),
            expectation_suite_name=expectation_suite_name
        )

        validator.save_expectation_suite(discard_failed_expectations=False)
        print("Validator created and expectation suite added")
        return validator

    def __add_expectations_to_validator(validator, expectations) -> None:
        """
        This function adds the provided expectations to the validation suite

        :param validator: Validator object
        :param expectations: List of expectations

        :return: None
        """
        
        if len(expectations) == 0:
            raise Exception("No expectations provided")
        
        # adding expectations to the validator
        for expectation in expectations:
            expectation_type = expectation.expectation_type
            kwargs = expectation.kwargs

            expectation_func = getattr(validator, expectation_type)
            expectation_func(**kwargs)
            
        # saving expectation suite
        validator.save_expectation_suite(discard_failed_expectations=False)
        print("Successfully added expectations")


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

        checkpoint = SimpleCheckpoint(
            f"{validator.active_batch_definition.data_asset_name}_{expectation_suite_name}",
            self.ge_context,
            **checkpoint_config
        )

        checkpoint_result = checkpoint.run()
        return checkpoint_result


    @staticmethod
    def run_quality_checks_for_db(datasource_type: str, hostname: str, password: str, username: str, 
                                    port: int, datasource_name: str, schema_name: str, database: str, 
                                    table_name: str, quality_checks: List[dict], batch_limit: Optional[int] = 0):
        
        expectation_suite_name_db = f"{datasource_name}_{username}_{table_name}_{port}_{hostname}"
        
        ge = GreatExpectationsModel(quality_checks=quality_checks)
        ge_sql = ge.GE_SQL_Datasource(datasource_type=datasource_type, host=hostname, password=password, 
                                        username=username, database=database, schema_name=schema_name, 
                                        datasource_name=datasource_name, port=port, table_name=table_name)
        
        database_config_yaml = ge_sql.get_database_config()
        ge.__create_sql_datasource(database_config_yaml=database_config_yaml)
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
        return checkpoint_results # TODO: add utils.find_validation_result()


    @staticmethod
    def run_quality_check_for_file(datasource_type: str, datasource_name: str, dir_name: str, quality_checks: List[dict], 
                                file_name: str, batch_limit: Optional[int] = 0):
        
        expectation_suite_name_file = f"{datasource_name}_{dir_name}_{datasource_type}_{file_name}"
        
        ge = GreatExpectationsModel(quality_checks=quality_checks)
        ge_file = ge.GE_File_Datasource(datasource_name=datasource_name, datasource_type=datasource_type, dir_name=dir_name)
        
        file_config_yaml = ge_file.get_file_config()
        ge.__create_file_datasource(file_config_yaml=file_config_yaml)
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
        return checkpoint_results # TODO: add utils.find_validation_result()
