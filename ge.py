import json
import yaml

import great_expectations as gx
from great_expectations.cli.datasource import sanitize_yaml_and_save_datasource
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError

from typing import Optional

from utils import find_validation_result
from request_models import connection_enum_and_metadata as conn

# TODO: Create this module
class GreatExpectations:
    context = None

    def __init__(self):
        self.context = gx.get_context()

    

context = gx.get_context()

def __create_new_datasource(datasource_name: str, datasource_type: str, host: str, port: int, 
                          username: str, password: str, database: Optional[str] = "test_db", 
                          table_name: Optional[str] = "test_table", 
                          schema_name: Optional[str] = "test_schema", 
                          dir_name: Optional[str] = "test_dir") -> None:
    """
    This function creates a new datasource for great_expectations library

    :param datasource_name: 
    :param datasource_type:
    :param host:
    :param port:
    :param username:
    :param password:
    :param database:
    :param table_name:
    :param schema_name:
    :param dir_name:

    :return: None
    """
    if datasource_type in {conn.ConnectionEnum.FILESERVER, conn.ConnectionEnum.CSV}:
        datasource_fileserver_json = {
            "name": datasource_name,
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "PandasExecutionEngine"
            },
            "data_connectors": {
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetFilesystemDataConnector",
                    "base_directory": dir_name,
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

        datasource_fileserver_yaml = yaml.dump(datasource_fileserver_json)

        try:
            context.test_yaml_config(yaml_config=datasource_fileserver_yaml)
            sanitize_yaml_and_save_datasource(context, datasource_fileserver_yaml, 
                                              overwrite_existing=True)
            print("DataSource creation successful")
        except Exception as e:
            raise Exception(f"Datasource could not be created\n{str(e)}")
        
    elif datasource_type == conn.ConnectionEnum.MYSQL:
        datasource_mysql_json = {
            "name": datasource_name,
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SqlAlchemyExecutionEngine",
                "credentials": {
                "host": host,
                "port": str(port),
                "username": username,
                "password": password,
                "database": database,
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
                        "schema_name": schema_name
                    }
                },
                "default_configured_data_connector_name": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                    "assets": {
                        table_name: {
                        "class_name": "Asset",
                        "schema_name": schema_name
                        }
                    }
                }
            }
        }
        
        datasource_mysql_yaml = yaml.dump(datasource_mysql_json)

        try:
            context.test_yaml_config(yaml_config=datasource_mysql_yaml)
            sanitize_yaml_and_save_datasource(context, datasource_mysql_yaml, 
                                              overwrite_existing=True)
        except Exception as e:
            raise Exception(f"Datasource could not be created\n{str(e)}")
    
    else:
        print("Invalid format for datasource type")


def __create_batch_request(datasource_name: str, data_asset_name: Optional[str] = "test_data_asset", 
                         limit: Optional[int] = 0) -> json:
    """
    This function creates a new batch request json

    :param datasource_name (str): The name of the data source
    :param data_asset_name (str): The name of the data asset
    :param limit (int) [optional]: If specified, the maximum number of *batches* to be returned
    (limit does not affect the number of records in each batch). The default value of this param is 0.

    :return batch_request (json): A json created for a batch request executed in a great_expectations 
    checkpoint
    """
    if limit == 0:
        batch_request = {'datasource_name': datasource_name, 
                         'data_connector_name': 'default_configured_data_connector_name', 
                         'data_asset_name': data_asset_name}
    else:
        batch_request = {'datasource_name': datasource_name, 
                        'data_connector_name': 'default_configured_data_connector_name', 
                        'data_asset_name': data_asset_name, 
                        'limit': limit}

    return batch_request


def __create_expectation_suite(expectation_suite_name: str) -> None:
    """
    This function creates a new expectations suite

    :param expectation_suite_name (str): Name of the newly created expectation suite

    :return: None
    """
    try:
        suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
        print(f"""Loaded ExpectationSuite "{suite.expectation_suite_name}" 
              containing {len(suite.expectations)} expectations.""")
    except DataContextError:
        suite = context.add_expectation_suite(expectation_suite_name=expectation_suite_name)
        print(f'Created ExpectationSuite "{suite.expectation_suite_name}".')


def __create_validator(expectation_suite_name: str, batch_request: json):
    """
    This function creates a validator using a batch request and expectation suite

    :param expectation_suite_name (str): Name of the expectation suite to be used
    :param batch_request (json): The batch request json that is to be processed

    :return validator: A validator used to execute expectations checks
    """
    validator = context.get_validator(
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


def __create_and_execute_checkpoint(expectation_suite_name: str, validator, 
                                  batch_request: json) -> json:
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
        context,
        **checkpoint_config
    )

    checkpoint_result = checkpoint.run()
    return checkpoint_result


def run_quality_checks(quality_checks: json, datasource_type: str, hostname: str, 
                       password: str, port: str, database: Optional[str], table_name: Optional[str], 
                       schema_name: Optional[str], datasource_name: str, username: str, dir_name :Optional[str]) -> json:
    """
    This function executes all the great_expectation functions 

    :quality_checks (json):
    :datasource_type (str):
    :hostname (str):
    :password (str):
    :port (str):
    :database (str):
    :table_name (str):
    :schema_name (str):
    :datasource_name (str):
    :username (str):

    :return: A JSON containing validation results
    """
    
    __create_new_datasource(datasource_type=datasource_type, port=port, host=hostname, password=password, database=database,
                              username=username, datasource_name=datasource_name, table_name=table_name, schema_name=schema_name)
    
    expectation_suite_name = f"{datasource_name}_{username}_{table_name}_{port}_{hostname}" # expectation suite name format
    __create_expectation_suite(expectation_suite_name=expectation_suite_name)

    batch_request_json = __create_batch_request(datasource_name=datasource_name, data_asset_name=table_name)
    validator = __create_validator(expectation_suite_name=expectation_suite_name, batch_request=batch_request_json)

    __add_expectations_to_validator(validator=validator,expectations=quality_checks)
    checkpoint_results = __create_and_execute_checkpoint(expectation_suite_name=expectation_suite_name, validator=validator, 
                                                       batch_request=batch_request_json)
    
    validation_results = find_validation_result(data=checkpoint_results) # final validation results
    return validation_results 
