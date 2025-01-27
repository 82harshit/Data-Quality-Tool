import csv
import dask
import dask.dataframe as dd
import dask.bag as db
from dask.delayed import delayed
import pandas as pd
import pyorc
from fastavro import reader
import pyarrow.parquet as pq
from openpyxl import load_workbook
from soda.scan import Scan

import yaml
import re
import json
import os
from typing import List, Dict, Union, Optional

from database.db_models.job_run_status import JobRunStatusEnum
from job_state_singleton import JobStateSingleton
from logging_config import dqt_logger
from request_models import connection_enum_and_metadata as conn_enum
from Soda.soda_results_models import CheckResults, CheckResult


class SodaModel:
    def __init__(self):
        """
        Creates a new Soda scan object
        """
        self.scan = Scan()
        
    class SodaSQLDatasource:
        def __init__(self, 
                     datasource_type: str, 
                     datasource_name: str, 
                     host: str, 
                     port: int, 
                     username: str, 
                     password: str, 
                     database: str, 
                     schema_name: str
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
            
            :return: None
            """
            self.datasource_type = datasource_type
            self.datasource_name = datasource_name
            self.host = host
            self.password = password
            self.database = database
            self.schema_name = schema_name
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
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.MSSQL:
                return self.__get_mssql_datasource_config() 
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.TRINO:
                return self.__get_trino_datasource_config()
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.AZURE_SYNAPSE:
                return self.__get_azure_synapse_datasource_config()
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.ATHENA:
                return self.__get_athena_datasource_config()
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.BIGQUERY:
                return self.__get_bigquery_datasource_config()
            elif self.datasource_type == conn_enum.Database_Datasource_Enum.CLICKHOUSE:
                return self.__get_clickhouse_datasource_config()
            else:
                error_msg = f"{self.datasource_type} datasource not found"
                dqt_logger.info(error_msg)
                JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
                return TypeError(error_msg)
            
        def __get_mysql_datasource_config(self) -> yaml:
            """
            Creates a JSON file with the predefined configurations for Soda library for mysql.

            :return datasource_config_for_mysql_yaml (yaml): The config file for mysql converted to YAML
            """
            datasource_config_for_mysql_json = {
                f"data_source {self.datasource_name}": {
                    "type": self.datasource_type,
                    "host": self.host,
                    "username": self.username,
                    "password": self.password,
                    "database": self.database
                }
            }
            
            datasource_config_for_mysql_yaml = yaml.dump(datasource_config_for_mysql_json)
            info_msg = "Created datasource config for mysql"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return datasource_config_for_mysql_yaml
        
        def __get_postgres_datasource_config(self) -> yaml:
            """
            Creates a JSON file with the predefined configurations for Soda library for postgres.

            :return datasource_config_for_postgres_yaml (yaml): The config file for postgres converted to YAML
            """
            datasource_config_for_postgres_json = {
                f"data_source {self.datasource_name}": {
                    "type": self.datasource_type,
                    "connection": {
                        "host": self.host,
                        "username": self.username,
                        "password": self.password,
                        "database": self.database,
                        "schema": self.schema_name,
                        "port": str(self.port),
                        "sslmode": "prefer" # can be [prefer, require, allow, disable]
                    }
                }
            }
            
            datasource_config_for_postgres_yaml = yaml.dump(datasource_config_for_postgres_json)
            info_msg = "Created datasource config for postgres"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return datasource_config_for_postgres_yaml
        
        def __get_mssql_datasource_config(self):
            """
            Creates a JSON file with the predefined configurations for Soda library for MSSQL.

            :return datasource_config_for_mssql_yaml (yaml): The config file for MSSQL converted to YAML
            """
            datasource_config_for_mssql_json = {
                f"data_source {self.datasource_name}": {
                    "type": {self.datasource_type},
                    "host": self.host,
                    "port": {self.port},
                    "username": {self.username},
                    "password": {self.password},
                    "database": {self.database},
                    "schema": {self.schema_name},
                    "trusted_connection": "false",
                    "encrypt": "false",
                    "trust_server_certificate": "false",
                    "driver": "ODBC Driver 18 for SQL Server",
                    "connection_parameters": {
                        "multi_subnet_failover": "true"
                    }
                }
            }
            
            datasource_config_for_mssql_yaml = yaml.dump(datasource_config_for_mssql_json)
            info_msg = "Created datasource config for mssql"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return datasource_config_for_mssql_yaml
        
        def __get_trino_datasource_config(self):
            """
            Creates a JSON file with the predefined configurations for Soda library for Trino.

            :return datasource_config_for_trino_yaml (yaml): The config file for Trino converted to YAML
            """
            datasource_config_for_trino_json = {
                f"data_source {self.datasource_name}": {
                    "type": {self.datasource_type},
                    "host": self.host,
                    "port": str(self.port),
                    "username": {self.username},
                    "password": {self.password},
                    "catalog": "hive",
                    "schema": {self.schema_name}
                }
            }
            
            datasource_config_for_trino_yaml = yaml.dump(datasource_config_for_trino_json)
            info_msg = "Created datasource config for trino"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return datasource_config_for_trino_yaml
        
        def __get_azure_synapse_datasource_config(self):
            datasource_config_for_azure_synapse_json = {
                f"data_source {self.datasource_name}": { 
                    "type": {self.datasource_type},
                    "driver": "SQL Server Native Client 11.0",
                    "host": "my_server.sql.azuresynapse.net",
                    "port": str(self.port),
                    "database": {self.database},
                    "username": {self.username},
                    "password": {self.password},
                    "encrypt": "true"
                }
            }
            
            datasource_config_for_azure_synapse_yaml = yaml.dump(datasource_config_for_azure_synapse_json)
            info_msg = "Created datasource config for azure synapse"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return datasource_config_for_azure_synapse_yaml
        
        def __get_bigquery_datasource_config(self) -> yaml:
            error_msg = "BigQuery datasource configuration is not yet implemented."
            dqt_logger.info(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
            raise NotImplementedError(error_msg)

        def __get_athena_datasource_config(self) -> yaml:
            error_msg = "Athena datasource configuration is not yet implemented."
            dqt_logger.info(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
            raise NotImplementedError(error_msg)

        def __get_clickhouse_datasource_config(self) -> yaml:
            error_msg = "Clickhouse datasource configuration is not yet implemented."
            dqt_logger.info(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg) 
            raise NotImplementedError(error_msg)
            

    class SodaFileDatasource:
        def __init__(self, datasource_path: str, datasource_type: str):
            self.datasource_path = datasource_path
            self.datasource_type = datasource_type
        
        def __is_valid_json(self, filepath: str):
            # Try to open and load the file as JSON
            try:
                with open(filepath, 'r') as file:
                    # Try parsing the file content as JSON
                    json.load(file)
                return True
            except json.JSONDecodeError as e:
                # If a JSONDecodeError occurs, it's not a valid JSON
                raise Exception(f"Invalid JSON in file {filepath}: {e}")
            except Exception as e:
                # Handle any other unexpected exceptions
                raise Exception(f"Error while reading file {filepath}: {e}")
            
        def __is_valid_orc(self, file_path: str):
            try:
                with open(file_path, 'rb') as file:
                    pyorc.Reader(file)  # Attempt to read as an ORC file
                return True
            except Exception as e:
                raise Exception(f"An error occurred: {e}")
 
        def __is_valid_parquet(self, file_path: str):
            try:
                pq.ParquetFile(file_path)  # Attempt to open the file as Parquet
                return True
            except (ValueError, IOError) as e:
                raise Exception(f"Invalid Parquet file: {e}")
            
        def __is_valid_csv(self, file_path: str):
            try:
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    for _ in reader:  # Attempt to read rows
                        pass
                return True
            except (csv.Error, IOError) as e:
                raise Exception(f"Invalid CSV file: {e}") 
            
        def __is_valid_avro(self, file_path: str):
            try:
                with open(file_path, 'rb') as file:
                    reader(file) # Attempt to open as an Avro file
                return True
            except (ValueError, IOError) as e:
                raise Exception(f"Invalid Avro file: {e}")
        
        def __is_valid_excel(self, file_path: str):
            try:
                # Attempt to load the Excel workbook
                load_workbook(file_path)
                return True
            except Exception as e:
                raise Exception(f"Invalid Excel file: {e}")
        
        def get_dataframe(self):
            # Check if file exists
            if not os.path.exists(self.datasource_path):
                raise FileNotFoundError(f"File not found: {self.datasource_path}")
            
            # Check if file is empty
            if os.path.getsize(self.datasource_path) == 0:
                raise Exception(f"File is empty: {self.datasource_path}")
            
            dask.config.set({"dataframe.convert-string": False})
            
            if self.datasource_type == conn_enum.File_Datasource_Enum.CSV:
                if self.__is_valid_csv(self.datasource_path):
                    return dd.read_csv(self.datasource_path)
                else:
                    error_msg = "Provided file is not a valid CSV file."
                    dqt_logger.info(error_msg)
                    JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                    raise Exception(error_msg)
            elif self.datasource_type == conn_enum.File_Datasource_Enum.AVRO:
                if self.__is_valid_avro(self.datasource_path):
                    return db.read_avro(self.datasource_path).to_dataframe()
                else:
                    error_msg = "Provided file is not a valid AVRO file."
                    dqt_logger.info(error_msg)
                    JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                    raise Exception(error_msg)
            elif self.datasource_type == conn_enum.File_Datasource_Enum.ORC:
                if self.__is_valid_orc(self.datasource_path):
                    return dd.read_orc(self.datasource_path)
                else:
                    error_msg = "Provided file is not a valid ORC file."
                    dqt_logger.info(error_msg)
                    JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                    raise Exception(error_msg)
            elif self.datasource_type == conn_enum.File_Datasource_Enum.PARQUET:
                if self.__is_valid_parquet(self.datasource_path):
                    return dd.read_parquet(self.datasource_path)
                else:
                    error_msg = "Provided file is not a valid parquet file."
                    dqt_logger.info(error_msg)
                    JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                    raise Exception(error_msg)
            elif self.datasource_type == conn_enum.File_Datasource_Enum.JSON:
                if self.__is_valid_json(filepath=self.datasource_path):
                    df = pd.read_json(self.datasource_path)
                    return dd.from_pandas(df, npartitions=1)
                else:
                    error_msg = "Provided file is not a valid JSON file."
                    dqt_logger.info(error_msg)
                    JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                    raise Exception(error_msg)
            elif self.datasource_type == conn_enum.File_Datasource_Enum.EXCEL:
                if self.__is_valid_excel(self.datasource_path):
                    parts = delayed(pd.read_excel)(self.datasource_path)
                    return dd.from_delayed(parts)
                else:
                    error_msg = "Provided file is not a valid excel file."
                    dqt_logger.info(error_msg)
                    JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                    raise Exception(error_msg)
            else:
                error_msg = "File type not recognised, cannot create dataframe."
                dqt_logger.info(error_msg)
                JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                return TypeError(error_msg)
        

def __remove_empty_dicts(data):
    """
    Removes all the empty dictionaries from the provided JSON.
    """
    if isinstance(data, dict):
        return {k: __remove_empty_dicts(v) for k, v in data.items() if v != {}}
    elif isinstance(data, list):
        return [__remove_empty_dicts(item) for item in data]
    return data

def __get_formatted_check_for_datasource(datasource_type: str, 
                                        expectation_type: str, 
                                        column: str, 
                                        condition: str, 
                                        percentile: Optional[str] = None):
    if datasource_type == conn_enum.Database_Datasource_Enum.MYSQL:
        if percentile:
            return f"{expectation_type}(`{column}`, {percentile}) {condition}"
        return f"{expectation_type}(`{column}`) {condition}"
    elif datasource_type == conn_enum.Database_Datasource_Enum.POSTGRES:
        if percentile:
            return f"""{expectation_type}("{column}", {percentile}) {condition}"""
        return f"""{expectation_type}("{column}") {condition}"""
    elif datasource_type == conn_enum.Database_Datasource_Enum.MSSQL:
        if percentile:
           return f"{expectation_type}([{column}], {percentile}) {condition}"
        return f"{expectation_type}([{column}]) {condition}"
    
    if datasource_type in [conn_enum.File_Datasource_Enum.CSV, conn_enum.File_Datasource_Enum.JSON,
                           conn_enum.File_Datasource_Enum.AVRO, conn_enum.File_Datasource_Enum.ORC,
                           conn_enum.File_Datasource_Enum.PARQUET]:
        column = (column.strip() # Remove leading/trailing spaces
                        .replace(" ", "_") # Replace spaces with underscores
                        .replace(r"[^\w\s]", "")  # Remove special characters
                        .lower() # Convert to lowercase
        )
        if percentile:
           return f"{expectation_type}({column}, {percentile}) {condition}"
        return f"{expectation_type}({column}) {condition}"

def __create_checks(datasource_type: str, datasource_name: str, quality_checks: dict) -> yaml:
    """
    Parses the quality checks JSON to a YAML format as required by the Soda library.
    
    :param datasource_name (str): The name of the table
    :param quality_checks (dict): A dictionary containing the quality checks
    
    :return yaml: Parsed quality checks JSON to YAML
    """
    checks = []
    
    for quality_check in quality_checks:
        expectation_type = quality_check.get("expectation_type", "")
        kwargs = quality_check.get("kwargs", "")
        if kwargs:
            condition = kwargs.get("condition", "")
            column = kwargs.get("column", "")
            if column:
                if expectation_type == "percentile":
                    percentile = kwargs.get("percentile", "")
                    check = __get_formatted_check_for_datasource(datasource_type=datasource_type, 
                                                        expectation_type=expectation_type, 
                                                        column=column, 
                                                        condition=condition, 
                                                        percentile=percentile
                                                        )
                else:
                    check = __get_formatted_check_for_datasource(datasource_type=datasource_type, 
                                                        expectation_type=expectation_type, 
                                                        column=column, 
                                                        condition=condition
                                                        )
                other_kwargs = {key:value for key, value in kwargs.items() if key not in ["column", "condition"]}
                other_kwargs = __remove_empty_dicts(other_kwargs) 
                if other_kwargs:
                    checks.append({check: other_kwargs})
                else:
                    checks.append(check)
            else:
                check = f"{expectation_type} {condition}"
                checks.append(check)
        else:
            checks.append(expectation_type)
    
    checks_yaml = yaml.dump({f"checks for {datasource_name}":checks}, default_flow_style=False, indent=2)
    dqt_logger.debug(f"Created checks:\n{checks_yaml}")
    return checks_yaml

def __parse_validation_result(validation_result: str) -> List[CheckResult]:
    check_lines = validation_result.strip().split("\n")
    results = []

    for line in check_lines:
        # Match the pattern and extract fields using regex
        # The original regex was likely not capturing the check_status correctly.
        # Updated regex to capture 'PASS' or 'FAIL' into the 'status' group
        match = re.match(r"\[(.+?)\]\s+(PASS|FAIL)\s+\(check_value:\s+(\d+)\)", line)
        if match:
            check_name, status, check_value = match.groups()
            # Create CheckResult object and add to results
            result = CheckResult(
                check_name=check_name,
                check_status=status,  # Assign the extracted status
                check_value=int(check_value)
            )
            results.append(result)

    return results

def __run_quality_checks(datasource_type: str,
                         datasource_name: str, 
                         config: Dict[str, Union[str, int]],
                         quality_checks: List[dict], 
                         is_file: bool = True):
    try:
        soda = SodaModel()
        
        if is_file:
            datasource = soda.SodaFileDatasource(datasource_path=config["file_path"],
                                                 datasource_type=datasource_type)
            file_dataframe = datasource.get_dataframe()
            dqt_logger.debug(f"Loaded file as dataframe: {file_dataframe}")
            # preprocessing dataframe
            try:
                file_dataframe.columns = (
                    file_dataframe.columns.str.strip() # Remove leading/trailing spaces
                            .str.replace(" ", "_") # Replace spaces with underscores
                            .str.replace(r"[^\w\s]", "")  # Remove special characters
                            .str.lower() # Convert to lowercase
                )
            except:
                error_msg = "Failed to preprocess dataframe."
                dqt_logger.error(error_msg)
                raise Exception(error_msg)
            
            soda.scan.add_dask_dataframe(dask_df=file_dataframe, dataset_name=datasource_name, data_source_name=datasource_name)
            
        else:
            datasource = soda.SodaSQLDatasource(datasource_type=datasource_type,
                                                datasource_name=datasource_name,
                                                database=config["database"],
                                                host=config["hostname"],
                                                schema_name=config["schema_name"],
                                                port=config["port"],
                                                username=config["username"],
                                                password=config["password"]
                                                )
            db_config = datasource.get_database_config()
            soda.scan.add_configuration_yaml_str(db_config)
        
        checks = __create_checks(datasource_type=datasource_type, datasource_name=datasource_name, quality_checks=quality_checks)
        soda.scan.set_data_source_name(data_source_name=datasource_name)
        soda.scan.add_sodacl_yaml_str(checks)
        soda.scan.execute()
        validation_results = soda.scan.get_all_checks_text()
        parsed_results = CheckResults(results=__parse_validation_result(validation_results))
        return json.loads(parsed_results.model_dump_json(indent=4))
            
    except Exception as e:
        error_msg = f"Failed to run quality checks: {str(e)}"
        dqt_logger.error(error_msg)
        raise RuntimeError(error_msg)
    
def run_quality_checks_for_db(datasource_type: str, hostname: str, password: str, username: str, 
                                port: int, datasource_name: str, schema_name: str, database: str, 
                                quality_checks: List[dict]) -> json:
    """
    Triggers the functions of great_expectations library in the required sequence

    :param datasource_type (str): The type of datasource, e.g.: file, mysql, snowflake, csv, etc.
    :param datasource_name (str): The name of datasource
    :param quality_checks (List[dict]): The list of checks that are to be performed on the file, formatted as required by 
    the great_expectations library
    :param hostname (str): The host IPv4 address to connect to
    :param password (str): The password required to connect to the host server
    :param username (str): The name of the user who wants to connect to the host server
    :param port (int): The port number to be connected on
    :param database (str): The name of the database that needs to be accessed
    
    :return checkpoint_results (json): The generated validation results
    """
    db_config = {
        "hostname": hostname,
        "password": password,
        "username": username,
        "port": port,
        "database": database,
        "schema_name": schema_name
    }
    return __run_quality_checks(
        datasource_type=datasource_type,
        datasource_name=datasource_name,
        quality_checks=quality_checks,
        config=db_config,
        is_file=False,
    )

def run_quality_checks_for_file(datasource_type: str, datasource_name: str, dir_path: str, quality_checks: List[dict], 
                            file_name: str) -> json:
    """
    Triggers the functions of great_expectations library in the required sequence

    :param datasource_type (str): The type of datasource, e.g.: file, mysql, snowflake, csv, etc.
    :param datasource_name (str): The name of datasource
    :param dir_path (str): The path where the file is stored
    :param quality_checks (List[dict]): The list of checks that are to be performed on the file, formatted as required by 
    the great_expectations library
    :param file_name (str): The name of the file

    :return checkpoint_results (json): The generated validation results
    """
    file_config = {"file_path": os.path.join(dir_path, file_name)}
    return __run_quality_checks(
        datasource_type=datasource_type,
        datasource_name=datasource_name,
        quality_checks=quality_checks,
        config=file_config,
        is_file=True,
    )
    