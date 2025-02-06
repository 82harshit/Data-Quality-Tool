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
import asyncio
import nest_asyncio
import asyncssh
from soda.scan import Scan
import posixpath

import yaml
import re
import json
import os
from typing import List, Dict, Union, Optional
from datetime import datetime
import shutil

from database.db_models.job_run_status import JobRunStatusEnum
from job_state_singleton import JobStateSingleton
from logging_config import dqt_logger
from request_models import connection_enum_and_metadata as conn_enum, job_model
from Soda.soda_results_models import CheckResults

nest_asyncio.apply()
TEMP_DIR = ".tmp"


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
        def __init__(self, datasource_type: str, file_name: str, dir_path: str, host: str, username: str, password: str):
            self.file_name = file_name
            self.dir_path = dir_path
            self.datasource_type = datasource_type
            self.username = username
            self.password = password
            self.host = host
        
        @staticmethod
        def __is_valid_json(filepath: str):
            """
            Verifies if the file at the provided filepath is a valid JSON file.
            """
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

        @staticmethod
        def __is_valid_orc(file_path: str):
            """
            Verifies if the file at the provided filepath is a valid ORC file.
            """
            try:
                with open(file_path, 'rb') as file:
                    pyorc.Reader(file)  # Attempt to read as an ORC file
                return True
            except Exception as e:
                raise Exception(f"An error occurred: {e}")
 
        @staticmethod
        def __is_valid_parquet(file_path: str):
            """
            Verifies if the file at the provided filepath is a valid Parquet file.
            """
            try:
                pq.ParquetFile(file_path)  # Attempt to open the file as Parquet
                return True
            except (ValueError, IOError) as e:
                raise Exception(f"Invalid Parquet file: {e}")
            
        @staticmethod    
        def __is_valid_csv(file_path: str):
            """
            Verifies if the file at the provided filepath is a valid CSV file.
            """
            try:
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    for _ in reader:  # Attempt to read rows
                        pass
                return True
            except (csv.Error, IOError) as e:
                raise Exception(f"Invalid CSV file: {e}") 
            
        @staticmethod
        def __is_valid_avro(file_path: str):
            """
            Verifies if the file at the provided filepath is a valid Avro file.
            """
            try:
                with open(file_path, 'rb') as file:
                    reader(file) # Attempt to open as an Avro file
                return True
            except (ValueError, IOError) as e:
                raise Exception(f"Invalid Avro file: {e}")
        
        @staticmethod
        def __is_valid_excel(file_path: str):
            """
            Verifies if the file at the provided filepath is a valid excel file.
            """
            try:
                # Attempt to load the Excel workbook
                load_workbook(file_path)
                return True
            except Exception as e:
                raise Exception(f"Invalid Excel file: {e}")
        
        async def __get_file_path(self):
            try:
                dqt_logger.debug(f"Initializing connection to {self.host}")
                async with asyncssh.connect(self.host, username=self.username, password=self.password, known_hosts=None) as conn:
                    dqt_logger.info(f"Successfully connected to {self.host}")
                    file_path = posixpath.join(self.dir_path, self.file_name)
                    
                    os.makedirs(TEMP_DIR, exist_ok=True)
                    local_temp_path = os.path.join(TEMP_DIR, self.file_name)
                    
                    async with conn.start_sftp_client() as sftp:
                        await sftp.get(file_path, local_temp_path)
                        dqt_logger.debug(f"Downloaded file {self.file_name} from {self.host}")
                    return local_temp_path
            except FileNotFoundError as fnf_error:
                dqt_logger.error(f"File not found: {file_path}")
                raise fnf_error
            except Exception as e:
                dqt_logger.error(f"Error: {e}")
                raise e
    
        def get_dataframe(self):
            """
            Returns the appropriate dataframe after reading the file from `datasource_path`, based on the file type.
            """
            try:
                local_temp_file_path = asyncio.run(self.__get_file_path())
                dqt_logger.debug(f"Downloaded file path: {local_temp_file_path}")
                
                # Check if file exists
                if not os.path.exists(local_temp_file_path):
                    raise FileNotFoundError(f"File not found at: {local_temp_file_path}")
                
                # Check if file is empty
                if os.path.getsize(local_temp_file_path) == 0:
                    raise Exception(f"File is empty: {local_temp_file_path}")
                
                dask.config.set({"dataframe.convert-string": False})
                
                if self.datasource_type == conn_enum.File_Datasource_Enum.CSV:
                    if self.__is_valid_csv(local_temp_file_path):
                        return dd.read_csv(local_temp_file_path)
                    else:
                        error_msg = "Provided file is not a valid CSV file."
                        dqt_logger.error(error_msg)
                        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                        raise Exception(error_msg)
                elif self.datasource_type == conn_enum.File_Datasource_Enum.AVRO:
                    if self.__is_valid_avro(local_temp_file_path):
                        return db.read_avro(local_temp_file_path).to_dataframe()
                    else:
                        error_msg = "Provided file is not a valid AVRO file."
                        dqt_logger.error(error_msg)
                        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                        raise Exception(error_msg)
                elif self.datasource_type == conn_enum.File_Datasource_Enum.ORC:
                    if self.__is_valid_orc(local_temp_file_path):
                        return dd.read_orc(local_temp_file_path)
                    else:
                        error_msg = "Provided file is not a valid ORC file."
                        dqt_logger.error(error_msg)
                        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                        raise Exception(error_msg)
                elif self.datasource_type == conn_enum.File_Datasource_Enum.PARQUET:
                    if self.__is_valid_parquet(local_temp_file_path):
                        return dd.read_parquet(local_temp_file_path)
                    else:
                        error_msg = "Provided file is not a valid parquet file."
                        dqt_logger.error(error_msg)
                        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                        raise Exception(error_msg)
                elif self.datasource_type == conn_enum.File_Datasource_Enum.JSON:
                    if self.__is_valid_json(local_temp_file_path):
                        df = pd.read_json(local_temp_file_path)
                        return dd.from_pandas(df, npartitions=1)
                    else:
                        error_msg = "Provided file is not a valid JSON file."
                        dqt_logger.error(error_msg)
                        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                        raise Exception(error_msg)
                elif self.datasource_type == conn_enum.File_Datasource_Enum.EXCEL:
                    if self.__is_valid_excel(local_temp_file_path):
                        parts = delayed(pd.read_excel)(local_temp_file_path)
                        return dd.from_delayed(parts)
                    else:
                        error_msg = "Provided file is not a valid excel file."
                        dqt_logger.error(error_msg)
                        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                        raise Exception(error_msg)
                else:
                    error_msg = "File type not recognised, cannot create dataframe."
                    dqt_logger.error(error_msg)
                    JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)  
                    return TypeError(error_msg)
            except Exception:
                error_msg = f"Could not retrieve file {self.file_name} from server {self.host}"
                dqt_logger.error(error_msg)
                raise Exception(error_msg)
        
@staticmethod
def __remove_empty_dicts(data):
    """
    Removes all the empty dictionaries from the provided JSON.
    """
    if isinstance(data, dict):
        return {k: __remove_empty_dicts(v) for k, v in data.items() if v != {}}
    elif isinstance(data, list):
        return [__remove_empty_dicts(item) for item in data]
    return data

@staticmethod
def __get_formatted_check_for_datasource(expectation_type: str, 
                                        datasource_type: str,
                                        condition: str,
                                        column: str, 
                                        percentile: Optional[str] = None) -> str:
    """
    Returns the formatted expectations as required by the Soda library.
    E.g.: 
        max(rainfall) <= 340.0
        
    :param datasource_type (str): The type of the datasource based on which the check needs to be returned
    :param expectation_type (str): The check that is to be applied
    :param column (str): The name of the column on which the check is applied
    :param condition (str): Condition used in the check
    :param percentile (str or None): Percentile values (used if using percentile check)
    
    :return str: Formatted check for check string
    """
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
    
    if datasource_type in conn_enum.File_Datasource_Enum.__members__.values():
        column = (column.strip() # Remove leading/trailing spaces
                        .replace(" ", "_") # Replace spaces with underscores
                        .replace(r"[^\w\s]", "")  # Remove special characters
                        .lower() # Convert to lowercase
        )
        if percentile:
           return f"{expectation_type}({column}, {percentile}) {condition}"
        return f"{expectation_type}({column}) {condition}"

# # Custom representer to ensure lists are serialized in flow style
# class FlowStyleDumper(yaml.Dumper):
#     def increase_indent(self, flow=False, indentless=False):
#         return super(FlowStyleDumper, self).increase_indent(flow=True)

@staticmethod
def __create_checks(datasource_type: str, datasource_name: str, quality_checks: List[job_model.QualityChecks]) -> yaml:
    """
    Parses the quality checks JSON to a YAML format as required by the Soda library.
    
    :param datasource_name (str): The name of the table
    :param quality_checks (dict): A dictionary containing the quality checks
    
    :return yaml: Parsed quality checks JSON to YAML
    """
    
    if not quality_checks:
        raise Exception("Empty list of checks provided")
    
    quality_checks_list = [check.model_dump() for check in quality_checks]
    checks = []

    try:
        for quality_check in quality_checks_list:
            expectation_type = quality_check.get("expectation_type", "")
            kwargs = quality_check.get("kwargs", "")
            if expectation_type == "schema":
                if kwargs:
                    status = kwargs.get("status", "")
                    condition = kwargs.get("condition", "")
                    values = kwargs.get("values", "")
                    other_kwargs = {key:value for key, value in kwargs.items() if key not in ["status", "condition", "values"]}
                    other_kwargs = __remove_empty_dicts(other_kwargs)
                    if other_kwargs:
                        check = {
                            expectation_type: {
                                status: {
                                    condition: other_kwargs
                                }
                            }
                        }
                    else:
                        check = {
                            expectation_type: {
                                status: {
                                    condition: values
                                }
                            }
                        }
                    checks.append(check)
            else:
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
        
        checks_yaml = yaml.dump({f"checks for {datasource_name}":checks}, default_flow_style=False, sort_keys=False)
        dqt_logger.debug(f"Created checks:\n{checks_yaml}")
        return checks_yaml
    except Exception as e:
        error_msg = f"Failed to create checks: {e}"
        dqt_logger.error(error_msg)
        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message="Failed to create checks")
        raise Exception(error_msg)

@staticmethod
def __parse_validation_result(validation_result: str) -> List[dict]:
    """
    Formats the resultant string list of `CheckResults`. Each of these `CheckResult` objects,
    contains the name of the check, the status of check i.e. PASS, FAIL, or ERROR, and the validation value for the check.
    
    :param validation_results (str): The string of results that need to be parsed.
    
    :return list of dict: A list of dictionaries containing check results.
    """
    check_lines = validation_result.strip().split("\n")
    results = []

    try:
        for line in check_lines:
            """
            Extracts strings of format:
            [avg(bathrooms) between 2 and 3] FAIL (check_value: 1.2862385321100918)
            """
            regex = r"\[(.+?)\]\s+(PASS|FAIL|ERROR|WARN)\s+\(check_value:\s+(\d+(\.\d+)?)\)"
            match = re.match(regex, line)
            if match:
                check_name = match.group(1) # First group: check name
                status = match.group(2) # Second group: status
                check_value = match.group(3) # Third group: full check value (integer or decimal)

                result = {
                    "check_name": check_name,
                    "check_status": status,
                    "check_value": check_value
                }
            else:
                """
                Extracts strings of format:
                [schema] FAIL (fail_missing_column_names = [room], schema_measured = [price bigint, area bigint, bedrooms bigint, bathrooms bigint, stories bigint, mainroad varchar, guestroom varchar, basement varchar, hotwaterheating varchar, airconditioning varchar, parking bigint, prefarea varchar, furnishingstatus varchar])
                """
                regex = r"\[(.+?)\]\s+(PASS|FAIL|ERROR|WARN)\s+\((.+?)\)"
                match = re.match(regex, line)
                if match:
                    check_name = match.group(1)  # Check name
                    status = match.group(2)  # Status (PASS, FAIL, ERROR)
                    metadata_raw = match.group(3)  # Metadata as raw string
                    
                    result = {
                        "check_name": check_name,
                        "check_status": status,
                        "check_value": metadata_raw
                    }
            results.append(result)
        return results
    except Exception as e:
        error_msg = f"Failed to parse validation results: {e}"
        dqt_logger.error(error_msg)
        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message="Failed to parse validation results")
        raise Exception(error_msg)

def __run_quality_checks(datasource_type: str,
                         datasource_name: str, 
                         config: Dict[str, Union[str, int]],
                         quality_checks: List[dict], 
                         is_file: bool = True):
    try:
        soda = SodaModel()
        
        if is_file:
            datasource = soda.SodaFileDatasource(file_name=config["file_name"],
                                                dir_path=config["dir_path"],
                                                host=config["hostname"],
                                                username=config["username"],
                                                password=config["password"],
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
                # removing all special characters
                datasource_name = re.sub(r'[^a-zA-Z0-9_]', '', datasource_name)
                # replacing spaces with '_'
                datasource_name = datasource_name.replace(" ", "_")
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
        dqt_logger.debug(validation_results)
        parsed_results = __parse_validation_result(validation_results)
        dqt_logger.debug(parsed_results)
        parsed_results = CheckResults(results=parsed_results)
        dqt_logger.debug(f"Parsed results:\n{parsed_results}")
        parsed_results_json = json.loads(parsed_results.model_dump_json(indent=4))
        parsed_results_json["validation_date"] = datetime.now().strftime("%Y-%m-%d") # add current date as validation date
        dqt_logger.debug(parsed_results_json)
        shutil.rmtree(".tmp")
        return parsed_results_json
            
    except Exception as e:
        error_msg = f"Failed to run quality checks: {str(e)}"
        dqt_logger.error(error_msg)
        raise RuntimeError(error_msg)

def run_quality_checks_for_db(datasource_type: str, hostname: str, password: str, username: str, 
                                port: int, datasource_name: str, schema_name: str, database: str, 
                                quality_checks: List[job_model.QualityChecks]) -> json:
    """
    Triggers the functions of soda library in the required sequence

    :param datasource_type (str): The type of datasource, e.g.: postgres, mysql, snowflake, mssql, etc.
    :param datasource_name (str): The name of datasource
    :param quality_checks (List[dict]): The list of checks that are to be performed on the data
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

def run_quality_checks_for_file(datasource_type: str, datasource_name: str, dir_path: str, hostname: str, password: str, username: str,
                                quality_checks: List[job_model.QualityChecks], file_name: str) -> json:
    """
    Triggers the functions of soda library in the required sequence

    :param datasource_type (str): The type of datasource, e.g.: csv, orc, avro, json, etc.
    :param datasource_name (str): The name of datasource
    :param dir_path (str): The path where the file is stored
    :param quality_checks (List[dict]): The list of checks that are to be performed on the data
    :param file_name (str): The name of the file

    :return checkpoint_results (json): The generated validation results
    """
    file_config = {
        "hostname": hostname,
        "password": password,
        "username": username,
        "dir_path": dir_path, 
        "file_name": file_name
    }
    return __run_quality_checks(
        datasource_type=datasource_type,
        datasource_name=datasource_name,
        quality_checks=quality_checks,
        config=file_config,
        is_file=True,
    )
    