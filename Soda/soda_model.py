import csv
import dask
import dask.dataframe as dd
import dask.bag as db
from dask.delayed import delayed
from io import StringIO
import pandas as pd
import pyorc
from fastavro import reader
import pyarrow.parquet as pq
from openpyxl import load_workbook
import asyncio
import nest_asyncio
import asyncssh
import posixpath

from soda.scan import Scan
from soda.sampler.sampler import Sampler
from soda.sampler.sample_context import SampleContext

import yaml
import re
import json
import os
from typing import List, Dict, Union
from datetime import datetime
import shutil

from database.db_models.job_run_status import JobRunStatusEnum
from job_state_singleton import JobStateSingleton
from logging_config import dqt_logger
from request_models import connection_enum_and_metadata as conn_enum, job_model
from Soda.soda_results_models import CheckResults
from Soda.soda_parser import SodaParser

nest_asyncio.apply()
TEMP_DIR = ".tmp"
FAILED_CHECKS = "failed_checks"


class SodaModel:
    def __init__(self):
        """
        Creates a new Soda scan object
        """
        self.scan = Scan()    
        
    class CustomSampler(Sampler):
        """Custom sampler class for Soda which collects failed rows from a dataset and stores them as a CSV file in `failed_values` dir.
        Docs: https://docs.soda.io/soda/route-failed-rows.html
        """
        def store_sample(self, sample_context: SampleContext):
            rows = sample_context.sample.get_rows()
            json_data = json.dumps(rows) # Convert failed rows to JSON
            json_data_io = StringIO(json_data) # Use StringIO to wrap the JSON string
            exceptions_df = pd.read_json(json_data_io) # create dataframe with failed rows
            # Define exceptions dataframe
            exceptions_schema = sample_context.sample.get_schema().get_dict()
            exception_df_schema = []
            for n in exceptions_schema:
                exception_df_schema.append(n["name"])
            exceptions_df.columns = exception_df_schema
            check_name = sample_context.check_name
            exceptions_df['failed_check'] = check_name
            exceptions_df['created_at'] = datetime.now()
            # Ensure the "failed_values" directory exists
            os.makedirs(FAILED_CHECKS, exist_ok=True)
            # Save the dataframe as a CSV file in the "failed_values" folder
            file_path = os.path.join(FAILED_CHECKS, f"{check_name}_{datetime.now().strftime('%d-%m-%y_%H-%M-%S')}.csv")
            exceptions_df.to_csv(file_path, sep=",", index=False, encoding="utf-8")
                
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
                with open(file_path, 'r', encoding='utf-8') as file:
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
        
        async def __get_file_path(self) -> str:
            """Checks if a file is present locally on the provided path, otherwise established an SFTP connection to the server, 
            downloads the required file and saves it locally in .tmp folder.
            This .tmp folder is created and then deleted after the valdation is completed.
            
            Raises:
                fnf_error: File not found error when the expected file is not present on server or at the given path
                e: Raises exceptions that are encountered while downloading the file

            :return str: File path where the downloaded file was stored
            """
            try:
                file_path = posixpath.join(self.dir_path, self.file_name)
                
                # check if file is present on the local system
                if self.host in ["127.0.0.1", "localhost"] and os.path.isfile(file_path):
                    return file_path
                
                #  search for file on remote server
                dqt_logger.debug(f"Initializing connection to {self.host}")
                async with asyncssh.connect(self.host, username=self.username, password=self.password, known_hosts=None) as conn:
                    dqt_logger.info(f"Successfully connected to {self.host}")    
                    os.makedirs(TEMP_DIR, exist_ok=True)
                    local_temp_path = os.path.join(TEMP_DIR, self.file_name) # create file path to .tmp dir
                    async with conn.start_sftp_client() as sftp:
                        await sftp.get(file_path, local_temp_path) # get file from server to .tmp using sftp
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
                        parts = delayed(pd.read_csv)(local_temp_file_path)
                        return dd.from_delayed(parts)
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
            

def __run_quality_checks(datasource_type: str,
                         datasource_name: str, 
                         config: Dict[str, Union[str, int]],
                         quality_checks: List[dict], 
                         is_file: bool = True):
    try:
        soda = SodaModel()
        soda_parser = SodaParser()
        
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
                            .str.replace(r'[^a-zA-Z0-9_]', '', regex=True) # Remove special characters
                            .str.lower() # Convert to lowercase
                )
                dqt_logger.debug(f"File dataframe column names: {file_dataframe.columns}")
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
            
        # creating YAML check string for scan
        checks = soda_parser.create_checks(datasource_type=datasource_type, datasource_name=datasource_name, quality_checks=quality_checks)
        
        soda.scan.set_data_source_name(data_source_name=datasource_name) # initializing scan datasource name
        soda.scan.add_sodacl_yaml_str(checks) # adding Soda checks as a YAML string to scan object
        soda.scan.sampler = soda.CustomSampler() # custom sampler to save failed expectations
        soda.scan.execute() # executing scan
        validation_results = soda.scan.get_all_checks_text()
        dqt_logger.debug(f"Validation results:\n{validation_results}")
        
        parsed_results = soda_parser.parse_validation_result(validation_results) # parsing validated result
        dqt_logger.debug(f"Parsed results:\n{parsed_results}")
        parsed_results = CheckResults(results=parsed_results)
        dqt_logger.debug(f"Check results:\n{parsed_results}")
        
        parsed_results_json = json.loads(parsed_results.model_dump_json(indent=4))
        parsed_results_json["validation_date"] = datetime.now().strftime("%Y-%m-%d") # add current date as validation date
        parsed_results_json["datasource_name"] = datasource_name # add datasource name
    
        dqt_logger.debug(f"Parser results JSON:\n{parsed_results_json}")
        
        shutil.rmtree(".tmp") # removing all downloaded files
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
    