import json
import random
from typing import Optional

from fastapi import HTTPException

from database import sql_queries as query_template
from database.app_connection import get_app_db_connection_object
from database.db_models.sql_query import SQLQuery
from database.db_models import table_db, file_db
from great_exp.great_exp_model import run_quality_checks_for_file, run_quality_checks_for_db
from interfaces import ge_api_interface
from logging_config import dqt_logger
from request_models import connection_enum_and_metadata as conn_enum, connection_model, job_model
from utils import generate_connection_name, generate_connection_string


class GEFastAPI(ge_api_interface.GE_API_Interface): 
    def __init__(self):
        self.connection_type = None
        self.db_instance = None
        self.unique_connection_name = None

    # for /create-connection endpoint
    def create_connection_based_on_type(self, connection: connection_model.Connection) -> None:
        """
        Creates the object based on connection_type

        :param connection (object): Object of Connection class containing connection credentials
        :return: None
        """
        # extracting user credentials from connection model
        hostname = connection.connection_credentials.server
        username = connection.user_credentials.username
        password = connection.user_credentials.password
        port = connection.connection_credentials.port
        # initializing instance variable self.connection_type
        self.connection_type = connection.connection_credentials.connection_type

        if self.connection_type in conn_enum.Database_Datasource_Enum.__members__.values():
            database = connection.connection_credentials.database

            table_db_obj = table_db.TableDatabase(hostname=hostname,
                                                  username=username,
                                                  password=password, 
                                                  port=port, 
                                                  database=database, 
                                                  connection_type=self.connection_type)
            self.db_instance = table_db_obj
        elif self.connection_type in conn_enum.File_Datasource_Enum.__members__.values():
            file_name = connection.connection_credentials.file_name
            dir_path = connection.connection_credentials.dir_path

            file_db_obj = file_db.FileDatabase(hostname=hostname, 
                                               username=username, 
                                               password=password, 
                                               port=port, 
                                               dir_path=dir_path, 
                                               file_name=file_name, 
                                               database=self.connection_type, # adding connection_type instead of a database name
                                               connection_type=self.connection_type)
            self.db_instance = file_db_obj
        elif self.connection_type in conn_enum.Other_Datasources_Enum.__members__.values():
            """
            # Placeholder for future implementation of 'OtherDatabase'
            # TODO: Implement a sub-class 'OtherDatabase' inheriting from 'UserCredentialsDatabase' as parent class
            Use as:

            other_db_obj = other_db.OtherDatabase()
            self.db_instance = other_db_obj
            """
            raise NotImplementedError("Other database connection type is not yet implemented")
        else:
            error_msg = {
                "error": "Unidentified connection source", 
                "request_json": connection.model_dump_json()
            }
            dqt_logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)

    # for /create-connection endpoint
    async def insert_user_credentials(self, connection: connection_model.Connection, expected_extension: Optional[str] = None) -> str:
        """
        Insert user credentials from connection model to the user_credentials database

        :param connection (object): Object of Connection class containing connection credentials

        :return json: Response json containing the connection status and unique connection name
        """
       
        if self.db_instance is None:
            warning_msg = "File object not initialized before establising connection"
            dqt_logger.warning(warning_msg)
            raise Exception(warning_msg) 
        
        self.db_instance.connect_to_db()  # Initialize the database connection
        
        # Generate unique connection name and connection string
        unique_connection_name = generate_connection_name(connection=connection)
        connection_string = generate_connection_string(connection=connection)

        if expected_extension: # Handle file-related connection
            file_name = connection.connection_credentials.file_name

            try:
                # Validate the file type
                if not file_name.endswith(expected_extension):
                    error_msg = f"The provided file is not a {expected_extension.upper()} file."
                    dqt_logger.error(error_msg)
                    raise HTTPException(status_code=400, detail=error_msg)

                # Search for the file on the server
                search_result = await self.db_instance.search_file_on_server()

                if not search_result["file_found"]:
                    error_msg = f"{file_name} file not found on server."
                    dqt_logger.error(error_msg)
                    raise HTTPException(status_code=404, detail=error_msg)
            except Exception as e:
                error_msg = f"Error processing {file_name} connection: {str(e)}"
                dqt_logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)
        else: # Handle database-related connection
            database_name = connection.connection_credentials.database
            
            # Check if database exists
            try:
                check_if_db_exists_query = query_template.CHECK_IF_DB_EXISTS.format(database_name)
                db_exists = SQLQuery(db_connection=get_app_db_connection_object(), query=check_if_db_exists_query).execute_query()
                if not db_exists:
                    error_msg = "Trying to connect to a database that does not exist on the given server"
                    dqt_logger.error(error_msg)
                    raise HTTPException(status_code=500, detail=error_msg)
            except Exception as e:
                error_msg = f"Error checking database existence for {database_name}: {str(e)}"
                dqt_logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)

        # Insert connection details into the database      
        try:              
            self.db_instance.insert_in_db(unique_connection_name=unique_connection_name,connection_string=connection_string)
            dqt_logger.info("Connection details insertion completed")
        except Exception as e:
            error_msg = f"Error inserting connection details into database: {str(e)}"
            dqt_logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        
        # Close the database connection
        self.db_instance.close_db_connection()
        return unique_connection_name
    
    
    def __get_user_conn_creds(self) -> json:
        """
        Searches for existence of user based on the provided connection name, if found
        retrieves the its credentials for establishing a remote connection

        :param unique_connection_name (str): Connection name passed in the JSON request

        :return json: A json containing user credentials
        """
        try:
            user_exists = self.db_instance.search_in_db(unique_connection_name=self.unique_connection_name) # search for user in login_credentials database
        
            if not user_exists:
                error_msg = {"error": "User not found", "connection_name": self.unique_connection_name}
                dqt_logger.error(f"User not found for connection name: {self.unique_connection_name}")
                raise HTTPException(status_code=404, detail=error_msg)
    
            user_conn_creds = self.db_instance.get_user_credentials(unique_connection_name=self.unique_connection_name)
            
            if not user_conn_creds:
                error_msg = f"No user connection credentials retrieved for connection name: {self.unique_connection_name}"
                dqt_logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)  
            
            dqt_logger.debug(f"Retrieved user connection credentials for {self.unique_connection_name}: {user_conn_creds}")
            return user_conn_creds       
        
        except HTTPException as http_error:
        # Reraise HTTP exceptions to preserve status code and message
            raise http_error
        except Exception as conn_cred_retrieval_error:
            error_msg = f"An unexpected error occurred while retrieving user connection credentials: {str(conn_cred_retrieval_error)}"
            dqt_logger.error(f"Error retrieving credentials for {self.unique_connection_name}: {str(conn_cred_retrieval_error)}")
            raise HTTPException(status_code=500, detail=error_msg)

    
    async def __handle_database_validation(self, job: job_model.SubmitJob, user_conn_creds: dict, quality_checks: list) -> dict:
        """
        Handle validation checks for a database data source.
        """
        table_name = job.data_source.table_name
        database = user_conn_creds.get('database')

        rand_int = random.randint(1000, 9999)  # random 4-digit integer
        datasource_name = f"{table_name}_table_{rand_int}"

        try:
            # Perform database validation checks
            validation_results = run_quality_checks_for_db(
                database=database,
                password=user_conn_creds.get('password'),
                port=user_conn_creds.get('port'),
                hostname=user_conn_creds.get('hostname'),
                quality_checks=quality_checks,
                username=user_conn_creds.get('username'),
                table_name=table_name,
                datasource_name=datasource_name,
                datasource_type=user_conn_creds.get('source_type'),
                schema_name=database
            )
            return json.loads(str(validation_results))  # Convert result to JSON format
        except Exception as ge_exception:
            error_msg = f"An error occurred while validating data for database {database}: {str(ge_exception)}"
            dqt_logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)

    async def __handle_file_validation(self, job: job_model.SubmitJob, quality_checks: list, datasource_type: str) -> dict:
        """
        Handle validation checks for a file data source.
        """
        dir_path = job.data_source.dir_path
        file_name = job.data_source.file_name

        rand_int = random.randint(1000, 9999)  # random 4-digit integer
        datasource_name = f"{file_name}_file_{rand_int}"

        try:
            # Perform file validation checks
            validation_results = run_quality_checks_for_file(
                datasource_type=datasource_type,
                datasource_name=datasource_name,
                file_name=file_name,
                dir_path=dir_path,
                quality_checks=quality_checks
            )
            return json.loads(str(validation_results))  # Convert result to JSON format
        except Exception as ge_exception:
            error_msg = f"An error occurred while validating data for file {file_name}: {str(ge_exception)}"
            dqt_logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)


    # for /submit-job endpoint
    async def validation_check_request(self, job: job_model.SubmitJob) -> json:
        """
        Retrieves user connection credentials, establishes a connection (in case of file source) 
        and executes the list of provided expectation checks on the data

        :param job (object): An object of class SubmitJob containing the validation checks and other details

        :return validation_results (json): A JSON containing the validation response from the great_expectations library
        """
        # extracting connection_name and quality_checks from submit job object
        self.unique_connection_name = job.connection_name # initializing self.unique_connection_name instance variable
        quality_checks = job.quality_checks

        self.db_instance = table_db.TableDatabase(hostname="",username="",password="",port=0,connection_type="",database="") 
        self.db_instance.connect_to_db()
        
        # Get user credentials
        try:
            user_conn_creds = self.__get_user_conn_creds()  # retrieving user credentials from login_credentials table
        except HTTPException as e:
            dqt_logger.error(f"Failed to retrieve user credentials: {e.detail}")
            raise e
        finally:
            self.db_instance.close_db_connection()

        dqt_logger.debug(f"User connection creds retrieved: {user_conn_creds}")

        datasource_type = user_conn_creds.get('source_type')

        if datasource_type in conn_enum.Database_Datasource_Enum.__members__.values():
            return await self.__handle_database_validation(job=job, 
                                                           user_conn_creds=user_conn_creds, 
                                                           quality_checks=quality_checks
                                                           )
        elif datasource_type in conn_enum.File_Datasource_Enum.__members__.values():
            return await self.__handle_file_validation(job=job, 
                                                       quality_checks=quality_checks, 
                                                       datasource_type=datasource_type
                                                       )     
        elif datasource_type in conn_enum.Other_Datasources_Enum.__members__.values():
            #FUTURE: implement a function 'run_quality_check_for_other_sources()' in great_exp_model.py
            raise NotImplementedError("Validation checks for other datasources are not yet implemented.")
        