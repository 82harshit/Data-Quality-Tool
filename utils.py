import configparser
from datetime import datetime
import random
import re
import json
import os

from request_models import connection_enum_and_metadata as conn_enum, connection_model
from logging_config import dqt_logger


def remove_special_characters(input_string) -> str:
    """
    Removes all special characters from the input string, keeping only alphanumeric characters.

    :param input_string: The input string.

    :return str: The cleaned string containing only alphanumeric characters.
    """
    return re.sub(r'[^a-zA-Z0-9]', '', str(input_string))

def generate_connection_name(connection: connection_model.Connection) -> str:
    """
    Generates a unique connection name using timestamp, connection credentials, and a random integer.
    Supports both database and file-based connections:
    - Database example: `20241120162230_test_3233347_3006_testdb_3694`
    - File-based example: `20241120162230_test_3233347_3006_datajson_3694`

    :param connection: Connection object

    :return (str): Unique connection name
    """
    hostname = remove_special_characters(connection.connection_credentials.server)
    username = remove_special_characters(connection.user_credentials.username)
    port = remove_special_characters(connection.connection_credentials.port)
    # Extract the connection type correctly
    connection_type = connection.connection_credentials.connection_type

    # Determine whether to use database or file_name
    target = (
        remove_special_characters(connection.connection_credentials.file_name)
        if connection_type in conn_enum.File_Datasource_Enum.__members__.values()
        else remove_special_characters(connection.connection_credentials.database)
    )
    
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    rand_int = random.randint(1000, 9999)  # Random integer in the range of 1000 to 9999
    
    # Generate the unique connection name
    return f"{timestamp}_{username}_{hostname}_{port}_{target}_{rand_int}"

def generate_connection_string(connection: connection_model.Connection) -> str:
    """
    Generates a connection string for the provided connection type.
    - Database connections: `mysql://test_user:test_password@0.0.0.0:3000/test_database`
    - File-based connections: `json://test_user:test_password@0.0.0.0:3000/test_file.json`

    :param connection: Connection object

    :return (str): Generated connection string
    """
    hostname = connection.connection_credentials.server
    username = connection.user_credentials.username
    password = connection.user_credentials.password
    port = connection.connection_credentials.port
    connection_type = connection.connection_credentials.connection_type

    # Determine whether to use database or file_name for connection string
    target = (
        connection.connection_credentials.file_name
        if connection_type in conn_enum.File_Datasource_Enum.__members__.values()
        else connection.connection_credentials.database
    ) # Use file name for file-based connections and database for database connections
    
    # Generate connection string
    return f"{connection_type}://{username}:{password}@{hostname}:{port}/{target}"

def find_validation_result(data):
    """
    This utility function finds the key 'validation_result' in the provided JSON data
    
    :param data (json): Validation result JSON

    :return: JSON containing validation results or None  
    """

    try:
        # Accessing the 'run_results' and then navigating to the specific validation result
        run_results = data.get('run_results', {})
        
        # Iterate over each key in the run_results
        for result_key, result_value in run_results.items():
            # Check if the 'validation_result' key exists
            if 'validation_result' in result_value:
                return result_value['validation_result']
        return None  # Return None if no validation_result is found
    except Exception as e:
        dqt_logger.error(f"Error extracting validation result: {e}")
        return None
    
def get_cred_db_connection_config() -> json:
    """
    This function reads the contents under the 'Database' section of 
    the configuration file: 'database_config.ini'

    :return (json): A JSON containing the connection credentials to the MySQL 
    database that contains user login credentials
    """

    config = configparser.ConfigParser()
    try:
        path_to_database_config = os.path.join('database', 'database_config.ini') # relative path to database_config.ini
        config.read(path_to_database_config)
    except FileNotFoundError as file_not_found:
        dqt_logger.error(f"{str(file_not_found)}\n `database_config.ini` file not found")
        raise FileNotFoundError(f"{str(file_not_found)}\n `database_config.ini` file not found")

    return {
        'app_database': config.get('Database', 'app_database'),
        'app_table': config.get('Database', 'app_table'),
        'app_port': int(config.get('Database', 'app_port')), # port must be of type 'int'
        'app_hostname': config.get('Database', 'app_hostname'),
        'app_username': config.get('Database', 'app_username'),
        'app_password': config.get('Database', 'app_password')
    }

def get_cred_db_table_config() -> json:
    """
    This function reads the contents under the 'Login Credentails Table' section of 
    the configuration file: 'database_config.ini'

    :return (json): A JSON containing the name of columns as defined
    in `login_credentials` table
    """

    config = configparser.ConfigParser()
    try:
        path_to_database_config = os.path.join('database', 'database_config.ini') # relative path to `database_config.ini`
        config.read(path_to_database_config)
    except FileNotFoundError as file_not_found:
        dqt_logger.error(f"{str(file_not_found)}\n `database_config.ini` file not found")
        raise FileNotFoundError(f"{str(file_not_found)}\n `database_config.ini` file not found")

    return {
        'connection_name': config.get('Login Credentials Table', 'connection_name'),
        'user_name': config.get('Login Credentials Table', 'user_name'),
        'source_type': config.get('Login Credentials Table', 'source_type'),
        'password': config.get('Login Credentials Table', 'password'),
        'port': config.get('Login Credentials Table', 'port'),
        'database_name': config.get('Login Credentials Table', 'database_name'),
        'hostname': config.get('Login Credentials Table', 'hostname')
    }

def get_job_run_status_table_config() -> json:
    """
    This function reads the contents under the 'Job Run Status Table' section of 
    the configuration file: 'database_config.ini'

    :return login_cred_columns (json): A JSON containing the name of columns and table name
    as defined in `job_run_status` table
    """
    
    config = configparser.ConfigParser()
    try:
        path_to_database_config = os.path.join('database', 'database_config.ini') # relative path to `database_config.ini`
        config.read(path_to_database_config)
    except FileNotFoundError as file_not_found:
        dqt_logger.error(f"{str(file_not_found)}\n `database_config.ini` file not found")
        raise FileNotFoundError(f"{str(file_not_found)}\n `database_config.ini` file not found")

    return {
        'job_id': config.get('Job Run Status Table','job_id'), 
        'job_status': config.get('Job Run Status Table','job_status'), 
        'status_message': config.get('Job Run Status Table','status_message'), 
        'job_status_table': config.get('Job Run Status Table','job_status_table')
    }

def generate_job_id() -> str:
    """
    Generates a random job run id using 8 digit randomly generated integer and timestamp
    :return (str): Random generated job id
    """
    rand_int = random.randint(10000000, 99999999)  # Random integer in the range of 10000000 to 99999999
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"Job_{rand_int}{timestamp}"
