import configparser
from request_models import connection_enum_and_metadata, connection_model
from datetime import datetime
import random
import re
import json


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
    - File-based example: `20241120162230_test_3233347_3006_data.json_3694`

    :param connection: Connection object

    :return: Unique connection name
    """
    hostname = connection.connection_credentials.server
    username = connection.user_credentials.username
    port = connection.connection_credentials.port
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    rand_int = random.randint(1000, 9999)  # Random integer in the range of 1000 to 9999

    # Extract the connection type correctly
    connection_type = connection.connection_credentials.connection_type

    # Determine whether to use database or file_name
    if connection_type in {
        connection_enum_and_metadata.ConnectionEnum.JSON,
        connection_enum_and_metadata.ConnectionEnum.CSV,
        connection_enum_and_metadata.ConnectionEnum.ORC,
        connection_enum_and_metadata.ConnectionEnum.PARQUET,
        connection_enum_and_metadata.ConnectionEnum.AVRO,
        connection_enum_and_metadata.ConnectionEnum.EXCEL,
    }:
        target = connection.connection_credentials.connection_type
    else:
        target = connection.connection_credentials.database

    # Remove special characters from all components
    hostname = remove_special_characters(hostname)
    username = remove_special_characters(username)
    port = remove_special_characters(port)
    target = remove_special_characters(target)

    # Generate the unique connection name
    unique_connection_name = f"{timestamp}_{username}_{hostname}_{port}_{target}_{rand_int}"
    return unique_connection_name


def generate_connection_string(connection: connection_model.Connection) -> str:
    """
    Generates a connection string for the provided connection type.
    - Database connections: `mysql://test_user:test_password@0.0.0.0:3000/test_database`
    - File-based connections: `json://test_user:test_password@0.0.0.0:3000/test_file.json`

    :param connection: Connection object

    :return: Generated connection string
    """
    hostname = connection.connection_credentials.server
    username = connection.user_credentials.username
    password = connection.user_credentials.password
    port = connection.connection_credentials.port
    connection_type = connection.connection_credentials.connection_type

    # Determine whether to use database or file_name for connection string
    if connection_type in {
        connection_enum_and_metadata.ConnectionEnum.JSON,
        connection_enum_and_metadata.ConnectionEnum.CSV,
        connection_enum_and_metadata.ConnectionEnum.ORC,
        connection_enum_and_metadata.ConnectionEnum.PARQUET,
        connection_enum_and_metadata.ConnectionEnum.AVRO,
    }:
        target = connection.connection_credentials.file_name  # Use file name for file-based connections
    else:
        target = connection.connection_credentials.database  # Use database for database connections

    # Generate connection string
    generated_connection_string = f"{connection_type}://{username}:{password}@{hostname}:{port}/{target}"
    return generated_connection_string


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
        print(f"Error extracting validation result: {e}")
        return None
    
    
def get_cred_db_connection_config() -> json:
    """
    This function reads the contents under the 'Database' section of 
    the configuration file: 'database_config.ini'

    :return db_connection_details (json): A JSON containing the connection credentials to the MySQL 
    database that contains user login credentials
    """

    config = configparser.ConfigParser()
    try:
        path_to_database_config = r'database\database_config.ini' # relative path to database_config.ini
        config.read(path_to_database_config)
    except FileNotFoundError as file_not_found:
        raise FileNotFoundError(f"{str(file_not_found)}\n `database_config.ini` file not found")

    app_database = config.get('Database', 'app_database')
    app_table = config.get('Database', 'app_table')
    app_port = config.get('Database', 'app_port')
    app_hostname = config.get('Database', 'app_hostname')
    app_username = config.get('Database', 'app_username')
    app_password = config.get('Database', 'app_password')
    app_status_table = config.get('Database', 'app_status_table')

    db_connection_details = {
        'app_database': app_database,
        'app_table': app_table,
        'app_port': int(app_port), # port must be of type 'int'
        'app_hostname': app_hostname,
        'app_username': app_username,
        'app_password': app_password,
        'app_status_table': app_status_table
    }

    return db_connection_details


def get_cred_db_table_config() -> json:
    """
    This function reads the contents under the 'Login Credentails Table' section of 
    the configuration file: 'database_config.ini'

    :return login_cred_columns (json): A JSON containing the name of columns as defined
    in `login_credentials` table
    """

    config = configparser.ConfigParser()
    try:
        path_to_database_config = r'database\database_config.ini' # relative path to `database_config.ini`
        config.read(path_to_database_config)
    except FileNotFoundError as file_not_found:
        raise FileNotFoundError(f"{str(file_not_found)}\n `database_config.ini` file not found")

    connection_name = config.get('Login Credentials Table', 'connection_name')
    user_name = config.get('Login Credentials Table', 'user_name')
    source_type = config.get('Login Credentials Table', 'source_type')
    password = config.get('Login Credentials Table', 'password')
    port = config.get('Login Credentails Table', 'port')
    database_name = config.get('Login Credentials Table', 'database_name')
    host_name =  config.get('Login Credentails Table', 'hostname')

    login_cred_columns = {
        'connection_name': connection_name,
        'user_name': user_name,
        'source_type': source_type,
        'password': password,
        'port': int(port), # port must be of type 'int'
        'database_name': database_name,
        'hostname': host_name
    }

    return login_cred_columns


def get_job_run_status_table_config() -> json:
    
    config = configparser.ConfigParser()
    try:
        path_to_database_config = r'database\database_config.ini' # relative path to `database_config.ini`
        config.read(path_to_database_config)
    except FileNotFoundError as file_not_found:
        raise FileNotFoundError(f"{str(file_not_found)}\n `database_config.ini` file not found")

    job_id = config.get('Job Run Status Table','job_id')
    job_status = config.get('Job Run Status Table','job_status')
    status_message = config.get('Job Run Status Table','status_message')

    job_status_columns = {'job_id': job_id, 'job_status': job_status, 'status_message': status_message}
    return job_status_columns


def generate_job_id() -> str:
    rand_int = random.randint(10000000, 99999999)  # Random integer in the range of 10000000 to 99999999
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    new_job_id =  f"Job_{rand_int}{timestamp}"
    return new_job_id