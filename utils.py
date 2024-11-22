from fastapi import HTTPException
from request_models import connection_enum_and_metadata, connection_model
import pymysql
from datetime import datetime
import random
import re


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

