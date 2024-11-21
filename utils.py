from fastapi import HTTPException
from request_models import connection_model
import pymysql
from datetime import datetime
import random
import re

def get_mysql_db(hostname: str, username: str, password: str, database: str, port: int):
    """
    This function establishes a MySQL connection using the provided credentials

    :param hostname: The server IP that the user wants to connect to
    :param username: Name of the user who wants to connect to the server
    :param database: Name of the database to connect to
    :param port: Port number to connect to
    
    :return conn: Established connection object 
    """

    try:
        conn = pymysql.connect(
                host=hostname,
                user=username,      
                password=password,  
                database=database,  
                port=port
            )
        print("Successfully connected")
        return conn
    except pymysql.MySQLError as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to database: {str(e)}")


def remove_special_characters(input_string) -> str:
    """
    Removes all special characters from the input string, keeping only alphanumeric characters.

    :param input_string: The input string.

    :return str: The cleaned string containing only alphanumeric characters.
    """
    return re.sub(r'[^a-zA-Z0-9]', '', str(input_string))


def generate_connection_name(connection: connection_model.Connection) -> str:
    """
    This function generates a unique connection name using timestamp, connection credentials and a random integer
    in the range of 1000 and 9999 like:
    `20241120162230_test_3233347_3006_testdb_3694`

    :param connection: Connection object

    :return unique_connection_name (str): Generated connection name created using connection credentials, random integer 
    and timestamp
    """
    hostname = connection.connection_credentials.server
    username = connection.user_credentials.username
    port = connection.connection_credentials.port
    database = connection.connection_credentials.database
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    rand_int = random.randint(1000,9999) # random generated integer in the range of 1000 to 9999

    # removing all alphanumeric characters
    hostname = remove_special_characters(hostname)
    username = remove_special_characters(username)
    port = remove_special_characters(port)
    database = remove_special_characters(database)

    unique_connection_name = f"{timestamp}_{username}_{hostname}_{port}_{database}_{rand_int}"
    return unique_connection_name


def generate_connection_string(connection: connection_model.Connection) -> str:
    """
    This function generates a connections string like:
    `mysql://test_user:test_password@0.0.0.0:3000/test_database`
    using the connection credentials

    :param connection: Connection object

    :return generated_connection_string (str): Generated connection string created using connection credentials
    """
    hostname = connection.connection_credentials.server
    username = connection.user_credentials.username
    password = connection.user_credentials.password
    port = connection.connection_credentials.port
    database = connection.connection_credentials.database
    connection_type = connection.connection_credentials.connection_type

    generated_connection_string = f"{connection_type}://{username}:{password}@{hostname}:{port}/{database}"
    return generated_connection_string