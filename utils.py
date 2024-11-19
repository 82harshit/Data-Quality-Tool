from fastapi import HTTPException
from request_models import connection_model
import pymysql
from datetime import datetime

def get_mysql_db(hostname, username, password, database, port):
    # hostname = connection.connection_credentials.server
    # username = connection.user_credentials.username
    # password = connection.user_credentials.password
    # port = connection.connection_credentials.port
    # database = connection.connection_credentials.database

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


def generate_connection_name(connection: connection_model.Connection):
    hostname = connection.connection_credentials.server
    username = connection.user_credentials.username
    password = connection.user_credentials.password
    port = connection.connection_credentials.port
    database = connection.connection_credentials.database

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_connection_name = f"{timestamp}_{username}_{password}_{database}_{hostname}_{port}_{database}"
    return unique_connection_name


def generate_connection_string(connection: connection_model.Connection):
    hostname = connection.connection_credentials.server
    username = connection.user_credentials.username
    password = connection.user_credentials.password
    port = connection.connection_credentials.port
    database = connection.connection_credentials.database
    connection_type = connection.connection_credentials.connection_type

    generated_connection_string = f"{connection_type}://{username}:{password}@{hostname}:{port}/{database}"
    return generated_connection_string