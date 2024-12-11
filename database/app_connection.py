from fastapi import HTTPException 
import pymysql
import psycopg2

from logging_config import dqt_logger
from request_models import connection_enum_and_metadata as conn_enum
from utils import get_cred_db_connection_config


def get_app_db_connection_object():
    """
    Connects to MySQL database using the login credentials from 'database_config.ini'

    :return pymysql.connections.Connection: Connection object of MySQL database
    """

    # app user logging in user_credentials database
    db_conn_details = get_cred_db_connection_config()       

    app_hostname = db_conn_details.get('app_hostname')
    app_username = db_conn_details.get('app_username')
    app_password = db_conn_details.get('app_password')
    app_port = db_conn_details.get('app_port')
    app_database = db_conn_details.get('app_database')

    try:
        mysql_connection_object_for_app = pymysql.connect(
                host=app_hostname,
                user=app_username,      
                password=app_password,  
                database=app_database,  
                port=app_port
            )
        dqt_logger.info("Successfully connected to app db")
        return mysql_connection_object_for_app
    except pymysql.MySQLError as e:
        dqt_logger.error(f"App database connection error: {str(e)}")
        raise HTTPException(status_code=500, detail="Unable to connect to app database")

def get_connection_object_for_db(hostname: str, username: str, password: str, database: str, port: int, connection_type: str):
    """
    Creates a database connection object using the provided login credentials

    :param database (str): Name of the database to connect to
    :param hostname (str): IP of the host to connect to
    :param password (str): The password of the user that wants to connect
    :param connection_type (str): The type of database to connect to (MySQL, Postgres, etc)
    :param port (int): The port of the server to connect to
    
    :return object: Connection object of database
    """
   
    try:
        if connection_type == conn_enum.Database_Datasource_Enum.MYSQL:
            mysql_connection_object_for_db = pymysql.connect(
                host=hostname,
                user=username,      
                password=password,  
                database=database,  
                port=port
            )
            dqt_logger.info(f"Successfully connected to db: {database}")
            return mysql_connection_object_for_db
        elif connection_type == conn_enum.Database_Datasource_Enum.POSTGRES:
            postgres_connection_object_for_db = psycopg2.connect(
                dbname=database,
                user=username,
                password=password,
                host=hostname,
                port=port
            )
            dqt_logger.info(f"Successfully connected to db: {database}")
            return postgres_connection_object_for_db

    except pymysql.MySQLError as e:
        dqt_logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(status_code=500, detail="Unable to connect to database")
    