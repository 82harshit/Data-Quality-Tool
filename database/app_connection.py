import pymysql
from fastapi import HTTPException 
from logging_config import dqt_logger
from utils import get_cred_db_connection_config


@staticmethod
def get_app_db_connection_object():
    """
    Connects to MySQL database using the login credentials from 'database_config.ini'

    :return obj: Connection object of MySQL database
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
        raise HTTPException(status_code=500, detail=f"Error connecting to database: {str(e)}")


@staticmethod
def get_connection_object_for_db(database: str):
    """
    Connects to MySQL database using the login credentials from 'database_config.ini'
    to the given database

    :param database (str): Name of the database to connect to
    
    :return obj: Connection object of MySQL database
    """
    
    # app user logging in user_credentials database
    db_conn_details = get_cred_db_connection_config()       

    app_hostname = db_conn_details.get('app_hostname')
    app_username = db_conn_details.get('app_username')
    app_password = db_conn_details.get('app_password')
    app_port = db_conn_details.get('app_port')
    
    try:
        mysql_connection_object_for_db = pymysql.connect(
                host=app_hostname,
                user=app_username,      
                password=app_password,  
                database=database,  
                port=app_port
            )
        dqt_logger.info(f"Successfully connected to db: {database}")
        return mysql_connection_object_for_db
    except pymysql.MySQLError as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to database: {str(e)}")
    