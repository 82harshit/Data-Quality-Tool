import json
from typing import Optional
import pymysql
from fastapi import HTTPException    

from utils import get_cred_db_connection_config, get_cred_db_table_config
from database import sql_queries as query_template
from database.db_models import sql_query
from interfaces import database_interface
from request_models import connection_enum_and_metadata as conn_enum
from logging_config import dqt_logger


class UserCredentialsDatabase(database_interface.DatabaseInterface):
    def __init__(self, hostname: str, username: str, password: str, port: int, connection_type: str, database: Optional[str] = None):
        """
        Initializes the UserCredentialsDatabase with the given parameters.
        These parameters are the user login credentials

        :param hostname: The hostname of the database server.
        :param username: The username for database authentication.
        :param password: The password for database authentication.
        :param port: The port number on which the database server is listening.
        :param database: The name of the database to connect to.
        :param connection_type: The type of connection (e.g., 'mysql', 'postgresql').
        """

        self.hostname = hostname
        self.username = username
        self.port = port
        self.database = database
        self.password = password
        self.connection_type = connection_type
        self.db_connection = None


    @staticmethod
    def __get_app_db_connection_object():
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


    def connect_to_db(self) -> None:
        """
        Initializes instance variable 'db_connection' with app credentials 
        
        :return: None
        """
        
        self.db_connection = self.__get_app_db_connection_object()
        
    
    def insert_in_db(self, unique_connection_name: str, connection_string: str) -> None:
        """
        Insert user connection credentials into user credentials database
        
        :param unique_connection_name (str): Unique connection name
        :param connection_string (str): Generated connection string from user cred

        :return: None
        """
        
        db_conn_details = get_cred_db_connection_config()  
        app_table = db_conn_details.get('app_table')

        insert_query = query_template.INSERT_CONN_DETAILS_QUERY.format(app_table)
        insert_params = (unique_connection_name,
                        connection_string,
                        self.username,
                        self.connection_type,
                        self.password,
                        self.hostname,
                        self.port,
                        self.database)
        
        cred_db = sql_query.SQLQuery(db_connection=self.db_connection, # user cred database connection obj
                                     query=insert_query,
                                     query_params=insert_params)
        cred_db.execute_query()


    def search_in_db(self, unique_connection_name: str):
        """
        Searches for the existence of a user in the user credentials database

        :param unique_connection_name (str): Generated unique connection name

        :return (str): Search results 
        """

        db_details = get_cred_db_connection_config()
        table_details = get_cred_db_table_config()  

        app_table = db_details.get('app_table')
        col_connection_name = table_details.get('connection_name')

        search_query = query_template.READ_FOR_CONN_NAME_QUERY.format(app_table, col_connection_name)
        search_params = (unique_connection_name,) # creating tuple for search param

        cred_db = sql_query.SQLQuery(db_connection=self.db_connection, # user cred database connection obj
                                     query=search_query,
                                     query_params=search_params)
        search_result = cred_db.execute_query()
        search_result = str(search_result[0][0]) # decoding tuple to get result and casting to string
        return search_result


    def close_db_connection(self):
        self.db_connection.close()


    def get_user_credentials(self, unique_connection_name: str) -> json:
        db_details = get_cred_db_connection_config()
        table_details = get_cred_db_table_config()  

        app_table = db_details.get('app_table')

        col_connection_name = table_details.get('connection_name')

        col_user_name = table_details.get('user_name')
        col_source_type = table_details.get('source_type')
        col_password = table_details.get('password')
        col_port = table_details.get('port')
        col_hostname = table_details.get('hostname')

        unique_connection_name = (unique_connection_name, ) # converting to tuple

        user_name_query = query_template.GET_LOGIN_CRED_QUERY.format(col_user_name, app_table, col_connection_name)
        user_name_cred_db = sql_query.SQLQuery(db_connection=self.db_connection,
                           query=user_name_query,
                           query_params=unique_connection_name)
        user_name = user_name_cred_db.execute_query()
        user_name = str(user_name[0][0])

        source_type_query = query_template.GET_LOGIN_CRED_QUERY.format(col_source_type, app_table, col_connection_name)
        source_type_cred_db = sql_query.SQLQuery(db_connection=self.db_connection,
                                                 query=source_type_query,
                                                 query_params=unique_connection_name)
        source_type = source_type_cred_db.execute_query()
        source_type = str(source_type[0][0])

        password_query = query_template.GET_LOGIN_CRED_QUERY.format(col_password, app_table, col_connection_name)
        password_cred_db = sql_query.SQLQuery(db_connection=self.db_connection,
                                              query=password_query,
                                              query_params=unique_connection_name)
        password = password_cred_db.execute_query()
        password = str(password[0][0])

        port_query = query_template.GET_LOGIN_CRED_QUERY.format(col_port, app_table, col_connection_name)
        port_cred_db = sql_query.SQLQuery(db_connection=self.db_connection,
                                          query=port_query,
                                          query_params=unique_connection_name)
        port = port_cred_db.execute_query()
        port = str(port[0][0])

        hostname_query = query_template.GET_LOGIN_CRED_QUERY.format(col_hostname, app_table, col_connection_name)
        hostname_cred_db = sql_query.SQLQuery(db_connection=self.db_connection,
                                              query=hostname_query,
                                              query_params=unique_connection_name)
        hostname = hostname_cred_db.execute_query()
        hostname = str(hostname[0][0])

        if source_type in conn_enum.Database_Datasource_Enum.__members__.values():
            col_database_name = table_details.get('database_name') 
            database_name_query = query_template.GET_LOGIN_CRED_QUERY.format(col_database_name, app_table, col_connection_name)
            database_cred_db = sql_query.SQLQuery(db_connection=self.db_connection,
                                                query=database_name_query,
                                                query_params=unique_connection_name)
            
            database_name = database_cred_db.execute_query()
            database_name = str(database_name[0][0])

            return {'database': database_name, 'username': user_name, 'source_type': source_type, 'port': port,
                    'hostname': hostname, 'password': password}
        elif source_type in conn_enum.File_Datasource_Enum.__members__.values():
            return {'username': user_name, 'password': password, 'port': port, 'hostname': hostname, 'source_type': source_type}
        else:
            # FUTURE: extract and return required credentials for other data sources
            pass
    