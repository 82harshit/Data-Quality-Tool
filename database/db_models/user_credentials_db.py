from fastapi import HTTPException
import pymysql
from utils import get_cred_db_connection_config, get_cred_db_table_config
import sql_queries as query_template
import sql_query

class UserCredentialsDatabase:
    def __init__(self, hostname: str, username: str, password: str, port: int, database: None, connection_type: str):
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

    @staticmethod
    def connect_to_user_cred_db():
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
            print("Successfully connected to app db")
            return mysql_connection_object_for_app
        except pymysql.MySQLError as e:
            raise HTTPException(status_code=500, detail=f"Error connecting to database: {str(e)}")
        
    
    def insert_creds_in_cred_db(self, unique_connection_name, connection_string):
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
        
        cred_db = sql_query.SQLQuery(db_connection=self.connect_to_user_cred_db(), # user cred database connection obj
                                     query=insert_query,
                                     query_params=insert_params)
        cred_db.execute_query()


    def search_user_in_cred_db(self, unique_connection_name):
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

        cred_db = sql_query.SQLQuery(db_connection=self.connect_to_user_cred_db(), # user cred database connection obj
                                     query=search_query,
                                     query_params=search_params)
        search_result = cred_db.execute_query()
        return search_result

    # TODO: complete
    def get_creds_for_file(self, unique_connection_name):
        db_details = get_cred_db_connection_config()
        table_details = get_cred_db_table_config()  

        app_table = db_details.get('app_table')

        col_connection_name = table_details.get('connection_name')

        col_user_name = table_details.get('user_name')
        col_source_type = table_details.get('source_type')
        col_password = table_details.get('password')
        col_port = table_details.get('port')
        col_database_name = table_details.get('database_name')
        col_hostname = table_details.get('hostname')

        unique_connection_name = (unique_connection_name, ) # converting to tuple

        user_name_query = query_template.GET_LOGIN_CRED_QUERY.format(col_user_name, app_table, col_connection_name)
        user_name_cred_db = sql_query.SQLQuery(db_connection=self.connect_to_user_cred_db(),
                           query=user_name_query,
                           query_params=unique_connection_name)
        user_name = user_name_cred_db.execute_query()

        source_type_query = query_template.GET_LOGIN_CRED_QUERY.format(col_source_type, app_table, col_connection_name)
        source_type_cred_db = sql_query.SQLQuery(db_connection=self.connect_to_user_cred_db(),
                                                 query=source_type_query,
                                                 query_params=unique_connection_name)
        source_type = source_type_cred_db.execute_query()

        return {'username': user_name, 'password': "", 'port': "", 'database': "", 'hostname': "", 'source_type': source_type}
    

    def get_creds_for_db():
        db_details = get_cred_db_connection_config()
        app_table = db_details.get('app_table')

        pass