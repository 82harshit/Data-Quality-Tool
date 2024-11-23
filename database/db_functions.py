from fastapi import HTTPException
import pymysql
from request_models import connection_enum_and_metadata
from utils import get_cred_db_connection_config
import json

class DBFunctions:
    # TODO: Test with non static
    @staticmethod
    def __get_mysql_db(hostname: str, username: str, password: str, database: str, port: int):
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
        

    def connect_to_credentials_db(self, connection_type: str) -> json:
        """
        This function connects to the `user_credentials` database using the connection
        credentials retrieved from the `database_config.ini`

        :param connection (Connection): An object of the Connection class that contains,
        the user connection request

        :return: A JSON containing the `unique connection name`, `connection string` and 
        the cursor created from the connection
        """
        # app user logging in user_credentials database
        db_conn_details = get_cred_db_connection_config()       

        app_hostname = db_conn_details.get('app_hostname')
        app_username = db_conn_details.get('app_username')
        app_password = db_conn_details.get('app_password')
        app_port = db_conn_details.get('app_port')
        app_database = db_conn_details.get('app_database')
        app_table = db_conn_details.get('app_table')

        # create connection using app credentials
        app_conn = DBFunctions.__get_mysql_db(
                            hostname=app_hostname, 
                            username=app_username, 
                            password=app_password, 
                            port=app_port, 
                            database=app_database
                            )
        
        if connection_type == connection_enum_and_metadata.ConnectionEnum.MYSQL:
            return {"app_connection": app_conn,
                    "app_table": app_table}
        elif connection_type in {connection_enum_and_metadata.ConnectionEnum.ORC, 
                             connection_enum_and_metadata.ConnectionEnum.AVRO,
                             connection_enum_and_metadata.ConnectionEnum.CSV,
                             connection_enum_and_metadata.ConnectionEnum.PARQUET,
                             connection_enum_and_metadata.ConnectionEnum.JSON
                             }:
            return {"app_connection": app_conn}
        
    
    def execute_sql_query(self, db_connection, sql_query: str, params: None):
        """
        This function executes the provided SQL query using connection and parameters

        :param sql_query (str): The query to be executed
        :param params (tuple): The parameters included in the query
        :param db_connection: The database connection

        :return query_result: The result from the executed query
        """
        cursor = db_connection.cursor() # cursor to execute query
        try:
            if params:
                cursor.execute(sql_query, params)
            else:
                cursor.execute(sql_query)

            query_result = cursor.fetchone()
            db_connection.commit() # saving transaction
            
            return query_result
        except Exception as sql_error:
            raise Exception(f"Failed to exceute SQL query:\n{sql_query}\nError: {sql_error}")
        finally: 
            # closing connections
            db_connection.close()
            cursor.close()
            print('Database connection closed')


    def check_and_get_user_creds(self, connection_name: str) -> json:
        """
        This function checks for the existence of a user in the `login_credentials` table
        and extracts the credentials, if present

        :param connection_name (str): A unique identifier that is used to search for user credentials
        
        :return user_credentials (json): A json containing all extracted user credentials
        """
        pass


    def save_validation_results_to_db(self, validation_results: json, database_connection_details: json) -> None:
        """
        This function saves the validation results to the given database
        
        :param validation_results (json): A json containing validation results that need to be saved in the database
        :param database_connection_details (json): A json containing details useful to connect to given database

        :return: None
        """
        pass