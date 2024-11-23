from fastapi import HTTPException
import pymysql
from request_models import connection_model
from utils import generate_connection_name, generate_connection_string, get_cred_db_connection_config
import json

class DBFunctions:
    # TODO: Test with non static
    @staticmethod
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
        

    def connect_to_credentials_db(self, connection: connection_model.Connection) -> json:
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
        app_conn = DBFunctions.get_mysql_db(
                            hostname=app_hostname, 
                            username=app_username, 
                            password=app_password, 
                            port=app_port, 
                            database=app_database
                            )
        
        # create unique connection name
        unique_connection_name = generate_connection_name(connection=connection)
        # create connection string
        connection_string = generate_connection_string(connection=connection)

        return {"unique_connection_name": unique_connection_name, 
                "connection_string": connection_string, 
                "app_connection": app_conn,
                "app_table": app_table}
    
    
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


    def save_validation_results_to_db(self, validation_results: json, database_connection_details: json) -> None:
        pass