import json
from typing import Optional
   
from database import sql_queries as query_template, app_connection
from database.db_models import sql_query
from interfaces import database_interface
from request_models import connection_enum_and_metadata as conn_enum
from utils import get_cred_db_connection_config, get_cred_db_table_config


class UserCredentialsDatabase(database_interface.DatabaseInterface):
    """
    A class to handle operations related to storing and retrieving user credentials
    from a database.

    Inherits from `DatabaseInterface` to perform common database operations.
    """
    def __init__(self, hostname: str, username: str, password: str, port: int, connection_type: str, 
                 database: Optional[str] = None):
        """
        Initializes the UserCredentialsDatabase with the given parameters.
        These parameters are the user login credentials

        :param hostname (str): The hostname of the database server.
        :param username (str): The username for database authentication.
        :param password (str): The password for database authentication.
        :param port (int): The port number on which the database server is listening.
        :param database (Optionals[str]): The name of the database to connect to.
        :param connection_type (str): The type of connection (e.g., 'mysql', 'postgresql').
        """
        self.hostname = hostname
        self.username = username
        self.port = port
        self.database = database
        self.password = password
        self.connection_type = connection_type
        self.db_connection = None

    def connect_to_db(self) -> None:
        """
        Establishes a connection to the user credentials database.
        This method initializes the `db_connection` instance variable with the app credentials. 
        
        :return: None
        """
        self.db_connection = app_connection.get_app_db_connection_object()
        
    
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

        :return (str): The search result as a string indicating if the connection exists
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
        return str(search_result[0][0]) # decoding tuple to get result and casting to string

    def close_db_connection(self) -> None:
        """
        Closes the database connection.

        This method ensures the connection to the database is closed after operations 
        are completed, freeing up resources.

        :return: None
        """
        self.db_connection.close()

    def update_in_db(self) -> None:
        """
        Updates records in the user credentials database.

        This method calls the `update_in_db` method of the parent `DatabaseInterface` 
        class to handle the update operation.

        :return: None
        """
        return super().update_in_db()
     
    def get_from_db(self):
        """
        Retrieves data from the user credentials database.

        This method calls the `get_from_db` method of the parent `DatabaseInterface` 
        class to handle the retrieval operation.

        :return: The retrieved data from the database.
        """
        return super().get_from_db()

    def get_user_credentials(self, unique_connection_name: str) -> json:
        """
        Retrieves the user credentials associated with the unique connection name.

        :param unique_connection_name (str): The unique connection name for which the 
                                           credentials are to be retrieved.

        :return json: A JSON object containing the user credentials for the connection.
        """
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
