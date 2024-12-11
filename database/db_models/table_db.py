from database.db_models import user_credentials_db, sql_query
from database import database_connection, sql_queries as query_template
from logging_config import dqt_logger


class TableDatabase(user_credentials_db.UserCredentialsDatabase):
    """
    Class for managing database operations related to user credentials, and modifying user privileges.
    Inherits from `UserCredentialsDatabase` to leverage existing database connection functionalities.
    """
    def __init__(self, hostname: str, username: str, password: str, port: int, database: str, connection_type: str):
        """
        Initializes the TableDatabase class with database connection parameters.

        :param hostname: The hostname or IP address of the database server.
        :param username: The username for database authentication.
        :param password: The password for database authentication.
        :param port: The port number for the database connection.
        :param database: The name of the database.
        :param connection_type: Type of connection (e.g., MySQL, PostgreSQL, etc.).
        """
        super().__init__(hostname=hostname, username=username, password=password, port=port, 
                         database=database, connection_type=connection_type)

    @staticmethod
    def __check_if_user_exists(hostname: str, username: str, db_connection) -> bool:
        """
        Checks if a user exists in the database based on the given hostname and username.

        :param hostname: The hostname or IP address of the database server.
        :param username: The username to check in the database.
        :param db_connection: The database connection object to execute the query.
        :return: True if the user exists, False otherwise.
        :raises Exception: If an error occurs while executing the query.
        """
        try:
            user_exists_query = query_template.CHECK_IF_USER_EXISTS
            query_params = (username, hostname) # converting to tuple
            user_exists = sql_query.SQLQuery(db_connection=db_connection,
                                             query=user_exists_query,
                                             query_params=query_params).execute_query()
            return True if user_exists else False
        except Exception as e:
            error_msg = f"An error occurred while checking existence of {username} @ {hostname}.\nError: {e}"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)
    
    @staticmethod
    def create_user_and_grant_read_access(hostname: str, username: str, database_name: str, table_name: str, 
                                          password: str) -> None: 
        """
        Creates a new user in the database and grants read access to the specified table.
        If the user already exists, it skips user creation and directly grants read access.

        :param hostname: The hostname of the database server.
        :param username: The username of the user to create or modify.
        :param database_name: The name of the database to which access is granted.
        :param table_name: The name of the table to which read access is granted.
        :param password: The password for the new user.
        :raises Exception: If an error occurs during user creation or granting access.
        """
        db_connection = database_connection.get_connection_object_for_db(database=database_name)
        user_exists = TableDatabase.__check_if_user_exists(hostname=hostname, username=username, db_connection=db_connection)
        
        if not user_exists:
            try:
                create_user_in_db_query = query_template.CREATE_USER_IN_DB.format(username, hostname, password)
                sql_query.SQLQuery(db_connection=db_connection,query=create_user_in_db_query).execute_query()
                info_msg = f"Successfully created user: {username}"
                dqt_logger.info(info_msg)
            except Exception as create_user_error:
                error_msg = f"An error occurred while creating user {username}\n{str(create_user_error)}"
                dqt_logger.error(error_msg)
                raise Exception(error_msg)
            
        try:
            grant_read_access_to_user_query = query_template.GRANT_READ_ACCESS_TO_USER.format(database_name, table_name, 
                                                                                            username, hostname)
            sql_query.SQLQuery(db_connection=db_connection,query=grant_read_access_to_user_query).execute_query()
            info_msg = f"Successfully granted read access to user: {username}"
            grant_temp_table_creation_query = query_template.GRANT_ACCESS_TO_CREATE_TEMP_TABLES.format(database_name, table_name, 
                                                                                                    username, hostname)
            sql_query.SQLQuery(db_connection=db_connection,query=grant_temp_table_creation_query).execute_query()
            info_msg = f"Successfully granted access to create temp tables to: {username}"
            dqt_logger.info(info_msg)
        except Exception as grant_access_error:
            error_msg = f"An error occurred while granting read access to user {username}\n{str(grant_access_error)}"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)
        finally:
            dqt_logger.info("Applying privileges")
            sql_query.SQLQuery(db_connection=db_connection,query=query_template.APPLY_PRIVILEGES)
            dqt_logger.info("Closing db connection")
            db_connection.close()
    
    @staticmethod
    def revoke_access_and_delete_user(hostname: str, username: str, database_name: str, table_name: str) -> None:
        """
        Revokes read access and deletes a user from the database.

        :param hostname: The hostname of the database server.
        :param username: The username of the user to remove.
        :param database_name: The name of the database from which access is revoked.
        :param table_name: The name of the table from which read access is revoked.
        :raises Exception: If an error occurs during revoking access or deleting the user.
        """
        db_connection = database_connection.get_connection_object_for_db(database=database_name)
        try:
            revoke_read_access_for_user_query = query_template.REVOKE_ACCESS_TO_USER.format(database_name, table_name,
                                                                                                 username, hostname)
            sql_query.SQLQuery(db_connection=db_connection,query=revoke_read_access_for_user_query).execute_query()
            info_msg = f"Successfully revoked read access to user: {username}"
            dqt_logger.info(info_msg)
            remove_user_from_db_query = query_template.DROP_USER_IN_DB.format(username, hostname)
            sql_query.SQLQuery(db_connection=db_connection,query=remove_user_from_db_query).execute_query()
            info_msg = f"Successfully removed user: {username} from database"
            dqt_logger.info(info_msg)
        except Exception as revoke_access_error:
            error_msg = f"An error occurred while revoking read access to user {username}\n{str(revoke_access_error)}"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)
        finally:
            dqt_logger.info("Applying privileges")
            sql_query.SQLQuery(db_connection=db_connection,query=query_template.APPLY_PRIVILEGES)
            dqt_logger.info("Closing db connection")
            db_connection.close()
            