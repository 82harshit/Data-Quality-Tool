from database.db_models import user_credentials_db
from database import sql_queries as query_template
from database.db_models import sql_query
from logging_config import dqt_logger
from database import app_connection


class TableDatabase(user_credentials_db.UserCredentialsDatabase):
    def __init__(self, hostname: str, username: str, password: str, port: int, database: str, connection_type: str):
        super().__init__(hostname=hostname, username=username, password=password, port=port, database=database, connection_type=connection_type)

    @staticmethod
    def __check_if_user_exists(hostname: str, username: str, db_connection) -> bool:
        try:
            user_exists_query = query_template.CHECK_IF_USER_EXISTS
            query_params = (username, hostname) # converting to tuple
            user_exists = sql_query.SQLQuery(db_connection=db_connection,query=user_exists_query,query_params=query_params).execute_query()
            return True if user_exists else False
        except Exception as e:
            error_msg = f"An error occurred while checking existence of {username} @ {hostname}.\nError: {e}"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)
    
    @staticmethod
    def create_user_and_grant_read_access(hostname: str, username: str, database_name: str, table_name: str, password: str) -> None: 
        db_connection = app_connection.get_connection_object_for_db(database=database_name)
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
        db_connection = app_connection.get_connection_object_for_db(database=database_name)
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
            