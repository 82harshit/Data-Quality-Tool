from database.db_models import user_credentials_db
from database import sql_queries as query_template
from database.db_models import sql_query
from logging_config import dqt_logger


class TableDatabase(user_credentials_db.UserCredentialsDatabase):
    def __init__(self, hostname: str, username: str, password: str, port: int, database: str, connection_type: str):
        super().__init__(hostname, username, password, port, database, connection_type)


    @staticmethod
    def grant_access(db_connection, hostname: str, username: str) -> None:
        try:
            grant_read_access_to_user_query = query_template.GRANT_READ_ACCESS_TO_USER.format(username, hostname)
            sql_query.SQLQuery(db_connection=db_connection,query=grant_read_access_to_user_query).execute_query()
            dqt_logger.info(f"Successfully granted read access to user: {username}")
        except Exception as grant_access_error:
            error_msg = f"An error occurred while granting read access to user {username}\n{str(grant_access_error)}"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)
        finally:
            sql_query.SQLQuery(db_connection=db_connection,query=query_template.APPLY_PRIVILEGES)
            
    
    @staticmethod
    def revoke_access(db_connection, hostname: str, username: str) -> None:
        try:
            revoke_read_access_for_user_query = query_template.REVOKE_READ_ACCESS_TO_USER.format(username, hostname)
            sql_query.SQLQuery(db_connection=db_connection,query=revoke_read_access_for_user_query).execute_query()
            dqt_logger.info(f"Successfully revoked read access to user: {username}")
        except Exception as revoke_access_error:
            error_msg = f"An error occurred while revoking read access to user {username}\n{str(revoke_access_error)}"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)
        finally:
            sql_query.SQLQuery(db_connection=db_connection,query=query_template.APPLY_PRIVILEGES)
            
        