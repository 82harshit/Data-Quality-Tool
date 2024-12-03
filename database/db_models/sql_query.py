from logging_config import dqt_logger
from typing import Optional


class SQLQuery:
    def __init__(self, db_connection, query: str, query_params: Optional[str] = None):
        self.db_connection = db_connection
        self.query = query
        self.query_params = query_params

    def execute_query(self) -> any:
        """
        Executes the given query using connection object of database, 
        query and query parameters
    
        :return query_result (any): The query result
        """
        # creating a connection cursor using database connection object
        connection_cursor = self.db_connection.cursor()
        try:
            connection_cursor.execute(self.query, self.query_params)
            dqt_logger.debug(f"Query:{self.query}, Params:{self.query_params}")
            query_result = connection_cursor.fetchall()
            self.db_connection.commit()
            dqt_logger.info("Successfully executed query")
            return query_result
        except Exception as sql_exe_error:
            error_msg = f"Failed to execute query: {self.query}\n Error: {str(sql_exe_error)}"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)
        finally:
            # closing connections
            dqt_logger.info("Closing cursor connection")
            connection_cursor.close()