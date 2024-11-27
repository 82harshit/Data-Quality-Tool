class SQLQuery:
    def __init__(self, db_connection, query: str, query_params: None):
        self.db_connection = db_connection
        self.query = query
        self.query_params = query_params

    def execute_query(self) -> any:
        # creating a connection cursor using database connection object
        connection_cursor = self.db_connection.cursor()
        try:
            connection_cursor.execute(self.query, self.query_params)
            query_result = connection_cursor.fetchone()
            self.db_connection.commit()
            return query_result
        except Exception as sql_exe_error:
            raise Exception(f"Failed to execute query: {self.query}\nError: {str(sql_exe_error)}")
        finally:
            # closing connections
            connection_cursor.close()
        #     self.db_connection.close()

    # def get_insert_query(dest_table: str, values: tuple):
    #     insert_query = f"INSERT INTO {dest_table} VALUES {values};"
    #     return insert_query

    # def get_delete_query(delete_value, source_table: str, condition, condition_value):
    #     delete_query = f"DELETE {delete_value} FROM {source_table} WHERE {condition} = {condition_value};"
    #     return delete_query

    # def get_select_query(select_value, source_table: str, condition, condition_value):
    #     select_query = f"SELECT {select_value} FROM {source_table} WHERE {condition} = {condition_value};"
    #     return select_query

    # def get_update_query():
    #     pass

    # def get_alter_table_query(table):
    #     alter_table_query = f"ALTER TABLE {table};" # TODO: Complete
    #     return alter_table_query

    # def get_create_user_query(username: str, host_address: str, identification: str):
    #     create_user_query = f"CREATE USER {username}@{host_address} IDENTIFIED BY {identification};"
    #     return create_user_query

    # def get_grant_privileges_query(priviliges, database: str, table: str, username: str, host_address: str):
    #     grant_privilieges_query = f"GRANT {priviliges} PRIVILEGES ON {database}.{table} TO {username}@{host_address};"
    #     return grant_privilieges_query