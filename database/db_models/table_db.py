import user_credentials_db

class TableDatabase(user_credentials_db.UserCredentialsDatabase):
    def __init__(self, hostname, username, password, port, database, connection_type, table_name: str):
        super().__init__(hostname, username, password, port, database, connection_type)
        self.table_name = table_name

    