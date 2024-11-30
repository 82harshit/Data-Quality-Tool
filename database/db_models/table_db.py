from database.db_models import user_credentials_db

class TableDatabase(user_credentials_db.UserCredentialsDatabase):
    def __init__(self, hostname: str, username: str, password: str, port: int, database: str, connection_type: str):
        super().__init__(hostname, username, password, port, database, connection_type)


    