from database.db_models import user_credentials_db

class TableDatabase(user_credentials_db.UserCredentialsDatabase):
    def __init__(self, hostname, username, password, port, database, connection_type):
        super().__init__(hostname, username, password, port, database, connection_type)


    