class DB_Instance_Singleton:
    _instance = None
    _db = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def set_db_instance(cls, db):
        cls._db = db

    @classmethod
    def get_db_instance(cls):
        return cls._db