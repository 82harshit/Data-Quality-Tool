from abc import ABC, abstractmethod

class DatabaseInterface(ABC):
    @abstractmethod
    def connect_to_db(self):
        pass

    @abstractmethod
    def insert_in_db(self):
        pass

    @abstractmethod
    def search_in_db(self):
        pass

    @abstractmethod
    def update_in_db(self):
        pass

    @abstractmethod
    def get_from_db(self):
        pass

    @abstractmethod
    def close_db_connection(self):
        pass
