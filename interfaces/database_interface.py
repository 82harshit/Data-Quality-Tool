from abc import ABC, abstractmethod

class DatabaseInterface(ABC):
    """
    Abstract base class for interacting with a database.
    
    This class defines the interface for all database operations, including
    connecting, inserting, searching, updating, retrieving, and closing the
    database connection. Implementing classes should provide concrete
    implementations for each of these operations.
    """

    @abstractmethod
    def connect_to_db(self):
        """
        Establishes a connection to the database.
        """
        pass

    @abstractmethod
    def insert_in_db(self):
        """
        Inserts data into the database.
        """
        pass

    @abstractmethod
    def search_in_db(self):
        """
        Searches for specific data in the database.
        """
        pass

    @abstractmethod
    def update_in_db(self):
        """
        Updates existing data in the database.
        """
        pass

    @abstractmethod
    def get_from_db(self):
        """
        Retrieves data from the database.
        """
        pass

    @abstractmethod
    def close_db_connection(self):
        """
        Closes the connection to the database.
        """
        pass
