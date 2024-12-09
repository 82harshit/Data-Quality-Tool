from abc import ABC, abstractmethod

class GE_API_Interface(ABC):
    """
    Abstract base class that defines the interface for interacting with the GE API.
    """
    @abstractmethod
    def create_connection_based_on_type(self):
        """
        Creates a connection based on the specified connection type (e.g., MySQL, PostgreSQL).
        """
        pass
    
    @abstractmethod
    def insert_user_credentials(self):
        """
        Inserts user credentials into the system or database.
        """
        pass
    
    @abstractmethod
    def validation_check_request(self):
        """
        Validates the credentials or connection request.
        """
        pass
    