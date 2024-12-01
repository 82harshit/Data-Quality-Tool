from abc import ABC, abstractmethod

class GE_API_Interface(ABC):
    @abstractmethod
    def create_connection_based_on_type(self):
        pass
    
    @abstractmethod
    def insert_user_credentials(self):
        pass
    
    @abstractmethod
    def validation_check_request(self):
        pass
    