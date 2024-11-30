from pydantic import BaseModel, Field, model_validator
from typing import Optional
from request_models import connection_enum_and_metadata as conn_enum
from logging_config import dqt_logger


class UserCredentials(BaseModel):
    username: str = Field("test", description="Name of the user connecting", min_length=1)
    password: Optional[str] = Field(None, description="Password for the user", min_length=8)
    access_token: Optional[str] = Field(None, description="Access token for authentication", min_length=10)

    class Config:
        # Ensuring that the password field is excluded from serialization when calling .dict() or .json()
        fields = {
            'password': {'exclude': True},  # This will exclude password field in the response
            'access_token' : {'exclude': True}
        } 


class ConnectionCredentials(BaseModel):
    connection_type: str
    database: Optional[str] = Field(None, description="Name of the database to connect to", min_length=1)
    server: str = Field("0.0.0.0", description="Name of the server to connect to")
    port: int = Field(5432, description="Port to connect to", gt=9, lt=10000)
    file_name: Optional[str] = Field(None, description="Name of the file to connect to", min_length=1)
    dir_path: Optional[str] = Field(None, description="Path to the directory")

    @model_validator(mode='before')
    def validate_connection_priority(cls, values):
        if values is None:
            error_msg = "conneection_credentials cannot be null or missing."
            dqt_logger.error(error_msg)
            raise ValueError(error_msg)

        file_name = values.get("file_name")
        dir_path = values.get("dir_path")
        database = values.get("database")
        connection_type = values.get("connection_type")

        if connection_type in conn_enum.Database_Datasource_Enum.__members__.values():
            if database:
                if file_name or dir_path:
                    error_msg = "If 'database' is provided, 'file_name' and 'dir_path' must not be specified."
                    dqt_logger.error(error_msg)
                    raise ValueError(error_msg)
            else:
                error_msg = f"Invalid request JSON, 'database' key and value must be provided"
                dqt_logger.error(error_msg)
                raise ValueError(error_msg)

        elif connection_type in conn_enum.File_Datasource_Enum.__members__.values():
            if dir_path:
                if not file_name:
                    error_msg = "If 'dir_path' is provided, 'file_name' must also be specified."
                    dqt_logger.error(error_msg)
                    raise ValueError(error_msg)
                if database:
                    error_msg = "If 'dir_path' is provided, 'database' must not be specified."
                    dqt_logger.error(error_msg)
                    raise ValueError(error_msg)
                if file_name:
                    # Check if file_name extension is valid
                    file_extension = file_name.split('.')[-1].lower()
                    if file_extension not in [ft.value for ft in conn_enum.File_Datasource_Enum]:
                        error_msg = f"""Invalid file type based on 'file_name' extension: '{file_extension}'. 
                        Must be one of {list(conn_enum.File_Datasource_Enum)}"""
                        dqt_logger.error(error_msg)
                        raise ValueError(error_msg)

            if file_name:
                if database:
                    error_msg = "If 'file_name' is provided, 'database' must not be specified."
                    dqt_logger.error(error_msg)
                    raise ValueError(error_msg)
                else:
                    # Check if file_name extension is valid
                    file_extension = file_name.split('.')[-1].lower()
                    if file_extension != connection_type:
                        error_msg = f"""Invalid value for 'connection_type' provided based on 'file_name' extension: '{file_extension}'. 
                        Must be one of {list(conn_enum.File_Datasource_Enum.__members__.values())}"""
                        dqt_logger.error(error_msg)
                        raise ValueError(error_msg)

        return values
    

class Connection(BaseModel):
    """
    This is the request body for API POST request for `create-connection` endpoint
    """
    user_credentials: UserCredentials
    connection_credentials: ConnectionCredentials
    metadata: conn_enum.Metadata
