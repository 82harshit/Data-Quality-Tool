from pydantic import BaseModel, Field, IPvAnyAddress, model_validator
from typing import Optional
from request_models import connection_enum_and_metadata as conn


class UserCredentials(BaseModel):
    username: str = Field("test", description="Name of the user connecting", min_length=1)
    password: Optional[str] = Field(None, description="Password for the user", min_length=8)
    access_token: Optional[str] = Field(None, description="Access token for authentication", min_length=10)

    class Config:
        # Ensuring that the password field is excluded from serialization when calling .dict() or .json()
        fields = {
            'password': {'exclude': True},  # This will exclude password field in the response
            'access_token' : {'exclude' : True}
        } 

class ConnectionCredentials(BaseModel):
    connection_type: str
    database: Optional[str] = Field("quality_tool", description="Name of the database to connect to", min_length=1)
    server: Optional[str] = Field("0.0.0.0", description="Name of the server to connect to")
    port: Optional[int] = Field(5432, description="Port to connect to", gt=9, lt=10000)
    file_name: Optional[str] = Field("", description="Name of the file to connect to", min_length=1)
    dir_path: Optional[str] = Field("", description="Path to the directory")

    @model_validator(mode='before')
    def validate_connection_priority(cls, values):
        file_name = values.get("file_name")
        dir_path = values.get("dir_path")

        # Handle None values explicitly

        if dir_path:
            if not file_name:
                raise ValueError(
                    "If 'dir_path' is provided, 'file_name' must also be specified."
                )

        if file_name and not dir_path:
            return values

        if not file_name and not dir_path:
            raise ValueError(
                "Insufficient information provided. "
                "You must provide either 'file_path', or 'dir_path' with 'file_name', or just 'file_name'."
            )

        return values
class Connection(BaseModel):
    """
    This is the request body for API POST request
    """
    user_credentials: UserCredentials
    connection_credentials: ConnectionCredentials
    metadata: conn.Metadata
