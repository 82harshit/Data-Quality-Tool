from pydantic import BaseModel, Field
from typing import Optional
from request_models import connection_enum as conn


class UserCredentials(BaseModel):
    server: Optional[str] = Field(str, description="Name of the server to connect to", min_length=1)
    database: Optional[str] = Field(str, description="Name of the database to connect to", min_length=1)
    port: Optional[int] = Field(5432, description="Port to connect to", gt=999, lt=10000)
    username: str = Field(str, description="Name of the user connecting", min_length=1)
    password: Optional[str] = None
    access_token: Optional[str] = None


class ConnectionCredentials(BaseModel):
    connection_type: str
    connect_to: str


class Connection(BaseModel):
    """
    This is the request body for API POST request
    """
    user_credentials: UserCredentials
    connection_credentials: ConnectionCredentials
    metadata: conn.Metadata
