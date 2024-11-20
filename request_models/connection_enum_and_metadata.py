from enum import Enum
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class ConnectionEnum(str, Enum):
    """
    This enum contains all the data sources
    """
    MYSQL = "mysql"
    POSTGRES = "postgres"
    CSV = "csv"
    JSON = "json"
    SAP = "sap"
    STREAMING = "streaming"
    REDSHIFT = "redshift"
    FILESERVER = "fileserver"
    PARQUET = "parquet"
    ORC = "orc"
    AVRO = "avro"

class Metadata(BaseModel):
    requested_by: str = Field(
        "user@example.com", description="This contains the name of the system sending the request")
    execution_time: datetime
    description: Optional[str] = Field(
        "This is a test description", description="This is the description of this request", max_length=100)
    
    class Config:
        # Ensuring that the password field is excluded from serialization when calling .dict() or .json()
        fields = {
            'execution_time': {'exclude': True}
        } 
    