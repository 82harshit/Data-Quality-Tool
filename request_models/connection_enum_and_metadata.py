from enum import Enum
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class File_Datasource_Enum(str, Enum):
    """
    This enum contains all the data sources of file type
    """
    
    CSV = "csv"
    JSON = "json"   
    FILESERVER = "fileserver"
    PARQUET = "parquet"
    ORC = "orc"
    AVRO = "avro"
    EXCEL = "xlsx"


class Database_Datasource_Enum(str, Enum):
    """
    This enum contains all the data sources of database type
    """
    MYSQL = "mysql"
    POSTGRES = "postgres"
    REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    ATHENA = "athena"
    TRINO = "trino"
    CLICKHOUSE = "clickhouse"


class Other_Datasources_Enum(str, Enum):
    """
    This enum contains all the other data sources
    """
    SAP = "sap"
    STREAMING = "streaming"


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
    