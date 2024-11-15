from pydantic import BaseModel, Field
from typing import Optional, List
from request_models import connection_enum as conn


class DataSource(BaseModel):
    source_file_type: str = Field(
        str, description="Format in which the file is stored at the source", min_length=1)
    source_path: Optional[str] = Field(
        None, description="Path on the server where the required file is stored", min_length=1)
    source_data_format: str = Field(
        str, description="Format in which data is stored", min_length=1)
    source_schema: str = Field(
        str, description="Schema of the data source")  # TODO: change


class DataTarget(BaseModel):
    target_data_type: str = Field(
        str, description="Format in which the file is to be stored at the destiantion", min_length=1)
    target_path: Optional[str] = Field(
        None, description="Path on the destination system where the file is to be stored")
    target_data_format: str = Field(
        str, description="Format in which data is stored", min_length=1)
    target_schema: str = Field(
        str, description="Schema of the data source")  # TODO: change


class QualityChecks(BaseModel):
    check_name: str = Field(
        str, description="Name of the check that is applied", min_length=1)
    applied_on_column: str = Field(
        str, description="Name of the column on which the check is applied", min_length=1)
    check_kwargs: dict


class SubmitJob(BaseModel):
    data_source: DataSource
    data_target: DataTarget
    quality_checks: List[QualityChecks]
    metadata: conn.Metadata
