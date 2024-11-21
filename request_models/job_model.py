from pydantic import BaseModel, Field
from typing import Optional, List
from request_models import connection_enum_and_metadata as conn


class DataSource(BaseModel):
    dir_path: Optional[str] = Field(str, description="Path of dir in which files need to be validated")
    file_name: Optional[str] = Field(str, description="Name of file to be validated")
    table_name: Optional[str] = Field(str, description="Name of table, data of which needs to be validated")


class DataTarget(BaseModel):
    target_data_type: str = Field(
        str, description="Format in which the file is to be stored at the destiantion", min_length=1)
    target_path: Optional[str] = Field(
        None, description="Path on the destination system where the file is to be stored")
    target_data_format: str = Field(
        str, description="Format in which data is stored", min_length=1)
    # target_schema: str = Field(
    #     str, description="Schema of the data source")  # TODO: change


class QualityChecks(BaseModel):
    expectation_type: str = Field(
        str, description="Name of the check that is applied", min_length=1)
    kwargs: dict = Field(dict, description="Contains the column on which the check is to be applied and the arguments for the check")


class SubmitJob(BaseModel):
    connection_name: str = Field(
        str, description="Unique connection name generated by create-connection endpoint", min_length=36)
    # data_source: DataSource
    # data_target: DataTarget
    quality_checks: List[QualityChecks]
    metadata: conn.Metadata
