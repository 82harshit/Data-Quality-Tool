from typing import Optional
from pydantic import BaseModel, Field


class AllExpectations(BaseModel):
    avg: Optional[str] = Field(None, description="Calculates the average (mean) of the values.")
    avg_length: Optional[str] = Field(None, description="Calculates the average length of string values.")
    duplicate_count: Optional[str] = Field(None, description="Counts the total number of duplicate rows.")
    duplicate_percent: Optional[str] = Field(None, description="Calculates the percentage of duplicate rows.")
    invalid_count: Optional[str] = Field(None, description="Counts the total number of invalid entries based on a rule.")
    invalid_percent: Optional[str] = Field(None, description="Calculates the percentage of invalid entries.")
    max: Optional[str] = Field(None, description="Finds the maximum value in the dataset.")
    max_length: Optional[str] = Field(None, description="Finds the maximum length of string values.")
    min: Optional[str] = Field(None, description="Finds the minimum value in the dataset.")
    min_length: Optional[str] = Field(None, description="Finds the minimum length of string values.")
    missing_count: Optional[str] = Field(None, description="Counts the total number of missing values.")
    missing_percent: Optional[str] = Field(None, description="Calculates the percentage of missing values.")
    percentile: Optional[str] = Field(None, description = "Finds a specific percentile value of the data.")
    row_count: Optional[str] = Field(None, description="Counts the total number of rows in the dataset.")
    stddev: Optional[str] = Field(None, description="Calculates the standard deviation of the values.")
    stddev_pop: Optional[str] = Field(None, description="Calculates the population standard deviation.")
    stddev_samp: Optional[str] = Field(None, description="Calculates the sample standard deviation.")
    sum: Optional[str] = Field(None, description="Calculates the sum of all values.")
    variance: Optional[str] = Field(None, description="Calculates the variance of the values.")
    var_pop: Optional[str] = Field(None, description="Calculates the population variance.")
    var_samp: Optional[str] = Field(None, description="Calculates the sample variance.")
    distribution_difference: Optional[str] = Field(None, description="""To determine whether the distribution of a 
                                                   column has changed between two points in time""")
    freshenss: Optional[str] = Field(None, description="To determine the relative age of the data in a column in your dataset.")
    schema_check: Optional[str] = Field(None, description="""To validate the presence, absence or position of columns in a dataset, 
                                        or to validate the type of data column contains. 
                                        The status for a schema check can be pass, fail or warn""", alias='schema')
    