"""
This file contains the following FastAPI endpoints:-

1. /create-connection: This endpoint is used to test connection with server or filesystem, 
                       it stores the user credentials and generates a 
                       unique `connection name` for the user

2. /submit-job:

    2.1 This endpoint is used to establish a connection with server or filesystem,
        using the `connection name` from the `create-connection` endpoint.
    2.2 Using the established connection it fetches the data from server on which the
        data quality checks need to be applied
    2.3 It validates the data using the requested checks on the data source,
        both of which are provided by the user
    2.4 The validation results generated are then saved in a relational database
    
3. /submit-job-status: This endpoint returns the execution status of the given job_id

4. /generate-suggestions: This endpoint generates data quality check suggestions using AI for 
                          data based on the provided metric
"""

import configparser
from fastapi import FastAPI, Body, HTTPException

from database.db_models.job_run_status import JobRunStatusEnum
from validation_fast_api_class import ValidationFastAPI
from job_state_singleton import JobStateSingleton
from request_models import connection_enum_and_metadata as conn_enum, connection_model, job_model
from database.save_validation_results import ValidationResult
from logging_config import dqt_logger
from utils import log_validation_results, generate_job_id
from suggestion_bi.suggestion import SuggestionBI


def get_job_id_and_initialize_job_state_singleton() -> str:
    """
    Creates a new job id and sets it up in the singleton object
    
    :return job_id(str): Generated job_id
    """
    job_id = generate_job_id() # creates a new job id
    dqt_logger.info(f"Generated Job_ID: {job_id}") # logs the job id
    JobStateSingleton.set_job_id(job_id=job_id) # sets the job_id in singleton object
    return job_id


app = FastAPI()


@app.get("/", description='This is the root route')
async def root():
    return {"message": "Welcome to Data Quality Tool"}


@app.get("/submit-job-status", description="This endpoint returns the application state for 'submit-job' endpoint")
async def submit_job_status(job_id: str):
    current_job_state = JobStateSingleton.get_state_of_job_id(job_id=job_id)
    return current_job_state


@app.post("/create-connection", description="This endpoint allows connection to the provided connection type")
async def create_connection(connection: connection_model.Connection = Body(...,
    examples = [{
        "user_credentials": {
            "username": "test",
            "password": "test123",
            "access_token": "test_at"
        },
        "connection_credentials": {
            "connection_type": "postgres",
            "port": 3000,
            "server": "server_IP",
            "database": "test_DB",
            "file_name" : "test-file.csv",
            "dir_path" : "/home/user/Desktop"
        },
        "metadata": {
            "requested_by": "user@example.com",
            "execution_time": "2024-09-26T10:00:00Z",
            "description": "This is a test description"
        }
    }]
)): 
    if not connection.user_credentials:
        error_msg = "Incorrect JSON request, missing user credentials"
        dqt_logger.error(error_msg)
        raise HTTPException(status_code=400, detail={"error": error_msg})
    
    if connection.connection_credentials:
        connection_type = connection.connection_credentials.connection_type
    else:
        error_msg = "Incorrect JSON request, missing connection credentials"
        dqt_logger.error(error_msg)
        raise HTTPException(status_code=400, detail={"error": error_msg})
    
    try:
      validation_api = ValidationFastAPI()
      validation_api.create_connection_based_on_type(connection=connection) # create connection to the user_credentials db
    except Exception as e:
      error_msg = f"Error creating connection: {str(e)}"
      dqt_logger.error(error_msg)
      raise HTTPException(status_code=503, detail={"error": error_msg})
    
    try:
      # insert credentials based on connection_type
      if connection_type in conn_enum.File_Datasource_Enum.__members__.values():
          unique_connection_name = await validation_api.insert_user_credentials(connection=connection, 
                                                                                  expected_extension=connection_type)
      elif connection_type in conn_enum.Database_Datasource_Enum.__members__.values():
          unique_connection_name = await validation_api.insert_user_credentials(connection=connection)
      else:
          error_msg = f"Unsupported connection type: {connection_type}"
          dqt_logger.error(error_msg)
          raise HTTPException(status_code=400, detail={"error": error_msg})
    except Exception as e:
        error_msg = f"Error inserting credentials: {str(e)}"
        dqt_logger.error(error_msg)
        raise HTTPException(status_code=503, detail={"error": error_msg})

    if unique_connection_name:
        return {"status": "connected", "connection_name": unique_connection_name}
    
    raise HTTPException(status_code=503, detail="Could not connect, an error occurred")


@app.post("/generate-suggestions", description="Generate AI-based data quality suggestions")
async def generate_suggestions(
    connection: connection_model.GenerateSuggestion = Body(...,
        examples= [{
            "username": "merit",
            "password": "sample_password",
            "host": "127.0.0.1",
            "database": "quality_tool",
            "table_name": "customers",
            "metric": "correctness"
        }]
    )
):
    try:
        # Build the database URI dynamically
        db_uri = f"mysql+pymysql://{connection.username}:{connection.password}@{connection.host}/{connection.database}"
        return SuggestionBI(db_uri=db_uri, table=connection.table_name).run_prompt(metric=connection.metric)
    except Exception as e:
        error_msg = f"Error generating AI suggestions: {str(e)}"
        dqt_logger.error(error_msg)
        raise HTTPException(status_code=500, detail={"error": error_msg})


@app.post("/submit-job", description="This endpoint allows to submit job requests")
async def submit_job(job: job_model.SubmitJob = Body(..., examples=[{
  "connection_name": "20250130172104_merit_3233347_3306_qualitytool_3757",
  "data_source": {
    "table_name": "customers",
    "schema_name": "quality_tool"
  },
  "quality_checks": [
        {
            "expectation_type": "row_count",
            "kwargs": {
                "condition": "> 0"
            }
        },
        {
            "expectation_type": "missing_count",
            "kwargs": {
                "column": "Index",
                "condition": "> 0"
            }
        },
        {
            "expectation_type": "missing_count",
            "kwargs": {
                "column": "Customer Id",
                "condition": "= 0"
            }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "Customer Id",
                "condition": "= 0",
                "valid regex": "^[a-zA-Z0-9]{15}+$"
            }
        },
        {
            "expectation_type": "missing_count",
            "kwargs": {
                "column": "First Name",
                "condition": "= 0"
            }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "City",
                "condition": "= 0",
                "valid regex": "^[A-Za-z\\s\\-]+$"
            }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "First Name",
                "condition": "= 0",
                "valid regex": "^[A-Za-z]{1,20}$"
            }
        },
        {
            "expectation_type": "missing_count",
            "kwargs": {
                "column": "Last Name",
                "condition": "= 0"
            }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "Last Name",
                "condition": "= 0",
                "valid regex": "^[A-Za-z]{1,20}$"
            }
        },
        {
            "expectation_type": "missing_count",
            "kwargs": {
                "column": "Company",
                "condition": "= 0"
            }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "City",
                "condition": "= 0",
                "valid regex": "^[A-Za-z\\s\\-]+$"
            }
        },
        {
            "expectation_type": "missing_count",
            "kwargs": {
                "column": "Country",
                "condition": "= 0"
            }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "City",
                "condition": "= 0",
                "valid regex": "^[A-Za-z]+$"
            }
        },
        {
            "expectation_type": "missing_count",
            "kwargs": {
                "column": "Phone 1",
                "condition": "= 0"
            }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "Phone 1",
                "condition": "= 0",
                "valid regex": "^[+()\\d\\s-]+$"
            }
        },
        {
            "expectation_type": "missing_count",
            "kwargs": {
                "column": "Phone 2",
                "condition": "= 0"
            }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "Phone 1",
                "condition": "= 0",
                "valid regex": "^[+()\\d\\s-]*$"
            }
        },
        {
            "expectation_type": "missing_count",
            "kwargs": {
                "column": "Email",
                "condition": "= 0"
            }
        },
        {
            "expectation_type": "duplicate_count",
            "kwargs": {
                "column": "Email",
                "condition": "= 0"
            }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "Phone 1",
                "condition": "= 0",
                "valid regex": "^[^@]+@[^@]+\\.[^@]+$"
            }
        },
        {
            "expectation_type": "missing_count",
            "kwargs": {
                "column": "Subscription Date",
                "condition": "= 0"
            }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "Subscription Date",
                "condition": "= 0",
                "valid regex": "^\\d{4}-\\d{2}-\\d{2}$"
            }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "Website",
                "condition": "= 0",
                "valid regex": "^(http|https)://[^\\s/$.?#].[^\\s]*$"
            }
        }
    ],
  "metadata": {
    "requested_by": "user@example.com",
    "execution_time": "2024-10-16T15:11:18.483Z",
    "description": "This is a test description"
  }
}])):
    job_id = get_job_id_and_initialize_job_state_singleton()
    
    if not job.connection_name:
        error_msg = "Incorrect JSON provided, missing connection name"
        dqt_logger.error(error_msg)
        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg) 
        raise HTTPException(status_code=400, detail={"error": error_msg})
    
    if not job.data_source:
        error_msg = "Incorrect JSON provided, missing data source"
        dqt_logger.error(error_msg)
        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
        raise HTTPException(status_code=400, detail={"error": error_msg})

    if not job.quality_checks:
        error_msg = "Incorrect JSON provided, missing quality checks"
        dqt_logger.error(error_msg)
        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg) 
        raise HTTPException(status_code=400, detail={"error": error_msg})

    validation_api = ValidationFastAPI()
    
    try:
      JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.STARTED)
      validation_results = await validation_api.validation_check_request(job=job)
      dqt_logger.debug(f"Validation results:\n{validation_results}")
    except Exception as validation_check_error:
      error_msg = f"An error occurred while validating data.\nError:{str(validation_check_error)}"
      dqt_logger.error(error_msg)
      JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, 
                                               status_message="An error occurred while validating data.")
      return {"job_id": job_id}
  
    if validation_results: 
        try:
            log_validation_results(validation_results)
            info_msg = "Saving validation results in database"
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            ValidationResult().save_result_for_job_id(json_response=validation_results, job_id=job_id) 
            return {"job_id": job_id}
        except Exception as saving_validation_error:
            error_msg = f"An error occurred, failed to save validation results in database\nError:{str(saving_validation_error)}"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, 
                                                      status_message="An error occurred, failed to save validation results in database")
            return {"job_id": job_id}
    else:
        error_msg = "Missing validation results"
        dqt_logger.error(error_msg)
        JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
        return {"job_id": job_id}
  