"""
This file contains two FastAPI endpoints:-

1. /create-connection: 

This endpoint is used to test connection with server or filesystem, it stores the user credentials 
and generates a unique `connection name` for the user

2. /submit-job:

    2.1 This endpoint is used to establish a connection with server or filesystem,
        using the `connection name` from the `create-connection` endpoint.
    2.2 Using the established connection it fetches the data from server on which the
        data quality checks need to be applied
    2.3 It validates the data using the requested checks on the data source,
        both of which are provided by the user
    2.4 The validation results generated are then saved in a relational database
"""

from fastapi import FastAPI, Body, HTTPException

from request_models import connection_enum_and_metadata as conn_enum, connection_model, job_model
from logging_config import dqt_logger
from ge_fast_api_interface import GE_Fast_API_Interface
from save_validation_results import DataQuality
from utils import generate_job_id
from job_state_singleton import JobIDSingleton


def get_and_initialize_job_id_singelton() -> str: # TODO: Complete
    """
    Creates a new job id and sets it up in the singleton object
    :return job_id(str): Generated job_id
    """
    job_id = generate_job_id() # creates a new job id
    dqt_logger.info(f"Job_ID: {job_id}") # logs the job id
    # db.insert_job_id(job_id=job_id, job_status="Started") # inserts the job id in job_run_status table
    JobIDSingleton().set_job_id(job_id=job_id) # sets the job_id in singleton object
    return job_id

app = FastAPI()

@app.get("/", description='This is the root route')
async def root():
    return {"message": "Welcome to Data Quality Tool"}

@app.get("/submit-job-status", description="This endpoint returns the application state for 'submit-job' endpoint")
async def submit_job_status(job_id: str): # TODO: Complete
    # job_id = JobIDSingleton.get_job_id()
    # current_job_state = db.get_status_of_job_id(job_id=job_id)
    # return current_job_state
    pass

@app.post("/create-connection", description="This endpoint allows connection to the provided connection type")
async def create_connection(connection: connection_model.Connection = Body(...,
    example = {
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
    }
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
    
    ge_fast_interface = GE_Fast_API_Interface() # interface object
    ge_fast_interface.create_connection_based_on_type(connection=connection) # create connection to the user_credentials db

    # insert credentials based on connection_type
    if connection_type in conn_enum.File_Datasource_Enum.__members__.values():
        unique_connection_name = await ge_fast_interface.insert_user_credentials(connection=connection, 
                                                                                 expected_extension=connection_type)
    elif connection_type in conn_enum.Database_Datasource_Enum.__members__.values():
        unique_connection_name = await ge_fast_interface.insert_user_credentials(connection=connection)

    if unique_connection_name:
        return {"status": "connected", "connection_name": unique_connection_name}
    else:
        return {"status": "could not connect, an error occurred", "connection_name": None}


@app.post("/submit-job", description="This endpoint allows to submit job requests")
async def submit_job(job: job_model.SubmitJob = Body(...,example={
  "connection_name": "20241120162230_test_1272990_4002_testdb_2314",
  "data_source": {
      "dir_path": "C:/user/Desktop",
      "file_name": "sample_file",
      "table_name": "test_table"
  },
  "quality_checks": [
      {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {
            "column": "Customer Id"
        },
      },{
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {
            "column": "Customer Id",
            "regex": "^[a-zA-Z0-9]{15}$"
        }
      }
  ],
  "metadata": {
    "requested_by": "user@example.com",
    "execution_time": "2024-10-16T15:11:18.483Z",
    "description": "This is a test description"
  }
})):
    if not job.connection_name:
        error_msg = "Incorrect JSON provided, missing connection name"
        dqt_logger.error(error_msg)
        raise HTTPException(status_code=400, detail={"error": error_msg})
    
    if not job.data_source:
        error_msg = "Incorrect JSON provided, missing data source"
        dqt_logger.error(error_msg)
        raise HTTPException(status_code=400, detail={"error": error_msg})

    if not job.quality_checks:
        error_msg = "Incorrect JSON provided, missing quality checks"
        dqt_logger.error(error_msg)
        raise HTTPException(status_code=400, detail={"error": error_msg})

    ge_fast_interface = GE_Fast_API_Interface() # interface object
    
    validation_results = await ge_fast_interface.validation_check_request(job=job)
    dqt_logger.info(validation_results)

    if validation_results: 
        try:
            dqt_logger.info("Saving validation results in database")
            DataQuality().fetch_and_process_data(validation_results) # FIXME: error: argument of type 'coroutine' is not iterable
            # return job_id
        except Exception as saving_validation_error:
            error_msg = f"An error occurred, failed to save validation results in database\n{str(saving_validation_error)}"
            dqt_logger.error(error_msg)
            # return job_id
    else:
        error_msg = f"Missing validation results."
        dqt_logger.error(error_msg)
        # return job_id
    