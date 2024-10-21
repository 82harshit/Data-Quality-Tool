from fastapi import FastAPI, Body, File, UploadFile
from request_models import connection_model, connection_enum, job_model
from response_models import data_quality_metric


app = FastAPI()

@app.get("/", description='This is the root route')
async def root():
    return {"message": "Hello World"}


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
            "connect_to": "server_name",
        },
        "metadata": {
            "requested_by": "user@example.com",
            "execution_time": "2024-09-26T10:00:00Z",
            "description": "This is a test description"
        }
    }
)):
    connection_dict = connection.model_dump()
    connection_type = connection_dict.get(
        'connection_credentials').get('connection_type')

    if connection_type == connection_enum.ConnectionEnum.POSTGRES:
        postgres_cred = connection_dict.get('user_credentials')

        hostname = postgres_cred.get('server')
        username = postgres_cred.get('username')
        password = postgres_cred.get('password')
        port = postgres_cred.get('port')
        database = postgres_cred.get('database')

        return {"connection": f"Test connection to postgres using: psql -h {hostname} -U {username} -d {database} -p {port} -W {password}"}
    elif connection_type == connection_enum.ConnectionEnum.JSON:
        return {"connection": "Test connection to json"}
    elif connection_type == connection_enum.ConnectionEnum.SAP:
        return {"connection": "Test connection to sap"}
    elif connection_type == connection_enum.ConnectionEnum.STREAMING:
        return {"connection": "Test connection to streaming"}
    elif connection_type == connection_enum.ConnectionEnum.FILE:
        return {"connection": "Test connection to file"}
    elif connection_type == connection_enum.ConnectionEnum.CSV:
        return {"connection": "Test connection to csv"}
    elif connection_type == connection_enum.ConnectionEnum.REDSHIFT:
        return {"connection": "Test connection to amazon redshift"}
    elif connection_type == connection_enum.ConnectionEnum.PARQUET:
        return {"connection": "Test connection to parquet"}
    else:
        return {"error": "Unidentified connection source"}

# Function to find and return the 'validation_result'
def find_validation_result(data, partial_key):
    for key in data:
        if key.startswith(partial_key):
            # Access the 'validation_result' inside the matched key
            return data[key].get("validation_result")
    return None  # Return None if no matching key is found

# Example usage
partial_key = "ValidationResultIdentifier::"


@app.post("/upload-file", description='This endpoint allows users to upload a file via the API')
def upload_file(file: UploadFile):
    return {"uploaded file name": file.filename, "uploaded file size": file.size}

@app.post("/submit-job", description="This endpoint allows to submit job requests") 
        #   response_model=data_quality_metric.DataQualityMetric)
def submit_job(run_name, 
#                job: job_model.SubmitJob = Body(...,example={
#     "data_source": {
#     "source_file_type": "csv",
#     "source_path": "/home/source_dataset",
#     "source_data_format": "string",
#     "source_schema": "string"
#   },
#   "data_target": {
#     "target_data_type": "csv",
#     "target_path": "C:/user/sink_dataset",
#     "target_data_format": "string",
#     "target_schema": "string"
#   },
#   "quality_checks": [{
#     "check_name": "range_check",
#     "applied_on_column": "index",
#     "check_kwargs": {
#         "min": 1,
#         "max": 1000
#     },
#   },{
#     "check_name": "range_check",
#     "applied_on_column": "index",
#     "check_kwargs": {
#         "min": 1,
#         "max": 1000
#     }
#   }
#   ],
#   "metadata": {
#     "requested_by": "user@example.com",
#     "execution_time": "2024-10-16T15:11:18.483Z",
#     "description": "This is a test description"
#   }
# })):
):
    """
    This function posts the checks on the data
    """
    from gx.uncommitted import run_customer_checkpoint as run
    result = run.run_checkpoint(run_name=run_name)
    result_dict = result.to_json_dict()

    validation_result = find_validation_result(result_dict['run_results'], partial_key)
    results = validation_result['results']
    for result in results:
        metric_type = result['expectation_config'].get('expectation_type')
        return metric_type
    # return result_dict
