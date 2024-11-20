from fastapi import FastAPI, Body, File, UploadFile
from request_models import connection_model, connection_enum, job_model
from response_models import data_quality_metric
# from sqlalchemy import create_engine
import pymysql

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
            "port": 3000,
            "server": "server_IP",
            "database": "test_DB"
        },
        "metadata": {
            "requested_by": "user@example.com",
            "execution_time": "2024-09-26T10:00:00Z",
            "description": "This is a test description"
        }
    }
)):
    
    if connection.connection_credentials:
        connection_type = connection.connection_credentials.connection_type
    else:
        return {"error": "No connection creds"}

    if connection_type == connection_enum.ConnectionEnum.POSTGRES:

        hostname = connection.connection_credentials.server
        username = connection.user_credentials.username
        password = connection.user_credentials.password
        port = connection.connection_credentials.port
        database = connection.connection_credentials.database

        try:
            # engine = create_engine(url=f"postgresql://{username}:{password}@{hostname}:{port}/{database}") # TODO: Connection fix required
            # with engine.connect() as conn:
            #     return {"message": conn}
            # print(engine)
           return {
                "status": "connected",
                 "connection": connection.model_dump(exclude={
                    "user_credentials": {"password", "access_token"},  # Exclude password and access_token
                    "metadata": {"execution_time"}  # Exclude execution_time from metadata
                })
            }
        except ConnectionAbortedError as car:
            return {"error": car, "request_json": connection.model_dump_json()}
        except ConnectionError as ce:
            return {"error": ce, "request_json": connection.model_dump_json()}
        except ConnectionRefusedError as cref:
            return {"error": cref, "request_json": connection.model_dump_json()}
        except ConnectionResetError as cres:
            return {"error": cres, "request_json": connection.model_dump_json()}
        
    elif connection_type == connection_enum.ConnectionEnum.MYSQL:
        
        hostname = connection.connection_credentials.server
        username = connection.user_credentials.username
        password = connection.user_credentials.password
        port = connection.connection_credentials.port
        database = connection.connection_credentials.database
    
        try:
            conn = pymysql.connect(
                host=hostname,
                user=username,      
                password=password,  
                database=database,  
                port=port
            )

            cursor = conn.cursor()
            sql_query = "SELECT * FROM customers"
            cursor.execute(sql_query)

            # Fetching the results
            rows = cursor.fetchall()
            for row in rows:
                print(row)

            # Closing the connection
            cursor.close()
            conn.close()

            return {
                "status": "connected",
                "results": rows,
                "connection": connection.model_dump(exclude={
                    "user_credentials": {"password", "access_token"},  # Exclude password and access_token
                    "metadata": {"execution_time"}  # Exclude execution_time from metadata
                })}
        except ConnectionAbortedError as car:
            return {"error": car, "request_json": connection.model_dump_json()}
        except ConnectionError as ce:
            return {"error": ce, "request_json": connection.model_dump_json()}
        except ConnectionRefusedError as cref:
            return {"error": cref, "request_json": connection.model_dump_json()}
        except ConnectionResetError as cres:
            return {"error": cres, "request_json": connection.model_dump_json()}
        
    elif connection_type == connection_enum.ConnectionEnum.JSON:
        return {"connection": "Test connection to json"}
    elif connection_type == connection_enum.ConnectionEnum.SAP:
        return {"connection": "Test connection to sap"}
    elif connection_type == connection_enum.ConnectionEnum.STREAMING:
        return {"connection": "Test connection to streaming"}
    elif connection_type == connection_enum.ConnectionEnum.FILESERVER:
        return {"connection": "Test connection to file server"}
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


partial_key = "ValidationResultIdentifier::"


@app.post("/upload-file", description='This endpoint allows users to upload a file via the API')
def upload_file(file: UploadFile):
    return {"uploaded file name": file.filename, "uploaded file size": file.size}

@app.post("/submit-job", description="This endpoint allows to submit job requests") 
        #   response_model=data_quality_metric.DataQualityMetric)
async def submit_job(run_name, 
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
    import run_customer_checkpoint as run
    result = run.run_checkpoint(run_name=run_name)
    result_dict = result.to_json_dict()

    validation_result = find_validation_result(result_dict['run_results'], partial_key)
    results = validation_result['results']
    for result in results:
        metric_type = result['expectation_config'].get('expectation_type')
        return metric_type
    # return result_dict
