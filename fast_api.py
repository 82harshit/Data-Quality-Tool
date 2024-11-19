from typing import Annotated
from fastapi import Depends, FastAPI, Body, File, HTTPException, UploadFile
from request_models import connection_model, connection_enum, job_model
from response_models import data_quality_metric
import pymysql
from utils import get_mysql_db, generate_connection_name, generate_connection_string
import db_constants


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
    
    if not connection.user_credentials:
        raise HTTPException(status_code=400, detail={"error": "Missing user credentials"})
    
    if connection.connection_credentials:
        connection_type = connection.connection_credentials.connection_type
    else:
        raise HTTPException(status_code=400, detail={"error": "Missing connection credentials"})

    if connection_type == connection_enum.ConnectionEnum.MYSQL:
        try:
            hostname = connection.connection_credentials.server
            username = connection.user_credentials.username
            password = connection.user_credentials.password
            port = connection.connection_credentials.port
            database = connection.connection_credentials.database

            conn = get_mysql_db(hostname=hostname, username=username, password=password, port=port, database=db_constants.USER_CREDENTIALS_DATABASE)
            # conn = get_mysql_db(connection=connection)
            
            cursor = conn.cursor()

            # create unique connection name
            unique_connection_name = generate_connection_name(connection=connection)

            USE_DATABASE_QUERY = f"USE {db_constants.USER_CREDENTIALS_DATABASE};"
            cursor.execute(USE_DATABASE_QUERY)
            
            print("Execution complete")

            # # create user
            # CREATE_USER_QUERY = f"CREATE USER '{username}'@'%' IDENTIFIED BY '{password}';"
            # cursor.execute(CREATE_USER_QUERY)

            # # grant privilieges
            # GRANT_PRIVILEGES_QUERY = f"GRANT ALL PRIVILEGES ON {db_constants.USER_CREDENTIALS_DATABASE}.{db_constants.USER_LOGIN_TABLE} TO '{username}'@'%';"
            # cursor.execute(GRANT_PRIVILEGES_QUERY)

            # # flush privilieges
            # FLUSH_PRIVILIEGES_QUERY = "FLUSH PRIVILEGES;"
            # cursor.execute(FLUSH_PRIVILIEGES_QUERY)

            # create connection string
            connection_string = generate_connection_string(connection=connection)

            # insert values in table
            INSERT_CONN_DETAILS_QUERY = f"INSERT INTO {db_constants.USER_LOGIN_TABLE} VALUES ({unique_connection_name},{connection_string},{username},{database},{password},{hostname},{port},{database});"
            cursor.execute(INSERT_CONN_DETAILS_QUERY)
        
            return {"status": "connected", "connection_name": unique_connection_name}
        
        except ConnectionAbortedError as car:
            raise HTTPException(status_code=503, detail={"error": str(car), "request_json": connection.model_dump_json()})
        except ConnectionError as ce:
            raise HTTPException(status_code=503, detail={"error": str(ce), "request_json": connection.model_dump_json()})
        except ConnectionRefusedError as cref:
            raise HTTPException(status_code=503, detail={"error": str(cref), "request_json": connection.model_dump_json()})
        except ConnectionResetError as cres:
            raise HTTPException(status_code=503, detail={"error": str(cres), "request_json": connection.model_dump_json()})
        except Exception as e:
            raise HTTPException(status_code=503, detail={"error": str(e), "request_json": connection.model_dump_json()})
        finally:
            print("Closing connection")
            conn.close()
            cursor.close()
        
    elif connection_type == connection_enum.ConnectionEnum.POSTGRES:
        
        hostname = connection.connection_credentials.server
        username = connection.user_credentials.username
        password = connection.user_credentials.password
        port = connection.connection_credentials.port
        database = connection.connection_credentials.database

        conn = pymysql.connect(
            host=hostname,
            user=username,      
            password=password,  
            database=database,  
            port=port
        )

        try:
            cursor = conn.cursor()
            sql_query = "SELECT * FROM customers"
            cursor.execute(sql_query)

            # Fetching the results
            rows = cursor.fetchall()

            return {"status": "connected", "results": rows}
        except ConnectionAbortedError as car:
            return {"error": car, "request_json": connection.model_dump_json()}
        except ConnectionError as ce:
            return {"error": ce, "request_json": connection.model_dump_json()}
        except ConnectionRefusedError as cref:
            return {"error": cref, "request_json": connection.model_dump_json()}
        except ConnectionResetError as cres:
            return {"error": cres, "request_json": connection.model_dump_json()}
        finally:
            cursor.close()
            conn.close()

        
    elif connection_type == connection_enum.ConnectionEnum.JSON:
        raise HTTPException(status_code=501, detail={"error": f"{connection_enum.ConnectionEnum.JSON} not implemented", "request_json": connection.model_dump_json()})
    elif connection_type == connection_enum.ConnectionEnum.SAP:
        raise HTTPException(status_code=501, detail={"error": f"{connection_enum.ConnectionEnum.SAP} not implemented", "request_json": connection.model_dump_json()})
    elif connection_type == connection_enum.ConnectionEnum.STREAMING:
        raise HTTPException(status_code=501, detail={"error": f"{connection_enum.ConnectionEnum.STREAMING} not implemented", "request_json": connection.model_dump_json()})
    elif connection_type == connection_enum.ConnectionEnum.FILESERVER:
        raise HTTPException(status_code=501, detail={"error": f"{connection_enum.ConnectionEnum.FILESERVER} not impelemented", "request_json": connection.model_dump_json()})
    elif connection_type == connection_enum.ConnectionEnum.CSV:
        raise HTTPException(status_code=501, detail={"error": f"{connection_enum.ConnectionEnum.CSV} not implemented", "request_json": connection.model_dump_json()})
    elif connection_type == connection_enum.ConnectionEnum.REDSHIFT:
        raise HTTPException(status_code=501, detail={"error": f"{connection_enum.ConnectionEnum.REDSHIFT} not implemented", "request_json": connection.model_dump_json()})
    elif connection_type == connection_enum.ConnectionEnum.PARQUET:
        raise HTTPException(status_code=501, detail={"error": f"{connection_enum.ConnectionEnum.PARQUET} not implemented", "request_json": connection.model_dump_json()})
    else:
        raise HTTPException(status_code=500, detail={"error": "Unidentified connection source", "request_json": connection.model_dump_json()})

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
