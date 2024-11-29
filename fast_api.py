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
from request_models import connection_enum_and_metadata, connection_model, job_model
from utils import generate_connection_name, generate_connection_string
import db_constants
from server_functions import get_mysql_db, handle_file_connection, read_file_columns, connect_to_server_SSH
from database import db_functions, sql_queries as query
from ge import run_quality_checks
import os
from logging_config import dqt_logger

app = FastAPI()

@app.get("/", description='This is the root route')
async def root():
    return {"message": "Welcome to Data Quality Tool"}


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
            "file_name" : "customers-100.csv",
            "dir_path" : "/home/merit/Desktop"
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

    # extracting user credentials from JSON
    hostname = connection.connection_credentials.server
    username = connection.user_credentials.username
    password = connection.user_credentials.password
    port = connection.connection_credentials.port
    database = connection.connection_credentials.database

    db = db_functions.DBFunctions() # database object

    if connection_type == connection_enum_and_metadata.ConnectionEnum.MYSQL:
        try:
            app_connection_response = db.connect_to_credentials_db(connection_type)

            app_connection = app_connection_response.get('app_connection')
            app_table = app_connection_response.get('app_table')

            # create unique connection name
            unique_connection_name = generate_connection_name(connection=connection)
            # create connection string
            connection_string = generate_connection_string(connection=connection)

            db.execute_sql_query(db_connection=app_connection, 
                    sql_query=query.INSERT_CONN_DETAILS_QUERY.format(app_table), # insert in app table
                    params=(
                        unique_connection_name,
                        connection_string,
                        username,
                        connection_type,
                        password,
                        hostname,
                        port,
                        database
                    ))
    
            print("Login credential insertion completed")
            return {"status": "connected", "connection_name": unique_connection_name}
        except Exception as e:
            raise HTTPException(status_code=503, detail={"error": str(e), "request_json": connection.model_dump_json()})

    elif connection_type in {connection_enum_and_metadata.ConnectionEnum.SAP, 
                             connection_enum_and_metadata.ConnectionEnum.STREAMING,
                             connection_enum_and_metadata.ConnectionEnum.FILESERVER,
                             connection_enum_and_metadata.ConnectionEnum.REDSHIFT
                             }:
        raise HTTPException(status_code=501, detail={"error": f"{connection_type} not implemented", 
                                                     "request_json": connection.model_dump_json()})
    elif connection_type in {connection_enum_and_metadata.ConnectionEnum.ORC, 
                             connection_enum_and_metadata.ConnectionEnum.AVRO,
                             connection_enum_and_metadata.ConnectionEnum.CSV,
                             connection_enum_and_metadata.ConnectionEnum.PARQUET,
                             connection_enum_and_metadata.ConnectionEnum.JSON
                             }:
        return await handle_file_connection(connection, expected_extension=f".{connection_type}")
    else:
        raise HTTPException(status_code=500, detail={"error": "Unidentified connection source", 
                                                     "request_json": connection.model_dump_json()})


@app.post("/submit-job", description="This endpoint allows to submit job requests")
async def submit_job(job: job_model.SubmitJob = Body(...,example={
  "connection_name": "20241120162230_test_1272990_4002_testdb_2314",
  "data_source": {
      "dir_path": "C:/user/Desktop",
      "file_name": "sample_file",
      "table_name": "test_table"
  },
  "data_target": {
    "target_data_type": "csv",
    "target_path": "C:/user/sink_dataset",
    "target_data_format": "string"
    # "target_schema": "string"
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
    """
    This function posts the checks on the data
    """

    if not job.connection_name:
        error_msg = "Incorrect JSON provided, missing connection name"
        dqt_logger.error(error_msg)
        raise HTTPException(status_code=400, detail={"error": error_msg})
    
    if not job.quality_checks:
        error_msg = "Incorrect JSON provided, missing quality checks"
        dqt_logger.error(error_msg)
        raise HTTPException(status_code=400, detail={"error": error_msg})

    if not job.data_target:
        error_msg = "Incorrect JSON provided, missing data target"
        dqt_logger.error(error_msg)
        raise HTTPException(status_code=400, detail={"error": error_msg})

    connection_name = job.connection_name
    quality_checks = job.quality_checks

    # check if the connection_name exists in the database
    # root user logging in user_credentials database
    app_conn = get_mysql_db(hostname=db_constants.APP_HOSTNAME, 
                        username=db_constants.APP_USERNAME, 
                        password=db_constants.APP_PASSWORD, 
                        port=db_constants.APP_PORT, 
                        database=db_constants.USER_CREDENTIALS_DATABASE
                        )
    
    app_cursor = app_conn.cursor()
    
    READ_FOR_CONN_NAME_QUERY = f"""SELECT 1 FROM {db_constants.USER_LOGIN_TABLE} 
    WHERE {db_constants.CONNECTION_NAME} = %s;"""
    app_cursor.execute(READ_FOR_CONN_NAME_QUERY,(connection_name,))
    exists = app_cursor.fetchone() is not None

    if not exists:
        raise HTTPException(status_code=404, detail={"error": "User not found", "connection_name": connection_name})
    
    print("User found, retrieving connection details")

    # retrieve user connection credentials

    GET_USERNAME_QUERY = f"""SELECT {db_constants.USERNAME} 
    FROM {db_constants.USER_LOGIN_TABLE} 
    WHERE {db_constants.CONNECTION_NAME} = %s;"""
    app_cursor.execute(GET_USERNAME_QUERY,(connection_name,))
    retrieved_username = app_cursor.fetchall()
    try:
        username = retrieved_username[0][0] # extracting data from tuple, tuple format: (('user'),)
        print(f"Username:{username}")
    except ValueError as ve:
        raise Exception(f"Required string type value for username\n{str(ve)}")

    print("Username successfully retrieved")

    GET_PASSWORD_QUERY = f"""SELECT {db_constants.PASSWORD} 
    FROM {db_constants.USER_LOGIN_TABLE} 
    WHERE {db_constants.CONNECTION_NAME} = %s;"""
    app_cursor.execute(GET_PASSWORD_QUERY,(connection_name,))
    retrieved_password = app_cursor.fetchall()
    try:
        password = retrieved_password[0][0]
        print(f"Password:{password}")
    except ValueError as ve:
        raise Exception(f"Required string type value for password\n{str(ve)}")

    print("Password successfully retrieved")

    GET_HOSTNAME_QUERY = f"""SELECT {db_constants.HOSTNAME} 
    FROM {db_constants.USER_LOGIN_TABLE} 
    WHERE {db_constants.CONNECTION_NAME} = %s;"""
    app_cursor.execute(GET_HOSTNAME_QUERY,(connection_name,))
    retrieved_hostname = app_cursor.fetchall()
    try:
        hostname = retrieved_hostname[0][0]
        print(f"Hostname:{hostname}")
    except ValueError as ve:
        raise Exception(f"Required string type value for hostname\n{str(ve)}")

    print("Hostname successfully retrieved")

    GET_PORT_QUERY = f"""SELECT {db_constants.PORT} 
    FROM {db_constants.USER_LOGIN_TABLE} 
    WHERE {db_constants.CONNECTION_NAME} = %s;"""
    app_cursor.execute(GET_PORT_QUERY,(connection_name,))
    retrieved_port = app_cursor.fetchall() 
    try:
        port = retrieved_port[0][0] # extracting data from tuple, tuple format: ((4000),)
        print(f"Port:{port}")
    except ValueError as ve:
        raise Exception(f"Required integer type value for port\n{str(ve)}")

    print("Port successfully retrieved")

    GET_SOURCE_TYPE_QUERY = f"""SELECT {db_constants.SOURCE_TYPE} 
    FROM {db_constants.USER_LOGIN_TABLE} 
    WHERE {db_constants.CONNECTION_NAME} = %s;"""
    app_cursor.execute(GET_SOURCE_TYPE_QUERY,(connection_name,))
    retrieved_data_source_type = app_cursor.fetchall()
    try:
        data_source_type = retrieved_data_source_type[0][0]
        print(f"Data source type:{data_source_type}")
    except ValueError as ve:
        raise Exception(f"Required string type value for data source type\n{str(ve)}")
    
    print("Data source type successfully retrieved")

    if data_source_type == connection_enum_and_metadata.ConnectionEnum.MYSQL:
        GET_SOURCE_QUERY = f"""SELECT {db_constants.DATABASE} 
        FROM {db_constants.USER_LOGIN_TABLE} 
        WHERE {db_constants.CONNECTION_NAME} = %s;"""
        app_cursor.execute(GET_SOURCE_QUERY,(connection_name,))
        retrieved_data_source = app_cursor.fetchall()
        try:
            data_source = retrieved_data_source[0][0]
            print(f"Data source:{data_source}")
        except ValueError as ve:
            raise Exception(f"Required string type value for data source\n{str(ve)}")

        print("Data source successfully retrieved")

    app_cursor.close()
    app_conn.close()

    print("Creating user connection")
    # create user connection to read file from SSH server

    validation_results = {}

    if data_source_type == connection_enum_and_metadata.ConnectionEnum.MYSQL:
        """
        TODO: Remove following temp vars
        """
        datasource_name = f"test_datasource_for_mysql" # TODO: Reformat as: datasource_name = f"{table_name}_table" if RDBMS
        table_name = "customers"

        validation_results = run_quality_checks(datasource_name=datasource_name, port=port, hostname=hostname, password=password,
                                                database=data_source, table_name=table_name, schema_name=data_source, 
                                                username=username, quality_checks=quality_checks, datasource_type=data_source_type)
    elif data_source_type == connection_enum_and_metadata.ConnectionEnum.CSV:
        user_conn = await connect_to_server_SSH(username=username, password=password, server=hostname, port=port)
        
        dir_path = job.data_source.dir_path
        dir_name = os.path.basename(dir_path)
        file_name = job.data_source.file_name

        file_path = f"{dir_path}/{file_name}"

        columns = await read_file_columns(conn=user_conn,file_path=file_path)
        print(f"Column names: {columns}")

        datasource_name = "test_datasource_for_csv" # TODO: Remove test param

        validation_results = run_quality_checks(datasource_name=datasource_name, port=port, hostname=hostname, password=password,
                                                username=username, quality_checks=quality_checks, datasource_type=data_source_type,
                                                dir_name=dir_name, file_name=file_name)


    return {"validation_results": validation_results} 

    # TODO: store these validation results in a database
    
