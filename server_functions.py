import asyncssh
from fastapi import HTTPException
import db_constants
from request_models import connection_model
import pymysql
import pandas as pd
from io import BytesIO, StringIO
import fastavro
import pyorc
from utils import generate_connection_name, generate_connection_string


async def search_file_on_server(connection: connection_model.Connection):
    try:
        # Establish SSH connection using asyncssh
        async with asyncssh.connect(
            connection.connection_credentials.server,
            username=connection.user_credentials.username,
            password=connection.user_credentials.password,
            port=connection.connection_credentials.port,
            known_hosts=None
        ) as conn:
            print("SSH connection established...")

            # Extract connection details
            file_name = connection.connection_credentials.file_name
            dir_path = connection.connection_credentials.dir_path

            # Case 1: If `dir_path` and `file_name` are provided, search within the directory (no subdirectories)
            if dir_path and file_name:
                # Handle wildcard pattern in file_name (e.g., '*.json')
                if '*' in file_name:
                    # Use globbing logic to find all matching files within the directory (not subdirectories)
                    search_command = f"find {dir_path} -maxdepth 1 -name '{file_name}' -type f"
                    result = await conn.run(search_command)

                    # Check if files are found
                    if result.exit_status == 0 and result.stdout.strip():
                        file_paths = result.stdout.strip().splitlines()
                        return {"file_found": True, "file_paths": file_paths}  # Return all matching file paths
                    else:
                        return {"file_found": False, "message": f"No files matching {file_name} found in {dir_path}."}
                else:
                    # If no wildcard, search for the specific file name (within the directory only)
                    search_command = f"find {dir_path} -maxdepth 1 -name '{file_name}' -type f"
                    result = await conn.run(search_command)

                    if result.exit_status == 0 and result.stdout.strip():
                        file_path = result.stdout.strip().split()[-1]
                        return {"file_found": True, "file_path": file_path}
                    else:
                        return {"file_found": False, "message": f"File {file_name} not found in {dir_path}."}

            # Case 2: If only `file_name` is provided, perform a global search
            elif file_name:
                # Handle wildcard pattern for global search
                if '*' in file_name:
                    search_command = f"find / -name '{file_name}' -type f"
                    result = await conn.run(search_command)

                    if result.exit_status == 0 and result.stdout.strip():
                        file_paths = result.stdout.strip().splitlines()
                        return {"file_found": True, "file_paths": file_paths}  # Return all matching file paths
                    else:
                        return {"file_found": False, "message": f"No files matching {file_name} found."}
                else:
                    search_command = f"find / -name '{file_name}' -type f -exec ls -lt {{}} + | head -n 1"
                    result = await conn.run(search_command)

                    if result.exit_status == 0 and result.stdout.strip():
                        file_path = result.stdout.strip().split()[-1]
                        return {"file_found": True, "file_path": file_path}
                    else:
                        return {"file_found": False, "message": f"File {file_name} not found."}

            # Case 3: Invalid configuration (fallback from `ConnectionCredentials` validation)
            else:
                raise ValueError("Invalid connection configuration. Please provide either 'dir_path' with 'file_name', or just 'file_name'.")

    except asyncssh.PermissionDenied:
        raise HTTPException(status_code=403, detail="SSH permission denied. Check your credentials.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


def get_mysql_db(hostname: str, username: str, password: str, database: str, port: int):
    """
    This function establishes a MySQL connection using the provided credentials

    :param hostname: The server IP that the user wants to connect to
    :param username: Name of the user who wants to connect to the server
    :param database: Name of the database to connect to
    :param port: Port number to connect to
    
    :return conn: Established connection object 
    """

    try:
        conn = pymysql.connect(
                host=hostname,
                user=username,      
                password=password,  
                database=database,  
                port=port
            )
        print("Successfully connected")
        return conn
    except pymysql.MySQLError as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to database: {str(e)}")
    

async def handle_file_connection(connection: connection_model.Connection, expected_extension):
    """
    Generalized function to handle file-based connections.

    :param connection: Connection object
    :param expected_extension: The file extension to validate (e.g., '.json', '.csv')
    :return: Response with connection details
    """
    try:
        # Validate the file type
        if not connection.connection_credentials.file_name.endswith(expected_extension):
            raise HTTPException(status_code=400, detail=f"The provided file is not a {expected_extension.upper()} file.")

        # Search for the file on the server
        result = await search_file_on_server(connection)

        if not result["file_found"]:
            raise HTTPException(status_code=404, detail=f"{expected_extension.upper()} file not found on server.")

        # Extract connection details
        file_name = connection.connection_credentials.file_name
        file_paths = result.get("file_paths", [])  # This can be a list of paths
        hostname = connection.connection_credentials.server
        username = connection.user_credentials.username
        password = connection.user_credentials.password
        port = connection.connection_credentials.port

        # Handle the case where there are multiple file paths or a single file path
        if file_paths:
            if len(file_paths) == 1:
                file_path = file_paths[0]  # Single file found
            else:
                file_path = file_paths  # Multiple files found
        else:
            raise HTTPException(status_code=404, detail=f"No valid file found for {expected_extension.upper()}.")

        # Generate unique connection name and string
        unique_connection_name = generate_connection_name(connection=connection)
        connection_string = generate_connection_string(connection=connection)

        print(f"Unique connection name: {unique_connection_name}")
        print(f"Connection string: {connection_string}")

        # Store connection details in the database
        conn = get_mysql_db(
            hostname=db_constants.APP_HOSTNAME,
            username=db_constants.APP_USERNAME,
            password=db_constants.APP_PASSWORD,
            port=db_constants.APP_PORT,
            database=db_constants.USER_CREDENTIALS_DATABASE
        )
        cursor = conn.cursor()

        INSERT_CONN_DETAILS_QUERY = f"INSERT INTO {db_constants.USER_LOGIN_TABLE} VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        cursor.execute(INSERT_CONN_DETAILS_QUERY, (
            unique_connection_name,
            connection_string,
            username,
            connection.connection_credentials.connection_type,
            password,
            hostname,
            port,
            file_name
        ))
        conn.commit()
        print(f"{expected_extension.upper()} connection details insertion completed.")

        # Close the database connection
        cursor.close()
        conn.close()

        # Return the response with connection details
        return {
            "status": "connected",
            "connection_name": unique_connection_name,
            "file_paths": file_path  # This will either be a list or a single file path
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing {expected_extension.upper()} connection: {str(e)}")

async def connect_to_server_SSH(server, username, password, port):
    try:
        ssh_server_conn = await asyncssh.connect(
            host=server,
            port=port,
            username=username,
            password=password,
            known_hosts=None
        )
        # async with asyncssh.connect(
        #     host=server,
        #     port=port,
        #     username=username,
        #     password=password,
        #     known_hosts=None
        # ) as ssh_server_conn:
        print(f"SSH connection to {server} established on port {port}\nSSH connection object: {ssh_server_conn}")
        return ssh_server_conn
    except Exception as e:
        raise e


async def read_file_columns(conn, file_path: str):
    try:
        # Read the file based on its extension
        if file_path.endswith(".csv"):
            # For CSV files
            command = f"cat {file_path}"
            result = await conn.run(command)
            file_content = result.stdout
            df = pd.read_csv(StringIO(file_content))

        elif file_path.endswith(".json"):
            # For JSON files
            command = f"cat {file_path}"
            result = await conn.run(command)
            file_content = result.stdout
            df = pd.read_json(StringIO(file_content))

        elif file_path.endswith(".parquet"):
            # For Parquet files, stream binary data
            command = f"cat {file_path}"
            result = await conn.run(command, encoding=None)  # Get binary output
            file_content = BytesIO(result.stdout)  # Convert to BytesIO for pandas
            df = pd.read_parquet(file_content)

        elif file_path.endswith(".avro"):
            # For Avro files
            command = f"cat {file_path}"
            result = await conn.run(command, encoding=None)  # Get binary output
            file_content = BytesIO(result.stdout)  # Convert to BytesIO for fastavro
            reader = fastavro.reader(file_content)
            # Extract field names from the Avro schema
            columns = [field['name'] for field in reader.schema['fields']]
            return columns

        elif file_path.endswith(".orc"):
            # For ORC files using the 'pyorc' library
            command = f"cat {file_path}"
            result = await conn.run(command, encoding=None)  # Get binary output
            file_content = BytesIO(result.stdout)  # Convert to BytesIO for pyorc
            reader = pyorc.Reader(file_content)
            
            # Inspect the schema to check the structure of the fields
            fields = reader.schema.fields
            
            # Extract column names properly from the schema
            columns = []
            for field in fields:
                # Check if the field has a 'name' attribute (this might be structured differently)
                if isinstance(field, dict):
                    columns.append(field.get('name', 'Unknown'))
                else:
                    columns.append(str(field))  # Fallback to string representation if structure is different

            return columns

        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")

        # Return the column names
        return df.columns.tolist()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
