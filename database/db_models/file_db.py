import user_credentials_db
import asyncssh
from fastapi import HTTPException
from io import BytesIO, StringIO
import pandas as pd
import fastavro
import pyorc
from logging_config import dqt_logger
import json


class FileDatabase(user_credentials_db.UserCredentialsDatabase):
    def __init__(self, hostname, username, password, port, database, connection_type, file_name: str, dir_path: str):
        super().__init__(hostname, username, password, port, database, connection_type)
        self.file_name = file_name
        self.dir_path = dir_path

    async def connect_to_server_SSH(self):
        try:
            connection = await asyncssh.connect(
                host=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                known_hosts=None
            )
            dqt_logger.info("SSH connection established...")
            return connection
        except Exception as ssh_conn_error:
            error_msg = f"An error occurred while connecting to the server using SSH:\n{str(ssh_conn_error)}"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)


    async def search_file_on_server(self):
        try:
          # Establish SSH connection using asyncssh
            async with asyncssh.connect(
                host=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                known_hosts=None
            ) as conn:
                dqt_logger.info("SSH connection established...")

                # Extract connection details
                file_name = self.file_name
                dir_path = self.dir_path

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
                    error_msg = "Invalid connection configuration. Please provide either 'dir_path' with 'file_name', or just 'file_name'."
                    dqt_logger.error(error_msg)
                    raise ValueError(error_msg)

        except asyncssh.PermissionDenied:
            error_msg = "SSH permission denied. Check your credentials."
            dqt_logger.error(error_msg)
            raise HTTPException(status_code=403, detail=error_msg)
        except Exception as e:
            error_msg = f"An error occurred: {str(e)}"
            dqt_logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)


    async def read_file_columns(self, conn):
        file_path = f"{self.dir_path}/{self.file_name}"
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
            
            elif file_path.endswith(".xlsx"):  # Excel handling
                command = f"cat {file_path}"
                result = await conn.run(command, encoding=None)  # Retrieve binary data
                file_content = BytesIO(result.stdout)
                df = pd.read_excel(file_content)
                
            else:
                error_msg = "Unsupported file format"
                dqt_logger.error(error_msg)
                raise HTTPException(status_code=400, detail=error_msg)

            # Return the column names
            return df.columns.tolist()
        except Exception as e:
            error_msg = f"Error processing file: {str(e)}"
            dqt_logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
    
    # TODO: test
    async def handle_file_connection(self, unique_connection_name: str, connection_string: str, expected_extension: str) -> json:
        """
        Generalized function to handle file-based connections.

        :param connection_string (str): Generated connection string 
        :param unique_connection_name (str): Generated connection string
        :param expected_extension (str): The file extension to validate (e.g., '.json', '.csv')

        :return (json): Response with connection details
        """
        try:
            # Validate the file type
            if not self.file_name.endswith(expected_extension):
                error_msg = f"The provided file is not a {expected_extension.upper()} file."
                dqt_logger.error(error_msg)
                raise HTTPException(status_code=400, detail=error_msg)

            # Search for the file on the server
            result = await self.search_file_on_server()
            if not result["file_found"]:
                error_msg = f"{expected_extension.upper()} file not found on server."
                dqt_logger.error(error_msg)
                raise HTTPException(status_code=404, detail=error_msg)

            user_cred_db = user_credentials_db.UserCredentialsDatabase() # user credentials db object
            
            user_cred_db.connect_to_db()
            user_cred_db.insert_in_db(unique_connection_name=unique_connection_name, 
                                                                     connection_string=connection_string)

            # Extract connection details
            # file_name = connection.connection_credentials.file_name
            # file_path = result["file_path"]
            # hostname = connection.connection_credentials.server
            # username = connection.user_credentials.username
            # password = connection.user_credentials.password
            # port = connection.connection_credentials.port

            # Generate unique connection name and string
            # unique_connection_name = generate_connection_name(connection=connection)
            # connection_string = generate_connection_string(connection=connection)

            # dqt_logger.debug(f"Unique connection name: {unique_connection_name}")
            # dqt_logger.debug(f"Connection string: {connection_string}")

            # # Store connection details in the database
            # conn = get_mysql_db(
            #     hostname=db_constants.APP_HOSTNAME,
            #     username=db_constants.APP_USERNAME,
            #     password=db_constants.APP_PASSWORD,
            #     port=db_constants.APP_PORT,
            #     database=db_constants.USER_CREDENTIALS_DATABASE
            # )
            # cursor = conn.cursor()

            # INSERT_CONN_DETAILS_QUERY = f"INSERT INTO {db_constants.USER_LOGIN_TABLE} VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
            # cursor.execute(INSERT_CONN_DETAILS_QUERY, (
            #     unique_connection_name,
            #     connection_string,
            #     username,
            #     connection.connection_credentials.connection_type,
            #     password,
            #     hostname,
            #     port,
            #     None
            # ))
            # conn.commit()
            dqt_logger.debug(f"{expected_extension.upper()} connection details insertion completed.")

            # # Close the database connection
            # cursor.close()
            # conn.close()

            return {
                "status": "connected",
                "connection_name": unique_connection_name
            }

        except Exception as e:
            error_msg = f"Error processing {expected_extension.upper()} connection: {str(e)}"
            dqt_logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
            
