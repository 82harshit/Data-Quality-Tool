import asyncssh
from fastapi import HTTPException
from io import BytesIO, StringIO
import pandas as pd
import fastavro
import pyorc

from database.db_models import user_credentials_db
from database.db_models.job_run_status import JobRunStatusEnum
from job_state_singleton import JobStateSingleton
from logging_config import dqt_logger


class FileDatabase(user_credentials_db.UserCredentialsDatabase):
    """
    A class to manage interactions with a database and file handling operations over SSH.
    Inherits from UserCredentialsDatabase for database connection functionality.
    
    Attributes:
        file_name (str): The name of the file to be processed.
        dir_path (str): The directory path where the file is located.
    """
    def __init__(self, hostname: str, username: str, password: str, port: int, database: str, connection_type: str, 
                 file_name: str, dir_path: str):
        """
        Initializes the FileDatabase instance with the given connection and file details.
        
        
        :param hostname (str): Hostname of the database server.
        :param username (str): Username for database connection.
        :param password (str): Password for database connection.
        :param port (int): Port for database connection.
        :param database (str): Name of the database to connect to.
        :param connection_type (str): Type of connection (e.g., SSH).
        :param file_name (str): Name of the file to be processed.
        :param dir_path (str): Path to the directory containing the file.
        """
        super().__init__(hostname=hostname, username=username, password=password, port=port, database=database, 
                         connection_type=connection_type)
        self.file_name = file_name
        self.dir_path = dir_path

    async def connect_to_server_SSH(self):
        """
        Establishes an SSH connection to the remote server.
        
        :return asyncssh.SSHClientConnection: The SSH connection object if successful.
        
        :raises Exception: If there is an error while connecting to the server.
        """
        try:
            connection = await asyncssh.connect(
                host=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                known_hosts=None
            )
            info_msg = "SSH connection established..."
            dqt_logger.info(info_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            return connection
        except Exception as ssh_conn_error:
            error_msg = f"An error occurred while connecting to the server using SSH:\n{str(ssh_conn_error)}"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, 
                                                     status_message="An error occurred while connecting to the server using SSH")
            raise Exception(error_msg)


    async def search_file_on_server(self):
        """
        Searches for the specified file on the remote server via SSH.
        
        :return dict: A dictionary indicating whether the file was found and its path(s), or an error message.
        
        :raises HTTPException: If there is a permission error or an invalid configuration.
        """
        try:
          # Establish SSH connection using asyncssh
            async with asyncssh.connect(
                host=self.hostname,
                username=self.username,
                password=self.password,
                port=self.port,
                known_hosts=None
            ) as conn:
                info_msg = "SSH connection established..."
                dqt_logger.info(info_msg)
                JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)

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
                    error_msg = """Invalid connection configuration. 
                    Please provide either 'dir_path' with 'file_name', or just 'file_name'."""
                    dqt_logger.error(error_msg)
                    JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
                    raise ValueError(error_msg)

        except asyncssh.PermissionDenied:
            error_msg = "SSH permission denied. Check your credentials."
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
            raise HTTPException(status_code=403, detail=error_msg)
        except Exception as e:
            error_msg = f"An error occurred: {str(e)}"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message="An error occurred.") 
            raise HTTPException(status_code=500, detail=error_msg)


    async def read_file_columns(self, conn):
        """
        Reads the columns of a file from the remote server based on its file extension.
        
        :param conn (asyncssh.SSHClientConnection): The SSH connection to the server.
        
        :return list: A list of column names from the file, or an error message if the file format is unsupported.
        
        :raises HTTPException: If there is an error processing the file or if the file format is unsupported.
        """
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
                df = pd.read_excel(file_content, engine='openpyxl')
                
            else:
                error_msg = "Unsupported file format"
                dqt_logger.error(error_msg)
                JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg) 
                raise HTTPException(status_code=400, detail=error_msg)

            # Return the column names
            return df.columns.tolist()
        except Exception as e:
            error_msg = f"Error processing file: {str(e)}"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message="Error processing file") 
            raise HTTPException(status_code=500, detail=error_msg)
