from io import BytesIO, StringIO
import asyncssh
from fastapi import HTTPException
import fastavro
import pandas as pd
import pyorc
from request_models import connection_model


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

# Function to read the file and return columns
# async def read_file_columns(conn, file_path: str):
#     try:
#         # Read the file based on its extension
#         if file_path.endswith(".csv"):
#             # For CSV files
#             command = f"cat {file_path}"
#             result = await conn.run(command)
#             file_content = result.stdout
#             df = pd.read_csv(StringIO(file_content))

#         elif file_path.endswith(".json"):
#             # For JSON files
#             command = f"cat {file_path}"
#             result = await conn.run(command)
#             file_content = result.stdout
#             df = pd.read_json(StringIO(file_content))

#         elif file_path.endswith(".parquet"):
#             # For Parquet files, stream binary data
#             command = f"cat {file_path}"
#             result = await conn.run(command, encoding=None)  # Get binary output
#             file_content = BytesIO(result.stdout)  # Convert to BytesIO for pandas
#             df = pd.read_parquet(file_content)

#         elif file_path.endswith(".avro"):
#             # For Avro files
#             command = f"cat {file_path}"
#             result = await conn.run(command, encoding=None)  # Get binary output
#             file_content = BytesIO(result.stdout)  # Convert to BytesIO for fastavro
#             reader = fastavro.reader(file_content)
#             # Extract field names from the Avro schema
#             columns = [field['name'] for field in reader.schema['fields']]
#             return columns

#         elif file_path.endswith(".orc"):
#             # For ORC files using the 'pyorc' library
#             command = f"cat {file_path}"
#             result = await conn.run(command, encoding=None)  # Get binary output
#             file_content = BytesIO(result.stdout)  # Convert to BytesIO for pyorc
#             reader = pyorc.Reader(file_content)
            
#             # Inspect the schema to check the structure of the fields
#             fields = reader.schema.fields
            
#             # Extract column names properly from the schema
#             columns = []
#             for field in fields:
#                 # Check if the field has a 'name' attribute (this might be structured differently)
#                 if isinstance(field, dict):
#                     columns.append(field.get('name', 'Unknown'))
#                 else:
#                     columns.append(str(field))  # Fallback to string representation if structure is different

#             return columns

#         else:
#             raise HTTPException(status_code=400, detail="Unsupported file format")

#         # Return the column names
#         return df.columns.tolist()

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")