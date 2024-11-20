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

            # Updated search command to get the last modified file
            search_command = f"find / -name {connection.connection_credentials.filename} -type f -exec ls -lt {{}} + | head -n 1"
            result = await conn.run(search_command)

            # Check if the file was found in the search result
            if result.exit_status == 0 and result.stdout.strip():
                file_path = result.stdout.strip().split()[-1]

                # Call the function to read the file and get column names
                # columns = await read_file_columns(conn, file_path)

                return {"file_found": True, "file_path": file_path}
            else:
                return {"file_found": False, "message": f"File {connection.connection_credentials.filename} not found."}
    except Exception as e:
        return {"error": str(e)}


# # Function to read the file and return columns
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