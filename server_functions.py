import asyncssh
from fastapi import HTTPException
from request_models import connection_model
import pymysql

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
