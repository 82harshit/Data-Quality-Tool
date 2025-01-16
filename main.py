import argparse
import json
import sys
import subprocess
from typing import Optional

from endpoint_enums import EndpointEnum
from logging_config import dqt_logger


def execute_api_request(host: str, port: int) -> None:
    """
    Function to handle Fast API request.
    
    :param host: IP of the host to connect to
    :param port: The port of the server to connect to
    
    :return: None
    """
    dqt_logger.info(f"Running Fast API on host: {host} on port: {port}")
    subprocess.run(f"uvicorn fast_api:app --host {host} --port {port}", shell=True)

def execute_standalone_script(endpoint:str, request_json: Optional[dict]=None, job_id: Optional[str]=None) -> None:
    """
    Function to execute standalone Python script logic.
    
    :param endpoint(str): The name of the endpoint to execute
    :param request_json(dict): The dictionary containing the request
    :param job_id (str): The job ID for which the state of validation job needs to be found
    
    :return: None
    """
    dqt_logger.info("Running standalone Python script...")
    if not request_json:
        subprocess.run([sys.executable, "standalone_script.py", endpoint, job_id])
    subprocess.run([sys.executable, "standalone_script.py", endpoint, request_json])

def main():
    """Determine execution path based on input."""
    parser = argparse.ArgumentParser(description="Script to execute API or standalone logic.")
    subparsers = parser.add_subparsers(dest="mode", required=True, help="Execution mode: 'api' or 'standalone'.")

    # Subparser for API mode
    api_parser = subparsers.add_parser("api", help="Run API request.")
    api_parser.add_argument("--host", type=str, help="Host for the API.")
    api_parser.add_argument("--port", type=int, help="Port for the API.")

    # Subparser for standalone mode
    standalone_parser = subparsers.add_parser("standalone", help="Run standalone script.")
    standalone_parser.add_argument("endpoint", type=str, help="The endpoint URL.")
    standalone_parser.add_argument("request_json_or_job_id", nargs="?", type=str,
                                    help="The request JSON as a string or file path, or a job ID for 'submit_job_status'.")
    standalone_parser.add_argument("--db", type=str, help="The name of the database (for 'generate_suggestions' endpoint only).")
    standalone_parser.add_argument("--table", type=str, help="The name of the table (for 'generate_suggestions' endpoint only).")
    standalone_parser.add_argument("--metric", type=str, 
                                   help="The data metric to be focused on (for 'generate_suggestions' endpoint only).")
    
    # Parse arguments
    args = parser.parse_args()

    if args.mode == "api":
        # Handle API execution
        execute_api_request(host=args.host, port=args.port)
    elif args.mode == "standalone":
        if args.endpoint == EndpointEnum.GENERATE_SUGGESTIONS:
            # Handle generate_suggestions with --db, --table and --metric arguments
            if not args.db or not args.table or not args.metric:
                raise ValueError("--db, --table and --metric are required for 'generate_suggestions' endpoint.")
            
            # Construct the request JSON for generate_suggestions
            request_json = json.dumps({
                "database": args.db,
                "table_name": args.table,
                "metric": args.metric
            })
            execute_standalone_script(endpoint=args.endpoint, request_json=request_json)
        elif args.endpoint == EndpointEnum.SUBMIT_JOB_STATUS:
        # Handle submit_job_status with a job_id
            if not args.request_json_or_job_id:
                raise ValueError("A job ID is required for 'submit_job_status'.")
            execute_standalone_script(endpoint=args.endpoint, job_id=args.request_json_or_job_id)
        else:
            # Handle other endpoints that use request_json
            if not args.request_json_or_job_id:
                raise ValueError("A request JSON is required for this endpoint.")
            try:
                # Check if request_json_or_job_id is a file path
                with open(args.request_json_or_job_id, 'r') as f:
                    request_json = json.load(f)
                try:
                    # Validate JSON format
                    request_json = json.dumps(request_json)  # Re-parse to ensure it's valid
                    dqt_logger.debug("The JSON is valid.")
                except (TypeError, ValueError) as e:
                    dqt_logger.error(f"Error: Invalid JSON format - {e}")
            except FileNotFoundError:
                # Fallback to treating it as a JSON string
                request_json = args.request_json_or_job_id

            execute_standalone_script(endpoint=args.endpoint, request_json=request_json)
        
if __name__ == "__main__":
    main()
