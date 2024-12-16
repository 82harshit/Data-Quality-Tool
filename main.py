import argparse
import json
import os
import subprocess
from typing import Optional
from dotenv import load_dotenv

from logging_config import dqt_logger


def execute_api_request(host: str, port: int):
    """Function to handle Fast API request."""
    dqt_logger.info(f"Running Fast API on host: {host} on port: {port}")
    subprocess.run(f"uvicorn fast_api:app --host {host} --port {port}")

def execute_standalone_script(endpoint:str, request_json:Optional[dict]=None, job_id:Optional[str]=None):
    """Function to execute standalone Python script logic."""
    dqt_logger.info("Running standalone Python script...")
    load_dotenv()
    python_executable = os.getenv('PYTHON_EXECUTABLE')
    if not request_json:
        subprocess.run([python_executable, "standalone_script.py",endpoint,job_id])
    else:
        subprocess.run([python_executable, "standalone_script.py",endpoint,request_json])

def main():
    """Determine execution path based on input."""
    parser = argparse.ArgumentParser(description="Script to execute API or standalone logic.")
    subparsers = parser.add_subparsers(dest="mode", required=True, help="Execution mode: 'api' or 'standalone'.")

    # Subparser for API mode
    api_parser = subparsers.add_parser("api", help="Run API request.")
    api_parser.add_argument("host", type=str, help="Host for the API.")
    api_parser.add_argument("port", type=int, help="Port for the API.")

    # Subparser for standalone mode
    standalone_parser = subparsers.add_parser("standalone", help="Run standalone script.")
    standalone_parser.add_argument("endpoint", type=str, help="The endpoint URL.")
    standalone_parser.add_argument("request_json", type=str, help="The request JSON as a string.")

    # Parse arguments
    args = parser.parse_args()

    if args.mode == "api":
        # Handle API execution
        execute_api_request(host=args.host, port=args.port)
    elif args.mode == "standalone":
        # Check if request_json is a file path
        try:
            with open(args.request_json, 'r') as f:
                request_json = json.load(f)
            try:
                # Check if the JSON in request_json is valid by re-parsing it
                request_json = json.dumps(request_json)  # If it raises no error, it's valid
                dqt_logger.debug("The JSON is valid.")
            except (TypeError, ValueError) as e:
                dqt_logger.error(f"Error: Invalid JSON format - {e}")
        except FileNotFoundError:
            # Fallback to treating it as a JSON string
            request_json = args.request_json
            
        execute_standalone_script(endpoint=args.endpoint, request_json=request_json)
        
if __name__ == "__main__":
    main()
