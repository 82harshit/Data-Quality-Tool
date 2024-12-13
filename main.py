import sys
import subprocess

from logging_config import dqt_logger

def execute_api_request(host: str, port: int):
    """Function to handle Fast API request."""
    dqt_logger.info(f"Running Fast API on host: {host} on port: {port}")
    subprocess.run(f"uvicorn fast_api:app --host {host} --port {port}")

def execute_standalone_script():
    """Function to execute standalone Python script logic."""
    dqt_logger.info("Running standalone Python script...")
    subprocess.run(["python", "standalone_script.py"])

def main():
    """Determine execution path based on input."""
    if len(sys.argv) > 1 and sys.argv[1] == "api":
        host = sys.argv[2]
        port = int(sys.argv[3])
        execute_api_request(host=host, port=port)
    else:
        execute_standalone_script()

if __name__ == "__main__":
    main()
