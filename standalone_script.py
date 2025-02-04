import json
import sys
import asyncio
from typing import Optional

from endpoint_enums import EndpointEnum
from request_models import connection_model, job_model
from logging_config import dqt_logger
from fast_api import submit_job, submit_job_status, create_connection, generate_suggestions


def request_json_parser(endpoint:str, request_json: Optional[dict]=None, job_id: Optional[str]=None) -> None:
    """
    This is an orchestrator function that executes the exepcted function based on the endpoint name.

    :param endpoint (str): Name of the endpoint that needs to be executed
    :param request_json (Optional[dict], optional): The request JSON that contains validation checks. Defaults to None.
    :param job_id (Optional[str], optional): The job ID for which the state needs to be found. Defaults to None.
    
    :return: None
    """
    if endpoint == EndpointEnum.CREATE_CONNECTION:
        connection = connection_model.Connection(**request_json)
        create_connection_result = asyncio.run(create_connection(connection=connection))
        dqt_logger.info(create_connection_result)
    elif endpoint == EndpointEnum.SUBMIT_JOB:
        job = job_model.SubmitJob(**request_json)
        submit_job_result = asyncio.run(submit_job(job=job))
        dqt_logger.info(submit_job_result)
    elif endpoint == EndpointEnum.SUBMIT_JOB_STATUS:
        submit_job_status_result = asyncio.run(submit_job_status(job_id=job_id))
        dqt_logger.info(submit_job_status_result)
    elif endpoint == EndpointEnum.GENERATE_SUGGESTIONS:
        connection = connection_model.GenerateSuggestion(**request_json)
        suggestions = asyncio.run(generate_suggestions(connection=connection))
        dqt_logger.info(suggestions)
    else:
        raise ValueError(f"Endpoint {endpoint} not found")
    
if __name__ == "__main__":
    endpoint = sys.argv[1]  # First argument
    second_arg = sys.argv[2]  # Second argument
    
    try:
        request_json = json.loads(second_arg)
        dqt_logger.debug("Parsed JSON:", request_json)
        request_json_parser(request_json=request_json, endpoint=endpoint)
    except json.JSONDecodeError:
        job_id = second_arg
        dqt_logger.debug("Detected job ID:", job_id)
        request_json_parser(endpoint=endpoint, job_id=job_id)
        