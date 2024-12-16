import json
import sys
import asyncio
from typing import Optional

from database.db_models.job_run_status import JobRunStatusEnum
from ge_fast_api_class import GEFastAPI
from helper import get_job_id_and_initialize_job_state_singleton
from job_state_singleton import JobStateSingleton
from request_models import connection_enum_and_metadata as conn_enum, connection_model, job_model
from save_validation_results import ValidationResult
from logging_config import dqt_logger
from utils import log_validation_results, cleanup


def request_json_parser(endpoint:str, request_json: Optional[dict]=None, job_id: Optional[str]=None) -> None:
    """
    This is an orchestrator function that executes the exepcted function based on the endpoint name.

    :param endpoint (str): Name of the endpoint that needs to be executed
    :param request_json (Optional[dict], optional): The request JSON that contains validation checks. Defaults to None.
    :param job_id (Optional[str], optional): The job ID for which the state needs to be found. Defaults to None.
    
    :return: None
    """
    if endpoint == "create_connection":
        connection = connection_model.Connection(**request_json)
        create_connection_result = asyncio.run(CreateConnection(connection=connection).establish_connection())
        dqt_logger.info(create_connection_result)
    elif endpoint == "submit_job":
        job = job_model.SubmitJob(**request_json)
        submit_job_result = asyncio.run(Submit_Job(job=job).execute_job())
        dqt_logger.info(submit_job_result)
    elif endpoint == "submit_job_status":
        submit_job_status_result = asyncio.run(SubmitJobStatus(job_id=job_id).retrieve_job_status())
        dqt_logger.info(submit_job_status_result)
    else:
        raise ValueError(f"Endpoint {endpoint} not found")

class CreateConnection:
    def __init__(self,connection: connection_model.Connection):
        self.connection = connection

    async def establish_connection(self):
        if not self.connection.user_credentials:
            error_msg = "Incorrect JSON request, missing user credentials"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)
        
        if self.connection.connection_credentials:
            connection_type = self.connection.connection_credentials.connection_type
        else:
            error_msg = "Incorrect JSON request, missing connection credentials"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)
        
        try:
            ge_fast_interface = GEFastAPI()
            ge_fast_interface.create_connection_based_on_type(connection=self.connection) # create connection to the user_credentials db
        except Exception as e:
            error_msg = f"Error creating connection: {str(e)}"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)
        
        try:
            # insert credentials based on connection_type
            if connection_type in conn_enum.File_Datasource_Enum.__members__.values():
                unique_connection_name = await ge_fast_interface.insert_user_credentials(connection=self.connection, 
                                                                                        expected_extension=connection_type)
            elif connection_type in conn_enum.Database_Datasource_Enum.__members__.values():
                unique_connection_name = await ge_fast_interface.insert_user_credentials(connection=self.connection)
            else:
                error_msg = f"Unsupported connection type: {connection_type}"
                dqt_logger.error(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            error_msg = f"Error inserting credentials: {str(e)}"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)

        if unique_connection_name:
            return {"status": "connected", "connection_name": unique_connection_name}
        
        raise Exception("Could not connect, an error occurred")


class Submit_Job:
    def __init__(self,job: job_model.SubmitJob):
        self.job = job

    async def execute_job(self):
        job_id = get_job_id_and_initialize_job_state_singleton()
    
        if not self.job.connection_name:
            error_msg = "Incorrect JSON provided, missing connection name"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg) 
            raise Exception(error_msg)
        
        if not self.job.data_source:
            error_msg = "Incorrect JSON provided, missing data source"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
            raise Exception(error_msg)

        if not self.job.quality_checks:
            error_msg = "Incorrect JSON provided, missing quality checks"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg) 
            raise Exception(error_msg)

        ge_fast_api = GEFastAPI()
        
        try:
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.STARTED)
            validation_results = await ge_fast_api.validation_check_request(job=self.job)
            dqt_logger.debug(f"Validation results:\n{validation_results}")
        except Exception as validation_check_error:
            error_msg = f"An error occurred while validating data.\nError:{str(validation_check_error)}"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, 
                                                    status_message="An error occurred while validating data.")
            return {"job_id": job_id}
    
        if validation_results: 
            try:
                log_validation_results(validation_results)
                info_msg = "Saving validation results in database"
                dqt_logger.info(info_msg)
                JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
                ValidationResult().save_result_for_job_id(validation_results, job_id=job_id) 
                return {"job_id": job_id}
            except Exception as saving_validation_error:
                error_msg = f"""An error occurred, failed to save validation results in database
                            \nError:{str(saving_validation_error)}"""
                dqt_logger.error(error_msg)
                JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, 
                                                        status_message="""An error occurred, failed to save validation 
                                                        results in database""")
                return {"job_id": job_id}
            finally:
                cleanup()
        else:
            error_msg = "Missing validation results"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
            return {"job_id": job_id}


class SubmitJobStatus:
    def __init__(self,job_id:str):
        self.job_id = job_id

    async def retrieve_job_status(self):
        current_job_state = JobStateSingleton.get_state_of_job_id(job_id=self.job_id)
        return current_job_state
    
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
        