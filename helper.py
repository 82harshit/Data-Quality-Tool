from utils import generate_job_id
from logging_config import dqt_logger
from job_state_singleton import JobStateSingleton


def get_job_id_and_initialize_job_state_singleton() -> str:
    """
    Creates a new job id and sets it up in the singleton object
    
    :return job_id(str): Generated job_id
    """
    job_id = generate_job_id() # creates a new job id
    dqt_logger.info(f"Job_ID: {job_id}") # logs the job id
    JobStateSingleton.set_job_id(job_id=job_id) # sets the job_id in singleton object
    return job_id
