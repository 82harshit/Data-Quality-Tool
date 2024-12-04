from database.db_models import job_run_status
from logging_config import dqt_logger
from typing import Optional


class JobStateSingleton:
    _instance = None
    _job_id = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def set_job_id(cls, job_id):
        cls._job_id = job_id

    @classmethod
    def get_job_id(cls):
        return cls._job_id
    
    @classmethod
    def update_state_of_job_id(cls, job_status: str, status_message: Optional[str] = None) -> None:
        _job_id = cls.get_job_id()
        if _job_id == None:
            error_msg = "Job ID not initialized in singleton"
            dqt_logger.error(error_msg)
            return Exception(error_msg)
        _job_run_status = job_run_status.Job_Run_Status(job_id=_job_id)
        _job_run_status.connect_to_db()
        dqt_logger.info(f"Updating state for {_job_id} to: status = {job_status}, logs = {status_message}")
        _job_run_status.update_in_db(job_status=job_status, status_message=status_message)
        _job_run_status.close_db_connection()
        
    def get_state_of_job_id(job_id: str):
        _job_run_status = job_run_status.Job_Run_Status(job_id=job_id)
        _job_run_status.connect_to_db()
        current_job_status = _job_run_status.get_from_db()
        _job_run_status.close_db_connection()
        return current_job_status