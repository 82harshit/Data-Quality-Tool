from typing import Optional
from enum import Enum
from fastapi import HTTPException

from interfaces import database_interface
from utils import get_job_run_status_table_config
from database.db_models import sql_query
from database import sql_queries as query_template, app_connection
from logging_config import dqt_logger
 

class Job_Run_Status_Enum(str, Enum):
    """
    This enum contains all the states in which a validation job can be
    """
    STARTED = "started"
    INPROGRESS= "in progress"
    ERROR = "error"
    COMPLETED = "completed"
    
 
class Job_Run_Status(database_interface.DatabaseInterface):
    def __init__(self, job_id: str):
        self.db_instance = None
        self.job_id = job_id
        
    def connect_to_db(self):
        self.db_instance = app_connection.get_app_db_connection_object()
    
    def insert_in_db(self):
        return super().insert_in_db()
    
    def update_in_db(self, job_status: str, status_message: Optional[str] = None):
        job_run_status_details = get_job_run_status_table_config()

        col_job_id = job_run_status_details.get('job_id')
        col_job_status = job_run_status_details.get('job_status')
        col_status_message = job_run_status_details.get('status_message')
        job_status_table = job_run_status_details.get('job_status_table')
        
        data_to_update = {
            col_job_status: job_status,
            col_status_message: status_message
        }

        set_clause = ", ".join([f"{key} = %s" for key in data_to_update.keys()])

        update_job_staus_for_job_id_query = sql_query.SQLQuery(db_connection=self.db_instance, 
                           query=query_template.UPDATE_JOB_STATUS_QUERY.format(job_status_table, 
                                                                               set_clause, col_job_id),
                           query_params=(job_status, status_message, self.job_id))
        update_job_staus_for_job_id_query.execute_query()
        
    def search_in_db(self):
        return super().search_in_db()
    
    def get_from_db(self):
        job_run_status_details = get_job_run_status_table_config()
        
        job_id_col = job_run_status_details.get('job_id')
        job_status_col = job_run_status_details.get('job_status')
        status_message_col = job_run_status_details.get('status_message')
        job_status_table = job_run_status_details.get('job_status_table')
        
        query_param = (self.job_id, ) # converting to tuple
        
        get_job_status_and_status_message_query = sql_query.SQLQuery(db_connection=self.db_instance,
                           query=query_template.GET_JOB_STATUS_DETAIL_QUERY.format(job_status_col, status_message_col, 
                                                                                    job_status_table, job_id_col),
                           query_params=query_param)
        
        job_status_response = get_job_status_and_status_message_query.execute_query()
        dqt_logger.debug(job_status_response)
        
        if job_status_response:
            job_status = job_status_response[0][0]
            status_message = job_status_response[0][1]
            return {job_status_col: job_status, status_message_col: status_message}
        else:
            error_msg = "Empty status response recieved"
            dqt_logger.error(error_msg)
            raise HTTPException(status_code=502, detail=error_msg)
        
    def close_db_connection(self):
        self.db_instance.close()
