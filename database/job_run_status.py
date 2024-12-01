from typing import Optional
import logging

from interfaces import database_interface
from utils import get_job_run_status_table_config, get_cred_db_connection_config
from database.db_models import sql_query
from database import sql_queries as query_template, app_connection, status_update_handler
from logging_config import dqt_logger
from database import job_run_status
 
 
class Job_Run_Status(database_interface.DatabaseInterface):
    def __init__(self, job_id: str):
        self.db_instance = None
        self.job_id = job_id
        
    def connect_to_db(self):
        self.db_instance = app_connection.get_app_db_connection_object()
    
    def insert_in_db(self):
        return super().insert_in_db()
    
    def update_in_db(self, job_status: str, status_message: Optional[str] = None):
        db_conn_details = get_job_run_status_table_config()
        app_status_table = db_conn_details.get('app_status_table')

        job_run_status_details = get_job_run_status_table_config()

        col_job_id = job_run_status_details.get('job_id')
        col_job_status = job_run_status_details.get('job_status')
        col_status_message = job_run_status_details.get('status_message')
        
        cred_db_conn = self.db_instance

        data_to_update = {
            col_job_status: job_status,
            col_status_message: status_message
        }

        set_clause = ", ".join([f"{key} = %s" for key in data_to_update.keys()])

        sql_query.SQLQuery(db_connection=cred_db_conn, 
                           sql_query=query_template.UPDATE_JOB_STATUS_QUERY.format(app_status_table, 
                                                                                   set_clause, col_job_id),
                           params=(job_status, status_message, self.job_id)).execute_query()
        
    def search_in_db(self):
        return super().search_in_db()
    
    def get_from_db(self):
        db_conn_details = get_cred_db_connection_config() 
        job_run_status_details = get_job_run_status_table_config()
        
        app_status_table = db_conn_details.get('app_status_table')
        
        job_id_col = job_run_status_details.get('job_id')
        job_status_col = job_run_status_details.get('job_status')
        status_message_col = job_run_status_details.get('status_message')
        
        job_status, status_message = sql_query.SQLQuery(db_connection=self.db_instance,
                           sql_query=query_template.GET_JOB_STATUS_DETAIL_QUERY.format(job_status_col, status_message_col, 
                                                                                       app_status_table, job_id_col),
                           query_params=(self.job_id))
        
        return {job_status_col: job_status, status_message_col: status_message}
    
    def close_db_connection(self):
        self.db_instance.close()
        

def add_status_update_handler_to_logger(job_status_instance: job_run_status.Job_Run_Status) -> None:
    job_status_instance.connect_to_db()
    db_handler = status_update_handler.DatabaseUpdateHandler(job_status_instance=job_status_instance)
    db_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    dqt_logger.addHandler(db_handler)
    