from enum import Enum
from typing import Optional
from fastapi import HTTPException

from database.db_models import sql_query
from database import sql_queries as query_template, app_connection
from interfaces import database_interface
from logging_config import dqt_logger
from utils import get_job_run_status_table_config
 

class JobRunStatusEnum(str, Enum):
    """
    Enum that represents the different statuses of a validation job.
    """
    STARTED = "started"
    INPROGRESS= "in progress"
    ERROR = "error"
    COMPLETED = "completed"
    
 
class JobRunStatus(database_interface.DatabaseInterface):
    """
    This class handles operations related to job run status, including 
    connecting to the database, inserting/updating records, 
    and retrieving job status details.
    """
    def __init__(self, job_id: str):
        """
        Initializes the JobRunStatus instance with the given job ID.

        :param job_id (str): The unique identifier for the job.
        """
        self.db_instance = None
        self.job_id = job_id
        
    def connect_to_db(self) -> None:
        """
        Establishes a connection to the database using the app connection.

        This method initializes the `db_instance` attribute with a 
        database connection object.
        """
        self.db_instance = app_connection.get_app_db_connection_object()
    
    def insert_in_db(self) -> None:
        """
        Inserts the job id and its details into the database.

        :param job_id (str): Job ID to be inserted in the table
        
        :return: None
        """
        job_run_status_details = get_job_run_status_table_config()

        job_status_table = job_run_status_details.get('job_status_table')
        
        query_params = (self.job_id, None, None) # add job_id in job_run_status table with status message and logs as `None`
        
        insert_job_id_query = sql_query.SQLQuery(db_connection=self.db_instance,
                                                 query=query_template.INSERT_JOB_STATUS_QUERY.format(job_status_table),
                                                 query_params=query_params
                                                 )
        insert_job_id_query.execute_query()
    
    def update_in_db(self, job_status: str, status_message: Optional[str] = None) -> None:
        """
        Updates the status of a job in the database.

        :param job_status (str): The status of the job (e.g., "started", "completed").
        :param status_message (Optional[str]): An optional message providing additional 
                                             details about the job's status.

        :raises HTTPException: If the update operation encounters any issues.
        """
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

        update_job_status_for_job_id_query = sql_query.SQLQuery(
                            db_connection=self.db_instance, 
                            query=query_template.UPDATE_JOB_STATUS_QUERY.format(
                               job_status_table, set_clause, col_job_id
                            ),
                            query_params=(job_status, status_message, self.job_id)
                           )
        update_job_status_for_job_id_query.execute_query()
        
    def search_in_db(self):
        """
        Searches for job status details in the database.

        This method calls the parent `search_in_db` method from the 
        `database_interface.DatabaseInterface` to perform the search operation.
        
        :return: The result of the search query.
        """
        return super().search_in_db()
    
    def get_from_db(self) -> dict:
        """
        Retrieves the current job status and status message from the database.

        :return dict: A dictionary containing the job status and status message.

        :raises HTTPException: If no job status is found for the given job ID.
        """
        job_run_status_details = get_job_run_status_table_config()
        
        job_id_col = job_run_status_details.get('job_id')
        job_status_col = job_run_status_details.get('job_status')
        status_message_col = job_run_status_details.get('status_message')
        job_status_table = job_run_status_details.get('job_status_table')
        
        query_param = (self.job_id, ) # converting to tuple
        
        get_job_status_and_status_message_query = sql_query.SQLQuery(
                            db_connection=self.db_instance,
                            query=query_template.GET_JOB_STATUS_DETAIL_QUERY.format(
                                job_status_col, status_message_col, job_status_table, job_id_col
                            ),
                            query_params=query_param)
        
        job_status_response = get_job_status_and_status_message_query.execute_query()
        dqt_logger.debug(job_status_response)
        
        if job_status_response:
            job_status = job_status_response[0][0]
            status_message = job_status_response[0][1]
            return {job_status_col: job_status, status_message_col: status_message}
        
        error_msg = "Empty status response recieved"
        dqt_logger.error(error_msg)
        raise HTTPException(status_code=502, detail=error_msg)
        
    def close_db_connection(self) -> None:
        """
        Closes the database connection.

        This method ensures that the `db_instance` is closed after database operations
        are completed to release the resources.
        """
        self.db_instance.close()
