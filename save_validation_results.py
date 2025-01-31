from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker, Session, declarative_base
import os
import json
from dotenv import load_dotenv
from retry import retry

from logging_config import dqt_logger
from job_state_singleton import JobStateSingleton
from database.db_models.job_run_status import JobRunStatusEnum

 
# Define Base class for models
Base = declarative_base()

class ExpectationResults(Base):
    __tablename__ = 'expectation_results'
    
    job_id = Column(String(255), primary_key=True, unique=True, nullable=False)  # Each batch has a unique job_id
    check_name = Column(String(255), nullable=False)
    check_status = Column(String(255), nullable=False)
    check_value = Column(String(255))
    
    
class ValidationResult:
    def __init__(self):
        load_dotenv()
        DATABASE_URL = os.getenv('DATABASE_URL')
        self.engine = create_engine(DATABASE_URL)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.job_state = JobStateSingleton
        self.retry_attempts = {}
  
    @retry(tries=3, delay=2, backoff=2, jitter=(1, 3), logger=dqt_logger)        
    def insert_expectation(self, expectation_data: dict, db_session: Session) -> None:
        """
        Inserts a new expectation record. If failed, retries 3 times.
        
        :param expecatation_date (dict): A dict containing the exepctations that is added in the current database session
        :db_seesion (Session): The database session in which the expecatation are inserted
        
        :return: None
        """
        function_name = "insert_expectation"
        self.retry_attempts[function_name] = self.retry_attempts.get(function_name, 0) + 1  # Increment retry count
        current_attempt = self.retry_attempts[function_name]
        max_attempts = 3  # Matches the `tries` parameter

        try:
            expectation = ExpectationResults(**expectation_data)
            db_session.add(expectation)
            db_session.commit()
            dqt_logger.info(f"Inserted expectation: {expectation_data['expectation_type']}")
        except Exception as e:
            db_session.rollback()
            error_msg = f"Error in inserting batch on attempt {current_attempt}/{max_attempts}: {e}"
            dqt_logger.error(error_msg)
            self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message="Error in inserting expectation.")
            raise Exception(error_msg)
 
    def save_result_for_job_id(self, json_response: json, job_id: str) -> None:
        """
        Accepts the validation JSON response and job id. Executes upsert_batches and insert_expecatations
        to add validation results in table

        :param json_response (json): A JSON containing the validation results that needs to be inserted
        :param job_id (str): The job for which the validation results need to be inserted

        :return: None
        """
        db_session = self.SessionLocal()
        try:
            if 'results' not in json_response:
                error_msg = "Error: 'results' key not found in the response"
                dqt_logger.error(error_msg)
                self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
                raise Exception(error_msg)
 
            results = json_response['results']
            if not results:
                error_msg = "Error: No results found in the response"
                dqt_logger.error(error_msg)
                self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
                raise Exception(error_msg)
 
            for result in results:
                check_name = result['check_name']
                check_status = result['check_status']
                check_value = result['check_value']
                
                expectation = {
                    'job_id': job_id,
                    'check_name': check_name,
                    'check_status': check_status,
                    'check_value': check_value
                }
                
                info_msg = f"Inserting expectation:{expectation}"
                dqt_logger.info(info_msg)
                self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message="Inserting expectation")
 
                self.insert_expectation(expectation, db_session)
 
            db_session.commit()
            dqt_logger.info("Data stored successfully.")
            self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.COMPLETED)
        except Exception as e:
            db_session.rollback()
            error_msg = f"Error processing data: {e}"
            dqt_logger.error(error_msg)
            self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message="Error processing data.")
        finally:
            db_session.close()
            