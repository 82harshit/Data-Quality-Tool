from sqlalchemy import create_engine, Column, String, Date, ForeignKey, Integer
from sqlalchemy.orm import sessionmaker, Session, declarative_base, relationship

import os
import json
from uuid import uuid4

from dotenv import load_dotenv
from retry import retry

from logging_config import dqt_logger
from job_state_singleton import JobStateSingleton
from database.db_models.job_run_status import JobRunStatusEnum

 
Base = declarative_base()

class Batches(Base):
    __tablename__ = 'batches'
    
    job_id = Column(String(255), unique=True, nullable=False)  # Each batch has a unique job_id
    batch_id = Column(String(255), primary_key=True)
    batch_date = Column(Date, nullable=False)

class Expectations(Base):
    __tablename__ = 'expectations'
    
    expectation_id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(255), ForeignKey('batches.batch_id'), nullable=False)
    check_name = Column(String(255), nullable=False)
    check_status = Column(String(255), nullable=False)
    check_value = Column(String(255), nullable=False)
    
    batch = relationship("Batches", back_populates="expectations")
    
Batches.expectations = relationship("Expectations", back_populates="batch", cascade="all, delete-orphan")

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
        
        :param expectation_data (dict): A dict containing the exepctations that is added in the current database session
        :db_session (Session): The database session in which the expecatation are inserted
        
        :return: None
        """
        function_name = "insert_expectation"
        self.retry_attempts[function_name] = self.retry_attempts.get(function_name, 0) + 1  # Increment retry count
        current_attempt = self.retry_attempts[function_name]
        max_attempts = 3  # Matches the `tries` parameter

        try:
            expectation = Expectations(**expectation_data)
            db_session.add(expectation)
            db_session.commit()
            dqt_logger.info(f"Inserted expectation: {expectation_data}")
        except Exception as e:
            db_session.rollback()
            error_msg = f"Error in inserting batch on attempt {current_attempt}/{max_attempts}: {e}"
            dqt_logger.error(error_msg)
            self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message="Error in inserting expectation.")
            raise Exception(error_msg)
 
    @retry(tries=3, delay=2, backoff=2, jitter=(1, 3), logger=dqt_logger) 
    def upsert_batch(self, batch_id: str, job_id: str, batch_date: str, db_session: Session) -> None:
        """
        Inserts or updates a batch record. If failed, retries 3 times.

        :param batch_id (str): The id of the batch that is to be inserted
        :param job_id (str): The job_id associated with the batch being inserted
        :param batch_date (str): The current date at which the batch is being inserted
        :param db_session (Session): The database session in which the data is to be inserted
        
        :return: None
        """
        
        function_name = "upsert_batch"
        self.retry_attempts[function_name] = self.retry_attempts.get(function_name, 0) + 1  # Increment retry count
        current_attempt = self.retry_attempts[function_name]
        max_attempts = 3  # Matches the `tries` parameter
        
        try:
            new_batch = Batches(batch_id=batch_id, job_id=job_id, batch_date=batch_date)
            db_session.add(new_batch)
            info_msg = f"Inserted new batch {batch_id}"
            dqt_logger.info(info_msg)
            self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message=info_msg)
            db_session.commit()
        except Exception as e:
            db_session.rollback()
            error_msg = f"Error in upserting batch on attempt {current_attempt}/{max_attempts}: {e}"
            dqt_logger.error(error_msg)
            self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message="Error in upserting batch.")
 
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
 
            if 'validation_date' not in json_response:
                error_msg = "Error: 'validation_date' key not found in response"
                dqt_logger.error(error_msg)
                self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
                raise Exception(error_msg)
  
            results = json_response['results']
            if not results:
                error_msg = "Error: No results found in the response"
                dqt_logger.error(error_msg)
                self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
                raise Exception(error_msg)

            batch_date = json_response['validation_date']
            if not batch_date:
                error_msg = "Error: No validation date found in response"
                dqt_logger.error(error_msg)
                self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message=error_msg)
                raise Exception(error_msg)
               
            batch_id = uuid4().hex
            self.upsert_batch(batch_id=batch_id, job_id=job_id, batch_date=batch_date, db_session=db_session)
            
            for result in results:       
                expectation = {
                    'batch_id': batch_id,
                    'check_name': result['check_name'],
                    'check_status': result['check_status'],
                    'check_value': result['check_value']
                }
                
                info_msg = f"Inserting expectation: {expectation}"
                dqt_logger.info(info_msg)
                self.job_state.update_state_of_job_id(job_status=JobRunStatusEnum.INPROGRESS, status_message="Inserting expectation")
 
                self.insert_expectation(expectation_data=expectation, db_session=db_session)
 
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
            