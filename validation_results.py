from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Date, JSON, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, Session, relationship
from datetime import datetime
import os
from dotenv import load_dotenv
from state_singelton import JobIDSingleton
from database.db_functions import DBFunctions
from logging_config import ge_logger
 
# Load environment variables
load_dotenv()
 
# Define database URL
DATABASE_URL = os.getenv('DATABASE_URL', 'mysql+pymysql://db_user:July$2018@localhost:3306/valdiation_results')
 
# Create an engine and session local factory
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
 
# Define Base class for models
Base = declarative_base()
 
# Batch Model (One batch -> one job_id)
class Batch(Base):
    __tablename__ = 'batches'
 
    batch_id = Column(String(255), primary_key=True)
    job_id = Column(String(255), unique=True, nullable=False)  # Each batch has a unique job_id
    batch_date = Column(Date)
    data_quality_score = Column(Float)
 
# Expectation Model (Many expectations per batch)
class Expectation(Base):
    __tablename__ = 'expectations'
 
    execution_id = Column(Integer, primary_key=True, autoincrement=True)  # Auto-increment primary key
    batch_id = Column(String(255), ForeignKey('batches.batch_id'), nullable=False)
    expectation_type = Column(String(255))
    column = Column(String(255))
    regex = Column(String(255), nullable=True)
    element_count = Column(Integer)
    unexpected_count = Column(Integer)
    unexpected_percent = Column(Float)
    missing_count = Column(Integer)
    missing_percent = Column(Float)
    unexpected_values = Column(JSON)
    success = Column(Boolean)
    exception_message = Column(String(255), nullable=True)
    exception_traceback = Column(String(255), nullable=True)
 
    batch = relationship("Batch", back_populates="expectations")
 
# Define the reverse relationship on the Batch model
Batch.expectations = relationship("Expectation", back_populates="batch", cascade="all, delete-orphan")
 
# DataQuality Class
class DataQuality:
    def __init__(self):
        self.engine = engine
        self.SessionLocal = SessionLocal
        self.db = DBFunctions()
        self.job_id = JobIDSingleton.get_job_id()
 
    def upsert_batch(self, batch_id: str, job_id: str, batch_date, data_quality_score: float, db_session: Session):
        """Inserts or updates a batch record."""
        try:
            existing_batch = db_session.query(Batch).filter(Batch.batch_id == batch_id).first()
            if existing_batch:
                existing_batch.data_quality_score = data_quality_score
                ge_logger.info(f"Updated batch {batch_id}")
            else:
                new_batch = Batch(batch_id=batch_id, job_id=job_id, batch_date=batch_date, data_quality_score=data_quality_score)
                db_session.add(new_batch)
                ge_logger.info(f"Inserted new batch {batch_id}")
            db_session.commit()
        except Exception as e:
            db_session.rollback()
            ge_logger.error(f"Error in upserting batch: {e}")
            self.db.update_status_of_job_id(job_id=self.job_id, job_status="Error", status_message="Error in upserting batch")
 
    def insert_expectation(self, expectation_data: dict, db_session: Session):
        """Inserts a new expectation record."""
        try:
            expectation = Expectation(**expectation_data)
            db_session.add(expectation)
            db_session.commit()
            ge_logger.info(f"Inserted expectation for batch_id: {expectation_data['batch_id']}")
        except Exception as e:
            db_session.rollback()
            ge_logger.error(f"Error in inserting expectation: {e}")
 
    def fetch_and_process_data(self, json_response, job_id: str):
        db_session = self.SessionLocal()  # Create a new session
        try:
            if 'results' not in json_response:
                ge_logger.error("Error: 'results' key not found in the response")
                self.db.update_status_of_job_id(job_id=self.job_id, job_status="Error", status_message="Error: 'results' key not found in the response")
                return
 
            results = json_response['results']
            if not results:
                ge_logger.error("Error: No results found in the response")
                self.db.update_status_of_job_id(job_id=self.job_id, job_status="Error", status_message="No results found in the response")
                return
 
            batch_id = results[0]['expectation_config']['kwargs']['batch_id']
            batch_date = datetime.now().date()
 
            statistics = json_response['statistics']
            success_percent = statistics.get('success_percent', 0.0)
            data_quality_score = success_percent
 
            self.upsert_batch(batch_id, job_id, batch_date, data_quality_score, db_session)
 
            for result in results:
                expectation_config = result['expectation_config']
                result_details = result['result']
                exception_info = result.get('exception_info', {})
 
                expectation_type = expectation_config.get('expectation_type')
                if not expectation_type:
                    ge_logger.error(f"Error: 'expectation_type' not found for batch_id {batch_id}")
                    self.db.update_status_of_job_id(job_id=self.job_id, job_status="Error", status_message="'expectation_type' not found for batch_id {batch_id}")
                    continue
 
                expectation = {
                    'batch_id': batch_id,
                    'expectation_type': expectation_type,
                    'column': expectation_config['kwargs'].get('column'),
                    'regex': expectation_config['kwargs'].get('regex'),
                    'element_count': result_details.get('element_count', 0),
                    'unexpected_count': result_details.get('unexpected_count', 0.0),
                    'unexpected_percent': result_details.get('unexpected_percent', 0.0),
                    'missing_count': result_details.get('missing_count', 0.0),
                    'missing_percent': result_details.get('missing_percent', 0.0),
                    'unexpected_values': result_details.get('partial_unexpected_list', []),
                    'success': result.get('success', False),
                    'exception_message': exception_info.get('exception_message', ''),
                    'exception_traceback': exception_info.get('exception_traceback', '')
                }
 
                self.insert_expectation(expectation, db_session)
 
            db_session.commit()
            ge_logger.info("Data stored successfully.")
            self.db.update_status_of_job_id(job_id=self.job_id, job_status="Completed")
        except Exception as e:
            db_session.rollback()
            ge_logger.error(f"Error processing data: {e}")
            self.db.update_status_of_job_id(job_id=self.job_id, job_status="Error", status_message="Error processsing data, cannot save in database.")
        finally:
            db_session.close()
