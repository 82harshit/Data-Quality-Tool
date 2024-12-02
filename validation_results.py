from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Date, JSON, ForeignKey
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from datetime import datetime
import os
from dotenv import load_dotenv  # Load environment variables from .env file
 
load_dotenv()  # Load environment variables from .env file
 
# Define database URL
DATABASE_URL = os.getenv('DATABASE_URL', 'mysql+pymysql://db_user:July$2018@32.33.34.7:3306/validation_results')
 
# Create an engine and session local factory
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
 
# Define Base class for models
Base = declarative_base()
 
# Define the Batch model
class Batch(Base):
    __tablename__ = 'batches'
 
    batch_id = Column(String(255), primary_key=True)
    batch_date = Column(Date)
    data_quality_score = Column(Float)
 
# Define the Expectation model
class Expectation(Base):
    __tablename__ = 'expectations'
 
    expectation_id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(255), ForeignKey('batches.batch_id'))
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
 
class DataQuality:
    def __init__(self):
        self.engine = engine
        self.SessionLocal = SessionLocal
 
    def upsert_batch(self, batch_id: str, batch_date, data_quality_score: float, db_session: Session):
        """Inserts or updates a batch record."""
        try:
            existing_batch = db_session.query(Batch).filter(Batch.batch_id == batch_id).first()
            if existing_batch:
                existing_batch.data_quality_score = data_quality_score
                print(f"Updated batch {batch_id}")
            else:
                new_batch = Batch(batch_id=batch_id, batch_date=batch_date, data_quality_score=data_quality_score)
                db_session.add(new_batch)
                print(f"Inserted new batch {batch_id}")
            db_session.commit()
        except Exception as e:
            db_session.rollback()
            print(f"Error in upserting batch: {e}")
 
    def insert_expectation(self, expectation_data: dict, db_session: Session):
        """Inserts a new expectation record."""
        try:
            expectation = Expectation(**expectation_data)
            db_session.add(expectation)
            db_session.commit()
            print(f"Inserted expectation: {expectation_data['expectation_type']}")
        except Exception as e:
            db_session.rollback()
            print(f"Error in inserting expectation: {e}")
 
    def fetch_and_process_data(self, json_response):
        db_session = self.SessionLocal()  # Create a new session
        try:
            # Check if 'results' key exists in json_response
            if 'results' not in json_response:
                print("Error: 'results' key not found in the response")
                return
 
            # Extract batch details from metadata
            results = json_response['results']
            if not results:
                print("Error: No results found in the response")
                return
 
            batch_id = results[0]['expectation_config']['kwargs']['batch_id']
            batch_date = datetime.now().date()  # Assuming current date for simplicity
 
            # Calculate data quality score (e.g., based on unexpected percentage)
            statistics = json_response['statistics']
            success_percent = statistics.get('success_percent', 0.0)
            data_quality_score = success_percent
 
            # Insert batch record (upsert logic to avoid duplicates)
            self.upsert_batch(batch_id, batch_date, data_quality_score, db_session)
 
            # Iterate through individual expectation results
            for result in results:
                expectation_config = result['expectation_config']
                result_details = result['result']
                exception_info = result.get('exception_info', {})
 
                # Ensure 'expectation_type' exists before accessing it
                expectation_type = expectation_config.get('expectation_type')
                if not expectation_type:
                    print(f"Error: 'expectation_type' not found for batch_id {batch_id}")
                    continue  # Skip processing this result if the expectation_type is missing
 
                # Prepare expectation data and log it
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
 
                # Log the expectation data
                print("Inserting expectation:", expectation)
 
                # Insert expectation into the database
                self.insert_expectation(expectation, db_session)
 
            # Commit the changes to the database
            db_session.commit()
            print("Data stored successfully.")
        except Exception as e:
            db_session.rollback()
            print(f"Error processing data: {e}")
        finally:
            db_session.close()
            return
            