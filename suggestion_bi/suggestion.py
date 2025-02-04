import os
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain.output_parsers import PydanticOutputParser

from soda_expectations import AllExpectations
from prompts import generate_expectation_prompt
from utils import clean_json_string, convert_to_json
from logging_config import dqt_logger


class SuggestionBI:
    def __init__(self, api_key_env_var="OPENAI_API_KEY", db_uri=None, table=None):
        self.api_key_env_var = api_key_env_var
        self.db_uri = db_uri
        self.table = table
        self.db = None
        self.llm = None
        self._initialize_environment()
        self._initialize_llm()
        if db_uri:
            self._initialize_database(db_uri)

    def _initialize_environment(self):
        """Load environment variables."""
        load_dotenv()
        if not os.environ.get(self.api_key_env_var):
            raise EnvironmentError(f"API key for {self.api_key_env_var} is not set in the .env file.")

    def _initialize_llm(self):
        """Initialize the language model."""
        self.llm = ChatOpenAI(model="gpt-4o-mini")

    def _initialize_database(self, db_uri):
        """Initialize the SQL database connection."""
        self.db = SQLDatabase.from_uri(db_uri)
        self.engine = create_engine(db_uri)

    def run_prompt(self, metric: Optional[str] = "correctness") -> dict:
        """
        Generate AI-based suggestions for the specified table and schema.

        :param metric (str): The metric to focus on.
        
        :return: List of suggestions as a Python dictionary.
        """
        if not self.db:
            error_msg = "Database not initialized for generating quality check suggestions."
            dqt_logger.error(error_msg)
            raise ValueError(error_msg)
        
        if not self.table:
            error_msg = "Table name not provided for generating quality check suggestions."
            dqt_logger.error(error_msg)
            raise ValueError(error_msg)

        expectation_parser = PydanticOutputParser(pydantic_object=AllExpectations)
        prompt = generate_expectation_prompt(
            table=self.table,
            table_schema=self.db.get_table_info(table_names=[self.table]),
            metric=metric,
            k=6,
            expectation_parser=expectation_parser
        )

        try:
            answer = self.llm.invoke(prompt)
            cleaned_json_string = clean_json_string(answer.content)
            final_answer = convert_to_json(cleaned_json_string)
            return final_answer  # Return as a Python dictionary
        except Exception as e:
            dqt_logger.error("Error in processing the prompt: %s", e)
            raise e
