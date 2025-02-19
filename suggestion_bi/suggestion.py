import os
from typing import Optional, List

from dotenv import load_dotenv
from retry import retry
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain.output_parsers import PydanticOutputParser

from .soda_expectations import AllExpectations
from .prompts import generate_expectation_prompt
from utils import clean_json_string, convert_to_json
from logging_config import dqt_logger


class SuggestionBI:
    def __init__(self, api_key_env_var="OPENAI_API_KEY", db_uri=None, table=None):
        self.api_key_env_var = api_key_env_var
        self.table = table
        self.llm = ChatOpenAI(model="o3-mini")
        self._initialize_environment()
        if db_uri:
            self.db = SQLDatabase.from_uri(db_uri)

    def _initialize_environment(self):
        """Load environment variables."""
        load_dotenv()
        if not os.environ.get(self.api_key_env_var):
            raise EnvironmentError(f"API key for {self.api_key_env_var} is not set in the .env file.")

    def run_prompt(self, metric: Optional[str] = "correctness") -> List[dict]:
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
        
        return self.invoke_llm(prompt=prompt)
    
    @retry(tries=3, delay=2, backoff=2, jitter=(1, 3), logger=dqt_logger)    
    def invoke_llm(self, prompt: str) -> List[dict]:
        """Invokes the LLM with the provided prompt.
        Retries 3 times if any exception or warning is raised.

        :param prompt (str): The prompt to be executed

        Raises:
            Warning: Raised when the response returned from LLM is incorrect
            e: Raised when an error occurs while invoking LLM

        :return List[dict]: A list of dictionaries containing suggested expectations
        """
        try:
            answer = self.llm.invoke(prompt)
            cleaned_json_string = clean_json_string(answer.content)
            response_json = convert_to_json(cleaned_json_string)
            if self.validate_llm_response(json_list=response_json):
                return response_json
            else:
                warning_msg = "Invalid response received from LLM"
                dqt_logger.warning(warning_msg)
                raise Warning(warning_msg)
        except Exception as e:
            dqt_logger.error("Error in processing the prompt: %s", e)
            raise e
    
    def validate_llm_response(self, json_list: List[dict]) -> bool:
        """
        Checks if each JSON object in the list contains the required keys.
        Required keys: "expectation_type", "kwargs", "condition"

        :param json_list: List of JSON objects (dictionaries)   
        :return: True if all required keys are present in each JSON, False otherwise
        """
        dqt_logger.debug(f"JSON response: {json_list}")
        
        for json_obj in json_list:
            if "expectation_type" not in json_obj or "kwargs" not in json_obj:
                return False
            # Check if 'expectation_type' starts with 'expect_column_values'
            if json_obj["expectation_type"].startswith("expect_column_values"):
                return False
            
            if "condition" not in json_obj.get("kwargs", {}):
                return False
        return True
    