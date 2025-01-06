import os
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain.output_parsers import PydanticOutputParser
from request_models.expectations import AllExpectations
from prompts import generate_expectation_prompt
from utils import clean_json_string, convert_to_json
from logging_config import dqt_logger

class SuggestionBI:
    def __init__(self, api_key_env_var="GROQ_API_KEY", db_uri=None, table=None):
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
        self.llm = ChatGroq(model="llama-3.3-70b-versatile")

    def _initialize_database(self, db_uri):
        """Initialize the SQL database connection."""
        self.db = SQLDatabase.from_uri(db_uri)
        self.engine = create_engine(db_uri)

    def run_prompt(self, table_name: str) -> dict:
        """
        Generate AI-based suggestions for the specified table and schema.

        :param table_name: Name of the table to analyze.
        :param schema_name: Schema name of the table (currently unused, but reserved for future use).
        :return: List of suggestions as a Python dictionary.
        """
        if not self.db:
            raise ValueError("Database not initialized.")
        
        if not table_name:
            raise ValueError("Table name not provided.")

        expectation_parser = PydanticOutputParser(pydantic_object=AllExpectations)
        prompt = generate_expectation_prompt(
            table=table_name,
            table_schema=self.db.get_table_info(table_names=[table_name]),
            metric="correctness",
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
            raise

# def main():
#     """Main function to get database name and table name from user input and run the process."""
#     # Prompt user for database name and table name
#     db_name = 'quality_tool'
#     table_name = 'customers'

#     # Construct the database URI from user input (assuming default username and password for simplicity)
#     db_uri = f"mysql+pymysql://root:July$2018@32.33.34.7/{db_name}"
    
#     # Create an instance of the SuggestionBI class
#     suggestion_bi = SuggestionBI(db_uri=db_uri, table=table_name)

#     # Run the prompt to get suggestions
#     try:
#         result = suggestion_bi.run_prompt()
#         print(result)
#     except Exception as e:
#         logging.error(f"An error occurred: {e}")

# if __name__ == "__main__":
#     main()
