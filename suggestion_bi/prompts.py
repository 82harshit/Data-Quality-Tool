from langchain.prompts import PromptTemplate
from langchain.output_parsers import PydanticOutputParser


def generate_expectation_prompt(table: str, table_schema: str, metric: str, k: int, expectation_parser: PydanticOutputParser) -> str:
    system_prompt = """You are an AI assistant expert at mapping columns to expectation checks. 
                        Do not include any comments or code from your side."""

    user_prompt = """You are tasked with analyzing a database table. Below, the table name and its schema are provided 
    within triple backticks (```).

    1. Map each column in the schema to the most appropriate expectations.
    2. For each expectation, return the results as a unique list of column names.
    3. Ensure there are no duplicate expectation names in the output.
    4. Avoid repetition in expectation names.
    5. Do not return more than k expectations.
    6. For each expectation, return it in the following format:
        {
            "expectation_type": "<expectation_name>",
            "kwargs": {
                "column": "<column_name>",
                <additional_parameters>
            }
        }

    Focus on the provided metric and create concise mappings.
    """

    final_prompt = PromptTemplate(
        template=""" 
        {system_prompt}
        {user_prompt}
        Metric: {metric}
        Table name: ```{table}```
        Table schema: ```{table_schema}```
        k: {k}
        {format_instructions}
        """,
        input_variables=["table", "table_schema", "system_prompt", "user_prompt", "metric", "k"],
        partial_variables={"format_instructions": expectation_parser.get_format_instructions()}
    )
    
    # Generate the final prompt
    formatted_prompt = final_prompt.format(
        table=table,
        table_schema=table_schema,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        metric=metric,
        k=k
    )

    return formatted_prompt
