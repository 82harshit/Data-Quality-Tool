from langchain.prompts import PromptTemplate
from langchain.output_parsers import PydanticOutputParser


def generate_expectation_prompt(table: str, table_schema: str, metric: str, k: int, expectation_parser: PydanticOutputParser) -> str:
    system_prompt = """You are an AI assistant expert at mapping columns to expectation checks. 
                        Do not include any comments or code from your side."""
    
    user_prompt = """You are tasked with analyzing a database table. Below, the table name and its schema are provided within triple backticks (```).  
    Follow these instructions carefully to generate well-structured expectation mappings:  

    1. Map each column** in the schema to the most appropriate expectations.  
    2. Ensure proper JSON format** by strictly following this structure:  
    ```json
    {
        "expectation_type": "<expectation_name>",
        "kwargs": {
            "column": "<column_name>",
            "condition": "<condition_value>",  # Include condition where applicable
            <additional_parameters>
        }
    }
    3. Exclude None values: If an expectation does not apply to a column, do not include it.
    4. Maintain uniqueness: Ensure there are no duplicate expectation names in the output.
    5. Avoid unnecessary repetition: Each expectation type should appear only once per relevant column.
    6. Limit expectations: Return at most k expectations.
    7. Ensure correct JSON output formatting: The final output must be a list of objects, like this:
    [
        {
            "expectation_type": "row_count",
            "kwargs": {
                "condition": "> 0"
            }
        },
        {
            "expectation_type": "missing_count",
            "kwargs": {
                "column": "furnishingstatus",
                "condition": "> 0",
                "missing values": [
                    "furnished",
                    "semi-furnished",
                    "unfurnished"
                ]
            }
        },
        {
            "expectation_type": "duplicate_count",
            "kwargs": {
                "column": "price",
                "condition": "= 0"
                }
        },
        {
            "expectation_type": "invalid_count",
            "kwargs": {
                "column": "bedrooms",
                "condition": "= 0",
                "valid values": [
                    1,
                    2,
                    3,
                    4
                        
                ]
            }
        }
    ]
    8. Do not return a dictionary-style mapping like this:
    {
        "avg": None,
        "duplicate_count": {
            "expectation_type": "duplicate_count",
            "kwargs": {
                "column": "Customer Id"
            }
        }
    }
    Instead, return it as a list of structured expectation objects.
    9. Do not define schema-related expectations.
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
