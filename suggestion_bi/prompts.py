from langchain.prompts import PromptTemplate
from langchain.output_parsers import PydanticOutputParser


def generate_expectation_prompt(table: str, table_schema: str, metric: str, k: int, expectation_parser: PydanticOutputParser) -> str:
    system_prompt = """You are an AI assistant expert at mapping columns to expectation checks. 
                        Do not include any comments or code from your side."""
    
    user_prompt = """Output must be in strict JSON format. Follow these guidelines to generate structured expectation mappings:

    Map Expectations: Assign relevant expectations to each column based on the schema (data type, constraints, and typical values).
    JSON Structure: Output a list of objects with:
    "expectation_type": The expectation name.
    "kwargs": A dictionary with necessary parameters:
    "column": Column name (if applicable).
    "condition": Derived from the schema (e.g., numerical ranges, allowed categories).
    Additional parameters where needed.
    Exclude None Values: Only include applicable expectations.
    No Duplicates: Each expectation should appear only once per column.
    Limit Expectations: Return at most k expectations, prioritizing the most relevant ones.
    Consistent Formatting: Ensure correct JSON syntax and structure.
    Schema-Based Conditions: Derive conditions dynamically from the schema:
    Numerical columns: Use range conditions (> 0, <= max_value).
    Categorical columns: Ensure values exist within a predefined set.
    Text columns: Validate non-null constraints or expected patterns.
    Avoid Schema Checks: Focus on metric-based expectations, not structural validations.
    Gracefully Handle Edge Cases: If no valid expectations apply, return an empty JSON list ([]).
    """

    final_prompt = PromptTemplate(
        template=""" 
        {system_prompt}
        {user_prompt}
        Metric: {metric}
        Table name: {table}
        Table schema: {table_schema}
        k: {k}
        {format_instructions}
        """,
        input_variables=["table", "table_schema", "system_prompt", "user_prompt", "metric", "k"],
        partial_variables={"format_instructions": expectation_parser.get_format_instructions()}
    )
    
    return final_prompt.format(
        table=table,
        table_schema=table_schema,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        metric=metric,
        k=k
    )
