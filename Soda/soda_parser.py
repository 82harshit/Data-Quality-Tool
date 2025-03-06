from typing import Optional, List
import yaml
import re

from request_models import connection_enum_and_metadata as conn_enum, job_model
from logging_config import dqt_logger
from job_state_singleton import JobStateSingleton
from database.db_models.job_run_status import JobRunStatusEnum


class SodaParser:
    def __remove_empty_dicts(self, data):
        """
        Removes all the empty dictionaries from the provided JSON.
        """
        if isinstance(data, dict):
            return {k: self.__remove_empty_dicts(v) for k, v in data.items() if v != {}}
        elif isinstance(data, list):
            return [self.__remove_empty_dicts(item) for item in data]
        return data

    @staticmethod
    def __get_formatted_check_for_datasource(expectation_type: str, 
                                            datasource_type: str,
                                            condition: str,
                                            column: str, 
                                            percentile: Optional[str] = None) -> str:
        """
        Returns the formatted expectations as required by the Soda library.
        E.g.: 
            max(rainfall) <= 340.0
            
        :param datasource_type (str): The type of the datasource based on which the check needs to be returned
        :param expectation_type (str): The check that is to be applied
        :param column (str): The name of the column on which the check is applied
        :param condition (str): Condition used in the check
        :param percentile (str or None): Percentile values (used if using percentile check)
        
        :return str: Formatted check for check string
        """
        if datasource_type == conn_enum.Database_Datasource_Enum.MYSQL:
            if percentile:
                return f"{expectation_type}(`{column}`, {percentile}) {condition}"
            return f"{expectation_type}(`{column}`) {condition}"
        elif datasource_type == conn_enum.Database_Datasource_Enum.POSTGRES:
            if percentile:
                return f"""{expectation_type}("{column}", {percentile}) {condition}"""
            return f"""{expectation_type}("{column}") {condition}"""
        elif datasource_type == conn_enum.Database_Datasource_Enum.MSSQL:
            if percentile:
                return f"{expectation_type}([{column}], {percentile}) {condition}"
            return f"{expectation_type}([{column}]) {condition}"
        
        if datasource_type in conn_enum.File_Datasource_Enum.__members__.values():
            column = (column.strip() # Remove leading/trailing spaces
                            .replace(" ", "_") # Replace spaces with underscores
                            .lower() # Convert to lowercase
            )
            column = re.sub(r'[^a-zA-Z0-9_]', '', column) # Remove special characters
            if percentile:
                return f"{expectation_type}({column}, {percentile}) {condition}"
            return f"{expectation_type}({column}) {condition}"
    
    @staticmethod
    def __filename_match_with_column(filename: str, filename_regex: str, column: str, condition: str) -> str:
        """Extracts the component from the provided filename using the filename_regex.
        A Soda check of `invalid_count` is generated using these params.

        :param filename (str): The name of the file that needs to be matched with
        :param filename_regex (str): The regex pattern to extract the component which needs to be matched from the filename
        :param column (str): The column on which check needs to be applied
        :param condition (str): The condition for Soda `invalid_count` check

        :raise Warning: If the match component cannnot be extracted from the filename using the filename_regex

        :returns str: Soda formatted check of `invalid_count`
        """
        match = re.match(filename_regex, filename)
        if match:
            country_code = match.group(1)
        else:
            warning_msg = f"Could not extract match component from filename {filename} for check: filename_match_with_column"
            dqt_logger.warning(warning_msg)
            raise Warning(warning_msg)
        
        check = {
            f"invalid_count({column}) {condition}": {
                "valid regex": country_code        
            }
        }
        
        return check
    
    @staticmethod
    def __check_filename_match(filename: str, filename_regex: str):
        """Performs a regex match of the filename with the provided filename regex.
        
        :param filename (str): The name of the file to be validated.
        :param filename_regex (str): The regex to match the filename with.
        
        :raises Exception: Incorrect filename format exception.
        
        :return None: If the filename matches with the provided regex.
        """
        if not bool(re.match(filename_regex, filename)):
            error_msg = f"Incorrect filename format: Filename '{filename}' does not match with the filename format provided."
            dqt_logger.error(error_msg)
            raise ValueError(error_msg) 

    @staticmethod
    def __sanitize_sql_query(query: str) -> str:
        """Function to replace spaces with underscores, remove special characters and lowercase the column names.
            Note: The column names must be provided under {{}}
            E.g.: SELECT COUNT(*) FROM [dataset_name] WHERE {{Manufacturer's code}} != '0';
    
            :param query (str): SQL query which needs to be cleaned
            
            :return sanitized_query (str): Cleaned query
        """
        def clean_column_name(match):
            column_name = match.group(1)  # Extract column name
            cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '', column_name.replace(' ', '_'))
            cleaned_name = cleaned_name.lower()
            return cleaned_name
        
        # Regex pattern to find column names inside {{<column name>}}
        pattern = r"\{\{(.*?)\}\}"
        
        # Replace matches using clean_column_name function
        sanitized_query = re.sub(pattern, clean_column_name, query)
        dqt_logger.debug(f"Sanitized query: {sanitized_query}")
        return sanitized_query
    
    def __create_user_defined_checks(self, expectation_type: str, kwargs: dict, datasource_name: str) -> dict:
        """Formats a user defined check in a dictionary format which is then parsed as YAML, as required by Soda.
        
        :param expectation_type (str): Type of user defined expectation
        :param kwargs (dict): Arguments like queries, expressions, threshold condition, etc to be included in check
        :param datasource_name (str): The name of the datasource on which the check is to be performed

        :return dict: Dictionary of formatted check
        """
        # replace the keyword '[dataset_name]' with the actual datasource name
        kwargs = {key:value.replace("[dataset_name]", datasource_name) for key, value in kwargs.items()}
        
        # Generates check of type: https://docs.soda.io/soda-cl/user-defined.html#example-with-check-name
        if expectation_type == "user_defined_query":
            query_name = kwargs.get("query_name", "")
            if not query_name:
                warning_msg = "Query name not provided"
                dqt_logger.warning(warning_msg)
                raise Warning(warning_msg)
            query_name = query_name.replace(" ", "_") # replacing spaces with '_'
        
            threshold_condition = kwargs.get("condition", "") # threshold value with condition, e.g.: > 0, = 5, between 4 and 10
            if not threshold_condition:
                warning_msg = f"No threshold provided for check {query_name}"
                dqt_logger.warning(warning_msg)
                raise Warning(warning_msg)
        
            # Ensure threshold_condition is a string and does not contain Python types
            if isinstance(threshold_condition, type):
                threshold_condition = str(threshold_condition.__name__)  # Convert to a valid string

            if not isinstance(threshold_condition, str):
                warning_msg = f"Threshold condition should be a string, got {type(threshold_condition)}"
                dqt_logger.warning(warning_msg)
                raise ValueError(warning_msg)
                    
            valid_query = kwargs.get("valid_query", "")
            if not valid_query:
                warning_msg = "Valid SQL query not provided"
                dqt_logger.warning(warning_msg)
                raise Warning(warning_msg)
            valid_query = self.__sanitize_sql_query(query=valid_query)
        
            other_kwargs = {key:value for key, value in kwargs.items() if key not in ["query_name", "valid_query", "condition"]} 
            check = {
                f"{query_name} {threshold_condition}": {
                    f"{query_name} query": valid_query,
                    **other_kwargs
                }
            }
        # Generates check for type: https://docs.soda.io/soda-cl/user-defined.html#example-with-alert-configuration
        elif expectation_type == "user_defined_expression": # TODO: test for user-defined expressions    
            expression_name = kwargs.get("expression_name", "")
            if not expression_name:
                warning_msg = "Expression name not provided"
                dqt_logger.warning(warning_msg)
                raise Warning(warning_msg)
            expression_name = expression_name.replace(" ", "_") # replacing spaces with '_'
            
            valid_expression = kwargs.get("valid_expression", "")
            if not valid_expression:
                warning_msg = "Valid SQL expression not provided"
                dqt_logger.warning(warning_msg)
                raise Warning(warning_msg)
            
            other_kwargs = {key:value for key, value in kwargs.items() if key not in ["expression_name, valid_expression"]}
            check = {
                expression_name: {
                    f"{expression_name} expression": valid_expression,
                    **other_kwargs
                }
            }
        
        return check

    def create_checks(self, datasource_type: str, datasource_name: str, quality_checks: List[job_model.QualityChecks]) -> yaml:
        """
        Parses the quality checks JSON to a YAML format as required by the Soda library.
        
        :param datasource_name (str): The name of the table
        :param quality_checks (dict): A dictionary containing the quality checks
        
        :return yaml: Parsed quality checks JSON to YAML
        """
        if not quality_checks:
            error_msg = "Cannot validate data, an empty list of checks was provided"
            dqt_logger.error(error_msg)
            raise Exception(error_msg)
        
        quality_checks_list = [check.model_dump() for check in quality_checks]
        checks = []

        try:
            for quality_check in quality_checks_list:
                expectation_type = quality_check.get("expectation_type", "")
                kwargs = quality_check.get("kwargs", "")
                if expectation_type in ["user_defined_query", "user_defined_expression"]:
                    user_defined_checks = self.__create_user_defined_checks(expectation_type=expectation_type, 
                                                                            kwargs=kwargs, datasource_name=datasource_name)
                    checks.append(user_defined_checks)
                elif expectation_type == "file_name_check": # filename checks
                    file_name = kwargs.get("file_name", "")
                    file_name_regex = kwargs.get("regex", "")
                    self.__check_filename_match(filename=file_name, filename_regex=file_name_regex)
                elif expectation_type == "file_name_match_with_column":
                    file_name = kwargs.get("file_name", "")
                    file_name_regex = kwargs.get("file_regex", "")
                    condition = kwargs.get("condition", "")
                    column = kwargs.get("column", "")
                    file_name_match_with_column_check = self.__filename_match_with_column(filename=file_name, filename_regex=file_name_regex, 
                                                                                          column=column, condition=condition)
                    checks.append(file_name_match_with_column_check)
                elif expectation_type == "schema": # schema checks: https://docs.soda.io/soda-cl/schema.html#schema-checks
                    if kwargs:
                        status = kwargs.get("status", "")
                        condition = kwargs.get("condition", "")
                        values = kwargs.get("values", "")
                        other_kwargs = {key:value for key, value in kwargs.items() if key not in ["status", "condition", "values"]}
                        other_kwargs = self.__remove_empty_dicts(other_kwargs)
                        if other_kwargs:
                            check = {
                                expectation_type: {
                                    status: {
                                        condition: other_kwargs
                                    }
                                }
                            }
                        else:
                            check = {
                                expectation_type: {
                                    status: {
                                        condition: values
                                    }
                                }
                            }
                        checks.append(check)
                else:
                    if kwargs:
                        condition = kwargs.get("condition", "")
                        column = kwargs.get("column", "")
                        if column:
                            if expectation_type == "percentile": # percentile check: https://docs.soda.io/soda-cl/numeric-metrics.html#numeric-metrics
                                percentile = kwargs.get("percentile", "")
                                check = self.__get_formatted_check_for_datasource(datasource_type=datasource_type, 
                                                                    expectation_type=expectation_type, 
                                                                    column=column, 
                                                                    condition=condition, 
                                                                    percentile=percentile
                                                                    )
                            else:
                                check = self.__get_formatted_check_for_datasource(datasource_type=datasource_type, 
                                                                    expectation_type=expectation_type, 
                                                                    column=column, 
                                                                    condition=condition
                                                                    )
                            other_kwargs = {key:value for key, value in kwargs.items() if key not in ["column", "condition"]}
                            other_kwargs = self.__remove_empty_dicts(other_kwargs)
                            if other_kwargs:
                                checks.append({check: other_kwargs})
                            else:
                                checks.append(check)
                        else:
                            check = f"{expectation_type} {condition}"
                            checks.append(check)
                    else:
                        checks.append(expectation_type)
            
            checks_yaml = yaml.dump({f"checks for {datasource_name}":checks}, default_flow_style=False, sort_keys=False)
            dqt_logger.info(f"Created checks:\n{checks_yaml}")
            return checks_yaml
        except Exception as e:
            error_msg = f"Failed to create checks: {e}"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message="Failed to create checks")
            raise Exception(error_msg)

    def parse_validation_result(self, validation_result: str) -> List[dict]:
        """
        Formats the resultant string list of `CheckResults`. Each of these `CheckResult` objects,
        contains the name of the check, the status of check i.e. PASS, FAIL, or ERROR, and the validation value for the check.
        
        :param validation_results (str): The string of results that need to be parsed.
        
        :return list of dict: A list of dictionaries containing check results.
        """
        check_lines = validation_result.strip().split("\n")
        results = []

        try:
            for line in check_lines:
                """
                Extracts strings of format:
                [avg(bathrooms) between 2 and 3] FAIL (check_value: 1.2862385321100918)
                """
                regex = r"\[(.+?)\]\s+(PASS|FAIL|ERROR|WARN|None)\s+\(check_value:\s+(\d+(\.\d+)?)\)"
                match = re.match(regex, line)
                if match:
                    check_name = match.group(1) # First group: check name
                    status = match.group(2) # Second group: status
                    check_value = match.group(3) # Third group: full check value

                    if status == 'None':
                        status = "No data found that statisfies the provided query or expression"

                    result = {
                        "check_name": check_name,
                        "check_status": status,
                        "check_value": check_value
                    }
                else:
                    """
                    Extracts strings of format:
                    [schema] FAIL (fail_missing_column_names = [room], schema_measured = [price bigint, area bigint, bedrooms bigint])
                    """
                    regex = r"\[(.+?)\]\s+(PASS|FAIL|ERROR|WARN|None)\s+\((.+?)\)"
                    match = re.match(regex, line)
                    if match:
                        check_name = match.group(1)  # Check name
                        status = match.group(2)  # Status (PASS, FAIL, ERROR)
                        metadata_raw = match.group(3)  # Metadata as raw string
                        
                        if status == 'None':
                            status = "No data found that statisfies the provided query or expression"
                        
                        result = {
                            "check_name": check_name,
                            "check_status": status,
                            "check_value": metadata_raw
                        }
                        
                results.append(result)
            return results
        except Exception as e:
            error_msg = f"Failed to parse validation results: {e}"
            dqt_logger.error(error_msg)
            JobStateSingleton.update_state_of_job_id(job_status=JobRunStatusEnum.ERROR, status_message="Failed to parse validation results")
            raise Exception(error_msg)
        