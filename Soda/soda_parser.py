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
                            .replace(r"[^\w\s]", "")  # Remove special characters
                            .lower() # Convert to lowercase
            )
            if percentile:
                return f"{expectation_type}({column}, {percentile}) {condition}"
            return f"{expectation_type}({column}) {condition}"
        
    @staticmethod
    def __check_filename_match(filename: str, filename_regex: str) -> str:
        """Performs a regex match of the filename with the provided filename regex.
        
        :param filename (str): The name of the file to be validated.
        :param filename_regex (str): The regex to match the filename with.
        
        :return (str): Match result formatted in the response style of Soda.
        """
        if bool(re.match(filename_regex, filename)):
            raise Exception("Incorrect filename: Filename does not match with the filename format provided.") 

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
                if expectation_type == "file_name_check":
                    file_name = kwargs.get("file_name", "")
                    file_name_regex = kwargs.get("regex", "")
                    self.__check_filename_match(filename=file_name, filename_regex=file_name_regex)
                elif expectation_type == "schema":
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
                            if expectation_type == "percentile":
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
            dqt_logger.debug(f"Created checks:\n{checks_yaml}")
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
                regex = r"\[(.+?)\]\s+(PASS|FAIL|ERROR|WARN)\s+\(check_value:\s+(\d+(\.\d+)?)\)"
                match = re.match(regex, line)
                if match:
                    check_name = match.group(1) # First group: check name
                    status = match.group(2) # Second group: status
                    check_value = match.group(3) # Third group: full check value (integer or decimal)

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
                    regex = r"\[(.+?)\]\s+(PASS|FAIL|ERROR|WARN)\s+\((.+?)\)"
                    match = re.match(regex, line)
                    if match:
                        check_name = match.group(1)  # Check name
                        status = match.group(2)  # Status (PASS, FAIL, ERROR)
                        metadata_raw = match.group(3)  # Metadata as raw string
                        
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
        