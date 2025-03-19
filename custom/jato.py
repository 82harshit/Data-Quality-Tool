"""This file creates custom checks for client: Jato

A check in the following manner:

1. It reads a file from the provided file path (the file must be a csv or excel file)
2. The user must provide the name of the master column and the slave columns.
    2.1. Master column is the column on which the value of the slave column depends.
    2.2. Slave columns contains the conditions which need to be validated.
3. After the name of the columns are provided, a list of user-defined checks (provided by the client) is generated using the slave columns.
"""

import csv
import pandas as pd
from typing import List, Optional
from openpyxl import load_workbook
import json
import re
from logging_config import dqt_logger
from Soda.sql_keywords import SQL_KEYWORDS

class Jato:   
    @staticmethod
    def __is_valid_csv(file_path: str) -> bool:
        """
        Verifies if the file at the provided filepath is a valid CSV file.
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader)
            return True
        except (csv.Error, IOError, UnicodeDecodeError, Exception) as e:
            return False
    
    @staticmethod
    def __is_valid_excel(file_path: str) -> bool:
        """
        Verifies if the file at the provided filepath is a valid excel file.
        """
        try:
            load_workbook(file_path)
            return True
        except Exception as e:
            return False
        
    @staticmethod
    def __sanitize_sql_query(query: str) -> str:
        """Function to replace spaces with underscores, remove special characters and lowercase the column names.
            Note: The column names must be provided under {{}}
            E.g.: SELECT COUNT(*) FROM [dataset_name] WHERE {{Manufacturer's code}} != '0';
    
            :param query (str): SQL query which needs to be cleaned
            
            :return sanitized_query (str): Cleaned query
        """
        def clean_column_name(match):
            column_name = match.group(1).strip()  # Extract column name
            if column_name.upper() in SQL_KEYWORDS: # check if column name is a reserved SQL keyword
                return f"'{column_name}'"
            cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '', column_name.replace(' ', '_'))
            cleaned_name = cleaned_name.lower()
            return cleaned_name
        
        # Regex pattern to find column names inside {{<column name>}}
        pattern = r"\{\{(.*?)\}\}"
        
        # Replace matches using clean_column_name function
        sanitized_query = re.sub(pattern, clean_column_name, query) 
        return sanitized_query
            
    def create_checks_from_file(self, file_path: str, master_column: str, slave_columns: List[str], sheet_name: Optional[str]=None) -> List[dict]:
        if self.__is_valid_csv(file_path=file_path):
            df = pd.read_csv(file_path, index_col=None)
        elif self.__is_valid_excel(file_path=file_path):
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, index_col=None, engine="openpyxl")
            except Exception as excel_exception:
                raise excel_exception
        else:
            raise Exception(f"File {file_path} provided is not a valid excel or csv file.")
        
        checks = []
        
        for _, row in df.iterrows():
            for column in slave_columns:
                if row[column] == "Y":
                    master_col_value = row[master_column].lower() if isinstance(row[master_column], str) else row[master_column]
                    valid_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) LIKE '%{master_col_value}%' AND (LOWER({{{{ {column} }}}}) IS NOT NULL OR LOWER({{{{ {column} }}}}) != '');"
                    failed_rows_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) NOT LIKE '%{master_col_value}%' AND (LOWER({{{{ {column} }}}}) IS NULL OR LOWER({{{{ {column}}}}}) = '');"
                    valid_query = self.__sanitize_sql_query(query=valid_query)
                    failed_rows_query = self.__sanitize_sql_query(query=failed_rows_query)
               
                    check = {
                        "expectation_type": "user_defined_query",
                        "kwargs": {
                            "query_name": f"{column} should be present",
                            "condition": "> 0",
                            "valid_query": valid_query,
                            "failed rows query": failed_rows_query
                        }
                    }
               
                elif row[column] == "N":
                    master_col_value = row[master_column].lower() if isinstance(row[master_column], str) else row[master_column]
                    valid_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) LIKE '%{master_col_value}%' AND (LOWER({{{{ {column} }}}}) IS NULL OR LOWER({{{{ {column} }}}}) = '');"
                    failed_rows_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) LIKE '%{master_col_value}%' AND (LOWER({{{{ {column} }}}}) IS NOT NULL OR LOWER({{{{ {column} }}}}) != '');"
                    failed_rows_query = self.__sanitize_sql_query(query=failed_rows_query)
                    valid_query = self.__sanitize_sql_query(query=valid_query)
                    
                    check = {
                        "expectation_type": "user_defined_query",
                        "kwargs": {
                            "query_name": f"{column} should not be present",
                            "condition": "= 0",
                            "valid_query": valid_query,
                            "failed rows query": failed_rows_query
                        }
                    }
                    
                else:
                    master_col_value = row[master_column].lower() if isinstance(row[master_column], str) else row[master_column]
                    slave_col_value = row[column].lower() if isinstance(row[column], str) else row[column]
                    valid_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) LIKE '%{master_col_value}%' AND LOWER({{{{ {column} }}}}) LIKE '%{slave_col_value}%';"
                    failed_rows_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) LIKE '%{master_col_value}%' AND LOWER({{{{ {column} }}}}) NOT LIKE '%{slave_col_value}%';"
                    failed_rows_query = self.__sanitize_sql_query(query=failed_rows_query)
                    valid_query = self.__sanitize_sql_query(query=valid_query)
                    
                    check = {
                        "expectation_type": "user_defined_query",
                        "kwargs": {
                            "query_name": f"{row[column]} should be present",
                            "condition": "> 0",
                            "valid_query": valid_query,
                            "failed rows query": failed_rows_query
                        }
                    }
            
                checks.append(check)
        
        dqt_logger.debug(f"Created checks in JATO.py\n{json.dumps(checks, indent=4)}") # list containing dictionaries of checks converted to json
        return checks
        