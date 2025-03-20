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
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
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
        sanitized_query = sanitized_query.strip().replace("\n", "").replace("\t", "")
        return sanitized_query
    
    def __read_file(self, file_path: str, sheet_name: Optional[str]=None):
        if self.__is_valid_csv(file_path=file_path):
            return pd.read_csv(file_path, index_col=None)
        elif self.__is_valid_excel(file_path=file_path):
            try:
                return pd.read_excel(file_path, sheet_name=sheet_name, index_col=None, engine="openpyxl")
            except Exception as excel_exception:
                raise excel_exception
        else:
            raise Exception(f"File {file_path} provided is not a valid excel or csv file.")
    
    def create_checks_from_file(self, file_path: str, master_columns: List[str], slave_columns: List[str], sheet_name: Optional[str]=None) -> List[dict]:
        df = self.__read_file(file_path=file_path, sheet_name=sheet_name)
        if sheet_name.lower().strip() == "payment type":
            return self.__create_checks_for_payment_type(master_column=master_columns[0], slave_columns=slave_columns, dataframe=df)
        elif sheet_name.lower().strip() == "product description":
            return self.__create_checks_for_product_description(master_columns=master_columns, slave_columns=slave_columns, dataframe=df)
        else:
            raise Exception(f"Sheet name {sheet_name} is not valid. Please provide a valid sheet name.")
    
    def __create_checks_for_product_description(self, master_columns: str, slave_columns: List[str], dataframe) -> List[dict]:
        checks = []
        
        product_description_column_mapping = {
            "Make": "make",
            "Region": "country",
            "Additional Fees Value on Website": "additional_fees__msrp",
            "Product Description 1": "product_description",
            "Product Description 2": "product_description",
            "Other mandatory costs Value on Website 1": "other_mandatory_costs"
        }
        
        for _, row in dataframe.iterrows():
            # for column in slave_columns:
            valid_query = f"""
            SELECT COUNT(*) FROM [dataset_name] WHERE 
                LOWER({{{{ {product_description_column_mapping[master_columns[0]]} }}}}) LIKE '%{row[master_columns[0]].lower()}%' 
            AND 
                LOWER({{{{ {product_description_column_mapping[master_columns[1]]} }}}}) LIKE '%{row[master_columns[1]].lower()}%' 
            AND
            CASE
                WHEN {{{{ {product_description_column_mapping[master_columns[2]]} }}}} IS NOT NULL THEN 
                    CASE
                        WHEN {{{{ {product_description_column_mapping[slave_columns[0]]} }}}} IS NOT NULL THEN 1
                        ELSE 0
                    END
                WHEN {{{{ {product_description_column_mapping[master_columns[3]]} }}}} IS NULL THEN
                    CASE
                        WHEN {{{{ {product_description_column_mapping[slave_columns[1]]} }}}} IS NULL THEN 1
                        ELSE 0
                    END
                WHEN {{{{ {product_description_column_mapping[master_columns[2]]} }}}} IS NOT NULL AND {{{{ {product_description_column_mapping[master_columns[3]]} }}}} IN NOT NULL THEN
                    CASE
                        WHEN {{{{ {product_description_column_mapping[slave_columns[0]]} }}}} LIKE '%|%' THEN 1
                        ELSE 0
                    END
            END
            ;"""
               
            valid_query = self.__sanitize_sql_query(query=valid_query)
            
            check = {
                "expectation_type": "user_defined_query",
                "kwargs": {
                    "query_name": f"{slave_columns[0]} should be present",
                    "condition": "> 0",
                    "valid_query": valid_query,
                    "failed rows query": None
                }
            }
            
            checks.append(check) 
        
        return checks
            
    def __create_checks_for_payment_type(self, master_column: str, slave_columns: List[str], dataframe) -> List[dict]:
        checks = []
         
        payment_type_column_mapping = {
            "monthly payment type": "monthly_payment_type",
            "final payment type": "final_payment_type",
            "final payment - retail": "final_payment__msrp",
            "residual value - retail": "residual_value__msrp",
        }
        
        for _, row in dataframe.iterrows():
            for column in slave_columns:
                if row[column] in ["Y", "y", "yes", "Yes", "YES"]:
                    master_col_value = row[master_column].lower() if isinstance(row[master_column], str) else row[master_column]
                    valid_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) LIKE '%{master_col_value}%' AND (LOWER({{{{ {payment_type_column_mapping[column]} }}}}) IS NOT NULL OR LOWER({{{{ {payment_type_column_mapping[column]} }}}}) != '');"
                    failed_rows_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) NOT LIKE '%{master_col_value}%' AND (LOWER({{{{ {payment_type_column_mapping[column]} }}}}) IS NULL OR LOWER({{{{ {payment_type_column_mapping[column]} }}}}) = '');"
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
               
                elif row[column] in ["N", "n", "no", "No", "NO"]:
                    master_col_value = row[master_column].lower() if isinstance(row[master_column], str) else row[master_column]
                    valid_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) LIKE '%{master_col_value}%' AND (LOWER({{{{ {payment_type_column_mapping[column]} }}}}) IS NULL OR LOWER({{{{ {payment_type_column_mapping[column]} }}}}) = '');"
                    failed_rows_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) LIKE '%{master_col_value}%' AND (LOWER({{{{ {payment_type_column_mapping[column]} }}}}) IS NOT NULL OR LOWER({{{{ {payment_type_column_mapping[column]} }}}}) != '');"
                    failed_rows_query = self.__sanitize_sql_query(query=failed_rows_query)
                    valid_query = self.__sanitize_sql_query(query=valid_query)
                    
                    check = {
                        "expectation_type": "user_defined_query",
                        "kwargs": {
                            "query_name": f"{column} should not be present",
                            "condition": "> 0",
                            "valid_query": valid_query,
                            "failed rows query": failed_rows_query
                        }
                    }
                    
                else:
                    master_col_value = row[master_column].lower() if isinstance(row[master_column], str) else row[master_column]
                    slave_col_value = row[column].lower() if isinstance(row[column], str) else row[column]
                    valid_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) LIKE '%{master_col_value}%' AND LOWER({{{{ {payment_type_column_mapping[column]} }}}}) LIKE '%{slave_col_value}%';"
                    failed_rows_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {master_column} }}}}) LIKE '%{master_col_value}%' AND LOWER({{{{ {payment_type_column_mapping[column]} }}}}) NOT LIKE '%{slave_col_value}%';"
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


if __name__ == '__main__':
    jato = Jato()
    check = jato.create_checks_from_file(file_path=r"C:\Users\harshit.tathagat\Downloads\Monthly Payments_Rule Validation.xlsx",
                                    master_columns= ["Make", "Region", "Additional Fees Value on Website", "Other mandatory costs Value on Website 1"],
                                    slave_columns= ["Product Description 1", "Product Description 2"],
                                    sheet_name= "Product Description",
                                    )
    
    # check = jato.create_checks_from_file(file_path=r"C:\Users\harshit.tathagat\Downloads\Monthly Payments_Rule Validation.xlsx",
    #                                 master_columns = ["monthly payment type"],
    #                                 slave_columns = ["final payment type", "final payment - retail", "residual value - retail"],
    #                                 sheet_name= "Payment Type",
    #                                 )
    
    print(json.dumps(check, indent=4))
    with open("custom\jato_checks.json", "w") as file:
        json.dump(check, file, indent=4)
    # checks_yaml = SodaParser().create_checks(datasource_name=jato.datasource_name, datasource_type=jato.datasource_type, quality_checks=check)
    # print(checks_yaml)