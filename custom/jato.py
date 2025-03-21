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
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logging_config import dqt_logger
from request_models.job_model import QualityChecksFile

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
    
    def create_checks_from_file(self, quality_checks_file: QualityChecksFile) -> List[dict]:
        dqt_logger.debug(f"Creating custom checks for Jato: {quality_checks_file}")
       
        file_path = quality_checks_file.file_path
        master_columns = quality_checks_file.master_columns
        slave_columns = quality_checks_file.slave_columns
        sheet_name = quality_checks_file.sheet_name
        
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
            "Product Description 3": "product_description",
            "Product Description 4": "product_description",
            "Other mandatory costs Value on Website 1": "other_mandatory_costs",
            "Yearly Mileage (miles)": "yearly_mileage_miles",
            "Yearly Mileage (km)": "yearly_mileage_km",
            "Total Contract Mileage (miles)": "total_contract_mileage_miles",
            "Total Contract Mileage (km)": "total_contract_mileage_km",
            "Product description": "product_description"
        }
        
        # for _, row in dataframe.iterrows():
        #     # for column in slave_columns:
        #     valid_query = f"""
        #     SELECT COUNT(*), 
        #     CASE 
        #         WHEN LOWER({{{{ {product_description_column_mapping[master_columns[0]]} }}}}) LIKE '%{row[master_columns[0]].lower()}%' 
        #         AND LOWER({{{{ {product_description_column_mapping[master_columns[1]]} }}}}) LIKE '%{row[master_columns[1]].lower()}%' 
        #     THEN 1 ELSE 0 END,
        #     CASE
        #         WHEN {{{{ {product_description_column_mapping[master_columns[2]]} }}}} IS NOT NULL THEN 
        #             CASE
        #                 WHEN {{{{ {product_description_column_mapping[slave_columns[0]]} }}}} IS NOT NULL THEN 1
        #                 ELSE 0
        #             END
        #         WHEN {{{{ {product_description_column_mapping[master_columns[3]]} }}}} IS NULL THEN
        #             CASE
        #                 WHEN {{{{ {product_description_column_mapping[slave_columns[1]]} }}}} IS NULL THEN 1
        #                 ELSE 0
        #             END
        #         WHEN {{{{ {product_description_column_mapping[master_columns[2]]} }}}} IS NOT NULL AND {{{{ {product_description_column_mapping[master_columns[3]]} }}}} IN NOT NULL THEN
        #             CASE
        #                 WHEN {{{{ {product_description_column_mapping[slave_columns[0]]} }}}} LIKE '%|%' THEN 1
        #                 ELSE 0
        #             END 
        #         WHEN {{{{ {product_description_column_mapping[master_columns[2]]} }}}} IS NULL AND {{{{ {product_description_column_mapping[master_columns[3]]} }}}} IN NULL THEN
        #             CASE
        #                 WHEN {{{{ {product_description_column_mapping[slave_columns[0]]} }}}} IS NULL THEN 1
        #                 ELSE 0
        #             END
        #     END
        #     FROM [dataset_name];
        #     """
               
        #     valid_query = self.__sanitize_sql_query(query=valid_query)
            
        #     check = {
        #         "expectation_type": "user_defined_query",
        #         "kwargs": {
        #             "query_name": f"{slave_columns[0]} should be present",
        #             "condition": "> 0",
        #             "valid_query": valid_query,
        #             "failed rows query": None
        #         }
        #     }
            
        #     checks.append(check) 
        
        # Query for: No mileage
        
        valid_query = f"""
        SELECT COUNT(
        CASE
            WHEN {{{{ {product_description_column_mapping["Yearly Mileage (miles)"]} }}}} = {{{{ {product_description_column_mapping["Yearly Mileage (km)"]} }}}}
                AND {{{{ {product_description_column_mapping["Yearly Mileage (miles)"]} }}}} = {{{{ {product_description_column_mapping["Total Contract Mileage (miles)"]} }}}}
                AND {{{{ {product_description_column_mapping["Yearly Mileage (miles)"]} }}}} = {{{{ {product_description_column_mapping["Total Contract Mileage (km)"]} }}}}
            THEN 
            CASE 
                WHEN {{{{ {product_description_column_mapping["Product description"]} }}}} = 'No mileage info available'
                THEN 1
                ELSE 0
            END
        END) AS no_mileage_count
        FROM [dataset_name]
        GROUP BY 
        {{{{ {product_description_column_mapping["Yearly Mileage (miles)"]} }}}}, 
        {{{{ {product_description_column_mapping["Yearly Mileage (km)"]} }}}}, 
        {{{{ {product_description_column_mapping["Total Contract Mileage (miles)"]} }}}}, 
        {{{{ {product_description_column_mapping["Total Contract Mileage (km)"]} }}}}, 
        {{{{ {product_description_column_mapping["Product description"]} }}}};
        """
        
        failed_rows_query = f"""
        SELECT {{{{ {product_description_column_mapping["Yearly Mileage (miles)"]} }}}}, 
                {{{{ {product_description_column_mapping["Yearly Mileage (km)"]} }}}},
                {{{{ {product_description_column_mapping["Total Contract Mileage (miles)"]} }}}},
                {{{{ {product_description_column_mapping["Total Contract Mileage (km)"]} }}}},
                {{{{ {product_description_column_mapping["Product description"]} }}}},
        COUNT(
        CASE
            WHEN {{{{ {product_description_column_mapping["Yearly Mileage (miles)"]} }}}} = {{{{ {product_description_column_mapping["Yearly Mileage (km)"]} }}}}
                AND {{{{ {product_description_column_mapping["Yearly Mileage (miles)"]} }}}} = {{{{ {product_description_column_mapping["Total Contract Mileage (miles)"]} }}}}
                AND {{{{ {product_description_column_mapping["Yearly Mileage (miles)"]} }}}} = {{{{ {product_description_column_mapping["Total Contract Mileage (km)"]} }}}}
            THEN 
            CASE 
                WHEN {{{{ {product_description_column_mapping["Product description"]} }}}} NOT LIKE '%No mileage info available%'
                THEN 1
                ELSE 0
            END
        END) AS mileage_mismatch_count
        FROM [dataset_name]
        GROUP BY 
        {{{{ {product_description_column_mapping["Yearly Mileage (miles)"]} }}}}, 
        {{{{ {product_description_column_mapping["Yearly Mileage (km)"]} }}}}, 
        {{{{ {product_description_column_mapping["Total Contract Mileage (miles)"]} }}}}, 
        {{{{ {product_description_column_mapping["Total Contract Mileage (km)"]} }}}}, 
        {{{{ {product_description_column_mapping["Product description"]} }}}};
        """
        
        check = {
            "expectation_type": "user_defined_query",
            "kwargs": {
                "query_name": "No mileage check",
                "condition": "> 0",
                "valid_query": valid_query,
                "failed rows query": failed_rows_query
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
                    valid_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {payment_type_column_mapping[master_column]} }}}}) LIKE '%{master_col_value}%' AND (LOWER({{{{ {payment_type_column_mapping[column]} }}}}) IS NOT NULL OR LOWER({{{{ {payment_type_column_mapping[column]} }}}}) != '');"
                    failed_rows_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {payment_type_column_mapping[master_column]} }}}}) NOT LIKE '%{master_col_value}%' AND (LOWER({{{{ {payment_type_column_mapping[column]} }}}}) IS NULL OR LOWER({{{{ {payment_type_column_mapping[column]} }}}}) = '');"
               
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
                    valid_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {payment_type_column_mapping[master_column]} }}}}) LIKE '%{master_col_value}%' AND (LOWER({{{{ {payment_type_column_mapping[column]} }}}}) IS NULL OR LOWER({{{{ {payment_type_column_mapping[column]} }}}}) = '');"
                    failed_rows_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {payment_type_column_mapping[master_column]} }}}}) LIKE '%{master_col_value}%' AND (LOWER({{{{ {payment_type_column_mapping[column]} }}}}) IS NOT NULL OR LOWER({{{{ {payment_type_column_mapping[column]} }}}}) != '');"
                    
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
                    valid_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {payment_type_column_mapping[master_column]} }}}}) LIKE '%{master_col_value}%' AND LOWER({{{{ {payment_type_column_mapping[column]} }}}}) LIKE '%{slave_col_value}%';"
                    failed_rows_query = f"SELECT COUNT(*) FROM [dataset_name] WHERE LOWER({{{{ {payment_type_column_mapping[master_column]} }}}}) LIKE '%{master_col_value}%' AND LOWER({{{{ {payment_type_column_mapping[column]} }}}}) NOT LIKE '%{slave_col_value}%';"
                    
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
        return checks
