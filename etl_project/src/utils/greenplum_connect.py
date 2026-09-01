import logging
from datetime import datetime, timedelta
import psycopg2
from urllib.parse import urlparse
import os
import random
import string
import shutil
import sys
from io import StringIO
import csv
import pandas as pd

# Class to get the data from the database
# Designed to work with "with" clause
# Can be used to run any sql command and get the output
class greenplum():
    def __init__(self, config, logger = None):
        self.username = config['username']
        self.password = config['password']
        self.driver = config['driver']
        self.schema = config['schema']
        self.url = config['url']
        
        self.hostname = None
        self.port = None
        self.dbname = None
        
        self.connection = None
        self.cursor = None
        self.logger = logger
        self._parse_url()

    def _logger(self, severity, msg):
        import inspect 
        function_name = inspect.currentframe().f_back.f_code.co_name
        if self.logger is not None:
            logger_method = getattr(self.logger, severity.lower(), None)
            if logger_method:
                logger_method(f'[Class funcName: {function_name}] - {msg}')
            else:
                print(f'Invalid severity level: {severity}')
        else:
            print(f'{severity} : {msg}')
            
    def _parse_url(self):
        import re
        self._logger("INFO", "Parsing the URL")
        # Regular expression to extract details
        pattern = r"jdbc:postgresql://(?P<hostname>[^:]+):(?P<port>\d+)/(?P<dbname>[^/]+)"
        # Match the pattern
        match = re.match(pattern, self.url)
        if match:
            details = match.groupdict()
            self.hostname = details['hostname']
            self.port = details['port']
            self.dbname = details['dbname']
        else:
            self._logger("ERROR", "Failed to extract details from the JDBC URL")

    def _create_session(self):
        self._logger("DEBUG", "Creating Connection")
        try:
            self.connection = psycopg2.connect(
                dbname = self.dbname,
                user = self.username,
                password = self.password,
                host = self.hostname,
                port = self.port)
            self.cursor = self.connection.cursor()
            self._logger("DEBUG", "Connection successful")
            return True
        except Exception as e:
            import traceback
            self._logger("ERROR", f'Error in connceting to database')
            self._logger("ERROR", f'Error is: {e}')
            self._logger("ERROR", f'Error traceback: \n{traceback.format_exc()}')
            return False

    def _close_session(self):
        self._logger("DEBUG", "Closing the database connection")
        if self.cursor is not None:
            self.cursor.close()
        if self.connection is not None:
            self.connection.close()
            self._logger("DEBUG", "Database connection closed")
            return True
        self._logger("ERROR", f'Error in closing the connection')
        return False

    def run_sql(self, script):
        self._create_session()
        try:
            self._logger("DEBUG", f"running the SQL: {script}")
            self.cursor.execute(script)
            if self.cursor.description:     # If cursor.description is not None, it means the query returned results
                results = self.cursor.fetchall()
                column_names = [desc[0] for desc in self.cursor.description]
                self.connection.commit()
                return {"type": "DATA", "column_names":column_names, "results": results}
            else:
                self.connection.commit()
                self._logger("INFO", "Script executed successfully")
                return {"type": "MODIFICATION", "results": "Script executed successfully"}
        except Exception as e:
            self._logger("ERROR", f"Error executing script: {script} \n error is : {e}")
            self.connection.rollback()
            return False
        finally:
            self._close_session()
    
    def _create_table_if_not_exists(self, dataframe, table_name):
        try:
            columns = []
            for col, dtype in zip(dataframe.columns, dataframe.dtypes):
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "BIGINT"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "FLOAT"
                elif pd.api.types.is_bool_dtype(dtype):
                    sql_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sql_type = "TIMESTAMP"
                else:
                    sql_type = "TEXT"
                columns.append(f'"{col.lower()}" {sql_type}')
            
            columns_sql = ", ".join(columns)
            create_table_sql = f"""
            CREATE TABLE {table_name.lower()} (
                {columns_sql}
            );
            """
            self.run_sql(create_table_sql)
        except Exception as e:
            self._logger("ERROR", f"Error creating table: Script is {create_table_sql} \n error is : {e}")
            raise ValueError(e)
            return False
    
    def check_table_exists(self, table_name):
        self._create_session()
        try:
            self._logger("INFO", f"Checking Table existance {table_name.lower()}")
            script = f"""SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name.lower()}')"""
            self.cursor.execute(script)
            return self.cursor.fetchone()[0]
        except Exception as e:
            self._logger("ERROR", f"Error Checking Table existance: {script} \n error is : {e}")
            self.connection.rollback()
            return False
        finally:
            self._close_session()
        
    def copy_from_dataframe(self, dataframe, table_name):
        try:
            self._logger("INFO", "Inserting data to the table.")
            if not self.check_table_exists(table_name):
                self._create_table_if_not_exists(dataframe, table_name)
            self._create_session()
            buffer = StringIO()
            dataframe.to_csv(buffer, index=False, header=False, sep="\t", quoting=csv.QUOTE_NONE, escapechar='\\',)
            buffer.seek(0)
            self.cursor.copy_from(buffer, f"{table_name.lower()}", sep="\t", null="")
            self.connection.commit()
            self._logger("INFO", "Data inserted successfully.")
        except Exception as e:
            import traceback
            self._logger("ERROR", f"Error in data insertion ('{self.schema}.{table_name}')")
            self._logger("ERROR", f"Error is: {e}")
            self._logger("ERROR", f"Error traceback: \n{traceback.format_exc()}")
            raise ValueError(e)
        finally:
            self._close_session()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._close_session()
