import yaml
import logging
from datetime import datetime, timedelta
import psycopg2
from urllib.parse import urlparse
import pandas as pd
import os
import string
import sys
import oracledb
import pandas as pd

oracledb.init_oracle_client(lib_dir="/usr/lib/oracle/12.2/client64/lib")

class oracle():
    def __init__(self, config, logger = None):
        """
        A utility class for establishing a connection to an Oracle database and executing queries.

        Parameters:
        ----------
        config : dict
            A dictionary containing Oracle database connection parameters. Expected keys:
                - 'username': Oracle DB username
                - 'password': Oracle DB password
                - 'driver': Oracle DB driver (e.g., 'oracle.jdbc.driver.OracleDriver')
                - 'url': Full connection URL (e.g., 'jdbc:oracle:thin:@host:port/service_name')

        logger : callable, optional
            A logging function to capture runtime messages. If not provided, logging will be skipped.

        Description:
        -----------
        This class simplifies Oracle database interactions by encapsulating connection setup and query execution.
        It parses the connection URL to extract host, port, and service name, and provides methods to:
            - Check table existence
            - Execute SQL queries
            - Return results as Pandas DataFrames

        Usage:
        ------
        oracle_config = {
            'username': 'your_username',
            'password': 'your_password',
            'driver': 'oracle.jdbc.driver.OracleDriver',
            'url': 'jdbc:oracle:thin:@host:port/service_name'
        }

        db = oracle_connect(config=oracle_config, logger=your_logger_function)
        """
        
        self.logger = logger
        self.oracle_config = config.copy()
        self.oracle_username = self.oracle_config['username']
        self.__oracle_password = self.oracle_config['password']
        self.oracle_driver = self.oracle_config['driver']
        self.oracle_url = self.oracle_config['url']
        self.__oracle_parse_url()
        

    def __logger(self, severity, msg):
        import inspect 
        function_name = inspect.currentframe().f_back.f_code.co_name
        if self.logger is not None:
            logger_method = getattr(self.logger, severity.lower(), None)
            if logger_method:
                logger_method(f'[Class funcName: {function_name}] - {msg}')
            else:
                print(f'Invalid severity level: {severity}')
        else:
            print(f'{datetime.now()}-[{severity}] : {msg}')
    
    def __oracle_parse_url(self):
        import re
        self.__logger("INFO", "Parsing the URL")
        # Regular expression to extract details
        pattern = r"jdbc:oracle:thin:(?P<username>[^/]+)/(?P<password>[^@]+)@//(?P<hostname>[^:]+):(?P<port>\d+)/(?P<service_name>[^/]+)"
        # Match the pattern
        match = re.match(pattern, self.oracle_url)
        if match:
            details = match.groupdict()
            self.oracle_hostname = details['hostname']
            self.oracle_port = details['port']
            self.oracle_service_name = details['service_name']
        else:
            self.__logger("ERROR", "Failed to extract details from the JDBC URL")

    def output_type_handler(self, cursor, name, default_type, size, precision, scale):
        if default_type == oracledb.DB_TYPE_CLOB:
            return self.cursor.var(oracledb.DB_TYPE_LONG, arraysize=self.cursor.arraysize)
    
    def _create_session(self):
        self.__logger("INFO", "Creating Connection")
        try:
            dsn = oracledb.makedsn(self.oracle_hostname, self.oracle_port, service_name=self.oracle_service_name)
            self.__logger("INFO", f'Starting database connection ({self.oracle_hostname})')
    
            # Establish a connection to the Oracle database
            self.connection = oracledb.connect(user=self.oracle_username, password=self.__oracle_password, dsn=dsn)
            self.connection.outputtypehandler = self.output_type_handler
            # Create a cursor object
            self.cursor = self.connection.cursor()
        except Exception as e:
            import traceback
            self.__logger("ERROR", f"Error in session creation ({self.oracle_hostname})")
            self.__logger("ERROR", f'Error is: {e}')
            self.__logger("ERROR", f'Error traceback: \n{traceback.format_exc()}')
            raise type(e)(f"Error in session creation ({self.oracle_hostname}): Error is: [ {e} ]")
        
    def _close_session(self):
        if self.cursor is not None:
            self.__logger("INFO", "Closing the database connection")
            self.cursor.close()
            self.cursor = None
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            self.__logger("INFO", "Database connection closed")
            return True
    
    def oracle_table_existance_check(self, schema, table_name):
        try:
            self._create_session()
            # Query to check if the table exists in the specified schema
            query = f"SELECT table_name FROM all_tables WHERE upper(table_name) = '{table_name.upper()}' AND owner = '{schema.upper()}'"
            # Execute the query
            self.cursor.execute(query)
            # Fetch the result
            result = self.cursor.fetchone()
            # Check if the table exists
            if result:
                self.__logger("INFO", f"Table '{table_name}' exists in schema '{schema}'.")
                return True
            else:
                self.__logger("ERROR", f"Table '{table_name}' does not exist in schema '{schema}'.")
                self.__logger("ERROR", f"Query results {result}")
                return False
        except Exception as e:
            import traceback
            self.__logger("ERROR", f"Error in connecting to the Database ({self.oracle_hostname})")
            self.__logger("ERROR", f'Error is: {e}')
            self.__logger("ERROR", f'Error traceback: \n{traceback.format_exc()}')
            raise type(e)(f"Error in connecting to the Database ({self.oracle_hostname}): Error is: [ {e} ]")
        finally:
            self._close_session()
    
    def run_sql(self, query, params=None):
        try:
            self._create_session()

            # Normalize statement type for logging/metadata
            first_token = (query or "").strip().split(None, 1)[0].upper() if query else "UNKNOWN"
            self.__logger("INFO", f"Executing SQL ({first_token})")

            # Execute (with params if provided)
            if params is not None:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            # If description is None => no result set (DDL/DML)
            if self.cursor.description is None:
                # Commit for DDL/DML
                try:
                    # Some DDL auto-commits in Oracle, but we explicitly commit to be safe
                    if self.connection:
                        self.connection.commit()
                except Exception as ce:
                    self.__logger("ERROR", f"Commit failed after {first_token}: {ce}")
                    raise
                # Rowcount for DDL can be -1 depending on driver; still useful metadata
                meta = {
                    "statement_type": first_token,
                    "rowcount": getattr(self.cursor, "rowcount", -1),
                }
                self.__logger("INFO", f"{first_token} executed successfully. Rowcount={meta['rowcount']}")
                return meta
            
            # Otherwise: we have a result set -> build DataFrame
            columns = [col[0] for col in self.cursor.description]
            rows = self.cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
            self.__logger("INFO", f"Query executed successfully. Retrieved {len(df)} rows.")
            return df
    
        except Exception as e:
            import traceback
            self.__logger("ERROR", f"Error in querying the Database ({self.oracle_hostname})")
            self.__logger("ERROR", f'Error is: {e}')
            self.__logger("ERROR", f'Error traceback: \n{traceback.format_exc()}')
            raise type(e)(f"Error in querying the Database ({self.oracle_hostname}): Error is: [ {e} ]")
    
        finally:
            self._close_session()

    def oracle_data_insert(self, schema, table, df):
        self._create_session()
        self.__logger("INFO", 'Oracle Connection creating completed')
    
        self.__logger("INFO", f'Creatig SQL statements. Schema: {schema} | Table name {table}')
        cols = df.columns.tolist()
        col_str = ", ".join(cols)
        placeholders = ", ".join([f":{i+1}" for i in range(len(cols))])
    
        sql = f"INSERT INTO {schema}.{table} ({col_str}) VALUES ({placeholders})"
        self.__logger("DEBUG", f'final SQL: {sql}')
        self.__logger("INFO", f'Inserting data to table :{schema}.{table}')
        # Convert DataFrame to list of tuples
        data = list(df.itertuples(index=False, name=None))
        
        # Bulk insert
        try:
            self.cursor.executemany(sql, data)
            self.connection.commit()
            self.__logger("INFO", f'Data insertion completed')
            self.__logger("INFO", f'Closing connections')
        except Exception as e:
            self.__logger("ERROR", f'Error in data insertion. Error is {e}')
            raise type(e)(e)
        finally:
            self._close_session()
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._close_session()
