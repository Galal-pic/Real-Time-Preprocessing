import pyodbc
import pandas as pd
import logging
from pathlib import Path
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def db_connection(
    server_name: str,
    database: str,
    username: str,
    password: str,
    table: str,
) -> Optional[pd.DataFrame]:
    try:
        # Create a connection to the database
        conn_str = f"DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database};UID={username};PWD={password};"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        query = f"SELECT * FROM {table}"
        cursor.execute(query)
        rows = cursor.fetchall()

        columns = [column[0] for column in cursor.description]
        logger.info(f"Database Schema: {columns}")

        df = pd.DataFrame.from_records(rows, columns=columns)
        return df
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
