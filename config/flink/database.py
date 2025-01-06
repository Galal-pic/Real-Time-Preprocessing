import pyodbc
import pandas as pd


def db_connection(
    server_name="DESKTOP-5DG0EQD\SQLEXPRESS",
    database="SIA",
    username="Galal",
    password="123456789",
    table="MCC_Categories",
):
    try:

        # Define your connection parameters
        server_name = server_name
        database = database
        username = username
        password = password

        # Create a connection to the database
        conn_str = f"DRIVER={{SQL Server}};SERVER={server_name};DATABASE={database};UID={username};PWD={password};"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        query = f"SELECT * FROM {table}"
        cursor.execute(query)
        rows = cursor.fetchall()

        columns = [column[0] for column in cursor.description]
        print(f"Database Schema: {columns}")

        df = pd.DataFrame.from_records(rows, columns=columns)

        cursor.close()
        conn.close()

        return df
    except Exception as e:
        print(f"Error connecting to the database: {e}")
