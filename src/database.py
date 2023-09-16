import sqlite3
import pandas as pd

# Name of the SQLite database file
DATABASE_NAME = "bioinfo_db.sqlite"

def create_connection():
    """
    Create a database connection to SQLite.

    Returns:
        conn: sqlite3.Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
    except sqlite3.Error as e:
        print(f"Error {e} occurred while trying to connect to the database.")
    return conn

def create_table(conn, create_table_sql):
    """
    Create a table using the provided SQL command.

    Parameters:
        conn (sqlite3.Connection): Database connection object.
        create_table_sql (str): SQL command to create a table.
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except sqlite3.Error as e:
        print(f"Error {e} occurred while trying to create a table.")

def load_data_into_table(conn, table_name, csv_path):
    """
    Load data from a CSV file into a specified table.

    Parameters:
        conn (sqlite3.Connection): Database connection object.
        table_name (str): Name of the table in the database where data should be loaded.
        csv_path (str): Path to the CSV file containing the data.
    """
    df = pd.read_csv(csv_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
