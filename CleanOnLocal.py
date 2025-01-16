import sys
import snowflake.connector
import logging
from datetime import datetime

# Snowflake connection credentials
SNOWFLAKE_USER = 'RishabhJain25'
SNOWFLAKE_PASSWORD = 'TRH#Equifax2024'
SNOWFLAKE_ACCOUNT = 'iwuglmq-up37130'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'DELETE_TABLES'
SNOWFLAKE_SCHEMA = 'SALES_DATA'

# Date threshold (tables created before this date will be deleted)
DELETE_BEFORE_DATE = '2025-01-15'  # Format: YYYY-MM-DD

# Set up logging
logging.basicConfig(
    filename='delete_tables.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

def connect_to_snowflake():
    """Establish a connection to Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            client_session_keep_alive=True,
            retry_count=5,  # Increase retry attempts
            connection_timeout=30,  # Increase connection timeout
            insecure_mode=True
        )
        logging.info("Successfully connected to Snowflake.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        raise

def get_old_tables(conn):
    """Retrieve a list of tables created before the DELETE_BEFORE_DATE."""
    query = f"""
    SELECT table_name
    FROM {SNOWFLAKE_DATABASE}.information_schema.tables
    WHERE table_schema = '{SNOWFLAKE_SCHEMA}'
      AND created < '{DELETE_BEFORE_DATE}'
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            tables = [row[0] for row in cursor.fetchall()]
        logging.info(f"Found {len(tables)} tables created before {DELETE_BEFORE_DATE}.")
        return tables
    except Exception as e:
        logging.error(f"Error retrieving table list: {e}")
        raise

def delete_table(conn, table_name):
    """Delete a single table."""
    query = f"DROP TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name};"
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            logging.info(f"Successfully deleted table: {table_name}")
    except Exception as e:
        logging.error(f"Error deleting table {table_name}: {e}")

def main():
    """Main function to delete old tables."""
    try:
        conn = connect_to_snowflake()
        table_list = get_old_tables(conn)
        for table_name in table_list:
            delete_table(conn, table_name)
        conn.close()
        logging.info("Completed deleting old tables.")
    except Exception as e:
        logging.error(f"Process failed: {e}")

if __name__ == "__main__":
    print("Python executable:", sys.executable)
    print("Python version:", sys.version)
    print("PYTHONPATH:", sys.path)
    main()
