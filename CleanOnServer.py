# Function to delete all tables in the current schema
def delete_all_tables():
    try:
        # Query to list all tables in the current schema
        query = """
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = CURRENT_SCHEMA()
        AND table_type = 'BASE TABLE';
        """
        
        # Fetch all table names in the current schema
        tables = session.sql(query).collect()
        
        # Loop through the list of tables and drop each one
        for table in tables:
            table_name = table["TABLE_NAME"]
            drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
            session.sql(drop_table_query).collect()
            print(f"Table {table_name} deleted successfully.")
        
    except Exception as e:
        print(f"Error occurred: {e}")

# Example usage: Delete all tables in the current schema
delete_all_tables()