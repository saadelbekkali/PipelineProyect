import duckdb
import pandas as pd
from pathlib import Path

# Set the path to the database
gold_dir = Path("data/gold")
db_path = gold_dir / "sales_warehouse.db"

# Connect to DuckDB
conn = duckdb.connect(str(db_path))

try:
    # Check if the `sales_data` table exists
    table_exists = conn.execute("SELECT * FROM information_schema.tables WHERE table_name = 'sales_data'").fetchone()
    
    if table_exists:
        # Execute query
        query = "SELECT * FROM monthly_sales"
        result_df = conn.execute(query).fetchdf()
        
        # Display results
        print(result_df)
    else:
        print("The table 'sales_data' does not exist in the database.")

except Exception as e:
    print(f"Error: {str(e)}")

finally:
    conn.close()
