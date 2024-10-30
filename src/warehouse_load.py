import duckdb
import pandas as pd
from pathlib import Path
from loguru import logger

class WarehouseLoader:
    """Class to handle loading data into DuckDB warehouse."""
    
    def __init__(self):
        """Initialize the warehouse loader."""
        self.silver_dir = Path("data/silver")
        self.gold_dir = Path("data/gold")
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize DuckDB connection
        self.db_path = self.gold_dir / "sales_warehouse.db"
        self.conn = duckdb.connect(str(self.db_path))
        
        # Setup specific logger for WarehouseLoader
        self.warehouse_logger = logger.bind(name="warehouse")
        self.warehouse_logger.add(
            "logs/gold_load.log",
            rotation="1 day",
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
            filter=lambda record: record["extra"].get("name") == "warehouse"
        )
        
    def check_table_schema(self):
        """Check the current schema of the sales_data table."""
        try:
            schema_info = self.conn.execute("DESCRIBE sales_data").fetchall()
            self.warehouse_logger.info("Current schema for sales_data table:")
            for column in schema_info:
                self.warehouse_logger.info(column)
        except Exception as e:
            self.warehouse_logger.error(f"Error checking table schema: {str(e)}")
            raise

    def create_table_schema(self):
        """Create the sales_data table with appropriate schema."""
        try:
            # Drop the existing table if it exists
            self.conn.execute("DROP TABLE IF EXISTS sales_data")
            
            # Create the main table with proper data types
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS sales_data (
                    sale_id VARCHAR PRIMARY KEY,
                    product_id VARCHAR,
                    sale_date DATE,
                    quantity INTEGER,
                    price_sale DECIMAL(10,2),
                    price_product DECIMAL(10,2),
                    product_name VARCHAR,
                    category VARCHAR,
                    total_sales DECIMAL(12,2),
                    ingestion_timestamp_sale TIMESTAMP,
                    ingestion_timestamp_product TIMESTAMP,
                    sale_year INTEGER,
                    sale_month INTEGER
                )
            """)
            self.warehouse_logger.info("Created sales_data table schema")
            
        except Exception as e:
            self.warehouse_logger.error(f"Error creating table schema: {str(e)}")
            raise

    def create_indexes(self):
        """Create indexes for efficient querying."""
        try:
            # Create indexes for commonly queried columns
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_sale_date 
                ON sales_data(sale_date);
            """)
            
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_product_id 
                ON sales_data(product_id);
            """)
            
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_category 
                ON sales_data(category);
            """)
            
            # Compound index for time-based queries
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_time_partition 
                ON sales_data(sale_year, sale_month);
            """)
            
            self.warehouse_logger.info("Created indexes for efficient querying")
            
        except Exception as e:
            self.warehouse_logger.error(f"Error creating indexes: {str(e)}")
            raise

    def load_data_from_silver(self):
        """
        Load transformed data from silver layer.
        
        Returns:
            pd.DataFrame: Loaded data
        """
        try:
            silver_file = self.silver_dir / "transformed_sales.parquet"
            df = pd.read_parquet(silver_file)
            
            # Convert sale_date to datetime if it's not already
            df['sale_date'] = pd.to_datetime(df['sale_date'])
            
            # Add partitioning columns
            df['sale_year'] = df['sale_date'].dt.year
            df['sale_month'] = df['sale_date'].dt.month
            
            self.warehouse_logger.info(f"Loaded {len(df)} records from silver layer")
            return df
            
        except Exception as e:
            self.warehouse_logger.error(f"Error loading data from silver layer: {str(e)}")
            raise

    def insert_data(self, df: pd.DataFrame):
        """
        Insert data into DuckDB table.
        
        Args:
            df: DataFrame containing the data to insert
        """
        try:
            # Create temporary view of the DataFrame
            self.conn.register('temp_df', df)
            
            # Insert data using SQL
            self.conn.execute("""
                INSERT INTO sales_data (
                    sale_id, product_id, sale_date, quantity, price_sale, 
                    price_product, product_name, category, total_sales, 
                    ingestion_timestamp_sale, ingestion_timestamp_product, 
                    sale_year, sale_month
                )
                SELECT 
                    sale_id, product_id, sale_date, quantity, price_sale, 
                    price_product, product_name, category, total_sales, 
                    ingestion_timestamp_sale, ingestion_timestamp_product, 
                    sale_year, sale_month
                FROM temp_df
            """)
            
            # Unregister temporary view
            self.conn.unregister('temp_df')
            
            self.warehouse_logger.info(f"Successfully inserted {len(df)} records into warehouse")
            
        except Exception as e:
            self.warehouse_logger.error(f"Error inserting data: {str(e)}")
            raise

    def create_materializes_views(self):
        """Create materialized views for common queries."""
        try:
            # Monthly sales summary
            self.conn.execute("""
                CREATE OR REPLACE VIEW monthly_sales AS
                SELECT 
                    sale_year,
                    sale_month,
                    COUNT(*) as total_transactions,
                    SUM(quantity) as total_quantity,
                    SUM(total_sales) as total_revenue,
                    AVG(total_sales) as avg_transaction_value
                FROM sales_data
                GROUP BY sale_year, sale_month
                ORDER BY sale_year, sale_month;
            """)
            
            # Category performance
            self.conn.execute("""
                CREATE OR REPLACE VIEW category_performance AS
                SELECT 
                    category,
                    COUNT(DISTINCT product_id) as total_products,
                    SUM(quantity) as total_quantity_sold,
                    SUM(total_sales) as total_revenue,
                    AVG(price_sale) as avg_price
                FROM sales_data
                GROUP BY category
                ORDER BY total_revenue DESC;
            """)
            
            self.warehouse_logger.info("Created materialized views for common analyses")
            
        except Exception as e:
            self.warehouse_logger.error(f"Error creating materialized views: {str(e)}")
            raise

    def run_warehouse_load(self):
        """Execute the complete warehouse loading process."""
        try:
            self.warehouse_logger.info("Starting warehouse loading process")
            
            # Step 1: Create table schema
            self.create_table_schema()
            
            # Check the schema after creation
            self.check_table_schema()
            
            # Step 2: Load data from silver layer
            df = self.load_data_from_silver()
            
            self.warehouse_logger.info(f"Enriched columns: {df.columns.tolist()}")
            
            # Step 3: Insert data
            self.insert_data(df)
            
            # Step 4: Create indexes
            self.create_indexes()
            
            # Step 5: Create materialized views
            self.create_materializes_views()
            
            self.warehouse_logger.info("Warehouse loading process completed successfully")
            
        except Exception as e:
            self.warehouse_logger.error(f"Warehouse loading process failed: {str(e)}")
            raise
        finally:
            self.conn.close()

def main():
    """Main function to run the warehouse loading process."""
    loader = WarehouseLoader()
    loader.run_warehouse_load()

if __name__ == "__main__":
    main()
