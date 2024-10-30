import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from loguru import logger

class DataTransformation:
    def __init__(self):
        """Initialize the data transformation class."""
        self.bronze_dir = Path("data/bronze")
        self.silver_dir = Path("data/silver")
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        
        # Specific logging configuration for DataTransformation
        self.ingestion_logger = logger.bind(name="transformation")  # Define a specific logger
        self.ingestion_logger.add(
            "logs/transformation.log",
            rotation="1 day",
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
            filter=lambda record: record["extra"].get("name") == "transformation",   # Filter only DataIngestion messages
        )

    def read_bronze_data(self):
        """
        Read JSON data from bronze layer.
        
        Returns:
            tuple: sales_df, products_df pandas DataFrames
        """
        try:
            # Construct paths for JSON files
            sales_path = self.bronze_dir / "sales_data.json"
            products_path = self.bronze_dir / "product_data.json"
            
            self.ingestion_logger.info(f"Reading sales data from: {sales_path}")
            self.ingestion_logger.info(f"Reading products data from: {products_path}")
            
            # Read JSON files using pandas
            sales_df = pd.read_json(sales_path)
            products_df = pd.read_json(products_path)
            
            self.ingestion_logger.info("Successfully read data from bronze layer")
            self.ingestion_logger.info(f"Sales records: {len(sales_df)}")
            self.ingestion_logger.info(f"Products records: {len(products_df)}")
            
            # Log DataFrame information
            self.ingestion_logger.info("\nSales DataFrame Info:")
            self.ingestion_logger.info(f"\nColumns: {sales_df.columns.tolist()}")
            self.ingestion_logger.info(f"Sample data:\n{sales_df.head()}")
            
            self.ingestion_logger.info("\nProducts DataFrame Info:")
            self.ingestion_logger.info(f"\nColumns: {products_df.columns.tolist()}")
            self.ingestion_logger.info(f"Sample data:\n{products_df.head()}")
            
            return sales_df, products_df
            
        except Exception as e:
            self.ingestion_logger.error(f"Error reading bronze data: {str(e)}")
            raise

    def clean_sales_data(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove rows with missing product_id or sale_date.
        """
        try:
            initial_rows = len(sales_df)
            self.ingestion_logger.info(f"Initial number of rows: {initial_rows}")
            
            # Remove rows with missing values
            sales_df = sales_df.dropna(subset=['product_id', 'sale_date'])
            
            rows_removed = initial_rows - len(sales_df)
            self.ingestion_logger.info(f"Rows removed: {rows_removed}")
            self.ingestion_logger.info(f"Remaining rows: {len(sales_df)}")
            
            return sales_df
            
        except Exception as e:
            self.ingestion_logger.error(f"Error cleaning sales data: {str(e)}")
            raise

    def join_and_enrich_data(self, sales_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """
        Join sales and products data and calculate total sales.
        """
        try:
            self.ingestion_logger.info("Starting data enrichment")
            self.ingestion_logger.info(f"Sales columns: {sales_df.columns.tolist()}")
            self.ingestion_logger.info(f"Products columns: {products_df.columns.tolist()}")
            
            # Join sales with products on product_id
            enriched_df = sales_df.merge(
                products_df,
                on='product_id',
                how='left',
                suffixes=('_sale', '_product')
            )
            
            self.ingestion_logger.info(f"Enriched columns: {enriched_df.columns.tolist()}")

            # Calculate total_sales
            enriched_df['total_sales'] = enriched_df['quantity'] * enriched_df['price_sale']
            
            # Get all columns for logging
            self.ingestion_logger.info(f"Available columns after merge: {enriched_df.columns.tolist()}")
            
            # Select and reorder columns
            columns_to_keep = [
                'sale_id',
                'product_id',
                'sale_date',
                'quantity',
                'price_sale',
                'price_product',
                'product_name',
                'category',
                'total_sales',
                'ingestion_timestamp_sale',
                'ingestion_timestamp_product',
            ]
            
            enriched_df = enriched_df[columns_to_keep]
            
            self.ingestion_logger.info(f"Successfully enriched data. Final record count: {len(enriched_df)}")
            self.ingestion_logger.info(f"Sample of enriched data:\n{enriched_df.head()}")
            
            return enriched_df
            
        except Exception as e:
            self.ingestion_logger.error(f"Error enriching data: {str(e)}")
            raise

    def save_to_silver(self, df: pd.DataFrame):
        """
        Save transformed data to silver layer in Parquet format.
        """
        try:
            output_path = self.silver_dir / "transformed_sales.parquet"
            
            # Convert to pyarrow table first
            table = pa.Table.from_pandas(df)
            
            # Write using pyarrow
            pq.write_table(table, output_path, compression='snappy')
            
            self.ingestion_logger.info(f"Successfully saved transformed data to {output_path}")
            
        except Exception as e:
            self.ingestion_logger.error(f"Error saving to silver layer: {str(e)}")
            raise

    def run_transformation(self):
        """Execute the complete transformation process."""
        try:
            self.ingestion_logger.info("Starting data transformation process")
            
            # Step 1: Read data from bronze layer
            sales_df, products_df = self.read_bronze_data()
            
            # Step 2: Clean sales data
            cleaned_sales_df = self.clean_sales_data(sales_df)
            
            # Step 3: Join and enrich data
            enriched_df = self.join_and_enrich_data(cleaned_sales_df, products_df)
            
            # Step 4: Save to silver layer
            self.save_to_silver(enriched_df)
            
            self.ingestion_logger.info("Data transformation process completed successfully")
            return enriched_df
            
        except Exception as e:
            self.ingestion_logger.error(f"Transformation process failed: {str(e)}")
            raise

def main():
    """Main function to run the transformation process."""
    transformer = DataTransformation()
    transformer.run_transformation()

if __name__ == "__main__":
    main()
