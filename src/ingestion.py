import json
import pandas as pd
from pathlib import Path
from loguru import logger
from datetime import datetime

class DataIngestion:
    def __init__(self):
        # Specific logging configuration for DataIngestion
        self.ingestion_logger = logger.bind(name="ingestion")  # Define a specific logger
        self.ingestion_logger.add(
            "logs/ingestion.log",
            rotation="1 day",
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
            filter=lambda record: record["extra"].get("name") == "ingestion",   # Filter only DataIngestion messages
        )
        
         # Create directories for raw and bronze
        self.raw_dir = Path("data/raw")
        self.bronze_dir = Path("data/bronze")
        self._create_directories()
        
        self.ingestion_logger.info("DataIngestion initialized")

    def _create_directories(self):
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.ingestion_logger.info(f"Directories created: {self.raw_dir}, {self.bronze_dir}")

    def process_file(self, filename: str):
        try:
            input_path = self.raw_dir / filename
            output_path = self.bronze_dir / filename
            self.ingestion_logger.info(f"Reading from: {input_path}")
            self.ingestion_logger.info(f"Writing to: {output_path}")

            df = pd.read_json(input_path)
            df['ingestion_timestamp'] = datetime.now()
            df.to_json(output_path, orient='records', date_format='iso', indent=2)
            self.ingestion_logger.info(f"Successfully wrote {filename} to bronze")
        except Exception as e:
            self.ingestion_logger.error(f"Error processing {filename}: {str(e)}")
            raise

    def run_ingestion(self):
        try:
            self.ingestion_logger.info("Starting ingestion process")

            # List of files to process
            files_to_process = ["sales_data.json", "product_data.json"]

            for filename in files_to_process:
                self.ingestion_logger.info(f"Processing {filename}")
                self.process_file(filename)

            self.ingestion_logger.info("Ingestion process completed successfully")

        except Exception as e:
            self.ingestion_logger.error(f"Ingestion process failed: {str(e)}")
            raise

def main():
    try:
        # Create instance and run ingestion
        ingestion = DataIngestion()
        ingestion.run_ingestion()
        logger.info("Main process completed successfully")
    except Exception as e:
        logger.error(f"Main process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
