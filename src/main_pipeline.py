from pathlib import Path
import sys
from datetime import datetime
from loguru import logger
from typing import Optional

# Import pipeline components
from data_generation import DataGenerator
from ingestion import DataIngestion
from transform import DataTransformation
from warehouse_load import WarehouseLoader
from quality_checks import DataQualityChecker

class PipelineOrchestrator:
    """Class to orchestrate the data pipeline processing."""
    
    def __init__(self):
        """Initialize the pipeline orchestrator."""
        self.setup_directory_structure()
        self.setup_logging()
        
        # Initialize pipeline components
        self.generator = DataGenerator()
        self.ingestion = DataIngestion()
        self.transformation = DataTransformation()
        self.warehouse = WarehouseLoader()
        self.quality = DataQualityChecker()

    def setup_directory_structure(self):
        """Create necessary directories for the pipeline."""
        directories = [
            'data/raw',
            'data/bronze',
            'data/silver',
            'data/gold',
            'logs'
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)

    def setup_logging(self):
        """Configure logging for the pipeline."""
        # Remove default logger
        logger.remove()
        
        # Add custom logging configuration
        logger.add(
            "logs/pipeline_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
            level="INFO"
        )
        
        # Add console output
        logger.add(
            sys.stdout,
            format="{level} | {message}",
            level="INFO"
        )

    def run_generation_stage(self) -> bool:
        """
        Run the data generation stage.
        
        Returns:
            bool: True if successful
        """
        try:
            logger.info("Starting data generation stage")
            self.generator.generate_all_data()
            logger.success("Data generation completed successfully")
            return True
        except Exception as e:
            logger.error(f"Data generation failed: {str(e)}")
            return False

    def run_ingestion_stage(self) -> bool:
        """
        Run the data ingestion stage (raw to bronze).
        
        Returns:
            bool: True if successful
        """
        try:
            logger.info("Starting data ingestion stage")
            self.ingestion.run_ingestion()
            logger.success("Data ingestion completed successfully")
            return True
        except Exception as e:
            logger.error(f"Data ingestion failed: {str(e)}")
            return False

    def run_transformation_stage(self) -> bool:
        """
        Run the data transformation stage.
        
        Returns:
            bool: True if successful
        """
        try:
            logger.info("Starting data transformation stage")
            self.transformation.run_transformation()
            logger.success("Data transformation completed successfully")
            return True
        except Exception as e:
            logger.error(f"Data transformation failed: {str(e)}")
            return False

    def run_warehouse_stage(self) -> bool:
        """
        Run the warehouse loading stage.
        
        Returns:
            bool: True if successful
        """
        try:
            logger.info("Starting warehouse loading stage")
            self.warehouse.run_warehouse_load()
            logger.success("Warehouse loading completed successfully")
            return True
        except Exception as e:
            logger.error(f"Warehouse loading failed: {str(e)}")
            return False

    def run_quality_checks(self) -> bool:
        """
        Run data quality checks.
        
        Returns:
            bool: True if all checks pass
        """
        try:
            logger.info("Starting data quality checks")
            checks_passed = self.quality.run_all_checks()
            
            if checks_passed:
                logger.success("All data quality checks passed")
            else:
                logger.warning("Some data quality checks failed")
            
            return checks_passed
            
        except Exception as e:
            logger.error(f"Data quality checks failed: {str(e)}")
            return False

    def run_pipeline(self, stop_on_failure: bool = True) -> bool:
        """
        Run the pipeline processing stages.
        
        Args:
            stop_on_failure: If True, pipeline stops on any stage failure
            
        Returns:
            bool: True if pipeline completes successfully
        """
        start_time = datetime.now()
        logger.info(f"Starting pipeline run at {start_time}")
        
        # Stage results
        results = {
            'generation': False,
            'ingestion': False,
            'transformation': False,
            'warehouse': False,
            'quality': False
        }
        
        try:
            # Stage 1: Generation
            results['generation'] = self.run_generation_stage()
            if stop_on_failure and not results['generation']:
                raise Exception("Generation stage failed")
            
            # Stage 2: Ingestion (raw to bronze)
            results['ingestion'] = self.run_ingestion_stage()
            if stop_on_failure and not results['ingestion']:
                raise Exception("Ingestion stage failed")
            
            # Stage 3: Transformation
            results['transformation'] = self.run_transformation_stage()
            if stop_on_failure and not results['transformation']:
                raise Exception("Transformation stage failed")
            
            # Stage 4: Warehouse Loading
            results['warehouse'] = self.run_warehouse_stage()
            if stop_on_failure and not results['warehouse']:
                raise Exception("Warehouse loading stage failed")
            
            # Stage 5: Quality Checks
            results['quality'] = self.run_quality_checks()
            if stop_on_failure and not results['quality']:
                raise Exception("Quality checks failed")
            
            # Calculate execution time
            end_time = datetime.now()
            duration = end_time - start_time
            
            # Log final status
            all_stages_passed = all(results.values())
            if all_stages_passed:
                logger.success(f"Pipeline completed successfully in {duration}")
            else:
                failed_stages = [stage for stage, passed in results.items() if not passed]
                logger.warning(f"Pipeline completed with failures in stages: {failed_stages}")
            
            return all_stages_passed
            
        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time
            logger.error(f"Pipeline failed after {duration}: {str(e)}")
            return False

    def generate_pipeline_report(self):
        """Generate a summary report of the pipeline run."""
        try:
            report_path = Path("logs/pipeline_report.txt")
            
            with open(report_path, 'w') as f:
                f.write("=== Pipeline Execution Report ===\n\n")
                
                # Add quality check results
                f.write("Data Quality Check Results:\n")
                for result in self.quality.check_results:
                    f.write(f"\nCheck: {result['check_name']}\n")
                    f.write(f"Status: {result['status']}\n")
                    f.write(f"Details: {result['details']}\n")
                
            logger.info(f"Pipeline report generated at {report_path}")
            
        except Exception as e:
            logger.error(f"Error generating pipeline report: {str(e)}")

def main():
    """Main function to run the pipeline."""
    pipeline = PipelineOrchestrator()
    
    # Run the pipeline and generate report
    success = pipeline.run_pipeline(stop_on_failure=True)
    pipeline.generate_pipeline_report()

if __name__ == "__main__":
    main()