import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
from typing import Dict, List, Tuple

class DataQualityChecker:
    """Class to perform data quality checks on the pipeline data."""
    
    def __init__(self):
        """Initialize the data quality checker."""
        # Set up paths
        self.bronze_dir = Path("data/bronze")
        self.silver_dir = Path("data/silver")
        
        # Setup specific logger for DataQualityChecker
        self.quality_logger = logger.bind(name="data_quality")
        self.quality_logger.add(
            "logs/data_quality.log",
            rotation="1 day",
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
            filter=lambda record: record["extra"].get("name") == "data_quality"
        )
        self.check_results = []  # Initialize check results list

    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load data from bronze and silver layers for quality checks."""
        try:
            # Load bronze layer data
            sales_df = pd.read_json(self.bronze_dir / "sales_data.json")
            products_df = pd.read_json(self.bronze_dir / "product_data.json")
            
            # Load silver layer data
            transformed_df = pd.read_parquet(self.silver_dir / "transformed_sales.parquet")
            
            self.quality_logger.info("Successfully loaded data for quality checks")
            return sales_df, products_df, transformed_df
            
        except Exception as e:
            self.quality_logger.error(f"Error loading data: {str(e)}")
            raise

    def check_missing_sale_ids(self, df: pd.DataFrame) -> bool:
        """Check 1: Ensure no sale_id is missing."""
        try:
            missing_sales = df['sale_id'].isnull().sum()
            check_passed = missing_sales == 0
            
            result = {
                'check_name': 'Missing Sale IDs',
                'status': 'PASSED' if check_passed else 'FAILED',
                'details': f'Found {missing_sales} missing sale IDs'
            }
            self.check_results.append(result)
            
            self.quality_logger.info(f"Missing sale IDs check: {result['status']}")
            return check_passed
            
        except Exception as e:
            self.quality_logger.error(f"Error checking missing sale IDs: {str(e)}")
            raise

    def check_negative_values(self, df: pd.DataFrame) -> bool:
        """Check 2: Ensure no negative values in quantity or price."""
        try:
            neg_quantity = (df['quantity'] < 0).sum()
            neg_price = (df['price_sale'] < 0).sum()
            
            check_passed = neg_quantity == 0 and neg_price == 0
            
            result = {
                'check_name': 'Negative Values',
                'status': 'PASSED' if check_passed else 'FAILED',
                'details': f'Found {neg_quantity} negative quantities and {neg_price} negative prices'
            }
            self.check_results.append(result)
            
            self.quality_logger.info(f"Negative values check: {result['status']}")
            return check_passed
            
        except Exception as e:
            self.quality_logger.error(f"Error checking negative values: {str(e)}")
            raise

    def check_product_id_consistency(self, products_df: pd.DataFrame, transformed_df: pd.DataFrame) -> bool:
        """Check 3: Verify product_id consistency between products and transformed data."""
        try:
            product_ids = set(products_df['product_id'])
            transformed_ids = set(transformed_df['product_id'])
            
            # Check if all product_ids in transformed data exist in products data
            invalid_ids = transformed_ids - product_ids
            check_passed = len(invalid_ids) == 0
            
            result = {
                'check_name': 'Product ID Consistency',
                'status': 'PASSED' if check_passed else 'FAILED',
                'details': f'Found {len(invalid_ids)} invalid product IDs'
            }
            self.check_results.append(result)
            
            self.quality_logger.info(f"Product ID consistency check: {result['status']}")
            return check_passed
            
        except Exception as e:
            self.quality_logger.error(f"Error checking product ID consistency: {str(e)}")
            raise

    def validate_total_sales_calculation(self, df: pd.DataFrame) -> bool:
        """Check 4: Validate total_sales calculation."""
        try:
            # Calculate expected total_sales
            expected_total = df['quantity'] * df['price_sale']
            
            # Compare with actual total_sales
            differences = np.abs(df['total_sales'] - expected_total)
            incorrect_calculations = (differences > 0.01).sum()
            
            check_passed = incorrect_calculations == 0
            
            result = {
                'check_name': 'Total Sales Calculation',
                'status': 'PASSED' if check_passed else 'FAILED',
                'details': f'Found {incorrect_calculations} incorrect total_sales calculations'
            }
            self.check_results.append(result)
            
            self.quality_logger.info(f"Total sales calculation check: {result['status']}")
            return check_passed
            
        except Exception as e:
            self.quality_logger.error(f"Error validating total_sales calculation: {str(e)}")
            raise

    def run_all_checks(self) -> bool:
        """Run all data quality checks."""
        try:
            self.quality_logger.info("Starting data quality checks")
            
            # Load data
            sales_df, products_df, transformed_df = self.load_data()
            
            # Run all checks
            checks = [
                self.check_missing_sale_ids(transformed_df),
                self.check_negative_values(transformed_df),
                self.check_product_id_consistency(products_df, transformed_df),
                self.validate_total_sales_calculation(transformed_df)
            ]
            
            # Check if all tests passed
            all_passed = all(checks)
            
            # Generate summary
            self.generate_summary()
            
            self.quality_logger.info("Data quality checks completed")
            return all_passed
            
        except Exception as e:
            self.quality_logger.error(f"Error running quality checks: {str(e)}")
            raise

    def generate_summary(self):
        """Generate and log a summary of all quality checks."""
        try:
            self.quality_logger.info("\n=== Data Quality Check Summary ===")
            
            for result in self.check_results:
                self.quality_logger.info(
                    f"\nCheck: {result['check_name']}"
                    f"\nStatus: {result['status']}"
                    f"\nDetails: {result['details']}"
                )
            
            total_checks = len(self.check_results)
            passed_checks = sum(1 for r in self.check_results if r['status'] == 'PASSED')
            
            self.quality_logger.info(
                f"\nOverall Summary:"
                f"\nTotal Checks: {total_checks}"
                f"\nPassed: {passed_checks}"
                f"\nFailed: {total_checks - passed_checks}"
            )
            
        except Exception as e:
            self.quality_logger.error(f"Error generating summary: {str(e)}")
            raise

def main():
    """Main function to run data quality checks."""
    checker = DataQualityChecker()
    all_passed = checker.run_all_checks()
    
    if all_passed:
        logger.info("All data quality checks passed!")
    else:
        logger.warning("Some data quality checks failed. Check the summary for details.")

if __name__ == "__main__":
    main()
