# src/data_generation.py
import json
import random
from datetime import datetime, timedelta
import uuid
from pathlib import Path
from loguru import logger

class DataGenerator:
    """Class for generating synthetic sales and product data."""
    
    def __init__(self):
        """Initialize the data generator."""
        self.raw_dir = Path("data/raw")
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
        
        logger.info("DataGenerator initialized")

    def generate_product_data(self, num_products: int = 100) -> list:
        """
        Generate simulated product data.
        
        Args:
            num_products: Number of products to generate
        
        Returns:
            list: List of product data
        """
        logger.info(f"Generating {num_products} products")
        products = []
        
        for i in range(num_products):
            product = {
                "product_id": f"P{str(uuid.uuid4())[:8]}",
                "product_name": f"Product_{i+1}",
                "category": random.choice(self.categories),
                "price": round(random.uniform(10.0, 1000.0), 2)
            }
            products.append(product)
        
        logger.info(f"Generated {len(products)} products")
        return products

    def generate_sales_data(self, products: list, num_sales: int = 1000) -> list:
        """
        Generate simulated sales data.
        
        Args:
            products: List of available products
            num_sales: Number of sales to generate
        
        Returns:
            list: List of sales data
        """
        logger.info(f"Generating {num_sales} sales records")
        sales = []
        start_date = datetime(2023, 1, 1)
        
        for i in range(num_sales):
            # Select random product
            product = random.choice(products)
            
            # Generate random date in 2023
            sale_date = start_date + timedelta(days=random.randint(0, 364))
            
            sale = {
                "sale_id": i + 1,
                "product_id": product["product_id"],
                "sale_date": sale_date.strftime("%Y-%m-%d"),
                "quantity": random.randint(1, 10),
                "price": product["price"]
            }
            
            # Introduce missing values (10% probability)
            if random.random() < 0.10:
                if random.random() < 0.5:
                    sale["sale_date"] = None
                else:
                    sale["product_id"] = None
            
            sales.append(sale)
        
        logger.info(f"Generated {len(sales)} sales records")
        return sales

    def save_json_data(self, data: list, filename: str) -> None:
        """
        Save data in JSON format.
        
        Args:
            data: Data to save
            filename: Name of the file to save
        """
        file_path = self.raw_dir / filename
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved data to {file_path}")

    def generate_all_data(self, num_products: int = 100, num_sales: int = 1000) -> tuple:
        """
        Generate all required test data.
        
        Args:
            num_products: Number of products to generate
            num_sales: Number of sales to generate
            
        Returns:
            tuple: (products_data, sales_data)
        """
        logger.info("Starting data generation process")
        
        try:
            # Generate product data
            products_data = self.generate_product_data(num_products)
            self.save_json_data(products_data, "product_data.json")
            
            # Generate sales data
            sales_data = self.generate_sales_data(products_data, num_sales)
            self.save_json_data(sales_data, "sales_data.json")
            
            logger.success("Data generation completed successfully")
            logger.info(f"Generated {len(products_data)} products and {len(sales_data)} sales records")
            
            return products_data, sales_data
            
        except Exception as e:
            logger.error(f"Error generating data: {str(e)}")
            raise

def main():
    """Main function to generate test data."""
    try:
        generator = DataGenerator()
        products, sales = generator.generate_all_data()
        logger.success(f"Successfully generated {len(products)} products and {len(sales)} sales records")
    except Exception as e:
        logger.error(f"Failed to generate data: {str(e)}")
        raise

if __name__ == "__main__":
    main()