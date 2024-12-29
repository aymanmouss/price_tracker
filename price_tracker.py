import requests
import psycopg2
import json
import logging
import configparser
import sys
import time
from datetime import datetime
from decimal import Decimal
from typing import Dict, List

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('price_tracker.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PriceTracker:
    def __init__(self):
        self.config = self._load_config()
        self.session = requests.Session()
        self.session.headers.update({"Authorization": self.config['bluefin']['api_key']})
        self.db_conn = None
        self.connect_db()

    def _load_config(self) -> configparser.ConfigParser:
        """Load configuration from file."""
        config = configparser.ConfigParser()
        config.read('config.ini')
        return config

    def connect_db(self):
        """Establish database connection."""
        try:
            self.db_conn = psycopg2.connect(
                host=self.config['database']['host'],
                database=self.config['database']['database'],
                user=self.config['database']['user'],
                password=self.config['database']['password'],
                port=self.config['database']['port']
            )
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise

    def fetch_stock_data(self) -> Dict:
        """Fetch stock data from Bluefin API."""
        try:
            response = self.session.get(
                self.config['bluefin']['url'],
                params={"lang_id": 0, "price_drop": 0}
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch stock data: {str(e)}")
            raise

    def update_product(self, product: Dict):
        """Update product information and track price changes."""
        try:
            cur = self.db_conn.cursor()
            
            # Check if product exists and get current price
            cur.execute(
                "SELECT current_price FROM products WHERE sku = %s",
                (product['sku'],)
            )
            result = cur.fetchone()
            
            new_price = Decimal(str(product['price']))
            
            if result is None:
                # Insert new product
                cur.execute("""
                    INSERT INTO products 
                    (sku, model, color, current_price, stock_level, ean, category)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    product['sku'],
                    product['model'],
                    product['color'],
                    new_price,
                    product['in_stock'],
                    product.get('ean', ''),
                    product.get('cat_name', '')
                ))
                
                # Record initial price
                cur.execute("""
                    INSERT INTO price_history (sku, price, stock_level)
                    VALUES (%s, %s, %s)
                """, (product['sku'], new_price, product['in_stock']))
                
                logger.info(f"Added new product: {product['sku']}")
                
            else:
                current_price = result[0]
                
                # Update product information
                cur.execute("""
                    UPDATE products 
                    SET current_price = %s,
                        stock_level = %s,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE sku = %s
                """, (new_price, product['in_stock'], product['sku']))
                
                # Record price change if different
                if current_price != new_price:
                    cur.execute("""
                        INSERT INTO price_history (sku, price, stock_level)
                        VALUES (%s, %s, %s)
                    """, (product['sku'], new_price, product['in_stock']))
                    
                    logger.info(f"Price change for {product['sku']}: {current_price} -> {new_price}")
            
            self.db_conn.commit()
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"Error updating product {product['sku']}: {str(e)}")
            raise

    def track_prices(self):
        """Main price tracking logic."""
        try:
            # Fetch current stock data
            stock_data = self.fetch_stock_data()
            products = stock_data.get('stock', [])
            
            logger.info(f"Fetched {len(products)} products from Bluefin")
            
            # Update each product
            for product in products:
                try:
                    self.update_product(product)
                except Exception as e:
                    logger.error(f"Failed to update product {product.get('sku', 'unknown')}: {str(e)}")
                    continue
            
            logger.info("Price tracking completed successfully")
            
        except Exception as e:
            logger.error(f"Price tracking failed: {str(e)}")
            raise
        finally:
            if self.db_conn:
                self.db_conn.close()

def main():
    check_interval = 60  # Default to 60 minutes if not specified
    
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        check_interval = int(config['settings'].get('check_interval', 60))
    except Exception as e:
        logger.warning(f"Failed to read check_interval from config, using default: {str(e)}")
    
    while True:
        try:
            tracker = PriceTracker()
            tracker.track_prices()
            
            # Wait for next check
            logger.info(f"Waiting {check_interval} minutes until next check...")
            time.sleep(check_interval * 60)
            
        except Exception as e:
            logger.error(f"Tracking cycle failed: {str(e)}")
            time.sleep(300)  # Wait 5 minutes before retrying after error

if __name__ == "__main__":
    main()