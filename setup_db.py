import psycopg2
import configparser
import logging
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('price_tracker_setup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def create_database():
    """Create the database if it doesn't exist."""
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    # Connect to default PostgreSQL database
    conn = psycopg2.connect(
        host=config['database']['host'],
        database='postgres',
        user=config['database']['user'],
        password=config['database']['password'],
        port=config['database']['port']
    )
    conn.autocommit = True
    
    try:
        cur = conn.cursor()
        
        # Check if database exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (config['database']['database'],))
        if not cur.fetchone():
            # Create database
            cur.execute(f"CREATE DATABASE {config['database']['database']}")
            logger.info(f"Database {config['database']['database']} created successfully")
        else:
            logger.info(f"Database {config['database']['database']} already exists")
            
    finally:
        conn.close()

def create_tables():
    """Create the necessary tables in the database."""
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    conn = psycopg2.connect(
        host=config['database']['host'],
        database=config['database']['database'],
        user=config['database']['user'],
        password=config['database']['password'],
        port=config['database']['port']
    )
    
    try:
        cur = conn.cursor()
        
        # Create products table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                sku VARCHAR(255) PRIMARY KEY,
                model VARCHAR(255),
                color VARCHAR(255),
                current_price DECIMAL(10, 2),
                last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                stock_level INTEGER,
                ean VARCHAR(255),
                category VARCHAR(255)
            )
        """)
        
        # Create price_history table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id SERIAL PRIMARY KEY,
                sku VARCHAR(255) REFERENCES products(sku),
                price DECIMAL(10, 2),
                stock_level INTEGER,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for better performance
        cur.execute("CREATE INDEX IF NOT EXISTS idx_price_history_sku ON price_history(sku)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_price_history_timestamp ON price_history(timestamp)")
        
        conn.commit()
        logger.info("Tables created successfully")
        
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        conn.rollback()
        raise
    finally:
        conn.close()

def main():
    try:
        logger.info("Starting database setup...")
        create_database()
        create_tables()
        logger.info("Database setup completed successfully")
    except Exception as e:
        logger.error(f"Database setup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()