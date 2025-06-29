from mysql_handler import MySQLHandler
import configparser
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def initialize_database():
    """Create database tables"""
    try:
        # Load configuration
        config = configparser.ConfigParser()
        config.read("config.ini")
        
        db_config = {
            'host': config['mysql']['host'],
            'user': config['mysql']['user'],
            'password': config['mysql']['password'],
            'database': config['mysql']['database']
        }
        
        # Create database handler
        db_handler = MySQLHandler(**db_config)
        
        # Create tables
        if db_handler.create_tables():
            logging.info("Database tables created successfully")
        else:
            logging.error("Failed to create tables")
        
        # Close connection
        db_handler.close()
        return True
    except Exception as e:
        logging.error(f"Initialization failed: {e}")
        return False

if __name__ == "__main__":
    initialize_database()