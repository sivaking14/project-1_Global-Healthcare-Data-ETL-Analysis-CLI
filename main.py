from api_client import APIClient
from data_transformer import DataTransformer
from mysql_handler import MySQLHandler
from cli_manager import CLIManager
import configparser
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    # Load configuration
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Database configuration
    db_config = {
        'host': config['mysql']['host'],
        'user': config['mysql']['user'],
        'password': config['mysql']['password'],
        'database': config['mysql']['database']
    }

    # API configuration
    api_base_url = config['api']['base_url']
    api_key = config['api'].get('api_key', '')

    db_handler = None
    try:
        # Initialize components
        db_handler = MySQLHandler(**db_config)
        api_client = APIClient(api_base_url, api_key)
        transformer = DataTransformer()

        # Initialize CLI
        cli = CLIManager(api_client, db_handler, transformer)
        cli.run()

    except Exception as e:
        logging.error(f"Application error: {e}")
    finally:
        if db_handler:
            db_handler.close()

if __name__ == "__main__":
    main()