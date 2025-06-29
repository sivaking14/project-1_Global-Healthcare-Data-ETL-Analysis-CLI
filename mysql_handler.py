import mysql.connector
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class MySQLHandler:
    def __init__(self, host, user, password, database):
        try:
            self.conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            logging.info(f"Connected to MySQL database: {database}")
        except mysql.connector.Error as err:
            logging.error(f"Database connection error: {err}")
            raise

    def create_tables(self):
        """Create database tables from SQL script"""
        try:
            cursor = self.conn.cursor()
            with open("sql/create_tables.sql", "r") as f:
                sql_script = f.read()
                for result in cursor.execute(sql_script, multi=True):
                    if result.with_rows:
                        logging.debug(f"Executed: {result.statement}")
            self.conn.commit()
            logging.info("Database tables created successfully")
            return True
        except FileNotFoundError:
            logging.error("SQL script file not found: sql/create_tables.sql")
            return False
        except mysql.connector.Error as err:
            logging.error(f"Error creating tables: {err}")
            self.conn.rollback()
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()

    def insert_data(self, table, data):
        """Insert data into specified table with duplicate prevention"""
        if not data:
            logging.warning(f"No data provided for insertion into {table}")
            return 0
            
        try:
            # Validate required fields
            required_fields = {'report_date', 'country_name'}
            if not required_fields.issubset(data[0].keys()):
                missing = required_fields - set(data[0].keys())
                logging.error(f"Data missing required fields {missing} for table {table}")
                return 0
                
            cursor = self.conn.cursor()
            
            # Get column names from first data item
            columns = list(data[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            
            # Build parameterized queries
            select_sql = f"""
                SELECT 1 FROM {table} 
                WHERE report_date = %s AND country_name = %s
                LIMIT 1
            """
            insert_sql = f"""
                INSERT INTO {table} ({', '.join(columns)}) 
                VALUES ({placeholders})
            """
            
            # Prepare data for insertion
            new_data = []
            skipped = 0
            
            for row in data:
                # Convert dates to proper format
                if 'report_date' in row and isinstance(row['report_date'], datetime):
                    row['report_date'] = row['report_date'].date()
                
                # Check for duplicates
                cursor.execute(select_sql, (row['report_date'], row['country_name']))
                if cursor.fetchone():
                    skipped += 1
                else:
                    new_data.append(tuple(row.values()))
            
            # Insert new records
            if new_data:
                cursor.executemany(insert_sql, new_data)
                self.conn.commit()
                inserted_count = len(new_data)
                logging.info(
                    f"Inserted {inserted_count} records into {table}, "
                    f"skipped {skipped} duplicates"
                )
                return inserted_count
            else:
                logging.info(f"No new records to insert into {table}, skipped {skipped} duplicates")
                return 0
                
        except mysql.connector.Error as err:
            logging.error(f"Database error during insertion into {table}: {err}")
            self.conn.rollback()
            return 0
        except Exception as e:
            logging.error(f"Unexpected error during insertion into {table}: {e}")
            self.conn.rollback()
            return 0
        finally:
            if 'cursor' in locals():
                cursor.close()

    def query_data(self, sql, params=None):
        """Execute SQL query and return results"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, params or ())
            result = cursor.fetchall()
            return result
        except mysql.connector.Error as err:
            logging.error(f"Query error: {err}\nSQL: {sql}")
            return []
        finally:
            if 'cursor' in locals():
                cursor.close()

    def list_tables(self):
        """List all tables in the database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SHOW TABLES")
            return [table[0] for table in cursor.fetchall()]
        except mysql.connector.Error as err:
            logging.error(f"Error listing tables: {err}")
            return []
        finally:
            if 'cursor' in locals():
                cursor.close()

    def drop_tables(self):
        """Drop all created tables"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS daily_cases, vaccination_data")
            self.conn.commit()
            logging.warning("All tables dropped successfully")
            return True
        except mysql.connector.Error as err:
            logging.error(f"Error dropping tables: {err}")
            self.conn.rollback()
            return False
        finally:
            if 'cursor' in locals():
                cursor.close()

    def close(self):
        """Close database connection"""
        try:
            if self.conn and self.conn.is_connected():
                self.conn.close()
                logging.info("Database connection closed")
        except mysql.connector.Error as err:
            logging.error(f"Error closing connection: {err}")
        except AttributeError:
            pass  # Connection was never established