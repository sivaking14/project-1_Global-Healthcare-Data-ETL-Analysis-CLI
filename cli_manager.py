import argparse
import logging
from tabulate import tabulate

class CLIManager:
    def __init__(self, api_client, db_handler, transformer):
        self.api_client = api_client
        self.db_handler = db_handler
        self.transformer = transformer
        self.parser = argparse.ArgumentParser(description='Healthcare Data ETL CLI')
        self._setup_commands()

    def _setup_commands(self):
        subparsers = self.parser.add_subparsers(
            dest='command',
            title='commands',
            description='valid commands'
        )

        # Fetch data command
        fetch_parser = subparsers.add_parser('fetch_data', help='Fetch and load data')
        fetch_parser.add_argument('--country', required=True, help='Country name')
        fetch_parser.add_argument('--start_date', help='Start date (YYYY-MM-DD)')
        fetch_parser.add_argument('--end_date', help='End date (YYYY-MM-DD)')
        fetch_parser.add_argument('--data_type', choices=['cases', 'vaccinations', 'all'], 
                                 default='all', help='Data type to fetch')

        # Query command parser
        query_parser = subparsers.add_parser('query_data', help='Query database')
        query_sub = query_parser.add_subparsers(
            dest='query_type',
            title='query types',
            description='valid query types'
        )
        
        # Total cases query
        total_cases = query_sub.add_parser('total_cases', help='Total cases for country')
        total_cases.add_argument('country', help='Country name')
        
        # Daily trends query
        trends = query_sub.add_parser('daily_trends', help='Daily trends for metric')
        trends.add_argument('country', help='Country name')
        trends.add_argument('metric', choices=['new_cases', 'new_deaths'], help='Metric to analyze')
        
        # Top countries query
        top = query_sub.add_parser('top_countries', help='Top N countries by metric')
        top.add_argument('n', type=int, help='Number of countries')
        top.add_argument('metric', choices=['total_cases', 'total_deaths'], help='Metric to rank')

        # DB management commands
        subparsers.add_parser('list_tables', help='List all tables')
        subparsers.add_parser('drop_tables', help='Drop all tables')

    def run(self):
        args = self.parser.parse_args()
        
        # Handle case where no command is provided
        if not args.command:
            self.parser.print_help()
            return
            
        if args.command == 'fetch_data':
            self._handle_fetch(args)
        elif args.command == 'query_data':
            # Handle case where no query type is provided
            if not args.query_type:
                print("Error: Missing query type for query_data command")
                self.parser.parse_args(['query_data', '-h'])
                return
            self._handle_query(args)
        elif args.command == 'list_tables':
            self._handle_list_tables()
        elif args.command == 'drop_tables':
            self._handle_drop_tables()

    def _handle_fetch(self, args):
        # Fetch entire dataset
        dataset = self.api_client.fetch_data()
        if not dataset:
            logging.error("Failed to fetch data from API")
            return
            
        country_data = next(
            (data for data in dataset.values() if data.get('location') == args.country), 
            None
        )
        
        if not country_data:
            logging.error(f"Data not found for country: {args.country}")
            return
        
        # Process cases data
        if args.data_type in ['cases', 'all']:
            cases = self.transformer.transform_cases(country_data)
            if cases:
                self.db_handler.insert_data('daily_cases', cases)
                logging.info(f"Inserted {len(cases)} case records")
            else:
                logging.warning("No case data to insert")
        
        # Process vaccination data
        if args.data_type in ['vaccinations', 'all']:
            vaccines = self.transformer.transform_vaccinations(country_data)
            if vaccines:
                self.db_handler.insert_data('vaccination_data', vaccines)
                logging.info(f"Inserted {len(vaccines)} vaccination records")
            else:
                logging.warning("No vaccination data to insert")

    def _handle_query(self, args):
        if args.query_type == 'total_cases':
            self._query_total_cases(args.country)
        elif args.query_type == 'daily_trends':
            self._query_daily_trends(args.country, args.metric)
        elif args.query_type == 'top_countries':
            self._query_top_countries(args.n, args.metric)

    def _query_total_cases(self, country):
        res = self.db_handler.query_data(
            "SELECT SUM(new_cases) FROM daily_cases WHERE country_name = %s",
            (country,)
        )
        if res and res[0][0] is not None:
            print(f"\nTotal cases in {country}: {res[0][0]:,}")
        else:
            print(f"\nNo case data found for {country}")

    def _query_daily_trends(self, country, metric):
        res = self.db_handler.query_data(
            f"SELECT report_date, {metric} FROM daily_cases WHERE country_name = %s",
            (country,)
        )
        if res:
            print(f"\nDaily {metric.replace('_', ' ')} in {country}:")
            print(tabulate(res, headers=['Date', metric.replace('_', ' ').title()]))
        else:
            print(f"\nNo trend data found for {country}")

    def _query_top_countries(self, n, metric):
        res = self.db_handler.query_data(
            f"""SELECT country_name, MAX({metric}) as total
                FROM daily_cases 
                GROUP BY country_name 
                ORDER BY total DESC 
                LIMIT %s""",
            (n,)
        )
        if res:
            print(f"\nTop {n} countries by {metric.replace('_', ' ')}:")
            print(tabulate(res, headers=['Country', 'Total']))
        else:
            print("\nNo country data found")

    def _handle_list_tables(self):
        tables = self.db_handler.list_tables()
        if tables:
            print("\nDatabase Tables:")
            print(tabulate([[table] for table in tables], headers=['Table Name']))
        else:
            print("\nNo tables found in database")

    def _handle_drop_tables(self):
        if self.db_handler.drop_tables():
            print("All tables dropped successfully")
        else:
            print("Failed to drop tables")