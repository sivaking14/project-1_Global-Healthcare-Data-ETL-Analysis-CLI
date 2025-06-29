## Project Overview
This CLI application implements a complete ETL pipeline for global healthcare data:
  Extract: Fetches COVID-19 data from the Our World in Data API
  Transform: Cleans and processes data using Pandas
  Load: Stores structured data in MySQL database
  Analyze: Provides query commands for data analysis
## Setup
1. Install requirements: pip install -r requirements.txt
2. Configure MySQL in config.ini
3. Follow the commands in the attachment to fetch and the test the data for all the countries
[project 2 test results.docx](https://github.com/user-attachments/files/20970435/project.2.test.results.docx)

## Project Structure
healthcare_etl_cli/
├── .gitignore
├── README.md
├── requirements.txt
├── config.ini.sample
├── main.py
├── init_db.py
├── api_client.py
├── data_transformer.py
├── mysql_handler.py
├── cli_manager.py
├── sql/
│   └── create_tables.sql

## API Documentation
The application uses the Our World in Data COVID-19 API:
Data Format: JSON
Update Frequency: Daily
Data Fields:
location: Country name
date: Report date (YYYY-MM-DD)
total_cases: Cumulative cases
new_cases: New daily cases
total_deaths: Cumulative deaths
new_deaths: New daily deaths
total_vaccinations: Total vaccine doses
people_vaccinated: People with ≥1 dose
people_fully_vaccinated: Fully vaccinated people

## Known Limitations
Limited to COVID-19 data from a single source
No support for incremental updates (full reload each time)
Basic error handling for API rate limits
Limited data validation capabilities
No automated testing suite
