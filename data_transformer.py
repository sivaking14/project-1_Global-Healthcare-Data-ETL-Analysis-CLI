import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataTransformer:
    def __init__(self):
        # Map OWID columns to our database schema
        self.cases_mapping = {
            'date': 'report_date',
            'total_cases': 'total_cases',
            'new_cases': 'new_cases',
            'total_deaths': 'total_deaths',
            'new_deaths': 'new_deaths'
        }
        
        self.vaccines_mapping = {
            'date': 'report_date',
            'total_vaccinations': 'total_vaccinations',
            'people_vaccinated': 'people_vaccinated',
            'people_fully_vaccinated': 'people_fully_vaccinated'
        }

    def _transform_dataset(self, data, columns_mapping, country_name):
        """Generic transformation for any dataset"""
        if not data:
            logging.warning(f"No data found for {country_name}")
            return []
            
        try:
            df = pd.DataFrame(data)
            
            # Return empty if no data
            if df.empty:
                logging.warning(f"Empty dataset for {country_name}")
                return []
            
            # Add country name to every record
            df['country_name'] = country_name
            
            # Rename columns and filter
            df = df.rename(columns=columns_mapping)
            
            # Ensure all required columns exist
            required_columns = list(columns_mapping.values()) + ['country_name']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = 0  # Add missing columns with default value
            
            # Select only required columns
            df = df[required_columns]
            
            # Convert date and handle NaNs
            if 'report_date' in df.columns:
                df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')
                df = df.dropna(subset=['report_date'])
                df['report_date'] = df['report_date'].dt.date
            
            # Convert numerical columns
            numeric_cols = [col for col in required_columns 
                            if col not in ['report_date', 'country_name']]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            
            return df.to_dict('records')
            
        except Exception as e:
            logging.error(f"Transformation error: {e}")
            return []

    def transform_cases(self, country_data):
        """Transforms case data for a country"""
        country_name = country_data.get('location', 'Unknown')
        data_list = country_data.get('data', [])
        return self._transform_dataset(
            data_list, 
            self.cases_mapping,
            country_name
        )

    def transform_vaccinations(self, country_data):
        """Transforms vaccination data for a country"""
        country_name = country_data.get('location', 'Unknown')
        data_list = country_data.get('data', [])
        return self._transform_dataset(
            data_list, 
            self.vaccines_mapping,
            country_name
        )