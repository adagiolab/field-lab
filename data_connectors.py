import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import time
from typing import Dict, List, Optional, Any
import utils

class SingaporeDataConnector:
    """Connector for Singapore government APIs (data.gov.sg)"""
    
    def __init__(self):
        self.base_url = "https://api-production.data.gov.sg/v2/public/api"
        self.weather_collection_id = 1459
        self.psi_url = "https://api.data.gov.sg/v1/environment/psi"
        self.forecast_url = "https://api.data.gov.sg/v1/environment/2-hour-weather-forecast"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SEA-Environmental-Platform/1.0'
        })
    
    def get_weather_data(self, days_back: int = 7) -> Optional[pd.DataFrame]:
        """
        Fetch weather data from Singapore's data.gov.sg API
        
        Args:
            days_back: Number of days of historical data to fetch
            
        Returns:
            DataFrame with weather data or None if failed
        """
        try:
            # Get weather data from the realtime weather readings collection
            url = f"{self.base_url}/collections/{self.weather_collection_id}/metadata"
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            metadata = response.json()
            
            # Get the actual data endpoint
            if 'data' in metadata and len(metadata['data']) > 0:
                data_url = f"{self.base_url}/collections/{self.weather_collection_id}/datasets"
                
                data_response = self.session.get(data_url, timeout=30)
                data_response.raise_for_status()
                
                datasets = data_response.json()
                
                if 'data' in datasets and len(datasets['data']) > 0:
                    # Get the latest dataset
                    latest_dataset = datasets['data'][0]
                    dataset_id = latest_dataset['id']
                    
                    # Fetch actual weather readings
                    readings_url = f"{self.base_url}/datasets/{dataset_id}/poll-download"
                    
                    readings_response = self.session.get(readings_url, timeout=30)
                    readings_response.raise_for_status()
                    
                    # Parse CSV data
                    from io import StringIO
                    weather_df = pd.read_csv(StringIO(readings_response.text))
                    
                    # Clean and standardize data
                    if not weather_df.empty:
                        # Convert timestamp if present
                        if 'timestamp' in weather_df.columns:
                            weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
                        
                        # Standardize column names
                        weather_df.columns = weather_df.columns.str.lower().str.replace(' ', '_')
                        
                        return weather_df
            
            return None
            
        except requests.RequestException as e:
            print(f"Error fetching weather data: {e}")
            return None
        except Exception as e:
            print(f"Error processing weather data: {e}")
            return None
    
    def get_psi_data(self) -> Optional[pd.DataFrame]:
        """
        Fetch PSI (Pollutant Standards Index) data from Singapore API
        
        Returns:
            DataFrame with PSI data or None if failed
        """
        try:
            response = self.session.get(self.psi_url, timeout=30)
            response.raise_for_status()
            
            psi_data = response.json()
            
            if 'items' in psi_data and len(psi_data['items']) > 0:
                psi_records = []
                
                for item in psi_data['items']:
                    timestamp = item.get('timestamp')
                    readings = item.get('readings', {})
                    
                    if 'psi_twenty_four_hourly' in readings:
                        psi_24h = readings['psi_twenty_four_hourly']
                        
                        for region, value in psi_24h.items():
                            psi_records.append({
                                'timestamp': timestamp,
                                'region': region,
                                'psi_24h': value,
                                'update_timestamp': item.get('update_timestamp')
                            })
                
                if psi_records:
                    psi_df = pd.DataFrame(psi_records)
                    psi_df['timestamp'] = pd.to_datetime(psi_df['timestamp'])
                    psi_df['update_timestamp'] = pd.to_datetime(psi_df['update_timestamp'])
                    
                    return psi_df
            
            return None
            
        except requests.RequestException as e:
            print(f"Error fetching PSI data: {e}")
            return None
        except Exception as e:
            print(f"Error processing PSI data: {e}")
            return None
    
    def get_2hour_forecast(self) -> Optional[Dict]:
        """
        Fetch 2-hour weather forecast data
        
        Returns:
            Dictionary with forecast data or None if failed
        """
        try:
            response = self.session.get(self.forecast_url, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.RequestException as e:
            print(f"Error fetching forecast data: {e}")
            return None
    
    def check_weather_api_status(self) -> bool:
        """Check if weather API is accessible"""
        try:
            url = f"{self.base_url}/collections/{self.weather_collection_id}/metadata"
            response = self.session.get(url, timeout=10)
            return response.status_code == 200
        except:
            return False
    
    def check_psi_api_status(self) -> bool:
        """Check if PSI API is accessible"""
        try:
            response = self.session.get(self.psi_url, timeout=10)
            return response.status_code == 200
        except:
            return False


class ASEANDataConnector:
    """Connector for ASEAN Statistics Portal (ASEANstats.org)"""
    
    def __init__(self):
        self.base_url = "https://www.aseanstats.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SEA-Environmental-Platform/1.0'
        })
        
        # ASEAN country codes
        self.country_codes = {
            'Singapore': 'SG',
            'Malaysia': 'MY', 
            'Thailand': 'TH',
            'Indonesia': 'ID',
            'Philippines': 'PH',
            'Vietnam': 'VN',
            'Myanmar': 'MM',
            'Cambodia': 'KH',
            'Laos': 'LA',
            'Brunei': 'BN'
        }
    
    def get_environmental_indicators(self, countries: List[str]) -> Optional[pd.DataFrame]:
        """
        Fetch environmental indicators for specified countries
        
        Args:
            countries: List of country names
            
        Returns:
            DataFrame with environmental indicators or None if failed
        """
        try:
            # Since ASEAN API details are not fully public, we'll simulate the structure
            # that would be returned from their statistics portal
            
            environmental_data = []
            
            for country in countries:
                if country in self.country_codes:
                    # In a real implementation, this would call the actual ASEAN API
                    # For now, we return a structured response indicating data availability
                    country_data = {
                        'country': country,
                        'country_code': self.country_codes[country],
                        'timestamp': datetime.now().isoformat(),
                        'data_available': True,
                        'indicators': [
                            'forest_coverage', 
                            'renewable_energy_share',
                            'co2_emissions_per_capita',
                            'waste_generation',
                            'water_quality_index'
                        ]
                    }
                    environmental_data.append(country_data)
            
            if environmental_data:
                return pd.DataFrame(environmental_data)
            
            return None
            
        except Exception as e:
            print(f"Error fetching ASEAN environmental data: {e}")
            return None
    
    def get_climate_data(self, country: str, date_range: tuple) -> Optional[pd.DataFrame]:
        """
        Fetch climate data for specific country and date range
        
        Args:
            country: Country name
            date_range: Tuple of (start_date, end_date)
            
        Returns:
            DataFrame with climate data or None if failed
        """
        try:
            if country not in self.country_codes:
                return None
            
            # Placeholder for ASEAN climate data structure
            climate_data = {
                'country': country,
                'date_range': f"{date_range[0]} to {date_range[1]}",
                'climate_indicators': [
                    'average_temperature',
                    'precipitation',
                    'humidity',
                    'extreme_weather_events'
                ],
                'data_status': 'available'
            }
            
            return pd.DataFrame([climate_data])
            
        except Exception as e:
            print(f"Error fetching ASEAN climate data: {e}")
            return None
    
    def check_api_status(self) -> bool:
        """Check if ASEAN API is accessible"""
        try:
            response = self.session.get(self.base_url, timeout=10)
            return response.status_code == 200
        except:
            return False


class ADBDataConnector:
    """Connector for Asian Development Bank Data Library"""
    
    def __init__(self):
        self.base_url = "https://data.adb.org"
        self.api_url = "https://data.adb.org/api"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SEA-Environmental-Platform/1.0'
        })
    
    def get_economic_indicators(self, countries: List[str]) -> Optional[pd.DataFrame]:
        """
        Fetch economic indicators from ADB for specified countries
        
        Args:
            countries: List of country names
            
        Returns:
            DataFrame with economic indicators or None if failed
        """
        try:
            # ADB data structure placeholder
            economic_data = []
            
            for country in countries:
                country_data = {
                    'country': country,
                    'gdp_per_capita': None,  # Would be fetched from actual ADB API
                    'industrial_output': None,
                    'energy_intensity': None,
                    'trade_openness': None,
                    'population_density': None,
                    'data_timestamp': datetime.now().isoformat(),
                    'data_source': 'ADB Data Library'
                }
                economic_data.append(country_data)
            
            return pd.DataFrame(economic_data)
            
        except Exception as e:
            print(f"Error fetching ADB economic data: {e}")
            return None
    
    def get_energy_data(self, countries: List[str]) -> Optional[pd.DataFrame]:
        """
        Fetch energy consumption and emissions data
        
        Args:
            countries: List of country names
            
        Returns:
            DataFrame with energy data or None if failed
        """
        try:
            energy_data = []
            
            for country in countries:
                country_data = {
                    'country': country,
                    'energy_consumption_per_capita': None,
                    'renewable_energy_share': None,
                    'co2_emissions': None,
                    'energy_efficiency_index': None,
                    'data_year': datetime.now().year - 1,  # Most recent available year
                    'data_source': 'ADB Energy Statistics'
                }
                energy_data.append(country_data)
            
            return pd.DataFrame(energy_data)
            
        except Exception as e:
            print(f"Error fetching ADB energy data: {e}")
            return None
    
    def get_environmental_performance(self, countries: List[str]) -> Optional[pd.DataFrame]:
        """
        Fetch Environmental Performance Index data
        
        Args:
            countries: List of country names
            
        Returns:
            DataFrame with EPI data or None if failed
        """
        try:
            epi_data = []
            
            for country in countries:
                country_data = {
                    'country': country,
                    'epi_score': None,  # Would be fetched from actual EPI data
                    'air_quality_score': None,
                    'water_quality_score': None,
                    'biodiversity_score': None,
                    'climate_change_score': None,
                    'data_year': 2024,
                    'data_source': 'Environmental Performance Index'
                }
                epi_data.append(country_data)
            
            return pd.DataFrame(epi_data)
            
        except Exception as e:
            print(f"Error fetching EPI data: {e}")
            return None
    
    def check_api_status(self) -> bool:
        """Check if ADB API is accessible"""
        try:
            response = self.session.get(self.base_url, timeout=10)
            return response.status_code == 200
        except:
            return False
