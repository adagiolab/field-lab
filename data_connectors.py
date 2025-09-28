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
        self.base_url = "https://api.data.gov.sg/v1/environment"
        self.psi_url = f"{self.base_url}/psi"
        self.temperature_url = f"{self.base_url}/air-temperature"
        self.humidity_url = f"{self.base_url}/relative-humidity"
        self.rainfall_url = f"{self.base_url}/rainfall"
        self.forecast_url = f"{self.base_url}/2-hour-weather-forecast"
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
            weather_records = []
            
            # Fetch temperature data
            temp_response = self.session.get(self.temperature_url, timeout=30)
            if temp_response.status_code == 200:
                temp_data = temp_response.json()
                if 'items' in temp_data and len(temp_data['items']) > 0:
                    for item in temp_data['items']:
                        timestamp = item.get('timestamp')
                        readings = item.get('readings', [])
                        for reading in readings:
                            station_id = reading.get('station_id')
                            value = reading.get('value')
                            weather_records.append({
                                'timestamp': timestamp,
                                'station_id': station_id,
                                'parameter': 'temperature',
                                'value': value
                            })
            
            # Fetch humidity data
            humidity_response = self.session.get(self.humidity_url, timeout=30)
            if humidity_response.status_code == 200:
                humidity_data = humidity_response.json()
                if 'items' in humidity_data and len(humidity_data['items']) > 0:
                    for item in humidity_data['items']:
                        timestamp = item.get('timestamp')
                        readings = item.get('readings', [])
                        for reading in readings:
                            station_id = reading.get('station_id')
                            value = reading.get('value')
                            weather_records.append({
                                'timestamp': timestamp,
                                'station_id': station_id,
                                'parameter': 'humidity',
                                'value': value
                            })
            
            # Fetch rainfall data
            rainfall_response = self.session.get(self.rainfall_url, timeout=30)
            if rainfall_response.status_code == 200:
                rainfall_data = rainfall_response.json()
                if 'items' in rainfall_data and len(rainfall_data['items']) > 0:
                    for item in rainfall_data['items']:
                        timestamp = item.get('timestamp')
                        readings = item.get('readings', [])
                        for reading in readings:
                            station_id = reading.get('station_id')
                            value = reading.get('value')
                            weather_records.append({
                                'timestamp': timestamp,
                                'station_id': station_id,
                                'parameter': 'rainfall',
                                'value': value
                            })
            
            if weather_records:
                weather_df = pd.DataFrame(weather_records)
                weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
                
                # Pivot to get temperature, humidity, rainfall as columns
                weather_pivot = weather_df.pivot_table(
                    index=['timestamp', 'station_id'], 
                    columns='parameter', 
                    values='value', 
                    aggfunc='mean'
                ).reset_index()
                
                # Flatten column names
                weather_pivot.columns.name = None
                
                return weather_pivot
            
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
            response = self.session.get(self.temperature_url, timeout=10)
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
