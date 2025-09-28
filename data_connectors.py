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
    """Connector for ASEAN environmental data using World Bank API"""
    
    def __init__(self):
        self.base_url = "https://api.worldbank.org/v2"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SEA-Environmental-Platform/1.0'
        })
        
        # ASEAN country codes (World Bank ISO codes)
        self.country_codes = {
            'Singapore': 'SGP',
            'Malaysia': 'MYS', 
            'Thailand': 'THA',
            'Indonesia': 'IDN',
            'Philippines': 'PHL',
            'Vietnam': 'VNM',
            'Myanmar': 'MMR',
            'Cambodia': 'KHM',
            'Laos': 'LAO',
            'Brunei': 'BRN'
        }
        
        # Environmental indicators from World Bank
        self.environmental_indicators = {
            'co2_emissions': 'EN.ATM.CO2E.KT',
            'co2_per_capita': 'EN.ATM.CO2E.PC',
            'forest_area_pct': 'AG.LND.FRST.ZS',
            'forest_area_km2': 'AG.LND.FRST.K2',
            'renewable_energy': 'EG.ELC.RNEW.ZS',
            'energy_consumption': 'EG.USE.ELEC.KH.PC',
            'urban_population': 'SP.URB.TOTL.IN.ZS',
            'pm25_exposure': 'EN.ATM.PM25.MC.M3',
            'renewable_energy_consumption': 'EG.FEC.RNEW.ZS'
        }
    
    def get_environmental_indicators(self, countries: List[str]) -> Optional[pd.DataFrame]:
        """
        Fetch environmental indicators for specified countries from World Bank API
        
        Args:
            countries: List of country names
            
        Returns:
            DataFrame with environmental indicators or None if failed
        """
        try:
            environmental_data = []
            
            # Get country codes for selected countries
            country_codes_list = []
            for country in countries:
                if country in self.country_codes:
                    country_codes_list.append(self.country_codes[country])
            
            if not country_codes_list:
                return None
            
            # Join country codes for API query
            countries_str = ';'.join(country_codes_list)
            
            # Fetch indicators one by one to avoid API limits
            for indicator_name, indicator_code in self.environmental_indicators.items():
                url = f"{self.base_url}/country/{countries_str}/indicator/{indicator_code}"
                params = {
                    'format': 'json',
                    'date': '2020:2024',  # Get recent 5 years of data
                    'per_page': 500
                }
                
                try:
                    response = self.session.get(url, params=params, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        
                        # World Bank API returns [metadata, data] array
                        if len(data) > 1 and data[1]:
                            for item in data[1]:
                                if item.get('value') is not None:
                                    environmental_data.append({
                                        'country': item.get('country', {}).get('value', ''),
                                        'country_code': item.get('countryiso3code', ''),
                                        'indicator': indicator_name,
                                        'indicator_code': indicator_code,
                                        'year': item.get('date', ''),
                                        'value': item.get('value'),
                                        'unit': item.get('unit', ''),
                                        'decimal': item.get('decimal', 0)
                                    })
                except requests.RequestException as e:
                    print(f"Failed to fetch {indicator_name}: {e}")
                    continue
            
            if environmental_data:
                df = pd.DataFrame(environmental_data)
                # Filter out null values and convert year to int
                df = df.dropna(subset=['value'])
                df['year'] = pd.to_numeric(df['year'], errors='coerce')
                df = df.dropna(subset=['year'])
                df['year'] = df['year'].astype(int)
                return df
            
            return None
            
        except requests.RequestException as e:
            print(f"Error fetching World Bank environmental data: {e}")
            return None
        except Exception as e:
            print(f"Error processing environmental data: {e}")
            return None
    
    def get_climate_data(self, country: str, date_range: tuple) -> Optional[pd.DataFrame]:
        """
        Fetch climate data for specific country and date range from World Bank
        
        Args:
            country: Country name
            date_range: Tuple of (start_date, end_date)
            
        Returns:
            DataFrame with climate data or None if failed
        """
        try:
            if country not in self.country_codes:
                return None
            
            country_code = self.country_codes[country]
            
            # Climate-related indicators
            climate_indicators = {
                'co2_emissions': 'EN.ATM.CO2E.KT',
                'pm25_exposure': 'EN.ATM.PM25.MC.M3',
                'forest_area': 'AG.LND.FRST.ZS',
                'urban_population': 'SP.URB.TOTL.IN.ZS'
            }
            
            # Calculate year range from date_range
            start_year = date_range[0].year if hasattr(date_range[0], 'year') else 2020
            end_year = date_range[1].year if hasattr(date_range[1], 'year') else 2024
            date_str = f"{start_year}:{end_year}"
            
            climate_data = []
            
            for indicator_name, indicator_code in climate_indicators.items():
                url = f"{self.base_url}/country/{country_code}/indicator/{indicator_code}"
                params = {
                    'format': 'json',
                    'date': date_str,
                    'per_page': 100
                }
                
                response = self.session.get(url, params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    if len(data) > 1 and data[1]:
                        for item in data[1]:
                            if item.get('value') is not None:
                                climate_data.append({
                                    'country': country,
                                    'country_code': country_code,
                                    'indicator': indicator_name,
                                    'year': int(item.get('date', 0)),
                                    'value': item.get('value'),
                                    'timestamp': f"{item.get('date', '')}-01-01"
                                })
            
            if climate_data:
                return pd.DataFrame(climate_data)
            
            return None
            
        except Exception as e:
            print(f"Error fetching climate data: {e}")
            return None
    
    def check_api_status(self) -> bool:
        """Check if World Bank API is accessible"""
        try:
            response = self.session.get(f"{self.base_url}/country/SGP/indicator/EN.ATM.CO2E.KT?format=json&date=2023", timeout=10)
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
