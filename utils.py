import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_date_range(start_date: Union[str, datetime], end_date: Union[str, datetime]) -> tuple:
    """
    Validate and normalize date range inputs
    
    Args:
        start_date: Start date (string or datetime)
        end_date: End date (string or datetime)
        
    Returns:
        Tuple of (start_date, end_date) as datetime objects
    """
    try:
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
        
        # Ensure start_date is before end_date
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        
        # Ensure dates are not in the future
        now = datetime.now()
        if start_date > now:
            start_date = now - timedelta(days=30)
        if end_date > now:
            end_date = now
        
        return start_date, end_date
        
    except Exception as e:
        logger.error(f"Date validation failed: {e}")
        # Return default date range (last 30 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        return start_date, end_date

def standardize_country_names(countries: List[str]) -> List[str]:
    """
    Standardize country names to match API requirements
    
    Args:
        countries: List of country names
        
    Returns:
        List of standardized country names
    """
    country_mapping = {
        'singapore': 'Singapore',
        'sg': 'Singapore',
        'malaysia': 'Malaysia',
        'my': 'Malaysia',
        'thailand': 'Thailand',
        'th': 'Thailand',
        'indonesia': 'Indonesia',
        'id': 'Indonesia',
        'philippines': 'Philippines',
        'ph': 'Philippines',
        'vietnam': 'Vietnam',
        'vn': 'Vietnam',
        'myanmar': 'Myanmar',
        'mm': 'Myanmar',
        'burma': 'Myanmar',
        'cambodia': 'Cambodia',
        'kh': 'Cambodia',
        'laos': 'Laos',
        'la': 'Laos',
        'brunei': 'Brunei',
        'bn': 'Brunei'
    }
    
    standardized = []
    for country in countries:
        normalized = country.lower().strip()
        if normalized in country_mapping:
            standardized.append(country_mapping[normalized])
        else:
            # Try partial matching
            for key, value in country_mapping.items():
                if key in normalized or normalized in key:
                    standardized.append(value)
                    break
            else:
                # Keep original if no match found
                standardized.append(country.title())
    
    return list(set(standardized))  # Remove duplicates

def clean_environmental_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize environmental data
    
    Args:
        data: Raw environmental data DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    try:
        if data.empty:
            return data
        
        # Make a copy to avoid modifying original
        cleaned_data = data.copy()
        
        # Standardize column names
        cleaned_data.columns = cleaned_data.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
        
        # Convert timestamp columns
        timestamp_cols = ['timestamp', 'datetime', 'date', 'time']
        for col in timestamp_cols:
            if col in cleaned_data.columns:
                try:
                    cleaned_data[col] = pd.to_datetime(cleaned_data[col])
                except:
                    logger.warning(f"Could not convert {col} to datetime")
        
        # Handle numeric columns
        numeric_cols = cleaned_data.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            # Remove outliers (values beyond 3 standard deviations)
            if cleaned_data[col].std() > 0:
                mean_val = cleaned_data[col].mean()
                std_val = cleaned_data[col].std()
                cleaned_data[col] = cleaned_data[col].clip(
                    lower=mean_val - 3*std_val,
                    upper=mean_val + 3*std_val
                )
        
        # Remove completely empty rows
        cleaned_data = cleaned_data.dropna(how='all')
        
        return cleaned_data
        
    except Exception as e:
        logger.error(f"Data cleaning failed: {e}")
        return data

def calculate_data_quality_score(data: pd.DataFrame) -> float:
    """
    Calculate a data quality score for a DataFrame
    
    Args:
        data: DataFrame to assess
        
    Returns:
        Quality score between 0 and 1
    """
    try:
        if data.empty:
            return 0.0
        
        scores = []
        
        # Completeness score (percentage of non-null values)
        completeness = 1 - (data.isnull().sum().sum() / (data.shape[0] * data.shape[1]))
        scores.append(completeness)
        
        # Consistency score (based on standard deviation of numeric columns)
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            consistency_scores = []
            for col in numeric_cols:
                if data[col].std() > 0:
                    cv = data[col].std() / abs(data[col].mean()) if data[col].mean() != 0 else 1
                    consistency_scores.append(min(1, 1/cv))  # Lower coefficient of variation = higher consistency
                else:
                    consistency_scores.append(1.0)
            scores.append(np.mean(consistency_scores))
        else:
            scores.append(0.5)  # Neutral score if no numeric data
        
        # Temporal coverage score (for time series data)
        timestamp_cols = ['timestamp', 'datetime', 'date', 'time']
        temporal_score = 0.5  # Default neutral score
        
        for col in timestamp_cols:
            if col in data.columns:
                try:
                    time_series = pd.to_datetime(data[col]).dropna()
                    if len(time_series) > 1:
                        time_range = time_series.max() - time_series.min()
                        expected_points = time_range.days if time_range.days > 0 else 1
                        actual_points = len(time_series)
                        temporal_score = min(1.0, actual_points / expected_points)
                    break
                except:
                    continue
        
        scores.append(temporal_score)
        
        # Overall quality score
        return float(np.mean(scores))
        
    except Exception as e:
        logger.error(f"Quality score calculation failed: {e}")
        return 0.5  # Return neutral score on error

def format_environmental_metrics(value: float, metric_type: str) -> str:
    """
    Format environmental metrics for display
    
    Args:
        value: Numeric value to format
        metric_type: Type of environmental metric
        
    Returns:
        Formatted string
    """
    try:
        if pd.isna(value):
            return "N/A"
        
        formatters = {
            'temperature': lambda x: f"{x:.1f}°C",
            'humidity': lambda x: f"{x:.1f}%",
            'rainfall': lambda x: f"{x:.1f}mm",
            'psi': lambda x: f"{x:.0f}",
            'co2_emissions': lambda x: f"{x:.2f} tons",
            'energy_consumption': lambda x: f"{x:,.0f} kWh",
            'percentage': lambda x: f"{x:.1f}%",
            'index': lambda x: f"{x:.1f}",
            'default': lambda x: f"{x:.2f}"
        }
        
        formatter = formatters.get(metric_type, formatters['default'])
        return formatter(value)
        
    except Exception as e:
        logger.error(f"Metric formatting failed: {e}")
        return str(value)

def extract_keywords_from_query(query: str) -> Dict[str, List[str]]:
    """
    Extract relevant keywords from natural language queries
    
    Args:
        query: Natural language query string
        
    Returns:
        Dictionary with categorized keywords
    """
    try:
        query_lower = query.lower()
        
        # Environmental keywords
        environmental_keywords = [
            'air quality', 'temperature', 'humidity', 'rainfall', 'precipitation',
            'psi', 'pollution', 'emissions', 'co2', 'carbon', 'energy',
            'renewable', 'climate', 'weather', 'environment', 'green'
        ]
        
        # Geographic keywords
        geographic_keywords = [
            'singapore', 'malaysia', 'thailand', 'indonesia', 'philippines',
            'vietnam', 'myanmar', 'cambodia', 'laos', 'brunei', 'asean',
            'southeast asia', 'region', 'regional', 'country', 'countries'
        ]
        
        # Temporal keywords
        temporal_keywords = [
            'daily', 'weekly', 'monthly', 'yearly', 'trend', 'over time',
            'recent', 'past', 'historical', 'current', 'latest', 'today',
            'yesterday', 'last week', 'last month', 'last year'
        ]
        
        # Analysis keywords
        analysis_keywords = [
            'correlation', 'relationship', 'compare', 'comparison', 'trend',
            'pattern', 'analysis', 'forecast', 'predict', 'model', 'hypothesis'
        ]
        
        extracted_keywords = {
            'environmental': [kw for kw in environmental_keywords if kw in query_lower],
            'geographic': [kw for kw in geographic_keywords if kw in query_lower],
            'temporal': [kw for kw in temporal_keywords if kw in query_lower],
            'analysis': [kw for kw in analysis_keywords if kw in query_lower]
        }
        
        return extracted_keywords
        
    except Exception as e:
        logger.error(f"Keyword extraction failed: {e}")
        return {'environmental': [], 'geographic': [], 'temporal': [], 'analysis': []}

def validate_api_response(response_data: Any, expected_structure: Dict[str, type]) -> bool:
    """
    Validate API response structure
    
    Args:
        response_data: API response data
        expected_structure: Expected structure with field types
        
    Returns:
        Boolean indicating if response is valid
    """
    try:
        if not isinstance(response_data, dict):
            return False
        
        for field, expected_type in expected_structure.items():
            if field not in response_data:
                logger.warning(f"Missing required field: {field}")
                return False
            
            if not isinstance(response_data[field], expected_type):
                logger.warning(f"Field {field} has incorrect type. Expected {expected_type}, got {type(response_data[field])}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"API response validation failed: {e}")
        return False

def cache_key_generator(*args, **kwargs) -> str:
    """
    Generate cache key from function arguments
    
    Args:
        *args: Positional arguments
        **kwargs: Keyword arguments
        
    Returns:
        Cache key string
    """
    try:
        # Convert arguments to string representation
        args_str = '_'.join(str(arg) for arg in args)
        kwargs_str = '_'.join(f"{k}_{v}" for k, v in sorted(kwargs.items()))
        
        # Combine and create hash-like key
        cache_key = f"{args_str}_{kwargs_str}"
        
        # Remove special characters and limit length
        cache_key = re.sub(r'[^\w\-_]', '', cache_key)[:100]
        
        return cache_key
        
    except Exception as e:
        logger.error(f"Cache key generation failed: {e}")
        return f"default_key_{datetime.now().timestamp()}"

def get_environmental_thresholds() -> Dict[str, Dict[str, float]]:
    """
    Get standard environmental thresholds for different metrics
    
    Returns:
        Dictionary with threshold values
    """
    return {
        'psi': {
            'good': 50,
            'moderate': 100,
            'unhealthy': 200,
            'very_unhealthy': 300,
            'hazardous': 400
        },
        'temperature': {
            'comfortable_min': 20,
            'comfortable_max': 28,
            'hot': 32,
            'very_hot': 35
        },
        'humidity': {
            'comfortable_min': 40,
            'comfortable_max': 70,
            'high': 80,
            'very_high': 90
        },
        'rainfall': {
            'light': 2.5,
            'moderate': 10,
            'heavy': 50,
            'very_heavy': 100
        }
    }

def get_asean_country_info() -> Dict[str, Dict[str, Any]]:
    """
    Get information about ASEAN countries
    
    Returns:
        Dictionary with country information
    """
    return {
        'Singapore': {
            'code': 'SG',
            'capital': 'Singapore',
            'population': 5900000,
            'area_km2': 721,
            'currency': 'SGD'
        },
        'Malaysia': {
            'code': 'MY',
            'capital': 'Kuala Lumpur',
            'population': 32700000,
            'area_km2': 330803,
            'currency': 'MYR'
        },
        'Thailand': {
            'code': 'TH',
            'capital': 'Bangkok',
            'population': 69800000,
            'area_km2': 513120,
            'currency': 'THB'
        },
        'Indonesia': {
            'code': 'ID',
            'capital': 'Jakarta',
            'population': 273500000,
            'area_km2': 1904569,
            'currency': 'IDR'
        },
        'Philippines': {
            'code': 'PH',
            'capital': 'Manila',
            'population': 109000000,
            'area_km2': 300000,
            'currency': 'PHP'
        },
        'Vietnam': {
            'code': 'VN',
            'capital': 'Hanoi',
            'population': 97300000,
            'area_km2': 331212,
            'currency': 'VND'
        },
        'Myanmar': {
            'code': 'MM',
            'capital': 'Naypyidaw',
            'population': 54400000,
            'area_km2': 676578,
            'currency': 'MMK'
        },
        'Cambodia': {
            'code': 'KH',
            'capital': 'Phnom Penh',
            'population': 16700000,
            'area_km2': 181035,
            'currency': 'KHR'
        },
        'Laos': {
            'code': 'LA',
            'capital': 'Vientiane',
            'population': 7300000,
            'area_km2': 236800,
            'currency': 'LAK'
        },
        'Brunei': {
            'code': 'BN',
            'capital': 'Bandar Seri Begawan',
            'population': 437000,
            'area_km2': 5765,
            'currency': 'BND'
        }
    }
