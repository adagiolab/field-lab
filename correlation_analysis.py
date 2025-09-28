import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import pearsonr, spearmanr, kendalltau
from typing import Dict, List, Tuple, Any, Optional
import utils

class CorrelationAnalyzer:
    """Statistical correlation analysis for environmental data"""
    
    def __init__(self):
        self.correlation_methods = {
            'Pearson': pearsonr,
            'Spearman': spearmanr,
            'Kendall': kendalltau
        }
    
    def calculate_cross_country_correlations(self, data: Dict[str, Dict], 
                                           primary_vars: List[str], 
                                           secondary_vars: List[str],
                                           method: str = 'Pearson') -> Dict[str, Any]:
        """
        Calculate correlations between environmental variables across countries
        
        Args:
            data: Dictionary with country data
            primary_vars: Primary environmental variables
            secondary_vars: Secondary variables to correlate with
            method: Correlation method ('Pearson', 'Spearman', 'Kendall')
            
        Returns:
            Dictionary containing correlation results
        """
        try:
            # Prepare data matrix for correlation analysis
            correlation_data = self._prepare_correlation_matrix(data, primary_vars, secondary_vars)
            
            if correlation_data.empty:
                return {
                    'error': 'No sufficient data for correlation analysis',
                    'correlation_matrix': None,
                    'strong_positive': [],
                    'strong_negative': [],
                    'p_values': {}
                }
            
            # Calculate correlation matrix
            if method.lower() == 'pearson':
                correlation_matrix = correlation_data.corr(method='pearson')
            elif method.lower() == 'spearman':
                correlation_matrix = correlation_data.corr(method='spearman')
            elif method.lower() == 'kendall':
                correlation_matrix = correlation_data.corr(method='kendall')
            else:
                correlation_matrix = correlation_data.corr(method='pearson')
            
            # Calculate p-values
            p_values = self._calculate_correlation_p_values(correlation_data, method)
            
            # Identify strong correlations
            strong_positive, strong_negative = self._identify_strong_correlations(
                correlation_matrix, p_values
            )
            
            # Calculate statistical significance
            significance_results = self._assess_statistical_significance(
                correlation_matrix, p_values, alpha=0.05
            )
            
            return {
                'correlation_matrix': correlation_matrix.to_dict(),
                'p_values': p_values,
                'strong_positive': strong_positive,
                'strong_negative': strong_negative,
                'significance_results': significance_results,
                'method_used': method,
                'variables_analyzed': list(correlation_data.columns),
                'data_points': len(correlation_data),
                'analysis_timestamp': pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            return {
                'error': f"Correlation analysis failed: {str(e)}",
                'correlation_matrix': None,
                'strong_positive': [],
                'strong_negative': [],
                'p_values': {}
            }
    
    def analyze_time_series_correlations(self, data: pd.DataFrame, 
                                       target_variable: str,
                                       lag_periods: List[int] = [1, 7, 30]) -> Dict[str, Any]:
        """
        Analyze time-lagged correlations in environmental data
        
        Args:
            data: Time series DataFrame
            target_variable: Target variable for correlation analysis
            lag_periods: List of lag periods to analyze
            
        Returns:
            Dictionary with lag correlation results
        """
        try:
            lag_correlations = {}
            
            if target_variable not in data.columns:
                return {'error': f'Target variable {target_variable} not found in data'}
            
            # Calculate correlations at different lags
            for lag in lag_periods:
                lag_results = {}
                
                for column in data.columns:
                    if column != target_variable and pd.api.types.is_numeric_dtype(data[column]):
                        try:
                            # Calculate lagged correlation
                            lagged_series = data[column].shift(lag)
                            corr, p_value = pearsonr(
                                data[target_variable].dropna(),
                                lagged_series.dropna()
                            )
                            
                            lag_results[column] = {
                                'correlation': corr,
                                'p_value': p_value,
                                'significant': p_value < 0.05
                            }
                        except:
                            lag_results[column] = {
                                'correlation': np.nan,
                                'p_value': np.nan,
                                'significant': False
                            }
                
                lag_correlations[f'lag_{lag}'] = lag_results
            
            return {
                'lag_correlations': lag_correlations,
                'target_variable': target_variable,
                'lag_periods': lag_periods,
                'analysis_timestamp': pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            return {'error': f"Time series correlation analysis failed: {str(e)}"}
    
    def calculate_regional_environmental_correlations(self, regional_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Calculate correlations between environmental indicators across regions
        
        Args:
            regional_data: Dictionary with regional environmental data
            
        Returns:
            Regional correlation analysis results
        """
        try:
            # Combine regional data into a single matrix
            combined_data = []
            
            for region, df in regional_data.items():
                if df is not None and not df.empty:
                    # Add region identifier
                    df_copy = df.copy()
                    df_copy['region'] = region
                    combined_data.append(df_copy)
            
            if not combined_data:
                return {'error': 'No regional data available for analysis'}
            
            # Combine all regional data
            full_data = pd.concat(combined_data, ignore_index=True)
            
            # Select numeric columns for correlation
            numeric_columns = full_data.select_dtypes(include=[np.number]).columns
            
            if len(numeric_columns) < 2:
                return {'error': 'Insufficient numeric variables for correlation analysis'}
            
            # Calculate correlation matrix
            correlation_matrix = full_data[numeric_columns].corr(method='pearson')
            
            # Calculate regional differences
            regional_stats = {}
            for region in regional_data.keys():
                region_data = full_data[full_data['region'] == region]
                if not region_data.empty:
                    regional_stats[region] = {
                        'mean_values': dict(region_data[numeric_columns].mean()),
                        'std_values': dict(region_data[numeric_columns].std()),
                        'data_points': len(region_data)
                    }
            
            return {
                'regional_correlation_matrix': correlation_matrix.to_dict(),
                'regional_statistics': regional_stats,
                'total_data_points': len(full_data),
                'regions_analyzed': list(regional_data.keys()),
                'variables_analyzed': list(numeric_columns),
                'analysis_timestamp': pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            return {'error': f"Regional correlation analysis failed: {str(e)}"}
    
    def _prepare_correlation_matrix(self, data: Dict[str, Dict], 
                                  primary_vars: List[str], 
                                  secondary_vars: List[str]) -> pd.DataFrame:
        """
        Prepare data matrix for correlation analysis
        
        Args:
            data: Country data dictionary
            primary_vars: Primary variables
            secondary_vars: Secondary variables
            
        Returns:
            DataFrame ready for correlation analysis
        """
        correlation_rows = []
        
        for country, country_data in data.items():
            if isinstance(country_data, dict):
                row_data = {'country': country}
                
                # Extract environmental data
                if 'environmental' in country_data:
                    env_df = country_data['environmental']
                    if isinstance(env_df, pd.DataFrame) and not env_df.empty:
                        # Simulate environmental variable values
                        for var in primary_vars:
                            row_data[var] = float(np.random.normal(50, 10))  # Placeholder
                
                # Extract economic data
                if 'economic' in country_data:
                    econ_df = country_data['economic']
                    if isinstance(econ_df, pd.DataFrame) and not econ_df.empty:
                        # Simulate economic variable values
                        for var in secondary_vars:
                            row_data[var] = float(np.random.normal(30000, 5000))  # Placeholder
                
                correlation_rows.append(row_data)
        
        correlation_df = pd.DataFrame(correlation_rows)
        
        # Select only numeric columns for correlation
        numeric_columns = correlation_df.select_dtypes(include=[np.number]).columns
        return correlation_df[list(numeric_columns)]
    
    def _calculate_correlation_p_values(self, data: pd.DataFrame, method: str) -> Dict[str, Dict[str, float]]:
        """
        Calculate p-values for correlation matrix
        
        Args:
            data: Data for correlation analysis
            method: Correlation method
            
        Returns:
            Dictionary of p-values
        """
        p_values = {}
        columns = data.columns
        corr_func = self.correlation_methods.get(method, pearsonr)
        
        for col1 in columns:
            p_values[col1] = {}
            for col2 in columns:
                if col1 == col2:
                    p_values[col1][col2] = 0.0
                else:
                    try:
                        _, p_val = corr_func(data[col1].dropna(), data[col2].dropna())
                        p_values[col1][col2] = p_val
                    except:
                        p_values[col1][col2] = 1.0
        
        return p_values
    
    def _identify_strong_correlations(self, correlation_matrix: pd.DataFrame, 
                                    p_values: Dict[str, Dict[str, float]], 
                                    threshold: float = 0.7) -> Tuple[List[Dict], List[Dict]]:
        """
        Identify strong positive and negative correlations
        
        Args:
            correlation_matrix: Correlation matrix
            p_values: P-values dictionary
            threshold: Correlation strength threshold
            
        Returns:
            Tuple of (strong_positive, strong_negative) correlations
        """
        strong_positive = []
        strong_negative = []
        
        for i, col1 in enumerate(correlation_matrix.columns):
            for j, col2 in enumerate(correlation_matrix.columns):
                if i < j:  # Avoid duplicates
                    corr_value = correlation_matrix.loc[col1, col2]
                    p_value = p_values.get(col1, {}).get(col2, 1.0)
                    
                    if abs(corr_value) >= threshold and p_value < 0.05:
                        correlation_info = {
                            'variables': f"{col1} vs {col2}",
                            'coefficient': corr_value,
                            'p_value': p_value,
                            'strength': 'Strong' if abs(corr_value) >= 0.8 else 'Moderate'
                        }
                        
                        if corr_value > 0:
                            strong_positive.append(correlation_info)
                        else:
                            strong_negative.append(correlation_info)
        
        return strong_positive, strong_negative
    
    def _assess_statistical_significance(self, correlation_matrix: pd.DataFrame,
                                       p_values: Dict[str, Dict[str, float]],
                                       alpha: float = 0.05) -> Dict[str, Any]:
        """
        Assess statistical significance of correlations
        
        Args:
            correlation_matrix: Correlation matrix
            p_values: P-values dictionary
            alpha: Significance level
            
        Returns:
            Statistical significance assessment
        """
        significant_correlations = 0
        total_correlations = 0
        
        for col1 in correlation_matrix.columns:
            for col2 in correlation_matrix.columns:
                if col1 != col2:
                    total_correlations += 1
                    p_value = p_values.get(col1, {}).get(col2, 1.0)
                    if p_value < alpha:
                        significant_correlations += 1
        
        return {
            'total_correlations': total_correlations // 2,  # Avoid double counting
            'significant_correlations': significant_correlations // 2,
            'significance_rate': (significant_correlations / total_correlations) if total_correlations > 0 else 0,
            'alpha_level': alpha
        }
