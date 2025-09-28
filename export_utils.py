import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Optional
import io
import zipfile
import utils

class ExportManager:
    """Utility class for exporting analysis results and visualizations"""
    
    def __init__(self):
        self.export_formats = ['json', 'csv', 'excel', 'pdf']
        
    def export_analysis_results(self, hypothesis: str, analysis_result: Dict[str, Any]) -> str:
        """
        Export AI analysis results as JSON
        
        Args:
            hypothesis: Original hypothesis
            analysis_result: Analysis results from AI
            
        Returns:
            JSON string of the analysis results
        """
        try:
            export_data = {
                'export_metadata': {
                    'export_timestamp': datetime.now().isoformat(),
                    'export_type': 'hypothesis_analysis',
                    'version': '1.0'
                },
                'hypothesis': hypothesis,
                'analysis_results': analysis_result,
                'data_sources': analysis_result.get('analysis_metadata', {}).get('data_sources', []),
                'confidence_assessment': {
                    'confidence_score': analysis_result.get('confidence_score', 0.0),
                    'data_points': analysis_result.get('data_points', 0),
                    'analysis_method': analysis_result.get('analysis_metadata', {}).get('analysis_type', 'unknown')
                }
            }
            
            return json.dumps(export_data, indent=2, default=self._json_serializer)
            
        except Exception as e:
            return json.dumps({
                'error': f'Export failed: {str(e)}',
                'export_timestamp': datetime.now().isoformat()
            }, indent=2)
    
    def export_query_results(self, query: str, query_response: Dict[str, Any], 
                           output_format: str) -> str:
        """
        Export custom query results
        
        Args:
            query: Original query
            query_response: AI response to query
            output_format: Format of the output
            
        Returns:
            JSON string of the query results
        """
        try:
            export_data = {
                'export_metadata': {
                    'export_timestamp': datetime.now().isoformat(),
                    'export_type': 'custom_query_results',
                    'output_format': output_format,
                    'version': '1.0'
                },
                'original_query': query,
                'query_results': query_response,
                'insights_summary': query_response.get('insights', ''),
                'key_findings': query_response.get('key_findings', []),
                'recommendations': query_response.get('recommendations', [])
            }
            
            return json.dumps(export_data, indent=2, default=self._json_serializer)
            
        except Exception as e:
            return json.dumps({
                'error': f'Query export failed: {str(e)}',
                'export_timestamp': datetime.now().isoformat()
            }, indent=2)
    
    def export_correlation_analysis(self, correlation_results: Dict[str, Any]) -> str:
        """
        Export correlation analysis results
        
        Args:
            correlation_results: Results from correlation analysis
            
        Returns:
            JSON string of correlation results
        """
        try:
            export_data = {
                'export_metadata': {
                    'export_timestamp': datetime.now().isoformat(),
                    'export_type': 'correlation_analysis',
                    'version': '1.0'
                },
                'correlation_analysis': correlation_results,
                'summary': {
                    'method_used': correlation_results.get('method_used', 'unknown'),
                    'variables_analyzed': correlation_results.get('variables_analyzed', []),
                    'total_data_points': correlation_results.get('data_points', 0),
                    'strong_correlations_found': len(correlation_results.get('strong_positive', [])) + len(correlation_results.get('strong_negative', []))
                }
            }
            
            return json.dumps(export_data, indent=2, default=self._json_serializer)
            
        except Exception as e:
            return json.dumps({
                'error': f'Correlation export failed: {str(e)}',
                'export_timestamp': datetime.now().isoformat()
            }, indent=2)
    
    def export_data_to_csv(self, data: pd.DataFrame, filename_prefix: str = 'environmental_data') -> str:
        """
        Export DataFrame to CSV string
        
        Args:
            data: DataFrame to export
            filename_prefix: Prefix for the filename
            
        Returns:
            CSV string
        """
        try:
            if data.empty:
                return "No data available for export"
            
            # Convert DataFrame to CSV string
            csv_buffer = io.StringIO()
            data.to_csv(csv_buffer, index=False)
            csv_string = csv_buffer.getvalue()
            csv_buffer.close()
            
            return csv_string
            
        except Exception as e:
            return f"CSV export failed: {str(e)}"
    
    def export_environmental_report(self, data_summary: Dict[str, Any], 
                                  analysis_results: List[Dict[str, Any]],
                                  time_period: str) -> str:
        """
        Export comprehensive environmental report
        
        Args:
            data_summary: Summary of environmental data
            analysis_results: List of analysis results
            time_period: Time period of the report
            
        Returns:
            JSON string of comprehensive report
        """
        try:
            report_data = {
                'report_metadata': {
                    'report_title': 'Southeast Asia Environmental Analysis Report',
                    'generation_timestamp': datetime.now().isoformat(),
                    'time_period': time_period,
                    'report_version': '1.0',
                    'data_sources': ['Singapore Data.gov.sg', 'ASEAN Statistics', 'ADB Data Library']
                },
                'executive_summary': {
                    'total_analyses_conducted': len(analysis_results),
                    'data_coverage': data_summary.get('data_coverage', {}),
                    'key_environmental_indicators': data_summary.get('key_indicators', []),
                    'overall_data_quality': data_summary.get('data_quality_score', 0.0)
                },
                'data_summary': data_summary,
                'analysis_results': analysis_results,
                'recommendations': self._generate_report_recommendations(analysis_results),
                'appendices': {
                    'data_sources_details': self._get_data_sources_info(),
                    'methodology_notes': self._get_methodology_notes(),
                    'glossary': self._get_environmental_glossary()
                }
            }
            
            return json.dumps(report_data, indent=2, default=self._json_serializer)
            
        except Exception as e:
            return json.dumps({
                'error': f'Report generation failed: {str(e)}',
                'report_timestamp': datetime.now().isoformat()
            }, indent=2)
    
    def create_data_package(self, datasets: Dict[str, pd.DataFrame], 
                           analysis_results: Dict[str, Any]) -> bytes:
        """
        Create a zip package containing multiple datasets and analysis results
        
        Args:
            datasets: Dictionary of DataFrames to include
            analysis_results: Analysis results to include
            
        Returns:
            Bytes of the zip file
        """
        try:
            zip_buffer = io.BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Add datasets as CSV files
                for name, df in datasets.items():
                    if not df.empty:
                        csv_string = self.export_data_to_csv(df, name)
                        zip_file.writestr(f'{name}.csv', csv_string)
                
                # Add analysis results as JSON
                analysis_json = json.dumps(analysis_results, indent=2, default=self._json_serializer)
                zip_file.writestr('analysis_results.json', analysis_json)
                
                # Add metadata file
                metadata = {
                    'package_created': datetime.now().isoformat(),
                    'datasets_included': list(datasets.keys()),
                    'analysis_types': list(analysis_results.keys()),
                    'package_version': '1.0'
                }
                zip_file.writestr('metadata.json', json.dumps(metadata, indent=2))
            
            zip_buffer.seek(0)
            return zip_buffer.getvalue()
            
        except Exception as e:
            # Return error information as a simple text file in bytes
            error_content = f"Package creation failed: {str(e)}"
            return error_content.encode('utf-8')
    
    def _json_serializer(self, obj):
        """JSON serializer for numpy and pandas objects"""
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif pd.isna(obj):
            return None
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    def _generate_report_recommendations(self, analysis_results: List[Dict[str, Any]]) -> List[str]:
        """Generate recommendations based on analysis results"""
        recommendations = [
            "Continue monitoring air quality trends across all regions",
            "Implement cross-border environmental cooperation initiatives",
            "Invest in renewable energy infrastructure development",
            "Establish regional environmental data sharing protocols",
            "Develop early warning systems for environmental hazards"
        ]
        
        # Add specific recommendations based on analysis results
        for result in analysis_results:
            if result.get('recommendations'):
                recommendations.extend(result['recommendations'])
        
        return list(set(recommendations))  # Remove duplicates
    
    def _get_data_sources_info(self) -> Dict[str, Any]:
        """Get information about data sources"""
        return {
            'singapore_data_gov': {
                'name': 'Singapore Data.gov.sg',
                'url': 'https://data.gov.sg',
                'description': 'Singapore government open data portal',
                'data_types': ['weather', 'air_quality', 'environmental_monitoring']
            },
            'asean_statistics': {
                'name': 'ASEAN Statistics Portal',
                'url': 'https://www.aseanstats.org',
                'description': 'Regional statistics for ASEAN member countries',
                'data_types': ['economic_indicators', 'demographic_data', 'trade_statistics']
            },
            'adb_data_library': {
                'name': 'Asian Development Bank Data Library',
                'url': 'https://data.adb.org',
                'description': 'Economic and development data for Asia-Pacific',
                'data_types': ['economic_indicators', 'development_metrics', 'climate_data']
            }
        }
    
    def _get_methodology_notes(self) -> Dict[str, str]:
        """Get methodology notes for the analysis"""
        return {
            'correlation_analysis': 'Pearson, Spearman, and Kendall correlation methods used based on data distribution',
            'statistical_significance': 'p-values calculated at 95% confidence level unless otherwise specified',
            'ai_analysis': 'OpenAI GPT-5 model used for hypothesis testing and data interpretation',
            'data_quality': 'Data quality assessed based on completeness, consistency, and temporal coverage',
            'missing_data': 'Missing data handled through appropriate imputation or exclusion methods'
        }
    
    def _get_environmental_glossary(self) -> Dict[str, str]:
        """Get glossary of environmental terms"""
        return {
            'PSI': 'Pollutant Standards Index - measure of air quality based on pollutant concentrations',
            'EPI': 'Environmental Performance Index - composite measure of environmental health and ecosystem vitality',
            'CO2 Emissions': 'Carbon dioxide emissions measured in metric tons per capita',
            'Energy Intensity': 'Energy consumption per unit of GDP',
            'Renewable Energy Share': 'Percentage of total energy consumption from renewable sources',
            'Air Quality Index': 'Standardized measure of air pollution levels',
            'Environmental Performance': 'Composite measure of environmental outcomes and policy effectiveness'
        }
