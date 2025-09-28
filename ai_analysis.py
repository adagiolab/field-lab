import json
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from openai import OpenAI
import utils

class AIAnalysisEngine:
    """AI-powered analysis engine using OpenAI for environmental data interpretation"""
    
    def __init__(self):
        # the newest OpenAI model is "gpt-5" which was released August 7, 2025.
        # do not change this unless explicitly requested by the user
        self.openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-5"
    
    def analyze_hypothesis(self, hypothesis: str, data: Dict[str, Any], 
                          analysis_type: str, confidence_level: str) -> Dict[str, Any]:
        """
        Analyze a hypothesis using AI interpretation of environmental data
        
        Args:
            hypothesis: The hypothesis statement to test
            data: Dictionary containing relevant datasets
            analysis_type: Type of analysis to perform
            confidence_level: Statistical confidence level
            
        Returns:
            Dictionary containing analysis results
        """
        try:
            # Prepare data summary for AI analysis
            data_summary = self._summarize_data_for_ai(data)
            
            # Create analysis prompt
            prompt = f"""
            You are an environmental data scientist analyzing Southeast Asian environmental data.
            
            HYPOTHESIS TO TEST: {hypothesis}
            
            ANALYSIS TYPE: {analysis_type}
            CONFIDENCE LEVEL: {confidence_level}
            
            AVAILABLE DATA SUMMARY:
            {data_summary}
            
            Please provide a comprehensive analysis in JSON format with the following structure:
            {{
                "hypothesis_assessment": "clear assessment of the hypothesis",
                "interpretation": "detailed interpretation of findings",
                "statistical_evidence": {{
                    "correlation_coefficients": [],
                    "p_values": [],
                    "confidence_intervals": [],
                    "sample_size": 0
                }},
                "confidence_score": 0.0,
                "data_points": 0,
                "key_findings": [],
                "recommendations": [],
                "limitations": [],
                "further_research": []
            }}
            """
            
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert environmental data scientist specializing in Southeast Asian environmental patterns and statistical analysis. Provide thorough, evidence-based analysis."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                response_format={"type": "json_object"}
            )
            
            analysis_result = json.loads(response.choices[0].message.content)
            
            # Add metadata
            analysis_result['analysis_metadata'] = {
                'timestamp': pd.Timestamp.now().isoformat(),
                'hypothesis': hypothesis,
                'analysis_type': analysis_type,
                'confidence_level': confidence_level,
                'data_sources': list(data.keys())
            }
            
            return analysis_result
            
        except Exception as e:
            return {
                'error': f"AI analysis failed: {str(e)}",
                'hypothesis_assessment': 'Analysis could not be completed',
                'interpretation': 'Please check data availability and try again.',
                'confidence_score': 0.0,
                'data_points': 0
            }
    
    def parse_natural_language_query(self, query: str, countries: List[str], 
                                   date_range: tuple) -> Dict[str, Any]:
        """
        Parse natural language query into structured data requirements
        
        Args:
            query: Natural language query
            countries: Selected countries
            date_range: Date range tuple
            
        Returns:
            Parsed query structure
        """
        try:
            prompt = f"""
            Parse the following natural language query into structured data requirements:
            
            QUERY: {query}
            AVAILABLE COUNTRIES: {countries}
            DATE RANGE: {date_range[0]} to {date_range[1]}
            
            Available data sources:
            - singapore_weather: Temperature, humidity, rainfall from Singapore
            - singapore_psi: Air quality index for Singapore regions
            - asean_stats: Regional environmental and economic indicators
            - adb_data: Economic and energy data from Asian Development Bank
            
            Return JSON with this structure:
            {{
                "data_sources": ["list of required data sources"],
                "variables": ["list of specific variables needed"],
                "analysis_type": "type of analysis required",
                "time_granularity": "hourly/daily/weekly/monthly",
                "geographic_scope": ["list of countries/regions"],
                "output_type": "chart/table/summary/insights",
                "filters": {{"any specific filters needed"}},
                "statistical_methods": ["methods to apply"]
            }}
            """
            
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert in parsing environmental data queries. Extract precise data requirements from natural language."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                response_format={"type": "json_object"}
            )
            
            return json.loads(response.choices[0].message.content)
            
        except Exception as e:
            return {
                'error': f"Query parsing failed: {str(e)}",
                'data_sources': [],
                'variables': [],
                'analysis_type': 'unknown'
            }
    
    def generate_query_response(self, original_query: str, query_results: Dict[str, Any], 
                               output_format: str) -> Dict[str, Any]:
        """
        Generate AI response to user query based on data results
        
        Args:
            original_query: Original user query
            query_results: Results from data queries
            output_format: Desired output format
            
        Returns:
            Formatted response with insights
        """
        try:
            # Summarize query results
            results_summary = self._summarize_query_results(query_results)
            
            prompt = f"""
            Generate a comprehensive response to the user's environmental data query:
            
            ORIGINAL QUERY: {original_query}
            OUTPUT FORMAT: {output_format}
            
            DATA RESULTS SUMMARY:
            {results_summary}
            
            Please provide response in JSON format:
            {{
                "insights": "detailed insights and interpretation",
                "key_findings": [],
                "chart_data": {{"data for visualization if applicable"}},
                "chart_type": "appropriate chart type",
                "table_data": {{"structured table data if applicable"}},
                "statistics": {{"statistical summary if applicable"}},
                "recommendations": [],
                "data_quality_notes": []
            }}
            """
            
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an environmental data analyst providing insights on Southeast Asian environmental data. Be specific and actionable."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                response_format={"type": "json_object"}
            )
            
            return json.loads(response.choices[0].message.content)
            
        except Exception as e:
            return {
                'error': f"Response generation failed: {str(e)}",
                'insights': 'Unable to generate insights from the provided data.',
                'key_findings': [],
                'recommendations': []
            }
    
    def generate_environmental_insights(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Generate general environmental insights from available data
        
        Args:
            data: Dictionary of DataFrames with environmental data
            
        Returns:
            Environmental insights and trends
        """
        try:
            data_summary = self._summarize_data_for_ai(data)
            
            prompt = f"""
            Analyze the following Southeast Asian environmental data and provide insights:
            
            DATA SUMMARY:
            {data_summary}
            
            Provide comprehensive environmental insights in JSON format:
            {{
                "overall_environmental_status": "assessment of current status",
                "key_trends": [],
                "risk_areas": [],
                "positive_developments": [],
                "cross_country_patterns": [],
                "policy_implications": [],
                "urgent_actions_needed": [],
                "data_confidence": 0.0
            }}
            """
            
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a senior environmental policy analyst specializing in Southeast Asia. Provide actionable insights for policymakers."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                response_format={"type": "json_object"}
            )
            
            return json.loads(response.choices[0].message.content)
            
        except Exception as e:
            return {
                'error': f"Insights generation failed: {str(e)}",
                'overall_environmental_status': 'Unable to assess with current data',
                'data_confidence': 0.0
            }
    
    def _summarize_data_for_ai(self, data: Dict[str, Any]) -> str:
        """
        Create a text summary of data for AI analysis
        
        Args:
            data: Dictionary containing datasets
            
        Returns:
            Text summary of the data
        """
        summary_parts = []
        
        for source_name, dataset in data.items():
            if dataset is not None:
                if isinstance(dataset, pd.DataFrame):
                    if not dataset.empty:
                        summary_parts.append(f"""
                        {source_name.upper()}:
                        - Shape: {dataset.shape}
                        - Columns: {list(dataset.columns)}
                        - Date range: {dataset.get('timestamp', pd.Series()).min()} to {dataset.get('timestamp', pd.Series()).max()}
                        - Sample data: {dataset.head(2).to_dict() if len(dataset) > 0 else 'No data'}
                        """)
                    else:
                        summary_parts.append(f"{source_name.upper()}: Empty dataset")
                else:
                    summary_parts.append(f"{source_name.upper()}: {str(dataset)[:200]}...")
            else:
                summary_parts.append(f"{source_name.upper()}: No data available")
        
        return "\n".join(summary_parts)
    
    def _summarize_query_results(self, query_results: Dict[str, Any]) -> str:
        """
        Summarize query results for AI processing
        
        Args:
            query_results: Results from data queries
            
        Returns:
            Text summary of results
        """
        return self._summarize_data_for_ai(query_results)
