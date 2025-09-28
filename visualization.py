import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import utils

class VisualizationEngine:
    """Interactive visualization engine using Plotly for environmental data"""
    
    def __init__(self):
        # Color schemes for different chart types
        self.color_schemes = {
            'environmental': ['#2E8B57', '#228B22', '#32CD32', '#90EE90', '#98FB98'],
            'air_quality': ['#FF6B6B', '#FFA500', '#FFD700', '#90EE90', '#32CD32'],
            'temperature': ['#1E90FF', '#4682B4', '#87CEEB', '#FFA500', '#FF6347'],
            'regional': ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57', '#FF9FF3', '#54A0FF']
        }
    
    def create_time_series(self, data: pd.DataFrame, time_col: str, 
                          value_col: str, title: str, **kwargs) -> go.Figure:
        """
        Create interactive time series visualization
        
        Args:
            data: DataFrame with time series data
            time_col: Name of time column
            value_col: Name of value column
            title: Chart title
            **kwargs: Additional parameters
            
        Returns:
            Plotly Figure object
        """
        try:
            if data.empty or time_col not in data.columns or value_col not in data.columns:
                return self._create_empty_chart(title, "No data available for time series")
            
            fig = go.Figure()
            
            # Add time series line
            fig.add_trace(go.Scatter(
                x=data[time_col],
                y=data[value_col],
                mode='lines+markers',
                name=value_col.replace('_', ' ').title(),
                line=dict(color=self.color_schemes['environmental'][0], width=2),
                marker=dict(size=4),
                hovertemplate=f'<b>Time:</b> %{{x}}<br><b>{value_col}:</b> %{{y}}<extra></extra>'
            ))
            
            # Update layout
            fig.update_layout(
                title=dict(text=title, font=dict(size=16)),
                xaxis_title=time_col.replace('_', ' ').title(),
                yaxis_title=value_col.replace('_', ' ').title(),
                hovermode='x unified',
                template='plotly_white',
                height=400
            )
            
            return fig
            
        except Exception as e:
            return self._create_error_chart(title, f"Error creating time series: {str(e)}")
    
    def create_psi_chart(self, psi_data: pd.DataFrame) -> go.Figure:
        """
        Create PSI (Pollutant Standards Index) visualization
        
        Args:
            psi_data: DataFrame with PSI data
            
        Returns:
            Plotly Figure object
        """
        try:
            if psi_data.empty:
                return self._create_empty_chart("Singapore PSI Data", "No PSI data available")
            
            fig = go.Figure()
            
            # Get unique regions
            regions = psi_data['region'].unique() if 'region' in psi_data.columns else ['overall']
            colors = self.color_schemes['air_quality']
            
            for i, region in enumerate(regions):
                if 'region' in psi_data.columns:
                    region_data = psi_data[psi_data['region'] == region]
                else:
                    region_data = psi_data
                    region = 'Overall'
                
                if not region_data.empty and 'psi_24h' in region_data.columns:
                    fig.add_trace(go.Scatter(
                        x=region_data['timestamp'] if 'timestamp' in region_data.columns else range(len(region_data)),
                        y=region_data['psi_24h'],
                        mode='lines+markers',
                        name=region.title(),
                        line=dict(color=colors[i % len(colors)], width=2),
                        marker=dict(size=6),
                        hovertemplate=f'<b>Region:</b> {region}<br><b>PSI:</b> %{{y}}<br><b>Time:</b> %{{x}}<extra></extra>'
                    ))
            
            # Add PSI threshold lines
            fig.add_hline(y=50, line_dash="dash", line_color="green", 
                         annotation_text="Good (0-50)")
            fig.add_hline(y=100, line_dash="dash", line_color="orange", 
                         annotation_text="Moderate (51-100)")
            fig.add_hline(y=200, line_dash="dash", line_color="red", 
                         annotation_text="Unhealthy (101-200)")
            
            fig.update_layout(
                title="Singapore Air Quality (PSI) - 24 Hour",
                xaxis_title="Time",
                yaxis_title="PSI Value",
                hovermode='x unified',
                template='plotly_white',
                height=500,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            return fig
            
        except Exception as e:
            return self._create_error_chart("Singapore PSI Data", f"Error creating PSI chart: {str(e)}")
    
    def create_correlation_heatmap(self, correlation_matrix: Dict[str, Dict[str, float]]) -> go.Figure:
        """
        Create correlation heatmap visualization
        
        Args:
            correlation_matrix: Correlation matrix as nested dictionary
            
        Returns:
            Plotly Figure object
        """
        try:
            if not correlation_matrix:
                return self._create_empty_chart("Correlation Heatmap", "No correlation data available")
            
            # Convert dictionary to DataFrame
            corr_df = pd.DataFrame(correlation_matrix)
            
            fig = go.Figure(data=go.Heatmap(
                z=corr_df.values,
                x=corr_df.columns,
                y=corr_df.index,
                colorscale='RdBu',
                zmid=0,
                colorbar=dict(title="Correlation Coefficient"),
                hoverongaps=False,
                hovertemplate='<b>X:</b> %{x}<br><b>Y:</b> %{y}<br><b>Correlation:</b> %{z:.3f}<extra></extra>'
            ))
            
            # Add correlation values as text
            annotations = []
            for i, row in enumerate(corr_df.index):
                for j, col in enumerate(corr_df.columns):
                    annotations.append(dict(
                        x=col, y=row,
                        text=f"{corr_df.loc[row, col]:.2f}",
                        showarrow=False,
                        font=dict(color="white" if abs(corr_df.loc[row, col]) > 0.5 else "black")
                    ))
            
            fig.update_layout(
                title="Environmental Variables Correlation Matrix",
                template='plotly_white',
                height=600,
                annotations=annotations
            )
            
            return fig
            
        except Exception as e:
            return self._create_error_chart("Correlation Heatmap", f"Error creating heatmap: {str(e)}")
    
    def create_multi_country_climate_chart(self, climate_data: Dict[str, pd.DataFrame]) -> go.Figure:
        """
        Create multi-country climate comparison chart
        
        Args:
            climate_data: Dictionary of climate data by country
            
        Returns:
            Plotly Figure object
        """
        try:
            if not climate_data:
                return self._create_empty_chart("Regional Climate Comparison", "No climate data available")
            
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Temperature Trends', 'Precipitation', 'Humidity', 'Climate Summary'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"type": "bar"}]]
            )
            
            colors = self.color_schemes['regional']
            
            for i, (country, data) in enumerate(climate_data.items()):
                color = colors[i % len(colors)]
                
                if isinstance(data, pd.DataFrame) and not data.empty:
                    # Temperature subplot (placeholder data)
                    temp_data = np.random.normal(28, 3, 30)  # Simulated temperature data
                    fig.add_trace(
                        go.Scatter(x=list(range(30)), y=temp_data, name=f"{country} Temp", 
                                 line=dict(color=color), showlegend=False),
                        row=1, col=1
                    )
                    
                    # Precipitation subplot (placeholder data)
                    precip_data = np.random.exponential(5, 30)  # Simulated precipitation
                    fig.add_trace(
                        go.Scatter(x=list(range(30)), y=precip_data, name=f"{country} Precip",
                                 line=dict(color=color), showlegend=False),
                        row=1, col=2
                    )
                    
                    # Humidity subplot (placeholder data)
                    humidity_data = np.random.normal(80, 10, 30)  # Simulated humidity
                    fig.add_trace(
                        go.Scatter(x=list(range(30)), y=humidity_data, name=f"{country} Humidity",
                                 line=dict(color=color), showlegend=False),
                        row=2, col=1
                    )
                    
                    # Summary bar chart
                    fig.add_trace(
                        go.Bar(x=[country], y=[np.mean(temp_data)], name=f"{country}",
                              marker_color=color, showlegend=True),
                        row=2, col=2
                    )
            
            fig.update_layout(
                title="Regional Climate Patterns Comparison",
                template='plotly_white',
                height=800,
                showlegend=True
            )
            
            return fig
            
        except Exception as e:
            return self._create_error_chart("Regional Climate Comparison", f"Error creating climate chart: {str(e)}")
    
    def create_energy_emissions_chart(self, energy_data: pd.DataFrame) -> go.Figure:
        """
        Create energy consumption and emissions visualization
        
        Args:
            energy_data: DataFrame with energy and emissions data
            
        Returns:
            Plotly Figure object
        """
        try:
            if energy_data.empty:
                return self._create_empty_chart("Energy & Emissions", "No energy data available")
            
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=('Energy Consumption per Capita', 'CO2 Emissions'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            countries = energy_data['country'].unique() if 'country' in energy_data.columns else ['Data Available']
            colors = self.color_schemes['environmental']
            
            # Simulate energy data for visualization
            energy_values = np.random.normal(5000, 1500, len(countries))  # kWh per capita
            co2_values = np.random.normal(8, 3, len(countries))  # tons per capita
            
            fig.add_trace(
                go.Bar(x=countries, y=energy_values, name="Energy Consumption",
                      marker_color=colors[0]),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Bar(x=countries, y=co2_values, name="CO2 Emissions",
                      marker_color=colors[1]),
                row=1, col=2
            )
            
            fig.update_layout(
                title="Regional Energy Consumption & CO2 Emissions",
                template='plotly_white',
                height=500,
                showlegend=False
            )
            
            fig.update_xaxes(title_text="Countries", row=1, col=1)
            fig.update_xaxes(title_text="Countries", row=1, col=2)
            fig.update_yaxes(title_text="kWh per capita", row=1, col=1)
            fig.update_yaxes(title_text="Tons CO2 per capita", row=1, col=2)
            
            return fig
            
        except Exception as e:
            return self._create_error_chart("Energy & Emissions", f"Error creating energy chart: {str(e)}")
    
    def create_epi_comparison(self, epi_data: pd.DataFrame) -> go.Figure:
        """
        Create Environmental Performance Index comparison
        
        Args:
            epi_data: DataFrame with EPI data
            
        Returns:
            Plotly Figure object
        """
        try:
            if epi_data.empty:
                return self._create_empty_chart("Environmental Performance Index", "No EPI data available")
            
            countries = epi_data['country'].unique() if 'country' in epi_data.columns else ['Data Available']
            
            # Simulate EPI scores for visualization
            epi_scores = np.random.normal(60, 15, len(countries))
            air_quality_scores = np.random.normal(55, 20, len(countries))
            water_quality_scores = np.random.normal(70, 15, len(countries))
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                name='Overall EPI Score',
                x=countries,
                y=epi_scores,
                marker_color=self.color_schemes['environmental'][0]
            ))
            
            fig.add_trace(go.Bar(
                name='Air Quality Score',
                x=countries,
                y=air_quality_scores,
                marker_color=self.color_schemes['environmental'][1]
            ))
            
            fig.add_trace(go.Bar(
                name='Water Quality Score',
                x=countries,
                y=water_quality_scores,
                marker_color=self.color_schemes['environmental'][2]
            ))
            
            fig.update_layout(
                title='Environmental Performance Index Comparison',
                xaxis_title='Countries',
                yaxis_title='EPI Score',
                barmode='group',
                template='plotly_white',
                height=500,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            return fig
            
        except Exception as e:
            return self._create_error_chart("Environmental Performance Index", f"Error creating EPI chart: {str(e)}")
    
    def create_psi_regional_view(self, psi_data: pd.DataFrame) -> go.Figure:
        """
        Create regional PSI overview chart
        
        Args:
            psi_data: DataFrame with PSI data
            
        Returns:
            Plotly Figure object
        """
        try:
            if psi_data.empty:
                return self._create_empty_chart("Regional PSI Overview", "No PSI data available")
            
            # Get latest PSI values by region
            if 'region' in psi_data.columns and 'psi_24h' in psi_data.columns:
                latest_psi = psi_data.groupby('region')['psi_24h'].last().reset_index()
                
                fig = go.Figure(data=go.Bar(
                    x=latest_psi['region'],
                    y=latest_psi['psi_24h'],
                    marker_color=[
                        '#32CD32' if val <= 50 else
                        '#FFD700' if val <= 100 else
                        '#FFA500' if val <= 200 else
                        '#FF6B6B'
                        for val in latest_psi['psi_24h']
                    ],
                    text=latest_psi['psi_24h'],
                    textposition='auto',
                    hovertemplate='<b>Region:</b> %{x}<br><b>PSI:</b> %{y}<extra></extra>'
                ))
                
                fig.update_layout(
                    title='Current PSI Levels by Region',
                    xaxis_title='Region',
                    yaxis_title='PSI Value',
                    template='plotly_white',
                    height=400
                )
                
                return fig
            else:
                return self._create_empty_chart("Regional PSI Overview", "Invalid PSI data structure")
            
        except Exception as e:
            return self._create_error_chart("Regional PSI Overview", f"Error creating PSI regional chart: {str(e)}")
    
    def create_custom_chart(self, chart_data: Any, chart_type: str = 'line') -> go.Figure:
        """
        Create custom chart based on provided data and type
        
        Args:
            chart_data: Data for the chart
            chart_type: Type of chart to create
            
        Returns:
            Plotly Figure object
        """
        try:
            if chart_data is None:
                return self._create_empty_chart("Custom Chart", "No data provided for custom chart")
            
            fig = go.Figure()
            
            if chart_type == 'line':
                if isinstance(chart_data, dict) and 'x' in chart_data and 'y' in chart_data:
                    fig.add_trace(go.Scatter(
                        x=chart_data['x'],
                        y=chart_data['y'],
                        mode='lines+markers',
                        name='Data Series',
                        line=dict(color=self.color_schemes['environmental'][0])
                    ))
            elif chart_type == 'bar':
                if isinstance(chart_data, dict) and 'x' in chart_data and 'y' in chart_data:
                    fig.add_trace(go.Bar(
                        x=chart_data['x'],
                        y=chart_data['y'],
                        name='Data Series',
                        marker_color=self.color_schemes['environmental'][0]
                    ))
            
            fig.update_layout(
                title='Custom Environmental Data Visualization',
                template='plotly_white',
                height=500
            )
            
            return fig
            
        except Exception as e:
            return self._create_error_chart("Custom Chart", f"Error creating custom chart: {str(e)}")
    
    def _create_empty_chart(self, title: str, message: str) -> go.Figure:
        """Create an empty chart with informational message"""
        fig = go.Figure()
        fig.add_annotation(
            x=0.5, y=0.5,
            text=message,
            showarrow=False,
            font=dict(size=16, color="gray"),
            xref="paper", yref="paper"
        )
        fig.update_layout(
            title=title,
            template='plotly_white',
            height=400,
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
        return fig
    
    def _create_error_chart(self, title: str, error_message: str) -> go.Figure:
        """Create an error chart with error message"""
        fig = go.Figure()
        fig.add_annotation(
            x=0.5, y=0.5,
            text=f"❌ {error_message}",
            showarrow=False,
            font=dict(size=14, color="red"),
            xref="paper", yref="paper"
        )
        fig.update_layout(
            title=title,
            template='plotly_white',
            height=400,
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
        return fig
