import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from data_connectors import (
    SingaporeDataConnector, 
    ASEANDataConnector, 
    ADBDataConnector
)
from ai_analysis import AIAnalysisEngine
from correlation_analysis import CorrelationAnalyzer
from visualization import VisualizationEngine
from export_utils import ExportManager
import utils

# Page configuration
st.set_page_config(
    page_title="Southeast Asia Environmental Data Platform",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'data_cache' not in st.session_state:
    st.session_state.data_cache = {}
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = {}

# Initialize connectors and engines
@st.cache_resource
def initialize_components():
    """Initialize data connectors and analysis engines"""
    sg_connector = SingaporeDataConnector()
    asean_connector = ASEANDataConnector()
    adb_connector = ADBDataConnector()
    ai_engine = AIAnalysisEngine()
    correlation_analyzer = CorrelationAnalyzer()
    viz_engine = VisualizationEngine()
    export_manager = ExportManager()
    
    return {
        'sg_connector': sg_connector,
        'asean_connector': asean_connector,
        'adb_connector': adb_connector,
        'ai_engine': ai_engine,
        'correlation_analyzer': correlation_analyzer,
        'viz_engine': viz_engine,
        'export_manager': export_manager
    }

components = initialize_components()

# Main app title and description
st.title("🌿 Southeast Asia Environmental Data Analysis Platform")
st.markdown("""
**AI-Powered Environmental Intelligence for Singapore & ASEAN Region**

Analyze environmental data, test hypotheses, and discover cross-country correlations using real-time data from Singapore government APIs, ASEAN statistics, and regional environmental indicators.
""")

# Sidebar for navigation and controls
st.sidebar.title("🔧 Analysis Controls")

# Navigation
page = st.sidebar.selectbox(
    "Select Analysis Mode",
    ["📊 Data Explorer", "🤖 AI Hypothesis Testing", "🔗 Correlation Analysis", "📈 Regional Dashboard", "💡 Custom Queries"]
)

# Common date range selector
st.sidebar.subheader("📅 Time Period")
date_range = st.sidebar.date_input(
    "Select date range",
    value=[datetime.now() - timedelta(days=30), datetime.now()],
    max_value=datetime.now()
)

# Country/Region selector
st.sidebar.subheader("🌏 Geographic Scope")
selected_countries = st.sidebar.multiselect(
    "Select countries/regions",
    ["Singapore", "Malaysia", "Thailand", "Indonesia", "Philippines", "Vietnam", "Myanmar", "Cambodia", "Laos", "Brunei"],
    default=["Singapore"]
)

# Data source selector
st.sidebar.subheader("📡 Data Sources")
data_sources = st.sidebar.multiselect(
    "Select data sources",
    ["Singapore Weather", "Singapore PSI", "ASEAN Statistics", "ADB Climate Data", "IEA Energy Data"],
    default=["Singapore Weather", "Singapore PSI"]
)

# Main content area based on selected page
if page == "📊 Data Explorer":
    st.header("📊 Environmental Data Explorer")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Real-time Data Overview")
        
        # Load and display Singapore data
        if "Singapore Weather" in data_sources:
            with st.spinner("Loading Singapore weather data..."):
                try:
                    weather_data = components['sg_connector'].get_weather_data()
                    if weather_data is not None and not weather_data.empty:
                        st.success(f"✅ Weather data loaded: {len(weather_data)} records")
                        
                        # Display latest readings
                        latest_weather = weather_data.iloc[-1] if len(weather_data) > 0 else None
                        if latest_weather is not None:
                            metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
                            with metrics_col1:
                                st.metric("Temperature", f"{latest_weather.get('temperature', 'N/A')}°C")
                            with metrics_col2:
                                st.metric("Humidity", f"{latest_weather.get('humidity', 'N/A')}%")
                            with metrics_col3:
                                st.metric("Rainfall", f"{latest_weather.get('rainfall', 'N/A')}mm")
                        
                        # Temperature trend chart
                        if 'timestamp' in weather_data.columns and 'temperature' in weather_data.columns:
                            fig = components['viz_engine'].create_time_series(
                                weather_data, 'timestamp', 'temperature', 
                                "Singapore Temperature Trend"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("⚠️ No weather data available or failed to load")
                except Exception as e:
                    st.error(f"❌ Error loading weather data: {str(e)}")
        
        if "Singapore PSI" in data_sources:
            with st.spinner("Loading Singapore PSI data..."):
                try:
                    psi_data = components['sg_connector'].get_psi_data()
                    if psi_data is not None and not psi_data.empty:
                        st.success(f"✅ PSI data loaded: {len(psi_data)} records")
                        
                        # PSI overview
                        if not psi_data.empty:
                            fig = components['viz_engine'].create_psi_chart(psi_data)
                            st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("⚠️ No PSI data available or failed to load")
                except Exception as e:
                    st.error(f"❌ Error loading PSI data: {str(e)}")
    
    with col2:
        st.subheader("Data Sources Status")
        
        # Check data source availability
        for source in data_sources:
            try:
                if source == "Singapore Weather":
                    status = components['sg_connector'].check_weather_api_status()
                elif source == "Singapore PSI":
                    status = components['sg_connector'].check_psi_api_status()
                elif source == "ASEAN Statistics":
                    status = components['asean_connector'].check_api_status()
                elif source == "ADB Climate Data":
                    status = components['adb_connector'].check_api_status()
                else:
                    status = True
                
                if status:
                    st.success(f"✅ {source}")
                else:
                    st.error(f"❌ {source}")
            except:
                st.warning(f"⚠️ {source} - Unknown status")

elif page == "🤖 AI Hypothesis Testing":
    st.header("🤖 AI-Powered Hypothesis Testing")
    
    st.markdown("""
    **Test your environmental hypotheses using AI analysis of real data**
    
    Enter your hypothesis or research question, and our AI will analyze available data to provide insights and statistical evidence.
    """)
    
    # Hypothesis input
    hypothesis = st.text_area(
        "Enter your hypothesis or research question:",
        placeholder="e.g., 'Air quality in Singapore correlates with weather patterns' or 'Economic growth in ASEAN countries affects environmental performance'",
        height=100
    )
    
    # Analysis scope
    col1, col2 = st.columns(2)
    with col1:
        analysis_type = st.selectbox(
            "Analysis Type",
            ["Correlation Analysis", "Trend Analysis", "Comparative Analysis", "Predictive Analysis"]
        )
    
    with col2:
        confidence_level = st.selectbox(
            "Confidence Level",
            ["95%", "90%", "99%"]
        )
    
    if st.button("🔍 Analyze Hypothesis", type="primary"):
        if hypothesis.strip():
            with st.spinner("🤖 AI is analyzing your hypothesis..."):
                try:
                    # Gather relevant data based on hypothesis
                    relevant_data = {}
                    
                    # Load Singapore data if relevant
                    if any(keyword in hypothesis.lower() for keyword in ['singapore', 'air quality', 'weather', 'temperature', 'psi']):
                        weather_data = components['sg_connector'].get_weather_data()
                        psi_data = components['sg_connector'].get_psi_data()
                        relevant_data['singapore_weather'] = weather_data
                        relevant_data['singapore_psi'] = psi_data
                    
                    # Load regional data if relevant
                    if any(keyword in hypothesis.lower() for keyword in ['asean', 'southeast asia', 'region', 'countries']):
                        asean_data = components['asean_connector'].get_environmental_indicators(selected_countries)
                        relevant_data['asean_data'] = asean_data
                    
                    # Perform AI analysis
                    analysis_result = components['ai_engine'].analyze_hypothesis(
                        hypothesis, relevant_data, analysis_type, confidence_level
                    )
                    
                    # Display results
                    st.subheader("🎯 Analysis Results")
                    
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.markdown("### AI Interpretation")
                        st.markdown(analysis_result.get('interpretation', 'No interpretation available'))
                        
                        if 'statistical_evidence' in analysis_result:
                            st.markdown("### Statistical Evidence")
                            st.json(analysis_result['statistical_evidence'])
                        
                        if 'recommendations' in analysis_result:
                            st.markdown("### Recommendations")
                            for rec in analysis_result['recommendations']:
                                st.markdown(f"• {rec}")
                    
                    with col2:
                        st.markdown("### Analysis Metadata")
                        st.metric("Confidence Score", f"{analysis_result.get('confidence_score', 0):.2f}")
                        st.metric("Data Points Analyzed", analysis_result.get('data_points', 0))
                        st.metric("Analysis Type", analysis_type)
                        
                        # Export option
                        if st.button("📤 Export Analysis"):
                            export_data = components['export_manager'].export_analysis_results(
                                hypothesis, analysis_result
                            )
                            st.download_button(
                                label="Download Analysis Report",
                                data=export_data,
                                file_name=f"hypothesis_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                mime="application/json"
                            )
                    
                    # Store results in session state
                    st.session_state.analysis_results[hypothesis] = analysis_result
                    
                except Exception as e:
                    st.error(f"❌ Analysis failed: {str(e)}")
                    st.info("Please check your hypothesis formulation and try again.")
        else:
            st.warning("⚠️ Please enter a hypothesis to analyze.")

elif page == "🔗 Correlation Analysis":
    st.header("🔗 Cross-Country Environmental Correlation Analysis")
    
    st.markdown("""
    **Discover environmental interdependencies between Southeast Asian countries**
    
    Analyze correlations between environmental indicators across the region to understand how countries influence each other.
    """)
    
    # Variable selection
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Primary Variables")
        primary_vars = st.multiselect(
            "Select primary environmental variables",
            ["Air Quality Index", "Temperature", "Precipitation", "Energy Consumption", "CO2 Emissions", "Forest Coverage"],
            default=["Air Quality Index"]
        )
    
    with col2:
        st.subheader("Secondary Variables")
        secondary_vars = st.multiselect(
            "Select secondary variables",
            ["GDP per Capita", "Population Density", "Industrial Output", "Tourism Index", "Trade Volume"],
            default=["GDP per Capita"]
        )
    
    # Correlation method
    correlation_method = st.selectbox(
        "Correlation Method",
        ["Pearson", "Spearman", "Kendall"]
    )
    
    if st.button("🔄 Calculate Correlations", type="primary"):
        with st.spinner("Calculating cross-country correlations..."):
            try:
                # Load data for selected countries
                correlation_data = {}
                
                for country in selected_countries:
                    # Get environmental data
                    env_data = components['asean_connector'].get_environmental_indicators([country])
                    econ_data = components['adb_connector'].get_economic_indicators([country])
                    
                    correlation_data[country] = {
                        'environmental': env_data,
                        'economic': econ_data
                    }
                
                # Perform correlation analysis
                correlation_results = components['correlation_analyzer'].calculate_cross_country_correlations(
                    correlation_data, primary_vars, secondary_vars, correlation_method
                )
                
                # Display correlation matrix
                st.subheader("📊 Correlation Matrix")
                
                if correlation_results and 'correlation_matrix' in correlation_results:
                    fig = components['viz_engine'].create_correlation_heatmap(
                        correlation_results['correlation_matrix']
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Display significant correlations
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("🔍 Strong Positive Correlations")
                    if 'strong_positive' in correlation_results:
                        for corr in correlation_results['strong_positive']:
                            st.markdown(f"• **{corr['variables']}**: {corr['coefficient']:.3f}")
                    else:
                        st.info("No strong positive correlations found")
                
                with col2:
                    st.subheader("🔍 Strong Negative Correlations")
                    if 'strong_negative' in correlation_results:
                        for corr in correlation_results['strong_negative']:
                            st.markdown(f"• **{corr['variables']}**: {corr['coefficient']:.3f}")
                    else:
                        st.info("No strong negative correlations found")
                
                # Statistical significance
                st.subheader("📈 Statistical Significance")
                if 'p_values' in correlation_results:
                    significance_df = pd.DataFrame(correlation_results['p_values'])
                    st.dataframe(significance_df, use_container_width=True)
                
            except Exception as e:
                st.error(f"❌ Correlation analysis failed: {str(e)}")

elif page == "📈 Regional Dashboard":
    st.header("📈 Southeast Asia Environmental Dashboard")
    
    # Dashboard layout
    tab1, tab2, tab3, tab4 = st.tabs(["🌡️ Climate Overview", "🏭 Air Quality", "⚡ Energy & Emissions", "🌳 Environmental Performance"])
    
    with tab1:
        st.subheader("Regional Climate Patterns")
        
        # Temperature trends across countries
        try:
            climate_data = {}
            for country in selected_countries:
                data = components['asean_connector'].get_climate_data(country, date_range)
                if data is not None:
                    climate_data[country] = data
            
            if climate_data:
                fig = components['viz_engine'].create_multi_country_climate_chart(climate_data)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No climate data available for selected countries and time period")
        except Exception as e:
            st.error(f"Error loading climate data: {str(e)}")
    
    with tab2:
        st.subheader("Air Quality Monitoring")
        
        # Singapore PSI data
        if "Singapore" in selected_countries:
            try:
                psi_data = components['sg_connector'].get_psi_data()
                if psi_data is not None and not psi_data.empty:
                    fig = components['viz_engine'].create_psi_regional_view(psi_data)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No PSI data available")
            except Exception as e:
                st.error(f"Error loading PSI data: {str(e)}")
        
        # Regional air quality comparison
        st.markdown("### Regional Air Quality Comparison")
        st.info("Regional air quality data integration in development")
    
    with tab3:
        st.subheader("Energy Consumption & CO2 Emissions")
        
        try:
            energy_data = components['adb_connector'].get_energy_data(selected_countries)
            if energy_data is not None:
                fig = components['viz_engine'].create_energy_emissions_chart(energy_data)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No energy data available for selected countries")
        except Exception as e:
            st.error(f"Error loading energy data: {str(e)}")
    
    with tab4:
        st.subheader("Environmental Performance Index")
        
        # Create EPI comparison chart
        try:
            epi_data = components['adb_connector'].get_environmental_performance(selected_countries)
            if epi_data is not None:
                fig = components['viz_engine'].create_epi_comparison(epi_data)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No EPI data available for selected countries")
        except Exception as e:
            st.error(f"Error loading EPI data: {str(e)}")

elif page == "💡 Custom Queries":
    st.header("💡 Custom Environmental Data Queries")
    
    st.markdown("""
    **Build custom queries to explore specific environmental questions**
    
    Use natural language to query environmental data across Singapore and Southeast Asia.
    """)
    
    # Query input
    query = st.text_area(
        "Enter your environmental data query:",
        placeholder="e.g., 'Show me the relationship between rainfall and air quality in Singapore for the past month' or 'Compare energy consumption trends across ASEAN countries'",
        height=120
    )
    
    # Query parameters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        query_type = st.selectbox(
            "Query Type",
            ["Data Extraction", "Trend Analysis", "Comparison", "Prediction"]
        )
    
    with col2:
        output_format = st.selectbox(
            "Output Format",
            ["Interactive Chart", "Data Table", "Statistical Summary", "AI Insights"]
        )
    
    with col3:
        time_granularity = st.selectbox(
            "Time Granularity",
            ["Hourly", "Daily", "Weekly", "Monthly"]
        )
    
    if st.button("🚀 Execute Query", type="primary"):
        if query.strip():
            with st.spinner("🔍 Processing your custom query..."):
                try:
                    # Use AI to parse and understand the query
                    parsed_query = components['ai_engine'].parse_natural_language_query(
                        query, selected_countries, date_range
                    )
                    
                    st.subheader("🎯 Query Interpretation")
                    st.json(parsed_query)
                    
                    # Execute the parsed query
                    if parsed_query.get('data_sources'):
                        query_results = {}
                        
                        for source in parsed_query['data_sources']:
                            if source == 'singapore_weather':
                                query_results['weather'] = components['sg_connector'].get_weather_data()
                            elif source == 'singapore_psi':
                                query_results['psi'] = components['sg_connector'].get_psi_data()
                            elif source == 'asean_stats':
                                query_results['asean'] = components['asean_connector'].get_environmental_indicators(selected_countries)
                        
                        # Generate AI-powered response
                        ai_response = components['ai_engine'].generate_query_response(
                            query, query_results, output_format
                        )
                        
                        st.subheader("📊 Query Results")
                        
                        if output_format == "Interactive Chart":
                            # Generate appropriate visualization
                            if ai_response.get('chart_data'):
                                fig = components['viz_engine'].create_custom_chart(
                                    ai_response['chart_data'], 
                                    ai_response.get('chart_type', 'line')
                                )
                                st.plotly_chart(fig, use_container_width=True)
                        
                        elif output_format == "Data Table":
                            if ai_response.get('table_data'):
                                st.dataframe(ai_response['table_data'], use_container_width=True)
                        
                        elif output_format == "Statistical Summary":
                            if ai_response.get('statistics'):
                                st.json(ai_response['statistics'])
                        
                        elif output_format == "AI Insights":
                            if ai_response.get('insights'):
                                st.markdown(ai_response['insights'])
                        
                        # Export option
                        if st.button("📤 Export Query Results"):
                            export_data = components['export_manager'].export_query_results(
                                query, ai_response, output_format
                            )
                            st.download_button(
                                label="Download Query Results",
                                data=export_data,
                                file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                mime="application/json"
                            )
                    
                except Exception as e:
                    st.error(f"❌ Query execution failed: {str(e)}")
                    st.info("Please refine your query and try again.")
        else:
            st.warning("⚠️ Please enter a query to execute.")

# Footer
st.markdown("---")
st.markdown("""
**Data Sources:** Singapore Data.gov.sg | ASEAN Statistics Portal | Asian Development Bank | IEA Southeast Asia Energy Data

**Last Updated:** {timestamp}

*This platform provides real-time environmental data analysis for research and policy-making purposes.*
""".format(timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
