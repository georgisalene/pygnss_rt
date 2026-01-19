"""
GNSS Quality Monitoring Dashboard

A Streamlit-based dashboard for monitoring PPP processing quality:
- Coordinate repeatability across days
- RMS residuals visualization
- Processing statistics
- Auto-refresh when new data arrives

Run with: streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import time

from db_models import QualityDatabase
from config import DB_PATH, DASHBOARD_CONFIG, MONITORED_STATIONS
from code_tro_parser import parse_code_tro_file, download_code_tro, get_code_ztd_for_station
from code_snx_parser import parse_code_snx_file, download_code_snx, get_code_coords_for_station, compute_enu_diff

# Colorful plot template
PLOT_TEMPLATE = {
    'layout': {
        'paper_bgcolor': '#1a1a2e',  # Dark blue background
        'plot_bgcolor': '#16213e',   # Slightly lighter plot area
        'font': {'color': '#eaeaea', 'family': 'Arial'},
        'title': {'font': {'size': 18, 'color': '#00d4ff'}},
        'xaxis': {
            'gridcolor': '#2a3f5f',
            'linecolor': '#4a6fa5',
            'tickfont': {'color': '#b8d4e3'}
        },
        'yaxis': {
            'gridcolor': '#2a3f5f',
            'linecolor': '#4a6fa5',
            'tickfont': {'color': '#b8d4e3'}
        },
        'colorway': ['#00d4ff', '#ff6b6b', '#4ecdc4', '#ffe66d', '#c44dff',
                     '#ff9f43', '#26de81', '#fd79a8', '#a29bfe', '#ffeaa7']
    }
}

def apply_colorful_style(fig):
    """Apply colorful dark theme to a Plotly figure"""
    fig.update_layout(
        paper_bgcolor='#1a1a2e',
        plot_bgcolor='#16213e',
        font=dict(color='#eaeaea', family='Arial'),
        title_font=dict(size=18, color='#00d4ff'),
        legend=dict(
            bgcolor='rgba(26, 26, 46, 0.8)',
            bordercolor='#4a6fa5',
            borderwidth=1,
            font=dict(color='#eaeaea')
        )
    )
    fig.update_xaxes(
        gridcolor='#2a3f5f',
        linecolor='#4a6fa5',
        tickfont=dict(color='#b8d4e3'),
        title_font=dict(color='#00d4ff')
    )
    fig.update_yaxes(
        gridcolor='#2a3f5f',
        linecolor='#4a6fa5',
        tickfont=dict(color='#b8d4e3'),
        title_font=dict(color='#00d4ff')
    )
    return fig

# Page configuration
st.set_page_config(
    page_title="GNSS Quality Monitor",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for scrollable tabs
st.markdown("""
<style>
/* Make tabs scrollable horizontally */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
    overflow-x: auto;
    overflow-y: hidden;
    flex-wrap: nowrap;
    scrollbar-width: thin;
    scrollbar-color: #00d4ff #1a1a2e;
    padding-bottom: 10px;
}

/* Webkit scrollbar styling */
.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {
    height: 8px;
}

.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-track {
    background: #1a1a2e;
    border-radius: 4px;
}

.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb {
    background: #00d4ff;
    border-radius: 4px;
}

.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb:hover {
    background: #00a3cc;
}

/* Make individual tabs not wrap */
.stTabs [data-baseweb="tab"] {
    white-space: nowrap;
    flex-shrink: 0;
}

/* Add scroll hint gradient on right side */
.stTabs {
    position: relative;
}

/* Hint text for scrolling */
.scroll-hint {
    font-size: 12px;
    color: #00d4ff;
    text-align: right;
    margin-bottom: 5px;
}
</style>
""", unsafe_allow_html=True)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        margin: 5px;
    }
    .good { color: #28a745; }
    .warn { color: #ffc107; }
    .bad { color: #dc3545; }
    .stMetric { background-color: #f8f9fa; border-radius: 5px; padding: 10px; }
</style>
""", unsafe_allow_html=True)


def get_database():
    """Get database connection in read-only mode for concurrent access"""
    db = QualityDatabase(DB_PATH)
    db.connect(read_only=True)
    return db


def get_rms_color(rms_value: float) -> str:
    """Get color based on RMS threshold"""
    if rms_value <= DASHBOARD_CONFIG['rms_threshold_good']:
        return 'green'
    elif rms_value <= DASHBOARD_CONFIG['rms_threshold_warn']:
        return 'orange'
    return 'red'


def format_rms(rms_mm: float) -> str:
    """Format RMS value in mm"""
    return f"{rms_mm * 1000:.2f} mm"


def main():
    # Header
    st.title("🛰️ GNSS Quality Monitoring Dashboard")
    st.markdown("Real-time monitoring of PPP processing quality metrics")

    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Settings")

        # Auto-refresh toggle
        auto_refresh = st.toggle("Auto-refresh", value=True)
        refresh_interval = st.slider(
            "Refresh interval (seconds)",
            min_value=30, max_value=300, value=60
        )

        st.divider()

        # Page Navigation
        st.subheader("🧭 Navigation")
        page_options = {
            "📊 Overview": 0,
            "📈 Coordinate Repeatability": 1,
            "📉 RMS Analysis": 2,
            "🌡️ ZTD Monitor": 3,
            "🎯 Ambiguity Resolution": 4,
            "🛰️ Satellite Tracking": 5,
            "⚙️ Processing Stats": 6,
            "🔗 Sat. Ambiguity PRN": 7,
            "📡 Obs. Residuals": 8,
            "📶 Data Availability": 9,
            "🚫 Outlier Statistics": 10,
            "🌬️ Trop. Gradients": 11,
            "⏱️ Receiver Clocks": 12,
            "📊 Station Completeness": 13,
        }
        selected_page = st.selectbox(
            "Jump to page",
            options=list(page_options.keys()),
            index=0,
            key='page_nav'
        )

        st.divider()

        # Date range selection
        st.subheader("📅 Date Range")
        current_year = datetime.now().year
        year = st.selectbox("Year", range(current_year, 2020, -1), index=0)

        col1, col2 = st.columns(2)
        with col1:
            start_doy = st.number_input("Start DOY", min_value=1, max_value=366, value=290)
        with col2:
            end_doy = st.number_input("End DOY", min_value=1, max_value=366, value=300)

        st.divider()

        # Station selection
        st.subheader("📍 Stations")
        db = get_database()
        try:
            available_stations = db.get_all_stations()
            if not available_stations:
                available_stations = MONITORED_STATIONS
        except:
            available_stations = MONITORED_STATIONS

        selected_stations = st.multiselect(
            "Select stations",
            options=available_stations,
            default=available_stations[:10] if len(available_stations) > 10 else available_stations
        )

        st.divider()

        # Manual refresh button
        if st.button("🔄 Refresh Now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        # Last update time
        st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

    # Main content
    try:
        db = get_database()

        # Scroll hint for tabs
        st.markdown('<p style="text-align: right; font-size: 12px; color: #00d4ff; margin-bottom: -10px;">Scroll tabs right for more options (Trop. Gradients, Receiver Clocks, Station Completeness) →</p>', unsafe_allow_html=True)

        # Create tabs
        tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11, tab12, tab13, tab14 = st.tabs([
            "📊 Overview",
            "📈 Coordinate Repeatability",
            "📉 RMS Analysis",
            "🌡️ ZTD Monitor",
            "🎯 Ambiguity Resolution",
            "🛰️ Satellite Tracking",
            "⚙️ Processing Stats",
            "🔗 Sat. Ambiguity PRN",
            "📡 Obs. Residuals",
            "📶 Data Availability",
            "🚫 Outlier Statistics",
            "🌬️ Trop. Gradients",
            "⏱️ Receiver Clocks",
            "📊 Station Completeness"
        ])

        # TAB 1: Overview
        with tab1:
            st.header("Processing Overview")

            # Get latest processing results
            latest = db.get_latest_processing(limit=100)

            if latest:
                df_latest = pd.DataFrame(latest)

                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    total_stations = df_latest['station_id'].nunique()
                    st.metric("Stations Processed", total_stations)

                with col2:
                    if 'rms_unit_weight' in df_latest.columns:
                        avg_rms = df_latest['rms_unit_weight'].mean() * 1000
                        st.metric("Avg RMS", f"{avg_rms:.2f} mm")

                with col3:
                    if 'num_observations' in df_latest.columns:
                        total_obs = df_latest['num_observations'].sum()
                        st.metric("Total Observations", f"{total_obs:,}")

                with col4:
                    latest_date = df_latest['created_at'].max() if 'created_at' in df_latest.columns else None
                    if latest_date:
                        st.metric("Latest Update", latest_date.strftime("%Y-%m-%d %H:%M"))

                st.divider()

                # Station status table
                st.subheader("Station Status (Latest Day)")

                # Create status dataframe
                status_data = []
                for _, row in df_latest.iterrows():
                    rms = row.get('rms_unit_weight', 0) or 0
                    status = "✅ Good" if rms <= 0.002 else ("⚠️ Warning" if rms <= 0.005 else "❌ Poor")
                    status_data.append({
                        'Station': row['station_id'],
                        'Year': row['year'],
                        'DOY': row['doy'],
                        'RMS (mm)': f"{rms * 1000:.2f}" if rms else "N/A",
                        'Observations': row.get('num_observations', 'N/A'),
                        'Status': status
                    })

                df_status = pd.DataFrame(status_data)
                st.dataframe(df_status, use_container_width=True, hide_index=True)

            else:
                st.info("No data available. Run the parser to populate the database.")

        # TAB 2: Coordinate Repeatability
        with tab2:
            st.header("Coordinate Repeatability Analysis")

            if available_stations:
                # Get solutions for ALL available stations
                all_solutions = []
                for station in available_stations:
                    solutions = db.get_solutions(
                        station_id=station,
                        year=year,
                        start_doy=start_doy,
                        end_doy=end_doy
                    )
                    all_solutions.extend(solutions)

                if all_solutions:
                    df = pd.DataFrame(all_solutions)

                    # Calculate repeatability for each station
                    st.subheader("Repeatability Statistics (Standard Deviation)")

                    repeat_data = []
                    for station in available_stations:
                        station_df = df[df['station_id'] == station]
                        if len(station_df) > 1:
                            repeat_data.append({
                                'Station': station,
                                'N days': len(station_df),
                                'X std (mm)': station_df['x'].std() * 1000,
                                'Y std (mm)': station_df['y'].std() * 1000,
                                'Z std (mm)': station_df['z'].std() * 1000,
                                '3D RMS (mm)': np.sqrt(
                                    station_df['x'].std()**2 +
                                    station_df['y'].std()**2 +
                                    station_df['z'].std()**2
                                ) * 1000
                            })

                    if repeat_data:
                        df_repeat = pd.DataFrame(repeat_data)
                        st.dataframe(
                            df_repeat.style.format({
                                'X std (mm)': '{:.2f}',
                                'Y std (mm)': '{:.2f}',
                                'Z std (mm)': '{:.2f}',
                                '3D RMS (mm)': '{:.2f}'
                            }),
                            use_container_width=True,
                            hide_index=True
                        )

                        # Bar chart of 3D repeatability
                        fig = px.bar(
                            df_repeat.sort_values('3D RMS (mm)'),
                            x='Station',
                            y='3D RMS (mm)',
                            title='3D Coordinate Repeatability by Station',
                            color='3D RMS (mm)',
                            color_continuous_scale='RdYlGn_r'
                        )
                        fig.add_hline(y=5, line_dash="dash", line_color="#ff6b6b",
                                      annotation_text="5mm threshold")
                        apply_colorful_style(fig)
                        st.plotly_chart(fig, use_container_width=True)

                    # Time series plot
                    st.subheader("Coordinate Time Series")

                    ts_stations = st.multiselect(
                        "Select stations for time series (multiple allowed)",
                        options=available_stations,
                        default=available_stations[:3] if len(available_stations) >= 3 else available_stations,
                        key='ts_stations'
                    )

                    if ts_stations:
                        fig = make_subplots(
                            rows=3, cols=1,
                            subplot_titles=['X deviation (mm)', 'Y deviation (mm)', 'Z deviation (mm)'],
                            shared_xaxes=True
                        )

                        colors = ['#00d4ff', '#ff6b6b', '#4ecdc4', '#ffe66d', '#c44dff',
                                  '#ff9f43', '#26de81', '#fd79a8', '#a29bfe', '#ffeaa7']
                        for idx, station in enumerate(ts_stations):
                            station_df = df[df['station_id'] == station].sort_values('doy').copy()

                            if len(station_df) > 0:
                                # Remove mean to show deviations
                                for coord in ['x', 'y', 'z']:
                                    station_df[f'{coord}_dev'] = (station_df[coord] - station_df[coord].mean()) * 1000

                                color = colors[idx % len(colors)]

                                # Remove periods from station name for display
                                station_display = station.replace('.', '')

                                for i, coord in enumerate(['x', 'y', 'z'], 1):
                                    fig.add_trace(
                                        go.Scatter(
                                            x=station_df['doy'],
                                            y=station_df[f'{coord}_dev'],
                                            mode='lines+markers',
                                            name=station_display,
                                            legendgroup=station_display,
                                            showlegend=(i == 1),
                                            line=dict(color=color),
                                            marker=dict(color=color)
                                        ),
                                        row=i, col=1
                                    )

                        fig.update_layout(
                            height=700,
                            title='Coordinate Deviations from Mean by Station',
                            showlegend=True,
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                        )
                        fig.update_xaxes(title_text="Day of Year", row=3, col=1)
                        apply_colorful_style(fig)
                        st.plotly_chart(fig, use_container_width=True)

                    # ========================================
                    # CODE COORDINATE COMPARISON
                    # ========================================
                    st.divider()
                    st.subheader("CODE Coordinate Comparison")
                    st.markdown("""
                    Compare local daily coordinates with CODE (Center for Orbit Determination in Europe)
                    final solutions from `ftp.aiub.unibe.ch/CODE/{year}/`
                    """)

                    # CODE comparison controls
                    code_coord_col1, code_coord_col2, code_coord_col3 = st.columns(3)
                    with code_coord_col1:
                        code_coord_start_doy = st.number_input(
                            "Start DOY",
                            min_value=1, max_value=366,
                            value=start_doy,
                            key='code_coord_start_doy'
                        )
                    with code_coord_col2:
                        code_coord_end_doy = st.number_input(
                            "End DOY",
                            min_value=1, max_value=366,
                            value=end_doy,
                            key='code_coord_end_doy'
                        )
                    with code_coord_col3:
                        code_coord_product = st.selectbox(
                            "Product Type",
                            options=["FIN", "RAP"],
                            index=0,
                            help="FIN=Final (13+ day latency), RAP=Rapid (1 day latency)",
                            key='code_coord_product_type'
                        )

                    # Load CODE products for date range
                    import os
                    code_cache_dir = "/tmp/code_products"
                    all_code_coords = {}  # DOY -> list of CODEStationCoord
                    loaded_coord_doys = []
                    failed_coord_doys = []

                    with st.spinner(f"Loading CODE coordinate products for DOY {code_coord_start_doy}-{code_coord_end_doy}..."):
                        for code_doy in range(code_coord_start_doy, code_coord_end_doy + 1):
                            # Check if file exists or needs download
                            filename = f"COD0OPS{code_coord_product}_{year}{code_doy:03d}0000_01D_01D_SOL.SNX"
                            local_path = os.path.join(code_cache_dir, filename)

                            if os.path.exists(local_path):
                                try:
                                    coords = parse_code_snx_file(local_path)
                                    all_code_coords[code_doy] = coords
                                    loaded_coord_doys.append(code_doy)
                                except Exception as e:
                                    failed_coord_doys.append(code_doy)
                            else:
                                # Try to download
                                downloaded = download_code_snx(year, code_doy, code_cache_dir, code_coord_product)
                                if downloaded:
                                    try:
                                        coords = parse_code_snx_file(downloaded)
                                        all_code_coords[code_doy] = coords
                                        loaded_coord_doys.append(code_doy)
                                    except Exception as e:
                                        failed_coord_doys.append(code_doy)
                                else:
                                    failed_coord_doys.append(code_doy)

                    # Show loading status
                    if loaded_coord_doys:
                        st.success(f"Loaded CODE coordinates for {len(loaded_coord_doys)} days: DOY {min(loaded_coord_doys)}-{max(loaded_coord_doys)}")
                    if failed_coord_doys:
                        st.warning(f"Could not load CODE coordinates for {len(failed_coord_doys)} days: {failed_coord_doys[:5]}{'...' if len(failed_coord_doys) > 5 else ''}")

                    if all_code_coords and ts_stations:
                        # Build comparison data
                        comparison_data = []

                        for station in ts_stations:
                            station_df = df[df['station_id'] == station]

                            for _, row in station_df.iterrows():
                                doy = int(row['doy'])
                                if doy in all_code_coords:
                                    code_coord = get_code_coords_for_station(all_code_coords[doy], station)
                                    if code_coord:
                                        # Compute ENU differences
                                        dE, dN, dU = compute_enu_diff(row['x'], row['y'], row['z'], code_coord)

                                        # Also compute XYZ differences
                                        dX = (row['x'] - code_coord.x) * 1000  # mm
                                        dY = (row['y'] - code_coord.y) * 1000
                                        dZ = (row['z'] - code_coord.z) * 1000

                                        comparison_data.append({
                                            'station': station,
                                            'doy': doy,
                                            'dE_mm': dE,
                                            'dN_mm': dN,
                                            'dU_mm': dU,
                                            'dX_mm': dX,
                                            'dY_mm': dY,
                                            'dZ_mm': dZ,
                                            'd3D_mm': np.sqrt(dX**2 + dY**2 + dZ**2),
                                            'dH_mm': np.sqrt(dE**2 + dN**2)  # Horizontal
                                        })

                        if comparison_data:
                            df_comp = pd.DataFrame(comparison_data)

                            # Summary statistics
                            st.subheader("Comparison Statistics (Local - CODE)")
                            stats_data = []
                            for station in df_comp['station'].unique():
                                sdf = df_comp[df_comp['station'] == station]
                                stats_data.append({
                                    'Station': station,
                                    'N Days': len(sdf),
                                    'dE RMS (mm)': np.sqrt((sdf['dE_mm']**2).mean()),
                                    'dN RMS (mm)': np.sqrt((sdf['dN_mm']**2).mean()),
                                    'dU RMS (mm)': np.sqrt((sdf['dU_mm']**2).mean()),
                                    'dE Mean (mm)': sdf['dE_mm'].mean(),
                                    'dN Mean (mm)': sdf['dN_mm'].mean(),
                                    'dU Mean (mm)': sdf['dU_mm'].mean(),
                                    '3D RMS (mm)': np.sqrt((sdf['d3D_mm']**2).mean())
                                })

                            df_stats = pd.DataFrame(stats_data)
                            st.dataframe(
                                df_stats.style.format({
                                    'dE RMS (mm)': '{:.2f}',
                                    'dN RMS (mm)': '{:.2f}',
                                    'dU RMS (mm)': '{:.2f}',
                                    'dE Mean (mm)': '{:.2f}',
                                    'dN Mean (mm)': '{:.2f}',
                                    'dU Mean (mm)': '{:.2f}',
                                    '3D RMS (mm)': '{:.2f}'
                                }),
                                use_container_width=True,
                                hide_index=True
                            )

                            # ENU time series plot
                            st.subheader("ENU Differences vs CODE")
                            colors = px.colors.qualitative.Set1

                            fig_enu = make_subplots(
                                rows=3, cols=1,
                                subplot_titles=['East (mm)', 'North (mm)', 'Up (mm)'],
                                shared_xaxes=True
                            )

                            for i, station in enumerate(df_comp['station'].unique()):
                                sdf = df_comp[df_comp['station'] == station].sort_values('doy')
                                color = colors[i % len(colors)]

                                fig_enu.add_trace(
                                    go.Scatter(
                                        x=sdf['doy'], y=sdf['dE_mm'],
                                        name=f'{station}', legendgroup=station,
                                        mode='lines+markers', marker=dict(color=color)
                                    ),
                                    row=1, col=1
                                )
                                fig_enu.add_trace(
                                    go.Scatter(
                                        x=sdf['doy'], y=sdf['dN_mm'],
                                        name=f'{station}', legendgroup=station,
                                        mode='lines+markers', marker=dict(color=color),
                                        showlegend=False
                                    ),
                                    row=2, col=1
                                )
                                fig_enu.add_trace(
                                    go.Scatter(
                                        x=sdf['doy'], y=sdf['dU_mm'],
                                        name=f'{station}', legendgroup=station,
                                        mode='lines+markers', marker=dict(color=color),
                                        showlegend=False
                                    ),
                                    row=3, col=1
                                )

                            # Add zero reference lines
                            for row in range(1, 4):
                                fig_enu.add_hline(y=0, line_dash="dash", line_color="gray", row=row, col=1)

                            fig_enu.update_layout(
                                height=600,
                                title='Coordinate Differences: Local PPP - CODE Final (ENU)',
                                showlegend=True,
                                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                            )
                            fig_enu.update_xaxes(title_text="Day of Year", row=3, col=1)
                            apply_colorful_style(fig_enu)
                            st.plotly_chart(fig_enu, use_container_width=True)

                            # Bar chart of 3D RMS by station
                            st.subheader("3D RMS vs CODE by Station")
                            fig_bar = px.bar(
                                df_stats.sort_values('3D RMS (mm)'),
                                x='Station',
                                y='3D RMS (mm)',
                                title='3D RMS Difference from CODE Final Solutions',
                                color='3D RMS (mm)',
                                color_continuous_scale='RdYlGn_r'
                            )
                            fig_bar.add_hline(y=10, line_dash="dash", line_color="#ff6b6b",
                                              annotation_text="10mm threshold")
                            apply_colorful_style(fig_bar)
                            st.plotly_chart(fig_bar, use_container_width=True)

                        else:
                            st.info("No matching stations found between local data and CODE products.")
                    elif not all_code_coords:
                        st.info("No CODE coordinate products loaded. Check if products are available for the selected dates.")

                else:
                    st.info("No solution data found for selected stations and date range.")
            else:
                st.warning("Please select at least one station.")

        # TAB 3: RMS Analysis
        with tab3:
            st.header("RMS Residual Analysis")

            # Get processing stats for ALL available stations
            all_stats = []
            for station in available_stations:
                stats = db.get_stats(station_id=station, year=year, limit=100)
                all_stats.extend(stats)

            if all_stats:
                df_stats = pd.DataFrame(all_stats)
                df_stats['rms_mm'] = df_stats['rms_unit_weight'] * 1000

                # Summary statistics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Mean RMS", f"{df_stats['rms_mm'].mean():.2f} mm")
                with col2:
                    st.metric("Min RMS", f"{df_stats['rms_mm'].min():.2f} mm")
                with col3:
                    st.metric("Max RMS", f"{df_stats['rms_mm'].max():.2f} mm")

                st.divider()

                # Station selection for RMS Distribution
                st.subheader("RMS Distribution")
                rms_dist_stations = st.multiselect(
                    "Select stations for RMS Distribution",
                    options=available_stations,
                    default=available_stations,
                    key='rms_dist_stations'
                )

                if rms_dist_stations:
                    df_dist = df_stats[df_stats['station_id'].isin(rms_dist_stations)]

                    # RMS distribution histogram
                    fig_hist = px.histogram(
                        df_dist,
                        x='rms_mm',
                        nbins=30,
                        title=f'Distribution of A Posteriori RMS ({len(rms_dist_stations)} stations)',
                        labels={'rms_mm': 'RMS (mm)'}
                    )
                    fig_hist.add_vline(x=2, line_dash="dash", line_color="#4ecdc4",
                                       annotation_text="Good (<2mm)")
                    fig_hist.add_vline(x=5, line_dash="dash", line_color="#ff6b6b",
                                       annotation_text="Warning (<5mm)")
                    apply_colorful_style(fig_hist)
                    st.plotly_chart(fig_hist, use_container_width=True)

                    # RMS by station boxplot
                    fig_box = px.box(
                        df_dist,
                        x='station_id',
                        y='rms_mm',
                        title='RMS Distribution by Station',
                        labels={'station_id': 'Station', 'rms_mm': 'RMS (mm)'}
                    )
                    fig_box.add_hline(y=2, line_dash="dash", line_color="#4ecdc4")
                    fig_box.add_hline(y=5, line_dash="dash", line_color="#ff6b6b")
                    apply_colorful_style(fig_box)
                    st.plotly_chart(fig_box, use_container_width=True)

                st.divider()

                # Station selection for RMS Time Series
                st.subheader("RMS Time Series")
                rms_ts_stations = st.multiselect(
                    "Select stations for RMS Time Series",
                    options=available_stations,
                    default=available_stations[:5] if len(available_stations) > 5 else available_stations,
                    key='rms_ts_stations'
                )

                if rms_ts_stations:
                    df_ts = df_stats[df_stats['station_id'].isin(rms_ts_stations)]

                    fig_ts = px.line(
                        df_ts.sort_values(['station_id', 'doy']),
                        x='doy',
                        y='rms_mm',
                        color='station_id',
                        title=f'RMS Over Time ({len(rms_ts_stations)} stations)',
                        labels={'doy': 'Day of Year', 'rms_mm': 'RMS (mm)'}
                    )
                    fig_ts.add_hline(y=2, line_dash="dash", line_color="#4ecdc4",
                                     annotation_text="Good (<2mm)")
                    fig_ts.add_hline(y=5, line_dash="dash", line_color="#ff6b6b",
                                     annotation_text="Warning (<5mm)")
                    apply_colorful_style(fig_ts)
                    st.plotly_chart(fig_ts, use_container_width=True)

            else:
                st.info("No processing statistics available.")

        # TAB 4: ZTD Monitor
        with tab4:
            st.header("Zenith Total Delay (ZTD) & Gradients Monitor")
            st.markdown("Troposphere estimates from TRO files: ZTD (TROTOT), North Gradient (TGNTOT), East Gradient (TGETOT)")

            # Selection controls
            col1, col2, col3 = st.columns(3)
            with col1:
                ztd_stations = st.multiselect(
                    "Select stations (multiple allowed)",
                    options=available_stations,
                    default=available_stations[:3] if len(available_stations) >= 3 else available_stations,
                    key='ztd_stations'
                )
            with col2:
                ztd_start_doy = st.number_input(
                    "Start DOY",
                    min_value=1, max_value=366,
                    value=start_doy,
                    key='ztd_start_doy'
                )
            with col3:
                ztd_end_doy = st.number_input(
                    "End DOY",
                    min_value=1, max_value=366,
                    value=end_doy,
                    key='ztd_end_doy'
                )

            if ztd_stations:
                # Collect ZTD data for all selected stations and DOY range
                all_ztd_data = []
                for station in ztd_stations:
                    for doy in range(ztd_start_doy, ztd_end_doy + 1):
                        ztd_data = db.get_ztd(station, year, doy)
                        if ztd_data:
                            for record in ztd_data:
                                record['station_id'] = station
                                all_ztd_data.append(record)

                if all_ztd_data:
                    df_ztd = pd.DataFrame(all_ztd_data)
                    df_ztd['ztd_mm'] = df_ztd['ztd'] * 1000
                    df_ztd['rms_mm'] = df_ztd['ztd_rms'] * 1000
                    df_ztd['grad_n_mm'] = df_ztd['grad_n'].fillna(0) * 1000
                    df_ztd['grad_e_mm'] = df_ztd['grad_e'].fillna(0) * 1000
                    # Create continuous time axis: DOY + hour/24
                    df_ztd['time'] = df_ztd['doy'] + df_ztd['hour'] / 24.0

                    # Statistics row
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Mean ZTD", f"{df_ztd['ztd_mm'].mean():.1f} mm")
                    with col2:
                        st.metric("ZTD Range", f"{df_ztd['ztd_mm'].max() - df_ztd['ztd_mm'].min():.1f} mm")
                    with col3:
                        st.metric("Mean STDDEV", f"{df_ztd['rms_mm'].mean():.2f} mm")
                    with col4:
                        st.metric("Records", len(df_ztd))

                    st.divider()

                    # Color palette for stations
                    colors = px.colors.qualitative.Plotly

                    # ZTD (TROTOT) with STDDEV subplot below
                    st.subheader("TROTOT (Zenith Total Delay)")
                    fig_ztd = make_subplots(
                        rows=2, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.08,
                        row_heights=[0.7, 0.3],
                        subplot_titles=['TROTOT (mm)', 'TROTOT STDDEV (mm)']
                    )
                    for i, station in enumerate(ztd_stations):
                        station_df = df_ztd[df_ztd['station_id'] == station].sort_values('time')
                        if len(station_df) > 0:
                            # TROTOT - line only (no markers)
                            fig_ztd.add_trace(go.Scatter(
                                x=station_df['time'],
                                y=station_df['ztd_mm'],
                                mode='lines',
                                name=station,
                                line=dict(color=colors[i % len(colors)], width=1.5),
                                legendgroup=station,
                                showlegend=True
                            ), row=1, col=1)
                            # TROTOT STDDEV - line only
                            fig_ztd.add_trace(go.Scatter(
                                x=station_df['time'],
                                y=station_df['rms_mm'],
                                mode='lines',
                                name=f'{station} STDDEV',
                                line=dict(color=colors[i % len(colors)], width=1.5),
                                legendgroup=station,
                                showlegend=False
                            ), row=2, col=1)
                    fig_ztd.update_layout(
                        title=f'TROTOT Time Series - DOY {ztd_start_doy} to {ztd_end_doy}',
                        hovermode='x unified',
                        height=600,
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    fig_ztd.update_xaxes(title_text='Day of Year', row=2, col=1)
                    apply_colorful_style(fig_ztd)
                    st.plotly_chart(fig_ztd, use_container_width=True)

                    # North Gradient (TGNTOT) with STDDEV subplot below
                    st.subheader("TGNTOT (North Gradient)")
                    fig_grad_n = make_subplots(
                        rows=2, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.08,
                        row_heights=[0.7, 0.3],
                        subplot_titles=['TGNTOT (mm)', 'TGNTOT STDDEV (mm)']
                    )
                    # Prepare gradient STDDEV columns if available
                    if 'grad_n_rms' in df_ztd.columns:
                        df_ztd['grad_n_rms_mm'] = df_ztd['grad_n_rms'].fillna(0) * 1000
                    else:
                        df_ztd['grad_n_rms_mm'] = 0.0

                    for i, station in enumerate(ztd_stations):
                        station_df = df_ztd[df_ztd['station_id'] == station].sort_values('time')
                        if len(station_df) > 0:
                            # TGNTOT - line only
                            fig_grad_n.add_trace(go.Scatter(
                                x=station_df['time'],
                                y=station_df['grad_n_mm'],
                                mode='lines',
                                name=station,
                                line=dict(color=colors[i % len(colors)], width=1.5),
                                legendgroup=station,
                                showlegend=True
                            ), row=1, col=1)
                            # TGNTOT STDDEV
                            fig_grad_n.add_trace(go.Scatter(
                                x=station_df['time'],
                                y=station_df['grad_n_rms_mm'],
                                mode='lines',
                                name=f'{station} STDDEV',
                                line=dict(color=colors[i % len(colors)], width=1.5),
                                legendgroup=station,
                                showlegend=False
                            ), row=2, col=1)
                    fig_grad_n.add_hline(y=0, line_dash="dash", line_color="#4a6fa5", row=1, col=1)
                    fig_grad_n.update_layout(
                        hovermode='x unified',
                        height=500,
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    fig_grad_n.update_xaxes(title_text='Day of Year', row=2, col=1)
                    apply_colorful_style(fig_grad_n)
                    st.plotly_chart(fig_grad_n, use_container_width=True)

                    # East Gradient (TGETOT) with STDDEV subplot below
                    st.subheader("TGETOT (East Gradient)")
                    fig_grad_e = make_subplots(
                        rows=2, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.08,
                        row_heights=[0.7, 0.3],
                        subplot_titles=['TGETOT (mm)', 'TGETOT STDDEV (mm)']
                    )
                    # Prepare gradient STDDEV columns if available
                    if 'grad_e_rms' in df_ztd.columns:
                        df_ztd['grad_e_rms_mm'] = df_ztd['grad_e_rms'].fillna(0) * 1000
                    else:
                        df_ztd['grad_e_rms_mm'] = 0.0

                    for i, station in enumerate(ztd_stations):
                        station_df = df_ztd[df_ztd['station_id'] == station].sort_values('time')
                        if len(station_df) > 0:
                            # TGETOT - line only
                            fig_grad_e.add_trace(go.Scatter(
                                x=station_df['time'],
                                y=station_df['grad_e_mm'],
                                mode='lines',
                                name=station,
                                line=dict(color=colors[i % len(colors)], width=1.5),
                                legendgroup=station,
                                showlegend=True
                            ), row=1, col=1)
                            # TGETOT STDDEV
                            fig_grad_e.add_trace(go.Scatter(
                                x=station_df['time'],
                                y=station_df['grad_e_rms_mm'],
                                mode='lines',
                                name=f'{station} STDDEV',
                                line=dict(color=colors[i % len(colors)], width=1.5),
                                legendgroup=station,
                                showlegend=False
                            ), row=2, col=1)
                    fig_grad_e.add_hline(y=0, line_dash="dash", line_color="#4a6fa5", row=1, col=1)
                    fig_grad_e.update_layout(
                        hovermode='x unified',
                        height=500,
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    fig_grad_e.update_xaxes(title_text='Day of Year', row=2, col=1)
                    apply_colorful_style(fig_grad_e)
                    st.plotly_chart(fig_grad_e, use_container_width=True)

                    # Statistics table by station
                    st.subheader("Statistics by Station")
                    stats_data = []
                    for station in ztd_stations:
                        station_df = df_ztd[df_ztd['station_id'] == station]
                        if len(station_df) > 0:
                            stats_data.append({
                                'Station': station,
                                'Records': len(station_df),
                                'Mean ZTD (mm)': station_df['ztd_mm'].mean(),
                                'Std ZTD (mm)': station_df['ztd_mm'].std(),
                                'Mean RMS (mm)': station_df['rms_mm'].mean(),
                                'Mean Grad N (mm)': station_df['grad_n_mm'].mean(),
                                'Mean Grad E (mm)': station_df['grad_e_mm'].mean()
                            })
                    if stats_data:
                        df_stats = pd.DataFrame(stats_data)
                        st.dataframe(
                            df_stats.style.format({
                                'Mean ZTD (mm)': '{:.1f}',
                                'Std ZTD (mm)': '{:.2f}',
                                'Mean RMS (mm)': '{:.2f}',
                                'Mean Grad N (mm)': '{:.3f}',
                                'Mean Grad E (mm)': '{:.3f}'
                            }),
                            use_container_width=True,
                            hide_index=True
                        )

                    # Detailed data table (collapsible)
                    with st.expander("Show detailed hourly data"):
                        display_ztd = df_ztd[['station_id', 'doy', 'hour', 'ztd_mm', 'rms_mm', 'grad_n_mm', 'grad_e_mm']].copy()
                        display_ztd.columns = ['Station', 'DOY', 'Hour', 'ZTD (mm)', 'RMS (mm)', 'Grad N (mm)', 'Grad E (mm)']
                        st.dataframe(
                            display_ztd.style.format({
                                'ZTD (mm)': '{:.1f}',
                                'RMS (mm)': '{:.2f}',
                                'Grad N (mm)': '{:.3f}',
                                'Grad E (mm)': '{:.3f}'
                            }),
                            use_container_width=True,
                            hide_index=True
                        )

                    # ========================================
                    # CODE TROPOSPHERE COMPARISON (within ZTD Monitor)
                    # ========================================
                    st.divider()
                    st.subheader("CODE Troposphere Comparison")
                    st.markdown("""
                    Compare local ZTD estimates with CODE (Center for Orbit Determination in Europe)
                    final troposphere products from `ftp.aiub.unibe.ch/CODE/{year}/`
                    """)

                    # CODE comparison controls - with date range
                    code_col1, code_col2, code_col3, code_col4 = st.columns(4)
                    with code_col1:
                        code_start_doy = st.number_input(
                            "Start DOY",
                            min_value=1, max_value=366,
                            value=ztd_start_doy,
                            key='code_start_doy'
                        )
                    with code_col2:
                        code_end_doy = st.number_input(
                            "End DOY",
                            min_value=1, max_value=366,
                            value=ztd_end_doy,
                            key='code_end_doy'
                        )
                    with code_col3:
                        code_product = st.selectbox(
                            "Product Type",
                            options=["FIN", "RAP"],
                            index=0,
                            help="FIN=Final (13+ day latency), RAP=Rapid (1 day latency)",
                            key='code_product_type'
                        )
                    with code_col4:
                        auto_download = st.checkbox(
                            "Auto-download",
                            value=True,
                            help="Automatically download CODE products if not cached",
                            key='code_auto_download'
                        )

                    # Load CODE products for date range
                    import os
                    code_cache_dir = "/tmp/code_products"
                    all_code_entries = []
                    all_code_sites = []
                    loaded_doys = []
                    failed_doys = []

                    with st.spinner(f"Loading CODE products for DOY {code_start_doy}-{code_end_doy}..."):
                        for code_doy in range(code_start_doy, code_end_doy + 1):
                            code_filename = f"COD0OPS{code_product}_{year}{code_doy:03d}0000_01D_01H_TRO.TRO"
                            code_filepath = os.path.join(code_cache_dir, code_filename)

                            if os.path.exists(code_filepath):
                                sites, entries = parse_code_tro_file(code_filepath)
                                all_code_entries.extend(entries)
                                if not all_code_sites:
                                    all_code_sites = sites
                                loaded_doys.append(code_doy)
                            elif auto_download:
                                downloaded_path = download_code_tro(year, code_doy, code_cache_dir, code_product)
                                if downloaded_path:
                                    sites, entries = parse_code_tro_file(downloaded_path)
                                    all_code_entries.extend(entries)
                                    if not all_code_sites:
                                        all_code_sites = sites
                                    loaded_doys.append(code_doy)
                                else:
                                    failed_doys.append(code_doy)
                            else:
                                failed_doys.append(code_doy)

                    if loaded_doys:
                        st.info(f"Loaded CODE products for {len(loaded_doys)} days: DOY {min(loaded_doys)}-{max(loaded_doys)}")
                        if failed_doys:
                            st.warning(f"Missing CODE products for DOY: {', '.join(map(str, failed_doys[:10]))}")

                    if all_code_entries:
                        # Find matching stations
                        code_station_4chars = set(e.station_4char for e in all_code_entries)
                        local_station_4chars = set(s[:4].upper() for s in ztd_stations)
                        matching_stations = code_station_4chars.intersection(local_station_4chars)

                        if matching_stations:
                            st.success(f"Found {len(matching_stations)} matching stations: {', '.join(sorted(matching_stations))}")

                            # Build comparison data for all DOYs
                            comparison_data = []
                            for sta_4char in matching_stations:
                                # Get local station ID
                                local_sta_id = None
                                for sta in ztd_stations:
                                    if sta[:4].upper() == sta_4char:
                                        local_sta_id = sta
                                        break

                                if local_sta_id:
                                    # Get all CODE data for this station
                                    code_sta_data = get_code_ztd_for_station(all_code_entries, sta_4char)

                                    for code_entry in code_sta_data:
                                        # Find matching local data (same DOY and hour)
                                        local_match = df_ztd[
                                            (df_ztd['station_id'] == local_sta_id) &
                                            (df_ztd['doy'] == code_entry.doy) &
                                            (df_ztd['hour'] == code_entry.hour)
                                        ]
                                        if len(local_match) > 0:
                                            local_ztd = local_match.iloc[0]['ztd'] * 1000  # mm
                                            code_ztd = code_entry.ztd * 1000  # mm
                                            diff = local_ztd - code_ztd
                                            # Continuous time: DOY + hour/24
                                            time_val = code_entry.doy + code_entry.hour / 24.0

                                            comparison_data.append({
                                                'station': sta_4char,
                                                'doy': code_entry.doy,
                                                'hour': code_entry.hour,
                                                'time': time_val,
                                                'local_ztd_mm': local_ztd,
                                                'code_ztd_mm': code_ztd,
                                                'diff_mm': diff
                                            })

                            if comparison_data:
                                df_compare = pd.DataFrame(comparison_data)
                                n_days = df_compare['doy'].nunique()

                                # ZTD Time Series Comparison
                                st.markdown(f"**Comparing {len(comparison_data)} epochs across {n_days} days**")

                                # ZTD Time Series (Local vs CODE)
                                fig_ztd = go.Figure()
                                colors = px.colors.qualitative.Plotly
                                for i, sta in enumerate(sorted(matching_stations)):
                                    sta_df = df_compare[df_compare['station'] == sta].sort_values('time')
                                    if len(sta_df) > 0:
                                        fig_ztd.add_trace(go.Scatter(
                                            x=sta_df['time'], y=sta_df['local_ztd_mm'],
                                            mode='lines', name=f'{sta} Local',
                                            line=dict(color=colors[i % len(colors)], width=1.5),
                                            legendgroup=sta
                                        ))
                                        fig_ztd.add_trace(go.Scatter(
                                            x=sta_df['time'], y=sta_df['code_ztd_mm'],
                                            mode='lines', name=f'{sta} CODE',
                                            line=dict(color=colors[i % len(colors)], width=1.5, dash='dash'),
                                            legendgroup=sta
                                        ))
                                fig_ztd.update_layout(
                                    title=f"Local vs CODE ZTD - DOY {code_start_doy} to {code_end_doy}",
                                    xaxis_title="Day of Year",
                                    yaxis_title="ZTD (mm)",
                                    hovermode='x unified',
                                    legend=dict(orientation="h", yanchor="bottom", y=1.02)
                                )
                                apply_colorful_style(fig_ztd)
                                st.plotly_chart(fig_ztd, use_container_width=True)

                                # ZTD Difference plots
                                col1, col2 = st.columns(2)
                                with col1:
                                    fig_diff = px.line(df_compare.sort_values('time'), x='time', y='diff_mm',
                                                       color='station',
                                                       title="ZTD Difference (Local - CODE)",
                                                       labels={'diff_mm': 'Difference (mm)', 'time': 'Day of Year'})
                                    fig_diff.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.5)
                                    fig_diff.add_hline(y=5, line_dash="dot", line_color="red", opacity=0.3)
                                    fig_diff.add_hline(y=-5, line_dash="dot", line_color="red", opacity=0.3)
                                    apply_colorful_style(fig_diff)
                                    st.plotly_chart(fig_diff, use_container_width=True)

                                with col2:
                                    # Histogram of differences
                                    fig_hist = px.histogram(df_compare, x='diff_mm', color='station',
                                                            nbins=50,
                                                            title="ZTD Difference Distribution",
                                                            labels={'diff_mm': 'Difference (mm)'})
                                    apply_colorful_style(fig_hist)
                                    st.plotly_chart(fig_hist, use_container_width=True)

                                # Statistics Summary
                                stats_data = []
                                for sta in sorted(matching_stations):
                                    sta_df = df_compare[df_compare['station'] == sta]
                                    if len(sta_df) > 0:
                                        diffs = sta_df['diff_mm'].values
                                        stats_data.append({
                                            'Station': sta,
                                            'N Days': sta_df['doy'].nunique(),
                                            'N Epochs': len(sta_df),
                                            'Mean Bias (mm)': np.mean(diffs),
                                            'Std Dev (mm)': np.std(diffs),
                                            'RMS (mm)': np.sqrt(np.mean(diffs**2))
                                        })

                                if stats_data:
                                    df_stats = pd.DataFrame(stats_data)

                                    # Overall statistics
                                    all_diffs = df_compare['diff_mm'].values
                                    overall_rms = np.sqrt(np.mean(all_diffs**2))

                                    st.markdown(f"""
                                    **Overall Statistics ({n_days} days):** Mean Bias: **{np.mean(all_diffs):.2f} mm** |
                                    Std Dev: **{np.std(all_diffs):.2f} mm** |
                                    RMS: **{overall_rms:.2f} mm** |
                                    N Epochs: **{len(all_diffs)}**
                                    """)

                                    # Per-station table
                                    st.dataframe(
                                        df_stats.style.format({
                                            'Mean Bias (mm)': '{:.2f}',
                                            'Std Dev (mm)': '{:.2f}',
                                            'RMS (mm)': '{:.2f}'
                                        }).background_gradient(subset=['RMS (mm)'], cmap='YlOrRd'),
                                        use_container_width=True,
                                        hide_index=True
                                    )

                                    # Quality assessment
                                    if overall_rms < 3:
                                        st.success(f"Excellent agreement with CODE (RMS < 3 mm)")
                                    elif overall_rms < 5:
                                        st.info(f"Good agreement with CODE (RMS < 5 mm)")
                                    elif overall_rms < 10:
                                        st.warning(f"Moderate agreement with CODE (RMS < 10 mm)")
                                    else:
                                        st.error(f"Poor agreement with CODE (RMS > 10 mm). Check processing configuration.")
                            else:
                                st.warning(f"No matching epochs found for DOY {code_start_doy}-{code_end_doy}. Make sure local data exists.")
                        else:
                            st.info(f"No matching stations found between local ({len(ztd_stations)} selected) and CODE ({len(all_code_sites)} stations)")
                    else:
                        if not auto_download:
                            st.info("Enable 'Auto-download' to fetch CODE products, or manually place files in /tmp/code_products/")

                else:
                    st.info(f"No ZTD data found for selected stations in DOY range {ztd_start_doy}-{ztd_end_doy}")
                    st.code("To load TRO data, run:\npython -m frontend.tro_parser <doy> --save")
            else:
                st.warning("Please select at least one station.")

        # TAB 5: Ambiguity Resolution
        with tab5:
            st.header("Ambiguity Resolution Statistics")
            st.markdown("GPS (G), Galileo (E), and Combined (G+E) ambiguity resolution rates")

            # Get ambiguity data
            amb_data = db.get_ambiguity(year=year, start_doy=start_doy, end_doy=end_doy)

            if amb_data:
                df_amb = pd.DataFrame(amb_data)

                # Summary metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    avg_wl = df_amb['wl_combined'].mean()
                    st.metric("Avg WL Combined", f"{avg_wl:.1f}%" if pd.notna(avg_wl) else "N/A")
                with col2:
                    avg_nl = df_amb['nl_combined'].mean()
                    st.metric("Avg NL Combined", f"{avg_nl:.1f}%" if pd.notna(avg_nl) else "N/A")
                with col3:
                    st.metric("Stations", df_amb['station_id'].nunique())

                st.divider()

                # Get latest DOY data for each station
                latest_doy = df_amb['doy'].max()
                df_latest = df_amb[df_amb['doy'] == latest_doy].copy()

                if len(df_latest) > 0:
                    # Widelane Resolution by Station
                    st.subheader("Widelane Resolution by Station")
                    fig_wl = px.bar(
                        df_latest.sort_values('wl_combined', ascending=False),
                        x='station_id',
                        y=['wl_gps', 'wl_gal', 'wl_combined'],
                        title=f'Widelane Ambiguity Resolution - DOY {latest_doy}',
                        labels={'value': 'Resolution Rate (%)', 'station_id': 'Station'},
                        barmode='group'
                    )
                    fig_wl.add_hline(y=90, line_dash="dash", line_color="#4ecdc4",
                                     annotation_text="90% target")
                    fig_wl.update_layout(legend_title_text='Constellation')
                    apply_colorful_style(fig_wl)
                    st.plotly_chart(fig_wl, use_container_width=True)

                    # Narrowlane Resolution by Station
                    st.subheader("Narrowlane Resolution by Station")
                    fig_nl = px.bar(
                        df_latest.sort_values('nl_combined', ascending=False),
                        x='station_id',
                        y=['nl_gps', 'nl_gal', 'nl_combined'],
                        title=f'Narrowlane Ambiguity Resolution - DOY {latest_doy}',
                        labels={'value': 'Resolution Rate (%)', 'station_id': 'Station'},
                        barmode='group'
                    )
                    fig_nl.add_hline(y=80, line_dash="dash", line_color="#4ecdc4",
                                     annotation_text="80% target")
                    fig_nl.update_layout(legend_title_text='Constellation')
                    apply_colorful_style(fig_nl)
                    st.plotly_chart(fig_nl, use_container_width=True)

                # Detailed table
                st.subheader("Detailed Ambiguity Statistics")

                # Format for display
                display_df = df_amb[['station_id', 'doy', 'receiver',
                                     'wl_gps', 'wl_gal', 'wl_combined',
                                     'nl_gps', 'nl_gal', 'nl_combined']].copy()
                display_df.columns = ['Station', 'DOY', 'Receiver',
                                      'WL GPS', 'WL GAL', 'WL G+E',
                                      'NL GPS', 'NL GAL', 'NL G+E']

                st.dataframe(
                    display_df.style.format({
                        'WL GPS': '{:.1f}', 'WL GAL': '{:.1f}', 'WL G+E': '{:.1f}',
                        'NL GPS': '{:.1f}', 'NL GAL': '{:.1f}', 'NL G+E': '{:.1f}'
                    }, na_rep='-'),
                    use_container_width=True,
                    hide_index=True
                )

                # Stations with low resolution
                st.subheader("Stations Needing Attention (NL < 70%)")
                low_nl = df_amb[df_amb['nl_combined'] < 70][['station_id', 'doy', 'nl_combined', 'receiver']]
                if len(low_nl) > 0:
                    st.dataframe(low_nl, use_container_width=True, hide_index=True)
                else:
                    st.success("All stations have NL resolution >= 70%")

            else:
                st.info("No ambiguity data found. Run: `python -m frontend.amb_report <doy> --save`")

        # TAB 6: Satellite Tracking
        with tab6:
            st.header("Satellite Tracking Statistics")
            st.markdown("GPS (PRN 1-32), GLONASS (PRN 101-128), Galileo (PRN 201-236)")

            # DOY selection for satellite data
            col1, col2 = st.columns(2)
            with col1:
                sat_doy = st.number_input(
                    "Select DOY",
                    min_value=1, max_value=366,
                    value=end_doy,
                    key='sat_doy'
                )

            # Get satellite tracking data
            sat_data = db.get_satellite_tracking(year=year, doy=sat_doy)

            if sat_data:
                df_sat = pd.DataFrame(sat_data)

                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    gps_count = len(df_sat[df_sat['constellation'] == 'GPS'])
                    st.metric("GPS Satellites", gps_count)
                with col2:
                    glo_count = len(df_sat[df_sat['constellation'] == 'GLONASS'])
                    st.metric("GLONASS Satellites", glo_count)
                with col3:
                    gal_count = len(df_sat[df_sat['constellation'] == 'GALILEO'])
                    st.metric("Galileo Satellites", gal_count)
                with col4:
                    total_obs = df_sat['obs_count'].sum()
                    st.metric("Total Observations", f"{total_obs:,}")

                st.divider()

                # Observations by PRN
                st.subheader("Observations by Satellite PRN")

                # Color by constellation
                constellation_colors = {
                    'GPS': '#00d4ff',
                    'GLONASS': '#ff6b6b',
                    'GALILEO': '#4ecdc4'
                }
                df_sat['color'] = df_sat['constellation'].map(constellation_colors)

                fig_obs = px.bar(
                    df_sat.sort_values(['constellation', 'prn']),
                    x='prn',
                    y='obs_count',
                    color='constellation',
                    color_discrete_map=constellation_colors,
                    title=f'Observation Count by PRN - DOY {sat_doy}',
                    labels={'prn': 'PRN', 'obs_count': 'Observations', 'constellation': 'Constellation'}
                )
                apply_colorful_style(fig_obs)
                st.plotly_chart(fig_obs, use_container_width=True)

                # RMS by PRN
                st.subheader("RMS by Satellite PRN")
                fig_rms = px.bar(
                    df_sat.sort_values(['constellation', 'prn']),
                    x='prn',
                    y='rms',
                    color='constellation',
                    color_discrete_map=constellation_colors,
                    title=f'RMS by PRN - DOY {sat_doy}',
                    labels={'prn': 'PRN', 'rms': 'RMS (mm)', 'constellation': 'Constellation'}
                )
                fig_rms.add_hline(y=3, line_dash="dash", line_color="#ffe66d",
                                  annotation_text="3mm threshold")
                apply_colorful_style(fig_rms)
                st.plotly_chart(fig_rms, use_container_width=True)

                # Observation percentage pie chart by constellation
                st.subheader("Observation Distribution by Constellation")
                obs_by_const = df_sat.groupby('constellation')['obs_count'].sum().reset_index()
                fig_pie = px.pie(
                    obs_by_const,
                    values='obs_count',
                    names='constellation',
                    title='Total Observations by Constellation',
                    color='constellation',
                    color_discrete_map=constellation_colors
                )
                fig_pie.update_traces(textposition='inside', textinfo='percent+label+value')
                apply_colorful_style(fig_pie)
                st.plotly_chart(fig_pie, use_container_width=True)

                # Detailed table
                st.subheader("Detailed Satellite Statistics")
                display_sat = df_sat[['prn', 'constellation', 'obs_percent', 'obs_count', 'rms']].copy()
                display_sat.columns = ['PRN', 'Constellation', 'Obs %', 'Obs Count', 'RMS (mm)']
                st.dataframe(
                    display_sat.style.format({
                        'Obs %': '{:.2f}',
                        'RMS (mm)': '{:.1f}'
                    }),
                    use_container_width=True,
                    hide_index=True
                )

                # Statistics summary by constellation
                st.subheader("Statistics by Constellation")
                const_stats = df_sat.groupby('constellation').agg({
                    'prn': 'count',
                    'obs_count': ['sum', 'mean'],
                    'rms': ['mean', 'min', 'max']
                }).round(2)
                const_stats.columns = ['Satellites', 'Total Obs', 'Mean Obs', 'Mean RMS', 'Min RMS', 'Max RMS']
                st.dataframe(const_stats, use_container_width=True)

            else:
                st.info(f"No satellite tracking data for DOY {sat_doy}. Run: `python -m frontend.ingest_all {sat_doy}`")

        # ═══════════════════════════════════════════════════════════════════════════════
        # TAB 7: Processing Statistics
        # ═══════════════════════════════════════════════════════════════════════════════
        with tab7:
            st.header("Processing Statistics")
            st.markdown("**A posteriori RMS, Chi²/DOF, and observation statistics from GPSEST**")

            # Parameter explanations in expandable section
            with st.expander("Parameter Definitions & Quality Thresholds", expanded=False):
                st.markdown("""
                ### A Posteriori RMS of Unit Weight (sigma_0)
                The **A Posteriori RMS of Unit Weight** is the fundamental quality indicator from least-squares adjustment.
                It represents the standard deviation of observations after adjustment, scaled by observation weights.

                - **Formula**: σ₀ = √(v'Pv / DOF), where v = residuals, P = weight matrix
                - **Physical meaning**: Average residual magnitude after accounting for observation precision
                - **Typical values**: 1-2 mm for high-quality PPP solutions
                - **Quality thresholds**:
                  - 🟢 **< 2 mm**: Excellent solution quality
                  - 🟡 **2-3 mm**: Acceptable, minor issues possible
                  - 🔴 **> 3 mm**: Poor quality, investigate multipath, cycle slips, or troposphere

                ---
                ### Chi²/DOF (Chi-squared per Degree of Freedom)
                The **Chi²/DOF** statistic tests whether the assumed observation uncertainties match the actual residuals.
                It validates the stochastic model used in the adjustment.

                - **Formula**: χ²/DOF = v'Pv / DOF
                - **Ideal value**: 1.0 (observations fit the model as expected)
                - **Interpretation**:
                  - **χ²/DOF ≈ 1.0**: Correct a priori variances, model fits well
                  - **χ²/DOF > 1.0**: Underestimated errors or unmodeled effects (multipath, atmosphere)
                  - **χ²/DOF < 1.0**: Overestimated errors (overly pessimistic weighting)
                - **Quality thresholds**:
                  - 🟢 **0.8 - 1.2**: Excellent stochastic model
                  - 🟡 **0.5 - 1.5**: Acceptable, minor weighting issues
                  - 🔴 **< 0.5 or > 1.5**: Review observation weighting and error models

                ---
                ### Number of Observations
                Total **phase and code observations** used in the least-squares solution after data screening.

                - **Includes**: L1/L2/L5 carrier phase, P1/P2/C5 pseudorange observations
                - **Typical values**: 30,000-100,000+ observations per day for multi-GNSS
                - **Low count causes**: Missing data, receiver issues, aggressive screening
                - **Quality indicator**: More observations = more redundancy = better solution

                ---
                ### Degrees of Freedom (DOF)
                **DOF = Number of Observations - Number of Parameters** represents the redundancy in the solution.

                - **Formula**: DOF = n - u (observations minus unknowns)
                - **Importance**: Higher DOF = more reliable solution, better outlier detection
                - **Typical values**: 20,000-80,000+ for daily PPP solutions
                - **Low DOF impact**: Less redundancy, weaker outlier detection, unreliable statistics
                """)

            st.divider()

            # DOY selector for processing stats
            col1, col2 = st.columns(2)
            with col1:
                stats_doy = st.number_input(
                    "Select DOY",
                    min_value=1, max_value=366,
                    value=end_doy,
                    key='stats_doy'
                )

            # Query processing stats for selected DOY (use higher limit to get all DOYs)
            stats_data = db.get_stats(year=year, limit=2000)
            df_stats = pd.DataFrame(stats_data)

            if len(df_stats) > 0:
                # Filter to selected DOY
                df_stats_day = df_stats[df_stats['doy'] == stats_doy].copy()

                if len(df_stats_day) > 0:
                    # Summary metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        mean_rms = df_stats_day['rms_unit_weight'].mean() * 1000  # Convert to mm
                        st.metric("Mean RMS", f"{mean_rms:.2f} mm")
                    with col2:
                        mean_chi2 = df_stats_day['chi2_dof'].mean() if 'chi2_dof' in df_stats_day.columns else 0
                        st.metric("Mean Chi²/DOF", f"{mean_chi2:.3f}")
                    with col3:
                        total_obs = df_stats_day['num_observations'].sum()
                        st.metric("Total Observations", f"{total_obs:,}")
                    with col4:
                        num_stations = len(df_stats_day)
                        st.metric("Stations Processed", num_stations)

                    # RMS by station bar chart
                    st.subheader("A Posteriori RMS by Station")
                    df_stats_day['rms_mm'] = df_stats_day['rms_unit_weight'] * 1000

                    # Color code: green for good (<2mm), yellow for moderate (2-3mm), red for high (>3mm)
                    df_stats_day['rms_color'] = df_stats_day['rms_mm'].apply(
                        lambda x: 'Good (<2mm)' if x < 2 else ('Moderate (2-3mm)' if x < 3 else 'High (>3mm)')
                    )

                    fig_rms = px.bar(
                        df_stats_day.sort_values('rms_mm', ascending=False),
                        x='station_id',
                        y='rms_mm',
                        color='rms_color',
                        color_discrete_map={
                            'Good (<2mm)': '#2ECC71',
                            'Moderate (2-3mm)': '#F39C12',
                            'High (>3mm)': '#E74C3C'
                        },
                        title=f'A Posteriori RMS of Unit Weight - DOY {stats_doy}',
                        labels={'station_id': 'Station', 'rms_mm': 'RMS (mm)', 'rms_color': 'Quality'}
                    )
                    fig_rms.add_hline(y=2.0, line_dash="dash", line_color="orange",
                                      annotation_text="2mm threshold")
                    apply_colorful_style(fig_rms)
                    st.plotly_chart(fig_rms, use_container_width=True)

                    # Chi²/DOF by station
                    if 'chi2_dof' in df_stats_day.columns and df_stats_day['chi2_dof'].notna().any():
                        st.subheader("Chi²/DOF by Station")

                        df_stats_day['chi2_color'] = df_stats_day['chi2_dof'].apply(
                            lambda x: 'Good (0.8-1.2)' if 0.8 <= x <= 1.2 else ('Acceptable (0.5-1.5)' if 0.5 <= x <= 1.5 else 'Check Required')
                        )

                        fig_chi2 = px.bar(
                            df_stats_day.sort_values('chi2_dof', ascending=False),
                            x='station_id',
                            y='chi2_dof',
                            color='chi2_color',
                            color_discrete_map={
                                'Good (0.8-1.2)': '#2ECC71',
                                'Acceptable (0.5-1.5)': '#F39C12',
                                'Check Required': '#E74C3C'
                            },
                            title=f'Chi²/DOF - DOY {stats_doy}',
                            labels={'station_id': 'Station', 'chi2_dof': 'Chi²/DOF', 'chi2_color': 'Quality'}
                        )
                        fig_chi2.add_hline(y=1.0, line_dash="dash", line_color="green",
                                          annotation_text="Ideal (1.0)")
                        fig_chi2.add_hline(y=1.5, line_dash="dash", line_color="orange",
                                          annotation_text="Upper threshold (1.5)")
                        apply_colorful_style(fig_chi2)
                        st.plotly_chart(fig_chi2, use_container_width=True)

                    # Observations vs RMS scatter plot
                    st.subheader("Observations vs RMS Correlation")
                    fig_scatter = px.scatter(
                        df_stats_day,
                        x='num_observations',
                        y='rms_mm',
                        text='station_id',
                        color='rms_color',
                        color_discrete_map={
                            'Good (<2mm)': '#2ECC71',
                            'Moderate (2-3mm)': '#F39C12',
                            'High (>3mm)': '#E74C3C'
                        },
                        title='Number of Observations vs A Posteriori RMS',
                        labels={'num_observations': 'Observations', 'rms_mm': 'RMS (mm)'}
                    )
                    fig_scatter.update_traces(textposition='top center', marker=dict(size=12))
                    apply_colorful_style(fig_scatter)
                    st.plotly_chart(fig_scatter, use_container_width=True)

                    # Detailed data table
                    st.subheader("Detailed Processing Statistics")
                    display_stats = df_stats_day[['station_id', 'doy', 'rms_mm', 'chi2_dof', 'num_observations', 'dof']].copy()
                    display_stats.columns = ['Station', 'DOY', 'RMS (mm)', 'Chi²/DOF', 'Observations', 'DOF']
                    st.dataframe(
                        display_stats.style.format({
                            'RMS (mm)': '{:.3f}',
                            'Chi²/DOF': '{:.4f}'
                        }).background_gradient(subset=['RMS (mm)'], cmap='RdYlGn_r'),
                        use_container_width=True,
                        hide_index=True
                    )

                    # Time series of RMS across all DOYs
                    st.subheader("RMS Time Series (All DOYs)")
                    df_stats['rms_mm'] = df_stats['rms_unit_weight'] * 1000
                    fig_ts = px.line(
                        df_stats.sort_values(['station_id', 'doy']),
                        x='doy',
                        y='rms_mm',
                        color='station_id',
                        markers=True,
                        title='A Posteriori RMS Over Time',
                        labels={'doy': 'Day of Year', 'rms_mm': 'RMS (mm)', 'station_id': 'Station'}
                    )
                    fig_ts.add_hline(y=2.0, line_dash="dash", line_color="orange")
                    apply_colorful_style(fig_ts)
                    st.plotly_chart(fig_ts, use_container_width=True)

                else:
                    st.info(f"No processing statistics for DOY {stats_doy}. Run: `python -m frontend.ingest_all {stats_doy}`")
            else:
                st.info("No processing statistics data available. Run: `python -m frontend.ingest_all <start_doy> <end_doy>`")

        # ==========================================
        # TAB 8: Satellite-wise Ambiguity PRN
        # ==========================================
        with tab8:
            st.header("Satellite-wise Ambiguity Resolution by PRN")
            st.markdown("""
            Per-satellite ambiguity resolution statistics showing L1/L2 and L5 resolution rates for each PRN.
            - **GPS**: PRN 1-32
            - **Galileo**: PRN 201-236
            """)

            # Query satellite ambiguity PRN data
            sat_amb_prn_data = db.get_satellite_ambiguity_prn(year=year)

            if sat_amb_prn_data:
                df_sat_amb_prn = pd.DataFrame(sat_amb_prn_data)

                # DOY selector
                available_doys_sat = sorted(df_sat_amb_prn['doy'].unique())
                if available_doys_sat:
                    sat_amb_doy = st.selectbox(
                        "Select Day of Year",
                        available_doys_sat,
                        index=len(available_doys_sat) - 1,
                        key="sat_amb_prn_doy"
                    )

                    # Filter by selected DOY
                    df_day = df_sat_amb_prn[df_sat_amb_prn['doy'] == sat_amb_doy].copy()

                    if not df_day.empty:
                        # Summary metrics at top
                        col1, col2, col3, col4 = st.columns(4)

                        # GPS stats
                        df_gps = df_day[df_day['constellation'] == 'GPS']
                        if not df_gps.empty:
                            gps_l1l2_avg = df_gps['l1l2_rel'].mean()
                            gps_l5_avg = df_gps['l5_rel'].mean()
                            col1.metric("GPS L1/L2 Avg", f"{gps_l1l2_avg:.1f}%")
                            col2.metric("GPS L5 Avg", f"{gps_l5_avg:.1f}%")

                        # Galileo stats
                        df_gal = df_day[df_day['constellation'] == 'GAL']
                        if not df_gal.empty:
                            gal_l1l2_avg = df_gal['l1l2_rel'].mean()
                            gal_l5_avg = df_gal['l5_rel'].mean()
                            col3.metric("Galileo L1/L2 Avg", f"{gal_l1l2_avg:.1f}%")
                            col4.metric("Galileo L5 Avg", f"{gal_l5_avg:.1f}%")

                        # Create constellation selector
                        constellation_choice = st.radio(
                            "Constellation",
                            ["GPS", "Galileo", "Both"],
                            horizontal=True,
                            key="sat_amb_prn_const"
                        )

                        if constellation_choice == "GPS":
                            df_plot = df_gps.copy()
                        elif constellation_choice == "Galileo":
                            df_plot = df_gal.copy()
                        else:
                            df_plot = df_day.copy()

                        if not df_plot.empty:
                            # Create PRN labels (e.g., G01, E201)
                            df_plot['prn_label'] = df_plot.apply(
                                lambda r: f"G{r['prn']:02d}" if r['constellation'] == 'GPS' else f"E{r['prn']}",
                                axis=1
                            )
                            df_plot = df_plot.sort_values('prn')

                            # L1/L2 Resolution Rate Bar Chart
                            st.subheader("L1/L2 Ambiguity Resolution Rate by PRN")
                            fig_l1l2 = px.bar(
                                df_plot,
                                x='prn_label',
                                y='l1l2_rel',
                                color='constellation',
                                color_discrete_map={'GPS': '#2ecc71', 'GAL': '#3498db'},
                                title=f'L1/L2 Ambiguity Resolution Rate - DOY {sat_amb_doy}',
                                labels={'prn_label': 'PRN', 'l1l2_rel': 'Resolution Rate (%)', 'constellation': 'Constellation'},
                                hover_data=['amb_total', 'l1l2_solved']
                            )
                            fig_l1l2.add_hline(y=80, line_dash="dash", line_color="orange", annotation_text="80% threshold")
                            fig_l1l2.update_layout(
                                xaxis_tickangle=-45,
                                yaxis_range=[0, 105],
                                bargap=0.2
                            )
                            apply_colorful_style(fig_l1l2)
                            st.plotly_chart(fig_l1l2, use_container_width=True)

                            # L5 Resolution Rate Bar Chart
                            st.subheader("L5 Ambiguity Resolution Rate by PRN")
                            fig_l5 = px.bar(
                                df_plot,
                                x='prn_label',
                                y='l5_rel',
                                color='constellation',
                                color_discrete_map={'GPS': '#e74c3c', 'GAL': '#9b59b6'},
                                title=f'L5 Ambiguity Resolution Rate - DOY {sat_amb_doy}',
                                labels={'prn_label': 'PRN', 'l5_rel': 'Resolution Rate (%)', 'constellation': 'Constellation'},
                                hover_data=['amb_total', 'l5_solved']
                            )
                            fig_l5.add_hline(y=90, line_dash="dash", line_color="green", annotation_text="90% threshold")
                            fig_l5.update_layout(
                                xaxis_tickangle=-45,
                                yaxis_range=[0, 105],
                                bargap=0.2
                            )
                            apply_colorful_style(fig_l5)
                            st.plotly_chart(fig_l5, use_container_width=True)

                            # Combined comparison chart (L1/L2 vs L5)
                            st.subheader("L1/L2 vs L5 Resolution Comparison")
                            df_melt = df_plot.melt(
                                id_vars=['prn_label', 'constellation', 'prn'],
                                value_vars=['l1l2_rel', 'l5_rel'],
                                var_name='frequency',
                                value_name='resolution_rate'
                            )
                            df_melt['frequency'] = df_melt['frequency'].map({'l1l2_rel': 'L1/L2', 'l5_rel': 'L5'})

                            fig_compare = px.bar(
                                df_melt,
                                x='prn_label',
                                y='resolution_rate',
                                color='frequency',
                                barmode='group',
                                color_discrete_map={'L1/L2': '#3498db', 'L5': '#e74c3c'},
                                title=f'Frequency Comparison by PRN - DOY {sat_amb_doy}',
                                labels={'prn_label': 'PRN', 'resolution_rate': 'Resolution Rate (%)', 'frequency': 'Frequency'}
                            )
                            fig_compare.update_layout(
                                xaxis_tickangle=-45,
                                yaxis_range=[0, 105],
                                bargap=0.15,
                                bargroupgap=0.1
                            )
                            apply_colorful_style(fig_compare)
                            st.plotly_chart(fig_compare, use_container_width=True)

                            # Scatter plot: L1/L2 vs L5 correlation
                            st.subheader("L1/L2 vs L5 Resolution Correlation")
                            fig_scatter = px.scatter(
                                df_plot,
                                x='l1l2_rel',
                                y='l5_rel',
                                color='constellation',
                                text='prn_label',
                                color_discrete_map={'GPS': '#2ecc71', 'GAL': '#9b59b6'},
                                title='L1/L2 vs L5 Resolution Rate Correlation',
                                labels={'l1l2_rel': 'L1/L2 Resolution (%)', 'l5_rel': 'L5 Resolution (%)'},
                                hover_data=['amb_total']
                            )
                            fig_scatter.update_traces(textposition='top center', marker=dict(size=12))
                            fig_scatter.add_shape(
                                type='line', x0=0, y0=0, x1=100, y1=100,
                                line=dict(color='gray', dash='dash')
                            )
                            fig_scatter.update_layout(xaxis_range=[0, 105], yaxis_range=[0, 105])
                            apply_colorful_style(fig_scatter)
                            st.plotly_chart(fig_scatter, use_container_width=True)

                            # Detailed data table
                            st.subheader("Detailed Satellite Ambiguity Statistics")
                            display_cols = ['prn_label', 'constellation', 'amb_total', 'l1l2_solved', 'l1l2_rel', 'l5_solved', 'l5_rel']
                            df_display = df_plot[display_cols].copy()
                            df_display.columns = ['PRN', 'Constellation', 'Total Amb', 'L1/L2 Solved', 'L1/L2 %', 'L5 Solved', 'L5 %']

                            st.dataframe(
                                df_display.style.format({
                                    'L1/L2 %': '{:.1f}',
                                    'L5 %': '{:.1f}'
                                }).background_gradient(subset=['L1/L2 %'], cmap='RdYlGn', vmin=50, vmax=100
                                ).background_gradient(subset=['L5 %'], cmap='RdYlGn', vmin=50, vmax=100),
                                use_container_width=True,
                                hide_index=True
                            )
                        else:
                            st.info("No data for selected constellation.")
                    else:
                        st.info(f"No satellite ambiguity data for DOY {sat_amb_doy}")
                else:
                    st.info("No satellite ambiguity PRN data available.")
            else:
                st.info("No satellite ambiguity PRN data available. Run: `python -m frontend.ingest_all <start_doy> <end_doy>`")

        # ==========================================
        # TAB 9: Observation Residuals
        # ==========================================
        with tab9:
            st.header("Observation Residuals by Station & Satellite")
            st.markdown("""
            Per-station, per-satellite observation residual RMS from GPSEST processing (EDL_*.SUM files).
            Shows how well each satellite's observations fit the adjustment for each station.
            - **Lower RMS**: Better fit (typically 1-3 mm)
            - **Higher RMS**: Potential issues with satellite tracking or multipath
            """)

            # DOY selection for residuals
            col1, col2 = st.columns(2)
            with col1:
                res_doy = st.number_input(
                    "Select DOY",
                    min_value=1, max_value=366,
                    value=end_doy,
                    key='res_doy'
                )

            # Get station residuals data
            res_data = db.get_station_residuals(year=year, doy=res_doy)

            if res_data:
                df_res = pd.DataFrame(res_data)

                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    num_stations = df_res['station_id'].nunique()
                    st.metric("Stations", num_stations)
                with col2:
                    mean_rms = df_res['rms'].mean()
                    st.metric("Mean RMS", f"{mean_rms:.2f} mm")
                with col3:
                    gps_count = len(df_res[df_res['constellation'] == 'GPS']['prn'].unique())
                    st.metric("GPS Satellites", gps_count)
                with col4:
                    gal_count = len(df_res[df_res['constellation'] == 'GALILEO']['prn'].unique())
                    st.metric("Galileo Satellites", gal_count)

                st.divider()

                # Station selection
                res_stations = st.multiselect(
                    "Select stations to display",
                    options=sorted(df_res['station_id'].unique()),
                    default=sorted(df_res['station_id'].unique())[:5],
                    key='res_stations'
                )

                if res_stations:
                    df_res_filtered = df_res[df_res['station_id'].isin(res_stations)]

                    # Constellation colors
                    constellation_colors = {
                        'GPS': '#00d4ff',
                        'GLONASS': '#ff6b6b',
                        'GALILEO': '#4ecdc4'
                    }

                    # Heatmap: Station vs PRN residuals
                    st.subheader("Residual RMS Heatmap (Station vs PRN)")

                    # Pivot for heatmap
                    df_pivot = df_res_filtered.pivot_table(
                        values='rms',
                        index='station_id',
                        columns='prn',
                        aggfunc='mean'
                    )

                    fig_heatmap = px.imshow(
                        df_pivot,
                        title=f'Observation Residual RMS by Station & PRN - DOY {res_doy}',
                        labels=dict(x='PRN', y='Station', color='RMS (mm)'),
                        color_continuous_scale='RdYlGn_r',
                        aspect='auto'
                    )
                    apply_colorful_style(fig_heatmap)
                    st.plotly_chart(fig_heatmap, use_container_width=True)

                    # Bar chart: Mean RMS by station
                    st.subheader("Mean Residual RMS by Station")
                    station_rms = df_res_filtered.groupby('station_id')['rms'].mean().reset_index()
                    station_rms['rms_color'] = station_rms['rms'].apply(
                        lambda x: 'Good (<2mm)' if x < 2 else ('Moderate (2-3mm)' if x < 3 else 'High (>3mm)')
                    )

                    fig_station = px.bar(
                        station_rms.sort_values('rms', ascending=False),
                        x='station_id',
                        y='rms',
                        color='rms_color',
                        color_discrete_map={
                            'Good (<2mm)': '#2ECC71',
                            'Moderate (2-3mm)': '#F39C12',
                            'High (>3mm)': '#E74C3C'
                        },
                        title=f'Mean Observation Residual RMS by Station - DOY {res_doy}',
                        labels={'station_id': 'Station', 'rms': 'Mean RMS (mm)', 'rms_color': 'Quality'}
                    )
                    fig_station.add_hline(y=2.0, line_dash="dash", line_color="orange",
                                          annotation_text="2mm threshold")
                    apply_colorful_style(fig_station)
                    st.plotly_chart(fig_station, use_container_width=True)

                    # Bar chart: RMS by PRN (constellation colored)
                    st.subheader("Residual RMS by Satellite PRN")
                    prn_rms = df_res_filtered.groupby(['prn', 'constellation'])['rms'].mean().reset_index()

                    fig_prn = px.bar(
                        prn_rms.sort_values(['constellation', 'prn']),
                        x='prn',
                        y='rms',
                        color='constellation',
                        color_discrete_map=constellation_colors,
                        title=f'Observation Residual RMS by PRN - DOY {res_doy}',
                        labels={'prn': 'PRN', 'rms': 'Mean RMS (mm)', 'constellation': 'Constellation'}
                    )
                    fig_prn.add_hline(y=2.0, line_dash="dash", line_color="orange")
                    apply_colorful_style(fig_prn)
                    st.plotly_chart(fig_prn, use_container_width=True)

                    # Detailed data table
                    st.subheader("Detailed Residual Statistics")
                    display_res = df_res_filtered[['station_id', 'prn', 'constellation', 'rms']].copy()
                    display_res.columns = ['Station', 'PRN', 'Constellation', 'RMS (mm)']
                    st.dataframe(
                        display_res.style.format({'RMS (mm)': '{:.2f}'}).background_gradient(
                            subset=['RMS (mm)'], cmap='RdYlGn_r', vmin=0, vmax=5
                        ),
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.info("Please select at least one station.")
            else:
                st.info(f"No observation residual data for DOY {res_doy}. Run: `python -m frontend.ingest_all {res_doy}`")

        # ==========================================
        # TAB 10: Data Availability
        # ==========================================
        with tab10:
            st.header("Satellite Data Availability")
            st.markdown("""
            Satellite observation statistics from RESCHK (CHK_*.SUM files).
            Shows observation counts and percentages for each satellite after outlier rejection.
            """)

            # DOY selection
            col1, col2 = st.columns(2)
            with col1:
                avail_doy = st.number_input(
                    "Select DOY",
                    min_value=1, max_value=366,
                    value=end_doy,
                    key='avail_doy'
                )

            # Get data availability
            avail_data = db.get_data_availability(year=year, doy=avail_doy)

            if avail_data:
                df_avail = pd.DataFrame(avail_data)

                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    total_obs = df_avail['obs_count_after'].sum()
                    st.metric("Total Observations", f"{total_obs:,}")
                with col2:
                    gps_count = len(df_avail[df_avail['constellation'] == 'GPS'])
                    st.metric("GPS Satellites", gps_count)
                with col3:
                    glo_count = len(df_avail[df_avail['constellation'] == 'GLONASS'])
                    st.metric("GLONASS Satellites", glo_count)
                with col4:
                    gal_count = len(df_avail[df_avail['constellation'] == 'GALILEO'])
                    st.metric("Galileo Satellites", gal_count)

                st.divider()

                # Constellation colors
                constellation_colors = {
                    'GPS': '#00d4ff',
                    'GLONASS': '#ff6b6b',
                    'GALILEO': '#4ecdc4'
                }

                # Observation count by PRN
                st.subheader("Observation Count by Satellite PRN")
                fig_obs = px.bar(
                    df_avail.sort_values(['constellation', 'prn']),
                    x='prn',
                    y='obs_count_after',
                    color='constellation',
                    color_discrete_map=constellation_colors,
                    title=f'Observation Count by PRN (After Screening) - DOY {avail_doy}',
                    labels={'prn': 'PRN', 'obs_count_after': 'Observations', 'constellation': 'Constellation'}
                )
                apply_colorful_style(fig_obs)
                st.plotly_chart(fig_obs, use_container_width=True)

                # Observation percentage distribution
                st.subheader("Observation Percentage Distribution")
                fig_pct = px.bar(
                    df_avail.sort_values(['constellation', 'prn']),
                    x='prn',
                    y='obs_pct_after',
                    color='constellation',
                    color_discrete_map=constellation_colors,
                    title=f'Observation Percentage by PRN - DOY {avail_doy}',
                    labels={'prn': 'PRN', 'obs_pct_after': 'Observation %', 'constellation': 'Constellation'}
                )
                apply_colorful_style(fig_pct)
                st.plotly_chart(fig_pct, use_container_width=True)

                # RMS by satellite
                st.subheader("RMS by Satellite PRN")
                fig_rms = px.bar(
                    df_avail.sort_values(['constellation', 'prn']),
                    x='prn',
                    y='rms_after',
                    color='constellation',
                    color_discrete_map=constellation_colors,
                    title=f'RMS by PRN (After Screening) - DOY {avail_doy}',
                    labels={'prn': 'PRN', 'rms_after': 'RMS (mm)', 'constellation': 'Constellation'}
                )
                fig_rms.add_hline(y=3.0, line_dash="dash", line_color="#ffe66d",
                                  annotation_text="3mm threshold")
                apply_colorful_style(fig_rms)
                st.plotly_chart(fig_rms, use_container_width=True)

                # Pie chart: Observations by constellation
                st.subheader("Observation Distribution by Constellation")
                obs_by_const = df_avail.groupby('constellation')['obs_count_after'].sum().reset_index()
                fig_pie = px.pie(
                    obs_by_const,
                    values='obs_count_after',
                    names='constellation',
                    title='Total Observations by Constellation',
                    color='constellation',
                    color_discrete_map=constellation_colors
                )
                fig_pie.update_traces(textposition='inside', textinfo='percent+label+value')
                apply_colorful_style(fig_pie)
                st.plotly_chart(fig_pie, use_container_width=True)

                # Detailed table
                st.subheader("Detailed Data Availability Statistics")
                display_avail = df_avail[['prn', 'constellation', 'obs_pct_after', 'obs_count_after', 'rms_after']].copy()
                display_avail.columns = ['PRN', 'Constellation', 'Obs %', 'Obs Count', 'RMS (mm)']
                st.dataframe(
                    display_avail.style.format({
                        'Obs %': '{:.2f}',
                        'RMS (mm)': '{:.1f}'
                    }),
                    use_container_width=True,
                    hide_index=True
                )

                # Statistics by constellation
                st.subheader("Statistics by Constellation")
                const_stats = df_avail.groupby('constellation').agg({
                    'prn': 'count',
                    'obs_count_after': ['sum', 'mean'],
                    'rms_after': ['mean', 'min', 'max']
                }).round(2)
                const_stats.columns = ['Satellites', 'Total Obs', 'Mean Obs', 'Mean RMS', 'Min RMS', 'Max RMS']
                st.dataframe(const_stats, use_container_width=True)

            else:
                st.info(f"No data availability statistics for DOY {avail_doy}. Run: `python -m frontend.ingest_all {avail_doy}`")

        # ==========================================
        # TAB 11: Outlier Statistics
        # ==========================================
        with tab11:
            st.header("Outlier Rejection Statistics")
            st.markdown("""
            Comparison of observations **before** and **after** outlier rejection from RESCHK.
            Identifies satellites with high rejection rates that may indicate tracking issues.
            - **Rejection Rate** = (Before - After) / Before x 100%
            - **High rejection** (>5%) may indicate satellite health issues, multipath, or cycle slips
            """)

            # DOY selection
            col1, col2 = st.columns(2)
            with col1:
                outlier_doy = st.number_input(
                    "Select DOY",
                    min_value=1, max_value=366,
                    value=end_doy,
                    key='outlier_doy'
                )

            # Get data availability (contains before/after stats)
            outlier_data = db.get_data_availability(year=year, doy=outlier_doy)

            if outlier_data:
                df_outlier = pd.DataFrame(outlier_data)

                # Calculate rejection metrics
                df_outlier['obs_rejected'] = df_outlier['obs_count_before'] - df_outlier['obs_count_after']
                df_outlier['rejection_rate'] = (df_outlier['obs_rejected'] / df_outlier['obs_count_before'] * 100).fillna(0)
                df_outlier['rms_change'] = df_outlier['rms_after'] - df_outlier['rms_before']

                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    total_rejected = df_outlier['obs_rejected'].sum()
                    st.metric("Total Rejected", f"{total_rejected:,}")
                with col2:
                    avg_rejection = df_outlier['rejection_rate'].mean()
                    st.metric("Avg Rejection Rate", f"{avg_rejection:.2f}%")
                with col3:
                    high_reject = len(df_outlier[df_outlier['rejection_rate'] > 5])
                    st.metric("High Rejection PRNs (>5%)", high_reject)
                with col4:
                    max_reject = df_outlier['rejection_rate'].max()
                    st.metric("Max Rejection Rate", f"{max_reject:.2f}%")

                st.divider()

                # Constellation colors
                constellation_colors = {
                    'GPS': '#00d4ff',
                    'GLONASS': '#ff6b6b',
                    'GALILEO': '#4ecdc4'
                }

                # Rejection rate by PRN
                st.subheader("Rejection Rate by Satellite PRN")
                df_outlier['reject_color'] = df_outlier['rejection_rate'].apply(
                    lambda x: 'Good (<1%)' if x < 1 else ('Moderate (1-5%)' if x < 5 else 'High (>5%)')
                )

                fig_reject = px.bar(
                    df_outlier.sort_values(['constellation', 'prn']),
                    x='prn',
                    y='rejection_rate',
                    color='constellation',
                    color_discrete_map=constellation_colors,
                    title=f'Observation Rejection Rate by PRN - DOY {outlier_doy}',
                    labels={'prn': 'PRN', 'rejection_rate': 'Rejection Rate (%)', 'constellation': 'Constellation'},
                    hover_data=['obs_count_before', 'obs_count_after', 'obs_rejected']
                )
                fig_reject.add_hline(y=1.0, line_dash="dash", line_color="#4ecdc4",
                                     annotation_text="1% threshold")
                fig_reject.add_hline(y=5.0, line_dash="dash", line_color="#ff6b6b",
                                     annotation_text="5% threshold")
                apply_colorful_style(fig_reject)
                st.plotly_chart(fig_reject, use_container_width=True)

                # Before vs After comparison
                st.subheader("Before vs After Observation Count Comparison")
                df_melt = df_outlier.melt(
                    id_vars=['prn', 'constellation'],
                    value_vars=['obs_count_before', 'obs_count_after'],
                    var_name='stage',
                    value_name='obs_count'
                )
                df_melt['stage'] = df_melt['stage'].map({
                    'obs_count_before': 'Before Screening',
                    'obs_count_after': 'After Screening'
                })

                fig_compare = px.bar(
                    df_melt.sort_values(['constellation', 'prn']),
                    x='prn',
                    y='obs_count',
                    color='stage',
                    barmode='group',
                    color_discrete_map={
                        'Before Screening': '#95a5a6',
                        'After Screening': '#2ecc71'
                    },
                    title=f'Observations Before vs After Screening - DOY {outlier_doy}',
                    labels={'prn': 'PRN', 'obs_count': 'Observations', 'stage': 'Stage'}
                )
                apply_colorful_style(fig_compare)
                st.plotly_chart(fig_compare, use_container_width=True)

                # RMS Before vs After
                st.subheader("RMS Before vs After Screening")
                df_rms_melt = df_outlier.melt(
                    id_vars=['prn', 'constellation'],
                    value_vars=['rms_before', 'rms_after'],
                    var_name='stage',
                    value_name='rms'
                )
                df_rms_melt['stage'] = df_rms_melt['stage'].map({
                    'rms_before': 'Before Screening',
                    'rms_after': 'After Screening'
                })

                fig_rms_compare = px.bar(
                    df_rms_melt.sort_values(['constellation', 'prn']),
                    x='prn',
                    y='rms',
                    color='stage',
                    barmode='group',
                    color_discrete_map={
                        'Before Screening': '#e74c3c',
                        'After Screening': '#2ecc71'
                    },
                    title=f'RMS Before vs After Screening - DOY {outlier_doy}',
                    labels={'prn': 'PRN', 'rms': 'RMS (mm)', 'stage': 'Stage'}
                )
                fig_rms_compare.add_hline(y=3.0, line_dash="dash", line_color="#ffe66d",
                                          annotation_text="3mm threshold")
                apply_colorful_style(fig_rms_compare)
                st.plotly_chart(fig_rms_compare, use_container_width=True)

                # Satellites with high rejection rates (attention needed)
                st.subheader("Satellites Needing Attention (Rejection Rate > 1%)")
                high_reject_df = df_outlier[df_outlier['rejection_rate'] > 1][
                    ['prn', 'constellation', 'obs_count_before', 'obs_count_after', 'obs_rejected', 'rejection_rate', 'rms_before', 'rms_after']
                ].copy()

                if len(high_reject_df) > 0:
                    high_reject_df.columns = ['PRN', 'Constellation', 'Obs Before', 'Obs After', 'Rejected', 'Rejection %', 'RMS Before', 'RMS After']
                    st.dataframe(
                        high_reject_df.sort_values('Rejection %', ascending=False).style.format({
                            'Rejection %': '{:.2f}',
                            'RMS Before': '{:.1f}',
                            'RMS After': '{:.1f}'
                        }).background_gradient(subset=['Rejection %'], cmap='Reds'),
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.success("All satellites have rejection rates below 1%!")

                # Detailed table
                st.subheader("Detailed Outlier Statistics")
                display_outlier = df_outlier[['prn', 'constellation', 'obs_count_before', 'obs_count_after',
                                               'obs_rejected', 'rejection_rate', 'rms_before', 'rms_after']].copy()
                display_outlier.columns = ['PRN', 'Constellation', 'Before', 'After', 'Rejected', 'Reject %', 'RMS Bef', 'RMS Aft']
                st.dataframe(
                    display_outlier.style.format({
                        'Reject %': '{:.2f}',
                        'RMS Bef': '{:.1f}',
                        'RMS Aft': '{:.1f}'
                    }),
                    use_container_width=True,
                    hide_index=True
                )

            else:
                st.info(f"No outlier statistics for DOY {outlier_doy}. Run: `python -m frontend.ingest_all {outlier_doy}`")

        # ==================================
        # TAB 12: TROPOSPHERIC GRADIENT MONITORING
        # ==================================
        with tab12:
            st.header("Tropospheric Gradient Monitoring")
            st.markdown("""
            Monitor tropospheric horizontal gradients (North/East components) which indicate
            atmospheric asymmetry. Large gradients often correlate with weather fronts and
            precipitation events.
            """)

            # Day selection for gradients
            col1, col2 = st.columns(2)
            with col1:
                grad_doy = st.number_input(
                    "Select DOY",
                    min_value=1, max_value=366,
                    value=end_doy,
                    key='grad_doy'
                )

            if grad_doy:
                # Get stations with gradient data
                grad_stations = db.get_gradient_stations(year, grad_doy)

                if grad_stations:
                    # Station selector
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        selected_grad_stations = st.multiselect(
                            "Select Stations",
                            options=grad_stations,
                            default=grad_stations[:min(5, len(grad_stations))],
                            key="grad_station_select"
                        )

                    if selected_grad_stations:
                        # Fetch gradient data for selected stations
                        all_grad_data = []
                        for sta in selected_grad_stations:
                            data = db.get_tropospheric_gradients(year, grad_doy, sta)
                            all_grad_data.extend(data)

                        if all_grad_data:
                            df_grad = pd.DataFrame(all_grad_data)

                            # Convert hour to datetime for plotting
                            df_grad['datetime'] = pd.to_datetime(
                                df_grad['hour'].apply(lambda x: f"{year}-{grad_doy:03d}") + ' ' +
                                df_grad['hour'].apply(lambda x: f"{int(x):02d}:00:00"),
                                format='%Y-%j %H:%M:%S'
                            )

                            # Row 1: North and East Gradient Time Series
                            st.subheader("Gradient Time Series")
                            col1, col2 = st.columns(2)

                            with col1:
                                # North Gradient (GN)
                                fig_gn = px.line(df_grad, x='datetime', y='grad_n',
                                                 color='station_id',
                                                 title="North Gradient (GN) Time Series",
                                                 labels={'grad_n': 'North Gradient (mm)', 'datetime': 'Time (UTC)'})
                                fig_gn.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
                                apply_colorful_style(fig_gn)
                                st.plotly_chart(fig_gn, use_container_width=True)

                            with col2:
                                # East Gradient (GE)
                                fig_ge = px.line(df_grad, x='datetime', y='grad_e',
                                                 color='station_id',
                                                 title="East Gradient (GE) Time Series",
                                                 labels={'grad_e': 'East Gradient (mm)', 'datetime': 'Time (UTC)'})
                                fig_ge.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
                                apply_colorful_style(fig_ge)
                                st.plotly_chart(fig_ge, use_container_width=True)

                            # Row 2: Gradient Magnitude and Azimuth
                            st.subheader("Gradient Magnitude & Direction")
                            col1, col2 = st.columns(2)

                            with col1:
                                # Gradient Magnitude
                                fig_mag = px.line(df_grad, x='datetime', y='grad_magnitude',
                                                  color='station_id',
                                                  title="Gradient Magnitude Time Series",
                                                  labels={'grad_magnitude': 'Magnitude (mm)', 'datetime': 'Time (UTC)'})
                                # Add threshold line for potential weather event indication
                                fig_mag.add_hline(y=1.0, line_dash="dash", line_color="red", opacity=0.7,
                                                  annotation_text="Weather Alert Threshold")
                                apply_colorful_style(fig_mag)
                                st.plotly_chart(fig_mag, use_container_width=True)

                            with col2:
                                # Gradient Azimuth (direction of maximum gradient)
                                fig_az = px.scatter(df_grad, x='datetime', y='grad_azimuth',
                                                    color='station_id',
                                                    title="Gradient Azimuth (Direction)",
                                                    labels={'grad_azimuth': 'Azimuth (degrees)', 'datetime': 'Time (UTC)'})
                                apply_colorful_style(fig_az)
                                fig_az.update_yaxes(range=[0, 360])
                                st.plotly_chart(fig_az, use_container_width=True)

                            # Row 3: Polar plot of gradients (wind rose style)
                            st.subheader("Gradient Vector Distribution")

                            # Select single station for polar plot
                            polar_station = st.selectbox("Select Station for Polar Plot",
                                                         options=selected_grad_stations,
                                                         key="polar_station")

                            df_polar = df_grad[df_grad['station_id'] == polar_station].copy()

                            if len(df_polar) > 0:
                                fig_polar = go.Figure()
                                fig_polar.add_trace(go.Scatterpolar(
                                    r=df_polar['grad_magnitude'],
                                    theta=df_polar['grad_azimuth'],
                                    mode='markers',
                                    marker=dict(
                                        size=8,
                                        color=df_polar['hour'],
                                        colorscale='Viridis',
                                        showscale=True,
                                        colorbar=dict(title="Hour (UTC)")
                                    ),
                                    text=df_polar['datetime'].dt.strftime('%H:%M'),
                                    hovertemplate="Magnitude: %{r:.2f} mm<br>Azimuth: %{theta:.1f}°<br>Time: %{text}<extra></extra>"
                                ))
                                fig_polar.update_layout(
                                    title=f"Gradient Vectors - {polar_station}",
                                    polar=dict(
                                        radialaxis=dict(visible=True, title="Magnitude (mm)"),
                                        angularaxis=dict(direction="clockwise", rotation=90)
                                    ),
                                    showlegend=False,
                                    height=500
                                )
                                st.plotly_chart(fig_polar, use_container_width=True)

                            # Statistics table
                            st.subheader("Gradient Statistics Summary")
                            stats_data = []
                            for sta in selected_grad_stations:
                                sta_df = df_grad[df_grad['station_id'] == sta]
                                if len(sta_df) > 0:
                                    stats_data.append({
                                        'Station': sta,
                                        'GN Mean (mm)': sta_df['grad_n'].mean(),
                                        'GN Std (mm)': sta_df['grad_n'].std(),
                                        'GE Mean (mm)': sta_df['grad_e'].mean(),
                                        'GE Std (mm)': sta_df['grad_e'].std(),
                                        'Max Magnitude (mm)': sta_df['grad_magnitude'].max(),
                                        'Samples': len(sta_df)
                                    })

                            if stats_data:
                                df_stats = pd.DataFrame(stats_data)
                                st.dataframe(
                                    df_stats.style.format({
                                        'GN Mean (mm)': '{:.3f}',
                                        'GN Std (mm)': '{:.3f}',
                                        'GE Mean (mm)': '{:.3f}',
                                        'GE Std (mm)': '{:.3f}',
                                        'Max Magnitude (mm)': '{:.3f}'
                                    }).background_gradient(subset=['Max Magnitude (mm)'], cmap='YlOrRd'),
                                    use_container_width=True,
                                    hide_index=True
                                )

                        else:
                            st.warning("No gradient data found for selected stations.")
                    else:
                        st.info("Please select at least one station.")
                else:
                    st.info(f"No gradient data for DOY {grad_doy}. Run: `python -m frontend.ingest_all {grad_doy}`")

        # ==================================
        # TAB 13: RECEIVER CLOCK ANALYSIS
        # ==================================
        with tab13:
            st.header("Receiver Clock Analysis")
            st.markdown("""
            Analyze receiver clock behavior including offset time series, drift rates,
            and stability metrics. Clock quality affects positioning accuracy and can
            indicate hardware issues.
            """)

            # Day selection for clock analysis
            col1, col2 = st.columns(2)
            with col1:
                clk_doy = st.number_input(
                    "Select DOY",
                    min_value=1, max_value=366,
                    value=end_doy,
                    key='clk_doy'
                )

            if clk_doy:
                # Get stations with clock data
                clk_stations = db.get_clock_stations(year, clk_doy)

                if clk_stations:
                    # Station selector
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        selected_clk_stations = st.multiselect(
                            "Select Stations",
                            options=clk_stations,
                            default=clk_stations[:min(5, len(clk_stations))],
                            key="clk_station_select"
                        )

                    if selected_clk_stations:
                        # Fetch clock data for selected stations
                        all_clk_data = []
                        for sta in selected_clk_stations:
                            data = db.get_receiver_clocks(year, clk_doy, sta)
                            all_clk_data.extend(data)

                        if all_clk_data:
                            df_clk = pd.DataFrame(all_clk_data)

                            # Convert epoch (seconds of day) to datetime
                            df_clk['datetime'] = pd.to_datetime(
                                df_clk.apply(lambda r: f"{year}-{clk_doy:03d}", axis=1),
                                format='%Y-%j'
                            ) + pd.to_timedelta(df_clk['epoch'], unit='s')

                            # Convert clock offset from seconds to microseconds for display
                            df_clk['clock_offset_us'] = df_clk['clock_offset'] * 1e6

                            # Row 1: Clock Offset Time Series
                            st.subheader("Clock Offset Time Series")
                            fig_offset = px.line(df_clk, x='datetime', y='clock_offset_us',
                                                 color='station_id',
                                                 title="Receiver Clock Offset",
                                                 labels={'clock_offset_us': 'Clock Offset (µs)', 'datetime': 'Time (UTC)'})
                            apply_colorful_style(fig_offset)
                            st.plotly_chart(fig_offset, use_container_width=True)

                            # Row 2: Clock Drift Analysis
                            st.subheader("Clock Drift Rate")
                            col1, col2 = st.columns(2)

                            # Calculate drift for each station
                            drift_data = []
                            for sta in selected_clk_stations:
                                sta_df = df_clk[df_clk['station_id'] == sta].sort_values('epoch')
                                if len(sta_df) > 1:
                                    # Calculate drift (derivative of clock offset)
                                    sta_df = sta_df.copy()
                                    sta_df['drift_ns_s'] = np.gradient(sta_df['clock_offset'].values * 1e9,
                                                                        sta_df['epoch'].values)
                                    drift_data.append(sta_df)

                            if drift_data:
                                df_drift = pd.concat(drift_data)

                                with col1:
                                    fig_drift = px.line(df_drift, x='datetime', y='drift_ns_s',
                                                        color='station_id',
                                                        title="Clock Drift Rate",
                                                        labels={'drift_ns_s': 'Drift Rate (ns/s)', 'datetime': 'Time (UTC)'})
                                    apply_colorful_style(fig_drift)
                                    st.plotly_chart(fig_drift, use_container_width=True)

                                with col2:
                                    # Drift distribution histogram
                                    fig_drift_hist = px.histogram(df_drift, x='drift_ns_s',
                                                                   color='station_id',
                                                                   nbins=50,
                                                                   title="Drift Rate Distribution",
                                                                   labels={'drift_ns_s': 'Drift Rate (ns/s)'})
                                    apply_colorful_style(fig_drift_hist)
                                    st.plotly_chart(fig_drift_hist, use_container_width=True)

                            # Row 3: Allan Deviation (Clock Stability)
                            st.subheader("Clock Stability (Allan Deviation)")
                            st.markdown("""
                            Allan deviation measures clock stability over different averaging intervals (tau).
                            Lower values indicate better clock stability.
                            """)

                            def compute_allan_deviation(data: np.ndarray, rate: float = 1.0,
                                                       taus: list = None) -> tuple:
                                """Compute overlapping Allan deviation."""
                                if taus is None:
                                    max_tau_idx = int(np.floor(len(data) / 2))
                                    taus = np.unique(np.logspace(0, np.log10(max_tau_idx), 20).astype(int))
                                    taus = taus[taus > 0]

                                adevs = []
                                valid_taus = []

                                for m in taus:
                                    if 2 * m > len(data):
                                        continue
                                    # Overlapping Allan deviation
                                    phase = np.cumsum(data) / rate
                                    d = phase[2*m:] - 2*phase[m:-m] + phase[:-2*m]
                                    if len(d) > 0:
                                        adev = np.sqrt(np.mean(d**2) / (2 * (m / rate)**2))
                                        adevs.append(adev)
                                        valid_taus.append(m / rate)

                                return np.array(valid_taus), np.array(adevs)

                            # Calculate Allan deviation for each station
                            allan_data = []
                            for sta in selected_clk_stations:
                                sta_df = df_clk[df_clk['station_id'] == sta].sort_values('epoch')
                                if len(sta_df) > 10:
                                    # Get clock offsets and sampling rate
                                    offsets = sta_df['clock_offset'].values
                                    epochs = sta_df['epoch'].values
                                    if len(epochs) > 1:
                                        avg_interval = np.median(np.diff(epochs))
                                        rate = 1.0 / avg_interval if avg_interval > 0 else 1.0

                                        taus, adevs = compute_allan_deviation(offsets, rate)
                                        for tau, adev in zip(taus, adevs):
                                            allan_data.append({
                                                'station_id': sta,
                                                'tau': tau,
                                                'adev': adev
                                            })

                            if allan_data:
                                df_allan = pd.DataFrame(allan_data)
                                fig_allan = px.line(df_allan, x='tau', y='adev',
                                                    color='station_id',
                                                    log_x=True, log_y=True,
                                                    title="Allan Deviation vs Averaging Time",
                                                    labels={'tau': 'Averaging Time τ (s)', 'adev': 'Allan Deviation (s)'})
                                apply_colorful_style(fig_allan)
                                st.plotly_chart(fig_allan, use_container_width=True)
                            else:
                                st.info("Insufficient data for Allan deviation calculation.")

                            # Statistics table
                            st.subheader("Clock Statistics Summary")
                            clk_stats = []
                            for sta in selected_clk_stations:
                                sta_df = df_clk[df_clk['station_id'] == sta]
                                if len(sta_df) > 0:
                                    offsets = sta_df['clock_offset'].values
                                    # Calculate drift rate (linear fit)
                                    epochs = sta_df['epoch'].values
                                    if len(epochs) > 1:
                                        coeffs = np.polyfit(epochs, offsets, 1)
                                        drift_rate = coeffs[0] * 1e9  # ns/s
                                    else:
                                        drift_rate = 0

                                    clk_stats.append({
                                        'Station': sta,
                                        'Mean Offset (µs)': offsets.mean() * 1e6,
                                        'Std Offset (ns)': offsets.std() * 1e9,
                                        'Drift Rate (ns/s)': drift_rate,
                                        'Range (µs)': (offsets.max() - offsets.min()) * 1e6,
                                        'Samples': len(sta_df)
                                    })

                            if clk_stats:
                                df_clk_stats = pd.DataFrame(clk_stats)
                                st.dataframe(
                                    df_clk_stats.style.format({
                                        'Mean Offset (µs)': '{:.3f}',
                                        'Std Offset (ns)': '{:.2f}',
                                        'Drift Rate (ns/s)': '{:.4f}',
                                        'Range (µs)': '{:.3f}'
                                    }),
                                    use_container_width=True,
                                    hide_index=True
                                )

                        else:
                            st.warning("No clock data found for selected stations.")
                    else:
                        st.info("Please select at least one station.")
                else:
                    st.info(f"No receiver clock data for DOY {clk_doy}. Clock data is parsed from RINEX CLK files (FIN_*.CLK).")

        # ========================================
        # TAB 14: Station Data Completeness
        # ========================================
        with tab14:
            st.header("Station Data Completeness")
            st.markdown("""
            Monitor station-level data completeness showing:
            - **Expected vs Actual Observations**: Compare theoretical maximum with actual processed observations
            - **Data Gaps Timeline**: Visualize when stations have missing data
            - **Rejection Statistics**: Track percentage of observations rejected during quality screening
            """)

            # Controls
            col1, col2 = st.columns(2)
            with col1:
                comp_view = st.radio(
                    "View Mode",
                    ["Single Day", "Multi-Day Timeline"],
                    key='completeness_view'
                )

            # Get data from ZTD (as proxy for data completeness)
            if comp_view == "Single Day":
                with col2:
                    comp_doy = st.number_input(
                        "Select DOY",
                        min_value=1, max_value=366,
                        value=end_doy,
                        key='comp_doy'
                    )

                # Compute completeness from ZTD data
                completeness_data = db.compute_station_completeness_from_ztd(year, comp_doy)

                if completeness_data:
                    # Convert to dataframe
                    df_comp = pd.DataFrame([{
                        'station_id': c.station_id,
                        'completeness_pct': c.completeness_pct,
                        'gap_hours': c.gap_hours,
                        'gap_count': c.gap_count,
                        'first_epoch': c.first_epoch,
                        'last_epoch': c.last_epoch,
                        'quality_flag': c.quality_flag,
                        'has_full_day': c.has_full_day
                    } for c in completeness_data])

                    # Summary metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Stations", len(df_comp))
                    with col2:
                        good_count = len(df_comp[df_comp['quality_flag'] == 'GOOD'])
                        st.metric("Good Quality", good_count, delta=f"{good_count/len(df_comp)*100:.0f}%")
                    with col3:
                        partial_count = len(df_comp[df_comp['quality_flag'] == 'PARTIAL'])
                        st.metric("Partial Data", partial_count)
                    with col4:
                        poor_count = len(df_comp[df_comp['quality_flag'] == 'POOR'])
                        st.metric("Poor Quality", poor_count, delta=f"-{poor_count}" if poor_count > 0 else "0", delta_color="inverse")

                    st.divider()

                    # Quality flag colors
                    quality_colors = {'GOOD': '#26de81', 'PARTIAL': '#ffa502', 'POOR': '#ff4757'}

                    # 1. Completeness bar chart
                    st.subheader("Data Completeness by Station")
                    df_sorted = df_comp.sort_values('completeness_pct', ascending=True)
                    fig_comp = px.bar(
                        df_sorted,
                        x='completeness_pct',
                        y='station_id',
                        color='quality_flag',
                        color_discrete_map=quality_colors,
                        orientation='h',
                        title=f'Station Data Completeness - DOY {comp_doy}',
                        labels={'completeness_pct': 'Completeness (%)', 'station_id': 'Station', 'quality_flag': 'Quality'}
                    )
                    fig_comp.add_vline(x=95, line_dash="dash", line_color="#26de81", annotation_text="95% threshold")
                    fig_comp.add_vline(x=70, line_dash="dash", line_color="#ffa502", annotation_text="70% threshold")
                    apply_colorful_style(fig_comp)
                    fig_comp.update_layout(height=max(400, len(df_comp) * 25))
                    st.plotly_chart(fig_comp, use_container_width=True)

                    # 2. Hourly data heatmap (timeline view)
                    st.subheader("Hourly Data Availability Heatmap")

                    # Get hourly ZTD data for the day
                    ztd_data = []
                    for station in df_comp['station_id'].tolist():
                        ztd_rows = db.get_ztd(station, year, comp_doy)
                        for row in ztd_rows:
                            # Valid if ZTD sigma < 100mm (0.1m)
                            valid = 1 if row.get('ztd_rms', 1) < 0.1 else 0
                            ztd_data.append({
                                'station_id': station,
                                'hour': row.get('hour', 0),
                                'valid': valid
                            })

                    if ztd_data:
                        df_ztd = pd.DataFrame(ztd_data)

                        # Create pivot table for heatmap
                        pivot = df_ztd.pivot_table(index='station_id', columns='hour', values='valid', aggfunc='first', fill_value=0)

                        # Ensure all hours 0-23 are present
                        for h in range(24):
                            if h not in pivot.columns:
                                pivot[h] = 0
                        pivot = pivot.reindex(columns=range(24))

                        fig_heatmap = go.Figure(data=go.Heatmap(
                            z=pivot.values,
                            x=[f'{h:02d}:00' for h in range(24)],
                            y=pivot.index.tolist(),
                            colorscale=[[0, '#ff4757'], [1, '#26de81']],
                            showscale=False,
                            hovertemplate='Station: %{y}<br>Hour: %{x}<br>Status: %{customdata}<extra></extra>',
                            customdata=[['Gap' if v == 0 else 'Valid' for v in row] for row in pivot.values]
                        ))
                        fig_heatmap.update_layout(
                            title=f'Hourly Data Availability - DOY {comp_doy} (Green=Valid, Red=Gap)',
                            xaxis_title='Hour (UTC)',
                            yaxis_title='Station',
                            height=max(400, len(pivot) * 25)
                        )
                        apply_colorful_style(fig_heatmap)
                        st.plotly_chart(fig_heatmap, use_container_width=True)

                    # 3. Gap statistics
                    st.subheader("Data Gap Statistics")
                    col1, col2 = st.columns(2)

                    with col1:
                        # Pie chart of quality distribution
                        quality_counts = df_comp['quality_flag'].value_counts().reset_index()
                        quality_counts.columns = ['Quality', 'Count']
                        fig_pie = px.pie(
                            quality_counts,
                            values='Count',
                            names='Quality',
                            color='Quality',
                            color_discrete_map=quality_colors,
                            title='Quality Distribution'
                        )
                        apply_colorful_style(fig_pie)
                        st.plotly_chart(fig_pie, use_container_width=True)

                    with col2:
                        # Histogram of gap hours
                        fig_gaps = px.histogram(
                            df_comp,
                            x='gap_hours',
                            nbins=24,
                            title='Distribution of Gap Hours',
                            labels={'gap_hours': 'Hours of Missing Data', 'count': 'Number of Stations'},
                            color_discrete_sequence=['#ff6b6b']
                        )
                        apply_colorful_style(fig_gaps)
                        st.plotly_chart(fig_gaps, use_container_width=True)

                    # 4. Detailed table
                    st.subheader("Station Completeness Details")
                    df_display = df_comp[['station_id', 'completeness_pct', 'gap_hours', 'gap_count',
                                         'first_epoch', 'last_epoch', 'quality_flag']].copy()
                    df_display.columns = ['Station', 'Completeness (%)', 'Gap Hours', 'Gap Count',
                                         'First Hour', 'Last Hour', 'Quality']

                    # Add observation count from processing stats
                    stats = db.get_stats(year=year)
                    stats_dict = {s['station_id']: s['num_observations'] for s in stats if s['doy'] == comp_doy}
                    df_display['Observations'] = df_display['Station'].map(stats_dict).fillna(0).astype(int)

                    st.dataframe(
                        df_display.style.format({
                            'Completeness (%)': '{:.1f}',
                            'Gap Hours': '{:.0f}',
                            'First Hour': '{:.0f}',
                            'Last Hour': '{:.0f}'
                        }).background_gradient(subset=['Completeness (%)'], cmap='RdYlGn', vmin=0, vmax=100),
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.info(f"No ZTD data available for DOY {comp_doy}.")

            else:  # Multi-Day Timeline
                st.subheader("Multi-Day Data Completeness Timeline")

                # Get timeline data
                timeline_data = db.get_data_gaps_timeline(year, start_doy, end_doy)

                if timeline_data:
                    df_timeline = pd.DataFrame(timeline_data)

                    # Calculate completeness per station per day
                    comp_summary = df_timeline.groupby(['station_id', 'doy']).agg({
                        'status': lambda x: (x == 'valid').sum() / len(x) * 100 if len(x) > 0 else 0
                    }).reset_index()
                    comp_summary.columns = ['station_id', 'doy', 'completeness']

                    # Create heatmap
                    if len(comp_summary) > 0:
                        pivot = comp_summary.pivot_table(index='station_id', columns='doy',
                                                        values='completeness', aggfunc='first', fill_value=0)

                        fig_timeline = go.Figure(data=go.Heatmap(
                            z=pivot.values,
                            x=[f'DOY {d}' for d in pivot.columns],
                            y=pivot.index.tolist(),
                            colorscale='RdYlGn',
                            colorbar=dict(title='Completeness %'),
                            hovertemplate='Station: %{y}<br>%{x}<br>Completeness: %{z:.1f}%<extra></extra>'
                        ))
                        fig_timeline.update_layout(
                            title=f'Station Data Completeness Timeline (DOY {start_doy}-{end_doy})',
                            xaxis_title='Day of Year',
                            yaxis_title='Station',
                            height=max(400, len(pivot) * 25)
                        )
                        apply_colorful_style(fig_timeline)
                        st.plotly_chart(fig_timeline, use_container_width=True)

                        # Summary statistics over period
                        st.subheader("Period Summary Statistics")
                        period_stats = comp_summary.groupby('station_id').agg({
                            'completeness': ['mean', 'min', 'max', 'std']
                        }).reset_index()
                        period_stats.columns = ['Station', 'Mean %', 'Min %', 'Max %', 'Std %']
                        period_stats['Quality'] = period_stats['Mean %'].apply(
                            lambda x: 'GOOD' if x >= 95 else ('PARTIAL' if x >= 70 else 'POOR')
                        )

                        st.dataframe(
                            period_stats.style.format({
                                'Mean %': '{:.1f}',
                                'Min %': '{:.1f}',
                                'Max %': '{:.1f}',
                                'Std %': '{:.2f}'
                            }).background_gradient(subset=['Mean %'], cmap='RdYlGn', vmin=0, vmax=100),
                            use_container_width=True,
                            hide_index=True
                        )

                        # Stations with issues
                        problematic = period_stats[period_stats['Quality'] != 'GOOD']
                        if len(problematic) > 0:
                            st.warning(f"**{len(problematic)} station(s) with data quality issues:**")
                            for _, row in problematic.iterrows():
                                st.markdown(f"- **{row['Station']}**: {row['Mean %']:.1f}% mean completeness ({row['Quality']})")
                    else:
                        st.info("No completeness data available for the selected period.")
                else:
                    st.info(f"No ZTD data available for DOY {start_doy}-{end_doy}.")

    except Exception as e:
        st.error(f"Database connection error: {e}")
        st.info("Make sure the database file exists and is accessible.")
        st.code(f"""
Database configuration:
- Path: {DB_PATH}

To initialize the database, run:
  python -c "from frontend.db_models import QualityDatabase; from frontend.config import DB_PATH; db = QualityDatabase(DB_PATH); db.create_tables()"
        """)

    # Auto-refresh (disabled to prevent crashes - use manual refresh button)
    # if auto_refresh:
    #     time.sleep(refresh_interval)
    #     st.rerun()


if __name__ == "__main__":
    main()
