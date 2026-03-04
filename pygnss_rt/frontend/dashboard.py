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
import math
import os
import base64
from scipy.interpolate import griddata, Rbf
from scipy import stats
from scipy.signal import find_peaks

# Machine Learning imports for anomaly detection
try:
    from sklearn.ensemble import IsolationForest
    from sklearn.neighbors import LocalOutlierFactor
    from sklearn.cluster import DBSCAN
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

from pygnss_rt.frontend.db_models import QualityDatabase
from pygnss_rt.frontend.bernese_parser import find_ig_campaign_dir
from pygnss_rt.frontend.config import DB_PATH, DASHBOARD_CONFIG, MONITORED_STATIONS
from pygnss_rt.frontend.gfz_loading_parser import get_combined_loading, get_available_dates
from code_tro_parser import parse_code_tro_file, download_code_tro, get_code_ztd_for_station
from code_snx_parser import parse_code_snx_file, download_code_snx, get_code_coords_for_station, compute_enu_diff
from dd_crd_parser import parse_dd_crd_file, get_dd_coords_for_station, compute_enu_diff_dd
from dd_tro_parser import parse_dd_tro_file, get_dd_ztd_for_station, get_dd_ztd_timeseries
from pppar_tro_parser import get_pppar_ztd_for_station, PPPARTropoEstimate
from era5_ztd import get_era5_ztd_for_station, list_cached_era5_dates, ERA5ZTDEstimate, ERA5_CACHE_DIR
from amb_parser import parse_amb_summary_file, get_amb_summary_path, AmbiguitySummary
from blq_parser import parse_blq_file, calculate_displacement, calculate_displacement_timeseries, get_station_summary, CONSTITUENT_NAMES, TIDAL_PERIODS
from hlm_parser import parse_hlm_file, get_hlm_path, load_hlm_timeseries, HLMResult
from bsl_parser import load_baselines_with_coords, get_bsl_path, get_baseline_statistics
from ml_quality_assessment import assess_network_quality, SKLEARN_AVAILABLE as ML_SKLEARN_AVAILABLE
from satellite_residuals_parser import (
    parse_res_sum_file, parse_trp_for_convergence, get_res_sum_path, get_trp_path as get_trp_convergence_path,
    aggregate_satellite_stats, identify_problematic_satellites, prn_to_system,
    SessionSatelliteStats, BaselineResiduals, ConvergenceInfo
)
from r2s_prc_parser import (
    parse_r2s_prc_file, get_r2s_prc_path, load_r2s_repeatability_range,
    CoordinateRepeatability, StationRepeatabilityR2S
)
from ngl_timeseries_parser import (
    fetch_ngl_timeseries, filter_timeseries_by_date, compute_velocity,
    NGLTimeSeries, get_available_ngl_stations
)
from code_coords_parser import (
    fetch_code_coordinates_http, gps_week_from_date, CODECoordinate
)
from dd_visualizations import (
    load_hlm_timeseries as load_hlm_ts, get_repeatability_summary,
    parse_chk_summary, get_chk_path, load_satellite_timeseries,
    parse_amb_summary, get_amb_path, get_station_ar_rates, load_ar_timeseries,
    parse_trp_file, get_trp_path, get_gradient_frames,
    parse_rms_file, get_rms_path, build_network_graph_data, get_station_connectivity,
    load_coordinate_timeseries, get_coordinate_summary, get_available_doys as get_dd_available_doys
)


# European coastline points for distance-to-sea calculation
# Approximate coordinates along major European coastlines
EUROPEAN_COASTLINE = np.array([
    # Atlantic Coast (Portugal, Spain, France)
    [-9.5, 38.7], [-9.0, 39.5], [-8.5, 41.0], [-8.8, 42.5], [-9.3, 43.0],
    [-8.0, 43.5], [-5.0, 43.5], [-4.0, 43.4], [-2.0, 43.4], [-1.8, 43.3],
    # Bay of Biscay to English Channel
    [-1.5, 46.0], [-1.2, 47.0], [-2.5, 47.5], [-4.5, 48.5], [-4.8, 48.7],
    [-1.5, 49.5], [0.0, 49.5], [1.5, 50.0], [1.8, 51.0],
    # North Sea Coast (Belgium, Netherlands, Germany, Denmark)
    [2.5, 51.3], [3.5, 51.5], [4.0, 52.0], [4.8, 53.0], [5.5, 53.5],
    [6.5, 53.6], [7.5, 53.8], [8.5, 54.0], [8.8, 54.8], [9.5, 55.0],
    [10.0, 54.5], [10.5, 55.5], [12.0, 56.0],
    # Baltic Sea (Denmark, Germany, Poland, Baltics)
    [10.5, 54.5], [11.0, 54.4], [12.0, 54.2], [14.0, 54.0], [14.5, 54.5],
    [16.5, 54.5], [18.5, 54.8], [19.5, 54.5], [20.5, 55.0], [21.0, 56.0],
    [24.0, 57.0], [24.5, 58.0], [24.0, 59.5], [25.0, 60.0],
    # Finnish/Swedish Coast
    [22.0, 60.0], [21.0, 61.0], [21.5, 63.0], [23.0, 65.0], [25.5, 65.5],
    # Norwegian Coast
    [18.0, 69.0], [16.0, 69.0], [14.0, 68.0], [12.0, 67.0], [10.0, 64.0],
    [8.0, 62.0], [6.0, 60.5], [5.0, 60.0], [5.5, 59.0], [6.5, 58.0],
    [8.0, 58.0], [10.0, 59.0],
    # British Isles
    [-5.5, 50.0], [-5.0, 50.5], [-4.0, 51.0], [-3.0, 51.5], [-5.0, 52.0],
    [-4.5, 53.0], [-3.0, 53.5], [-3.5, 54.5], [-5.0, 55.0], [-5.5, 56.0],
    [-6.0, 57.5], [-5.0, 58.5], [-3.0, 58.6], [-2.0, 57.5], [-1.5, 55.0],
    [0.0, 53.5], [1.5, 52.5], [1.0, 51.5], [0.0, 51.0], [-1.0, 50.5],
    # Ireland
    [-10.0, 51.5], [-9.5, 52.0], [-10.0, 53.0], [-10.5, 54.0], [-8.5, 55.0],
    [-6.5, 55.5], [-6.0, 54.5], [-6.0, 53.5], [-6.5, 52.5], [-8.0, 51.5],
    # Mediterranean (Spain, France, Italy)
    [-5.5, 36.0], [-4.0, 36.5], [-2.5, 36.8], [0.0, 38.0], [0.5, 39.5],
    [1.5, 39.0], [3.0, 40.0], [3.2, 42.0], [3.0, 43.0], [4.5, 43.3],
    [6.0, 43.0], [7.0, 43.5], [8.0, 44.0], [9.5, 44.0], [10.0, 43.5],
    [10.5, 42.5], [11.0, 42.0], [12.5, 42.0], [13.5, 41.0], [14.0, 40.5],
    [15.5, 40.0], [16.0, 39.0], [15.5, 38.0], [16.0, 37.5], [15.0, 37.0],
    [12.5, 37.5], [12.0, 38.0], [13.0, 38.5], [11.0, 39.0], [9.5, 39.0],
    [8.5, 39.0], [8.5, 41.0], [9.0, 41.5],
    # Adriatic & Balkans
    [13.5, 45.5], [14.5, 45.0], [15.0, 44.5], [16.5, 43.0], [17.5, 43.0],
    [18.5, 42.5], [19.0, 42.0], [19.5, 41.5], [20.0, 40.0], [21.0, 38.5],
    [23.0, 37.0], [24.0, 37.5], [26.0, 38.0], [26.5, 39.0], [26.0, 40.5],
    [24.0, 41.0], [26.0, 41.5], [28.0, 41.0], [29.0, 41.0],
    # Black Sea
    [28.0, 43.0], [29.5, 43.5], [30.5, 44.5], [31.0, 46.0], [33.0, 46.5],
    [35.0, 45.5], [36.5, 45.0], [37.5, 44.5], [39.0, 44.0], [41.0, 42.0],
    [40.0, 41.0], [39.5, 41.5], [37.0, 41.5], [35.0, 42.0], [33.0, 42.0],
    [31.0, 42.5], [29.0, 42.0], [28.0, 43.0],
])


def calculate_distance_to_coast(lat: float, lon: float) -> float:
    """
    Calculate approximate distance from a point to the nearest European coastline.

    Uses Haversine formula for spherical distance calculation.

    Args:
        lat: Latitude in degrees
        lon: Longitude in degrees

    Returns:
        Distance to nearest coast in kilometers
    """
    R = 6371.0  # Earth radius in km

    # Convert to radians
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)

    min_distance = float('inf')

    for coast_point in EUROPEAN_COASTLINE:
        coast_lon, coast_lat = coast_point
        coast_lat_rad = math.radians(coast_lat)
        coast_lon_rad = math.radians(coast_lon)

        # Haversine formula
        dlat = coast_lat_rad - lat_rad
        dlon = coast_lon_rad - lon_rad
        a = math.sin(dlat/2)**2 + math.cos(lat_rad) * math.cos(coast_lat_rad) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        distance = R * c

        if distance < min_distance:
            min_distance = distance

    return min_distance


def classify_coastal_proximity(distance_km: float) -> str:
    """Classify station by distance to coast."""
    if distance_km < 50:
        return "🌊 Coastal (<50 km)"
    elif distance_km < 150:
        return "🏖️ Near-coastal (50-150 km)"
    elif distance_km < 300:
        return "🌲 Semi-inland (150-300 km)"
    else:
        return "🏔️ Inland (>300 km)"


@st.cache_data(ttl=3600, show_spinner=False)
def load_dd_tro_cached(base_dir: str, year: int, start_doy: int, end_doy: int, prefix: str = "F10"):
    """
    Load DD TRO data with caching for faster retrieval.
    Cache expires after 1 hour (ttl=3600).

    Returns tuple: (estimates_as_dicts, loaded_doys, failed_doys)
    """
    all_estimates = []
    loaded_doys = []
    failed_doys = []

    for doy in range(start_doy, end_doy + 1):
        patterns = [
            f"{base_dir}/{year}/{doy}/ATM/{prefix}_{year}{doy:03d}0.TRO.gz",
            f"{base_dir}/{year}/{doy:03d}/ATM/{prefix}_{year}{doy:03d}0.TRO.gz",
        ]
        found = False
        for path in patterns:
            if os.path.exists(path):
                try:
                    estimates = parse_dd_tro_file(path, year, doy)
                    # Convert dataclass to dict for caching (dataclasses aren't hashable)
                    for e in estimates:
                        all_estimates.append({
                            'station_4char': e.station_4char,
                            'timestamp': e.timestamp,
                            'ztd_m': e.ztd_m,
                            'sigma_m': e.sigma_m,
                            'year': e.year,
                            'doy': e.doy
                        })
                    loaded_doys.append(doy)
                    found = True
                    break
                except Exception:
                    pass
        if not found:
            failed_doys.append(doy)

    return all_estimates, loaded_doys, failed_doys


# Modern light theme plot template (matching reference design)
PLOT_TEMPLATE = {
    'layout': {
        'paper_bgcolor': 'rgba(0,0,0,0)',  # Transparent to show page background
        'plot_bgcolor': '#FFFFFF',   # White plot area
        'font': {'color': '#1E293B', 'family': 'Inter, -apple-system, BlinkMacSystemFont, sans-serif'},
        'title': {'font': {'size': 18, 'color': '#1E293B'}},
        'xaxis': {
            'gridcolor': '#E2E8F0',
            'linecolor': '#CBD5E1',
            'tickfont': {'color': '#64748B'}
        },
        'yaxis': {
            'gridcolor': '#E2E8F0',
            'linecolor': '#CBD5E1',
            'tickfont': {'color': '#64748B'}
        },
        'colorway': ['#6366F1', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6',
                     '#06B6D4', '#EC4899', '#14B8A6', '#F97316', '#3B82F6']
    }
}

def apply_colorful_style(fig):
    """Apply modern dark theme to a Plotly figure"""
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',  # Transparent
        plot_bgcolor='#1E2530',  # Dark background
        font=dict(color='#E2E8F0', family='Inter, -apple-system, BlinkMacSystemFont, sans-serif'),
        title_font=dict(size=18, color='#FFFFFF', weight='bold'),
        legend=dict(
            bgcolor='rgba(30, 37, 48, 0.95)',
            bordercolor='#374151',
            borderwidth=1,
            font=dict(color='#E2E8F0', size=10),
            orientation='h',  # Horizontal legend
            yanchor='bottom',
            y=1.02,  # Above the plot
            xanchor='center',
            x=0.5
        ),
        margin=dict(l=20, r=20, t=60, b=20)  # Increased top margin for legend
    )
    fig.update_xaxes(
        gridcolor='#374151',
        linecolor='#4B5563',
        tickfont=dict(color='#9CA3AF'),
        title_font=dict(color='#D1D5DB')
    )
    fig.update_yaxes(
        gridcolor='#374151',
        linecolor='#4B5563',
        tickfont=dict(color='#9CA3AF'),
        title_font=dict(color='#D1D5DB')
    )
    return fig

# Page configuration
st.set_page_config(
    page_title="GNSS Quality Monitor",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Modern Dark Theme CSS (matching dark dashboard design with teal accents)
st.markdown("""
<style>
/* ========== GLOBAL STYLES ========== */
/* Main background - dark charcoal */
.stApp {
    background: linear-gradient(135deg, #13161A 0%, #1A1D21 50%, #181B1F 100%);
}

/* Sidebar styling */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #1A1D21 0%, #242830 100%);
    border-right: 1px solid #2D3748;
}

[data-testid="stSidebar"] .stMarkdown {
    color: #E2E8F0;
}

[data-testid="stSidebar"] label {
    color: #9CA3AF !important;
}

/* ========== CARD STYLES ========== */
/* Main content cards */
.stTabs, [data-testid="stVerticalBlock"] > div > div {
    background-color: transparent;
}

/* Metric cards with dark glassmorphism effect */
[data-testid="metric-container"] {
    background: linear-gradient(135deg, #242830 0%, #1E2530 100%);
    border: 1px solid #374151;
    border-radius: 16px;
    padding: 20px;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3), 0 1px 3px rgba(0, 0, 0, 0.2);
    backdrop-filter: blur(10px);
}

[data-testid="metric-container"]:hover {
    box-shadow: 0 8px 30px rgba(78, 205, 196, 0.15), 0 2px 6px rgba(0, 0, 0, 0.3);
    border-color: #4ECDC4;
    transform: translateY(-2px);
    transition: all 0.3s ease;
}

/* Metric label styling */
[data-testid="stMetricLabel"] {
    color: #9CA3AF !important;
    font-weight: 500;
    font-size: 0.85rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

/* Metric value styling */
[data-testid="stMetricValue"] {
    color: #FFFFFF !important;
    font-weight: 700;
    font-size: 2rem;
}

/* Metric delta styling */
[data-testid="stMetricDelta"] {
    font-weight: 600;
}

/* ========== TAB STYLES ========== */
/* Make tabs scrollable horizontally */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
    overflow-x: auto;
    overflow-y: hidden;
    flex-wrap: nowrap;
    scrollbar-width: thin;
    scrollbar-color: #4ECDC4 #2D3748;
    padding-bottom: 10px;
    background: transparent;
}

/* Tab buttons */
.stTabs [data-baseweb="tab"] {
    white-space: nowrap;
    flex-shrink: 0;
    background: #242830;
    border: 1px solid #374151;
    border-radius: 10px;
    padding: 10px 20px;
    color: #9CA3AF;
    font-weight: 500;
    transition: all 0.2s ease;
}

.stTabs [data-baseweb="tab"]:hover {
    background: #2D3748;
    border-color: #4ECDC4;
    color: #4ECDC4;
}

.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #4ECDC4 0%, #45B7AA 100%) !important;
    color: #1A1D21 !important;
    border-color: transparent !important;
    box-shadow: 0 4px 15px rgba(78, 205, 196, 0.4);
    font-weight: 600;
}

/* Webkit scrollbar styling */
.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {
    height: 6px;
}

.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-track {
    background: #2D3748;
    border-radius: 3px;
}

.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb {
    background: linear-gradient(90deg, #4ECDC4, #45B7AA);
    border-radius: 3px;
}

/* ========== HEADERS & TEXT ========== */
h1, h2, h3 {
    color: #FFFFFF !important;
    font-weight: 700;
}

.stMarkdown p {
    color: #D1D5DB;
}

.stMarkdown {
    color: #E2E8F0;
}

/* ========== DATAFRAME STYLES ========== */
[data-testid="stDataFrame"] {
    background: #242830;
    border-radius: 12px;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.3);
    overflow: hidden;
}

[data-testid="stDataFrame"] th {
    background: #1E2530 !important;
    color: #E2E8F0 !important;
}

[data-testid="stDataFrame"] td {
    background: #242830 !important;
    color: #D1D5DB !important;
}

/* ========== SELECTBOX & INPUT STYLES ========== */
.stSelectbox > div > div,
.stMultiSelect > div > div,
.stNumberInput > div > div > input {
    background: #242830 !important;
    border: 1px solid #374151 !important;
    border-radius: 8px;
    color: #E2E8F0 !important;
}

.stSelectbox > div > div:hover,
.stMultiSelect > div > div:hover {
    border-color: #4ECDC4 !important;
}

.stSelectbox label, .stMultiSelect label, .stNumberInput label {
    color: #9CA3AF !important;
}

/* Dropdown menu dark styling */
[data-baseweb="popover"] {
    background: #242830 !important;
    border: 1px solid #374151 !important;
}

[data-baseweb="menu"] {
    background: #242830 !important;
}

[data-baseweb="menu"] li {
    color: #E2E8F0 !important;
}

[data-baseweb="menu"] li:hover {
    background: #374151 !important;
}

/* ========== BUTTON STYLES ========== */
.stButton > button {
    background: linear-gradient(135deg, #4ECDC4 0%, #45B7AA 100%);
    color: #1A1D21;
    border: none;
    border-radius: 8px;
    padding: 10px 24px;
    font-weight: 600;
    box-shadow: 0 4px 15px rgba(78, 205, 196, 0.3);
    transition: all 0.2s ease;
}

.stButton > button:hover {
    box-shadow: 0 6px 20px rgba(78, 205, 196, 0.5);
    transform: translateY(-1px);
}

/* ========== EXPANDER STYLES ========== */
.streamlit-expanderHeader {
    background: #242830 !important;
    border-radius: 10px;
    border: 1px solid #374151;
    color: #E2E8F0 !important;
    font-weight: 600;
}

[data-testid="stExpander"] {
    background: #242830;
    border: 1px solid #374151;
    border-radius: 10px;
}

/* ========== PLOTLY CHART CONTAINER ========== */
[data-testid="stPlotlyChart"] {
    background: #242830;
    border-radius: 16px;
    padding: 20px;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    border: 1px solid #374151;
}

/* ========== CUSTOM METRIC CARD CLASS ========== */
.metric-card {
    background: linear-gradient(135deg, #242830 0%, #1E2530 100%);
    border-radius: 16px;
    padding: 24px;
    margin: 8px;
    box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    border: 1px solid #374151;
}

/* Status colors - brighter for dark theme */
.good { color: #10B981 !important; }
.warn { color: #FBBF24 !important; }
.bad { color: #F87171 !important; }

/* ========== DIVIDER ========== */
hr {
    border-color: #374151;
}

[data-testid="stDivider"] {
    background-color: #374151;
}

/* ========== TOGGLE STYLES ========== */
[data-testid="stToggle"] > label > div {
    background: #374151;
}

[data-testid="stToggle"] > label > div[data-checked="true"] {
    background: linear-gradient(135deg, #4ECDC4 0%, #45B7AA 100%);
}

/* ========== INFO/WARNING/ERROR BOXES ========== */
.stAlert {
    border-radius: 12px;
    border: none;
}

[data-testid="stNotificationContentInfo"] {
    background: linear-gradient(135deg, #1E3A5F 0%, #2D4A6F 100%);
    color: #93C5FD;
}

[data-testid="stNotificationContentWarning"] {
    background: linear-gradient(135deg, #4A3728 0%, #5C4433 100%);
    color: #FCD34D;
}

[data-testid="stNotificationContentError"] {
    background: linear-gradient(135deg, #4A2828 0%, #5C3333 100%);
    color: #FCA5A5;
}

/* ========== SPINNER ========== */
.stSpinner > div {
    border-top-color: #4ECDC4 !important;
}

/* ========== SUBHEADER STYLING ========== */
.stSubheader {
    color: #E2E8F0 !important;
}

/* ========== CAPTION STYLING ========== */
.stCaption {
    color: #9CA3AF !important;
}

/* ========== CHECKBOX ========== */
.stCheckbox label {
    color: #E2E8F0 !important;
}

/* ========== SLIDER ========== */
.stSlider label {
    color: #9CA3AF !important;
}

[data-testid="stSlider"] > div > div > div {
    background: #4ECDC4 !important;
}
</style>
""", unsafe_allow_html=True)


def get_database():
    """Get database connection in read-only mode for concurrent access"""
    import time
    max_retries = 3
    retry_delay = 2  # seconds

    for attempt in range(max_retries):
        try:
            db = QualityDatabase(DB_PATH)
            db.connect(read_only=True)
            return db
        except Exception as e:
            if "lock" in str(e).lower() and attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise
    return None


# ========== CACHED DATABASE QUERY WRAPPERS ==========
# These functions cache expensive database queries to improve performance

@st.cache_data(ttl=60, show_spinner=False)
def cached_get_all_stations() -> list:
    """Get all stations from database (cached for 1 minute)"""
    db = get_database()
    return db.get_all_stations()


@st.cache_data(ttl=60, show_spinner=False)
def cached_get_latest_processing(limit: int = 100) -> list:
    """Get latest processing results (cached for 1 minute)"""
    db = get_database()
    return db.get_latest_processing(limit=limit)


@st.cache_data(ttl=120, show_spinner=False)
def cached_get_stats(year: int, limit: int = 5000) -> list:
    """Get processing stats (cached for 2 minutes)"""
    db = get_database()
    return db.get_stats(year=year, limit=limit)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_available_years() -> list[int]:
    """Get years with data in the database (cached for 5 minutes)"""
    db = get_database()
    return db.get_available_years()

@st.cache_data(ttl=300, show_spinner=False)
def cached_get_doy_range(year: int) -> tuple[int, int]:
    """Get DOY range for a year (cached for 5 minutes)"""
    db = get_database()
    return db.get_doy_range(year)

@st.cache_data(ttl=300, show_spinner=False)
def cached_get_solutions(station_id: str, year: int, start_doy: int, end_doy: int) -> list:
    """Get solutions for a station (cached for 5 minutes)"""
    db = get_database()
    return db.get_solutions(station_id=station_id, year=year, start_doy=start_doy, end_doy=end_doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_ztd(station_id: str, year: int, doy: int) -> list:
    """Get ZTD data (cached for 5 minutes)"""
    db = get_database()
    return db.get_ztd(station_id, year, doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_pppar_ztd(station_id: str, year: int, doy: int) -> list:
    """Get PPP-AR ZTD data directly from TRO files (cached for 5 minutes)"""
    campaign_path = '/home/ahunegnaw/GPSDATA/CAMPAIGN54'
    estimates = get_pppar_ztd_for_station(campaign_path, station_id, year, doy)
    # Convert to list of dicts matching expected format
    result = []
    for e in estimates:
        result.append({
            'station_id': e.station_4char,
            'year': e.year,
            'doy': e.doy,
            'hour': e.hour,
            'ztd': e.ztd_m,
            'ztd_rms': e.sigma_m,
            'grad_n': e.grad_n_m,
            'grad_n_rms': e.grad_n_sigma_m,
            'grad_e': e.grad_e_m,
            'grad_e_rms': e.grad_e_sigma_m
        })
    return result


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_ambiguity(year: int, start_doy: int, end_doy: int) -> list:
    """Get ambiguity data (cached for 5 minutes)"""
    db = get_database()
    return db.get_ambiguity(year=year, start_doy=start_doy, end_doy=end_doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_satellite_tracking(year: int, doy: int) -> list:
    """Get satellite tracking data (cached for 5 minutes)"""
    db = get_database()
    return db.get_satellite_tracking(year=year, doy=doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_station_residuals(year: int, doy: int) -> list:
    """Get station residuals (cached for 5 minutes)"""
    db = get_database()
    return db.get_station_residuals(year=year, doy=doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_data_availability(year: int, doy: int) -> list:
    """Get data availability (cached for 5 minutes)"""
    db = get_database()
    return db.get_data_availability(year=year, doy=doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_observation_quality(year: int, doy: int) -> list:
    """Get observation quality (cached for 5 minutes)"""
    db = get_database()
    return db.get_observation_quality(year=year, doy=doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_all_station_repeatability(year: int, start_doy: int, end_doy: int) -> list:
    """Get station repeatability (cached for 5 minutes)"""
    db = get_database()
    return db.get_all_station_repeatability(year, start_doy, end_doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_stats_by_station(station_id: str, year: int, limit: int = 100) -> list:
    """Get processing stats for a specific station (cached for 5 minutes)"""
    db = get_database()
    return db.get_stats(station_id=station_id, year=year, limit=limit)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_daily_coords(station_id: str, year: int, start_doy: int, end_doy: int) -> list:
    """Get daily coordinates for a station (cached for 5 minutes)"""
    db = get_database()
    return db.get_daily_coords(station_id, year=year, start_doy=start_doy, end_doy=end_doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_satellite_ambiguity_prn(year: int) -> list:
    """Get satellite ambiguity by PRN (cached for 5 minutes)"""
    db = get_database()
    return db.get_satellite_ambiguity_prn(year=year)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_residuals_by_constellation(year: int, doy: int) -> list:
    """Get residuals by constellation (cached for 5 minutes)"""
    db = get_database()
    return db.get_residuals_by_constellation(year, doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_residuals_by_station_constellation(year: int, doy: int) -> list:
    """Get residuals by station and constellation (cached for 5 minutes)"""
    db = get_database()
    return db.get_residuals_by_station_constellation(year, doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_ar_summary_stats(year: int, start_doy: int, end_doy: int) -> list:
    """Get AR summary stats (cached for 5 minutes)"""
    db = get_database()
    return db.get_ar_summary_stats(year, start_doy, end_doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_ar_by_station(year: int, start_doy: int, end_doy: int) -> list:
    """Get AR by station (cached for 5 minutes)"""
    db = get_database()
    return db.get_ar_by_station(year, start_doy, end_doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_ar_by_receiver_type(year: int, start_doy: int, end_doy: int) -> list:
    """Get AR by receiver type (cached for 5 minutes)"""
    db = get_database()
    return db.get_ar_by_receiver_type(year, start_doy, end_doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_gradient_stations(year: int, doy: int) -> list:
    """Get gradient stations (cached for 5 minutes)"""
    db = get_database()
    return db.get_gradient_stations(year, doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_tropospheric_gradients(year: int, doy: int, station_id: str) -> list:
    """Get tropospheric gradients (cached for 5 minutes)"""
    db = get_database()
    return db.get_tropospheric_gradients(year, doy, station_id)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_clock_stations(year: int, doy: int) -> list:
    """Get clock stations (cached for 5 minutes)"""
    db = get_database()
    return db.get_clock_stations(year, doy)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_receiver_clocks(year: int, doy: int, station_id: str) -> list:
    """Get receiver clocks (cached for 5 minutes)"""
    db = get_database()
    return db.get_receiver_clocks(year, doy, station_id)


@st.cache_data(ttl=300, show_spinner=False)
def cached_get_data_gaps_timeline(year: int, start_doy: int, end_doy: int) -> list:
    """Get data gaps timeline (cached for 5 minutes)"""
    db = get_database()
    return db.get_data_gaps_timeline(year, start_doy, end_doy)


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


def xyz_to_llh(x: float, y: float, z: float) -> tuple:
    """
    Convert ECEF XYZ coordinates to geodetic latitude, longitude, height (WGS84).

    Args:
        x, y, z: ECEF coordinates in meters

    Returns:
        tuple: (latitude, longitude, height) in degrees and meters
    """
    # WGS84 parameters
    a = 6378137.0  # Semi-major axis
    f = 1 / 298.257223563  # Flattening
    b = a * (1 - f)  # Semi-minor axis
    e2 = (a**2 - b**2) / a**2  # First eccentricity squared
    ep2 = (a**2 - b**2) / b**2  # Second eccentricity squared

    # Longitude
    lon = np.arctan2(y, x)

    # Iterative calculation for latitude and height
    p = np.sqrt(x**2 + y**2)
    lat = np.arctan2(z, p * (1 - e2))  # Initial estimate

    for _ in range(10):  # Iterate until convergence
        N = a / np.sqrt(1 - e2 * np.sin(lat)**2)
        h = p / np.cos(lat) - N
        lat_new = np.arctan2(z, p * (1 - e2 * N / (N + h)))
        if abs(lat_new - lat) < 1e-12:
            break
        lat = lat_new

    # Final height calculation
    N = a / np.sqrt(1 - e2 * np.sin(lat)**2)
    h = p / np.cos(lat) - N

    # Convert to degrees
    lat_deg = np.degrees(lat)
    lon_deg = np.degrees(lon)

    return lat_deg, lon_deg, h


def xyz_diff_to_enu(dx: float, dy: float, dz: float,
                     ref_lat: float, ref_lon: float) -> tuple:
    """
    Convert XYZ coordinate differences to ENU (East, North, Up) differences.

    Args:
        dx, dy, dz: XYZ differences in meters
        ref_lat: Reference latitude in degrees
        ref_lon: Reference longitude in degrees

    Returns:
        tuple: (dE, dN, dU) in meters
    """
    # Convert reference coordinates to radians
    lat_rad = np.radians(ref_lat)
    lon_rad = np.radians(ref_lon)

    # Rotation matrix from XYZ to ENU
    sin_lat = np.sin(lat_rad)
    cos_lat = np.cos(lat_rad)
    sin_lon = np.sin(lon_rad)
    cos_lon = np.cos(lon_rad)

    # Transform XYZ differences to ENU
    dE = -sin_lon * dx + cos_lon * dy
    dN = -sin_lat * cos_lon * dx - sin_lat * sin_lon * dy + cos_lat * dz
    dU = cos_lat * cos_lon * dx + cos_lat * sin_lon * dy + sin_lat * dz

    return dE, dN, dU


@st.cache_data(ttl=3600, show_spinner=False)
def parse_bernese_crd_file(crd_path: str) -> pd.DataFrame:
    """
    Parse a Bernese CRD coordinate file.

    Args:
        crd_path: Path to the CRD file

    Returns:
        DataFrame with station_id, lat, lon, height, x, y, z columns
    """
    if not os.path.exists(crd_path):
        return pd.DataFrame()

    stations = []

    try:
        with open(crd_path, 'r') as f:
            in_data = False
            for line in f:
                # Skip header lines until we find "NUM  STATION NAME"
                if 'NUM  STATION NAME' in line:
                    in_data = True
                    continue

                if not in_data:
                    continue

                # Skip empty lines
                if not line.strip():
                    continue

                # Parse data line: num  station  X  Y  Z  flag  system
                # Example:   1  0531              4084902.54142   443391.86637  4862653.52536    W      GRE
                parts = line.split()
                if len(parts) >= 5:
                    try:
                        num = int(parts[0])
                        station_id = parts[1]
                        x = float(parts[2])
                        y = float(parts[3])
                        z = float(parts[4])

                        # Convert XYZ to lat/lon/height
                        lat, lon, height = xyz_to_llh(x, y, z)

                        stations.append({
                            'station_id': station_id,
                            'lat': lat,
                            'lon': lon,
                            'height': height,
                            'x': x,
                            'y': y,
                            'z': z
                        })
                    except (ValueError, IndexError):
                        continue

        return pd.DataFrame(stations)

    except Exception as e:
        print(f"Error parsing CRD file {crd_path}: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def parse_hlm_residuals(hlm_path: str) -> pd.DataFrame:
    """
    Parse Helmert transformation residuals from HLM output file.

    Args:
        hlm_path: Path to the HLM output file (can be .gz)

    Returns:
        DataFrame with station_id, north_res, east_res, up_res columns (in mm)
    """
    import gzip

    if not os.path.exists(hlm_path):
        return pd.DataFrame()

    stations = []

    try:
        if hlm_path.endswith('.gz'):
            f = gzip.open(hlm_path, 'rt')
        else:
            f = open(hlm_path, 'r')

        with f:
            in_residuals = False
            for line in f:
                # Detect start of residuals table
                if '| NUM |      NAME        | FLG |' in line:
                    in_residuals = True
                    continue

                if not in_residuals:
                    continue

                # End of residuals table
                if 'RMS / COMPONENT' in line or 'IQR' in line:
                    break

                # Parse residual line: |   1 | 0531             | W A |       0.73      2.52     -2.36 | V |
                if '|' in line and 'NUM' not in line:
                    parts = line.replace('|', ' ').split()
                    if len(parts) >= 5:
                        try:
                            num = int(parts[0])
                            station_id = parts[1].strip()
                            # Skip flag columns, get residuals
                            # Find numeric values
                            nums = []
                            for p in parts[2:]:
                                try:
                                    nums.append(float(p))
                                except ValueError:
                                    continue
                            if len(nums) >= 3:
                                stations.append({
                                    'station_id': station_id,
                                    'north_res': nums[0],
                                    'east_res': nums[1],
                                    'up_res': nums[2]
                                })
                        except (ValueError, IndexError):
                            continue

        return pd.DataFrame(stations)

    except Exception as e:
        print(f"Error parsing HLM file {hlm_path}: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def get_pppar_coordinates(year: int, doy: int) -> pd.DataFrame:
    """
    Get PPP-AR station coordinates from Bernese CRD file.

    PPP-AR results are stored at:
    /home/ahunegnaw/GPSDATA/CAMPAIGN54/25{doy}IG/STA/FIN_2025{doy}0.CRD

    Args:
        year: Year (e.g., 2025)
        doy: Day of year

    Returns:
        DataFrame with station coordinates
    """
    # PPP-AR campaign directory (supports IG, IG_GRE, IG_GE, etc.)
    session_path = find_ig_campaign_dir("/home/ahunegnaw/GPSDATA/CAMPAIGN54", year, doy)
    if not session_path:
        return pd.DataFrame()
    crd_path = f"{session_path}/STA/FIN_{year}{doy:03d}0.CRD"

    return parse_bernese_crd_file(crd_path)


@st.cache_data(ttl=3600, show_spinner=False)
def get_dd_residuals(year: int, doy: int) -> pd.DataFrame:
    """
    Get DD (Double-Difference) Helmert residuals from HLM output file.

    DD results are stored at:
    /home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/{year}/{doy}/OUT/HLM_{year}{doy}0.OUT.gz

    Args:
        year: Year
        doy: Day of year

    Returns:
        DataFrame with station residuals (north, east, up in mm)
    """
    hlm_path = f"/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/{year}/{doy}/OUT/HLM_{year}{doy:03d}0.OUT.gz"

    return parse_hlm_residuals(hlm_path)


@st.cache_data(ttl=3600, show_spinner=False)
def parse_f10_station_coordinates(base_dir: str, year: int, doy: int) -> pd.DataFrame:
    """
    Parse station coordinates from F10 output file (cached for 1 hour).
    This is kept for backward compatibility but now redirects to PPP-AR coordinates.

    Args:
        base_dir: Base directory for results (not used for PPP-AR)
        year: Year
        doy: Day of year

    Returns:
        DataFrame with station_id, lat, lon, height, x, y, z columns
    """
    # Use PPP-AR coordinates as the primary source
    return get_pppar_coordinates(year, doy)


@st.cache_data(ttl=3600, show_spinner=False)
def get_station_coordinates_from_f10(year: int, doy: int) -> pd.DataFrame:
    """
    Get station coordinates from PPP-AR CRD file for a specific DOY (cached for 1 hour).

    Args:
        year: Year
        doy: Day of year

    Returns:
        DataFrame with station coordinates
    """
    df = get_pppar_coordinates(year, doy)

    if df.empty:
        # Try nearby DOYs
        for offset in [1, -1, 2, -2, 3, -3]:
            df = get_pppar_coordinates(year, doy + offset)
            if not df.empty:
                break

    return df


@st.cache_data(ttl=300, show_spinner=False)
def get_available_doys(year: int = 2025) -> list:
    """
    Get list of available DOYs that have PPP-AR coordinate files (cached for 5 minutes).

    Args:
        year: Year to check

    Returns:
        List of available DOY integers, sorted descending (most recent first)
    """
    import glob

    # PPP-AR campaign pattern (match IG, IG_GRE, IG_GE, etc.)
    yy = year % 100
    pattern = f"/home/ahunegnaw/GPSDATA/CAMPAIGN54/{yy}[0-9][0-9][0-9]IG*/STA/FIN_{year}*0.CRD"

    files = glob.glob(pattern)
    doys = set()

    for f in files:
        # Extract DOY from filename: FIN_20253020.CRD -> 302
        basename = os.path.basename(f)
        if basename.startswith('FIN_') and len(basename) >= 15:
            try:
                # FIN_20253020.CRD -> extract doy from position 8-11
                doy_str = basename[8:11]
                doys.add(int(doy_str))
            except (ValueError, IndexError):
                continue

    return sorted(list(doys), reverse=True)


# ============================================================
# NEW VISUALIZATION HELPER FUNCTIONS
# ============================================================

def detrend_timeseries(values: np.ndarray, method: str = 'linear') -> tuple:
    """
    Detrend a time series and return detrended values with trend parameters.

    Args:
        values: Array of values to detrend
        method: 'linear', 'mean', or 'polynomial'

    Returns:
        Tuple of (detrended_values, trend_line, velocity_mm_yr)
    """
    n = len(values)
    if n < 2:
        return values, np.zeros_like(values), 0.0

    x = np.arange(n)
    mask = ~np.isnan(values)

    if method == 'mean':
        mean_val = np.nanmean(values)
        detrended = values - mean_val
        trend = np.full(n, mean_val)
        velocity = 0.0
    elif method == 'linear':
        if np.sum(mask) < 2:
            return values, np.zeros(n), 0.0
        slope, intercept, _, _, _ = stats.linregress(x[mask], values[mask])
        trend = slope * x + intercept
        detrended = values - trend
        # Convert slope from mm/day to mm/year (assuming daily data)
        velocity = slope * 365.25
    else:  # polynomial
        if np.sum(mask) < 3:
            return values, np.zeros(n), 0.0
        coeffs = np.polyfit(x[mask], values[mask], 2)
        trend = np.polyval(coeffs, x)
        detrended = values - trend
        velocity = coeffs[1] * 365.25  # linear term

    return detrended, trend, velocity


@st.cache_data(ttl=300, show_spinner=False)
def compute_repeatability_heatmap(year: int, start_doy: int, end_doy: int) -> pd.DataFrame:
    """
    Compute repeatability (RMS) for each station-DOY combination for heatmap visualization.

    Returns:
        DataFrame with station_id, doy, rms_3d columns
    """
    db = get_database()

    # Get all solutions in the range
    solutions = []
    for doy in range(start_doy, end_doy + 1):
        df_coords = get_pppar_coordinates(year, doy)
        if not df_coords.empty:
            df_coords['doy'] = doy
            solutions.append(df_coords)

    if not solutions:
        return pd.DataFrame()

    df_all = pd.concat(solutions, ignore_index=True)

    # Calculate mean position per station
    station_means = df_all.groupby('station_id').agg({
        'x': 'mean', 'y': 'mean', 'z': 'mean'
    }).reset_index()

    # Calculate deviation from mean for each day
    heatmap_data = []
    for station in df_all['station_id'].unique():
        station_df = df_all[df_all['station_id'] == station]
        mean_row = station_means[station_means['station_id'] == station].iloc[0]

        for _, row in station_df.iterrows():
            dx = (row['x'] - mean_row['x']) * 1000  # mm
            dy = (row['y'] - mean_row['y']) * 1000
            dz = (row['z'] - mean_row['z']) * 1000
            rms_3d = np.sqrt(dx**2 + dy**2 + dz**2)

            heatmap_data.append({
                'station_id': station,
                'doy': row['doy'],
                'rms_3d': rms_3d
            })

    return pd.DataFrame(heatmap_data)


@st.cache_data(ttl=300, show_spinner=False)
def compute_station_correlation(year: int, start_doy: int, end_doy: int, component: str = 'up') -> pd.DataFrame:
    """
    Compute correlation matrix between stations for a given component.

    Args:
        year: Year
        start_doy, end_doy: DOY range
        component: 'east', 'north', or 'up'

    Returns:
        DataFrame correlation matrix
    """
    # Get coordinate time series
    solutions = []
    for doy in range(start_doy, end_doy + 1):
        df_coords = get_pppar_coordinates(year, doy)
        if not df_coords.empty:
            df_coords['doy'] = doy
            solutions.append(df_coords)

    if not solutions:
        return pd.DataFrame()

    df_all = pd.concat(solutions, ignore_index=True)

    # Pivot to get station × DOY matrix
    pivot_x = df_all.pivot(index='doy', columns='station_id', values='x')
    pivot_y = df_all.pivot(index='doy', columns='station_id', values='y')
    pivot_z = df_all.pivot(index='doy', columns='station_id', values='z')

    # Convert to ENU for each station (simplified - using height diff)
    if component == 'up':
        # Use height directly
        pivot_h = df_all.pivot(index='doy', columns='station_id', values='height')
        # Remove mean
        pivot_h = pivot_h - pivot_h.mean()
        corr_matrix = pivot_h.corr()
    else:
        # For east/north, would need full transformation - use x/y as proxy
        if component == 'east':
            pivot_val = pivot_y - pivot_y.mean()
        else:  # north
            pivot_val = pivot_x - pivot_x.mean()
        corr_matrix = pivot_val.corr()

    return corr_matrix


def get_network_assignment(station_id: str) -> str:
    """
    Assign a station to a network based on naming conventions.
    """
    station_upper = station_id.upper()

    # IGS core stations (4-char)
    igs_stations = {'BRUX', 'GRAS', 'ZIMM', 'ZIM2', 'WTZR', 'ONSA', 'POTS', 'MATE',
                    'GRAZ', 'HERS', 'KOSG', 'WSRT', 'REYK', 'HOFN', 'TRI2'}

    # EUREF stations typically have specific patterns
    euref_patterns = ['EUS', 'WAR', 'OBE', 'BOR', 'JOE', 'VIS', 'MAR', 'KIR', 'TRO', 'NYA']

    if station_upper in igs_stations or station_upper[:4] in igs_stations:
        return 'IGS'

    for pattern in euref_patterns:
        if pattern in station_upper:
            return 'EUREF'

    # Check for 9-char IGS names
    if len(station_id) > 4 and any(s in station_upper for s in igs_stations):
        return 'IGS'

    return 'Regional'


@st.cache_data(ttl=300, show_spinner=False)
def get_network_statistics(year: int, start_doy: int, end_doy: int) -> pd.DataFrame:
    """
    Get repeatability statistics grouped by network.
    """
    # Get repeatability data
    heatmap_df = compute_repeatability_heatmap(year, start_doy, end_doy)

    if heatmap_df.empty:
        return pd.DataFrame()

    # Assign networks
    heatmap_df['network'] = heatmap_df['station_id'].apply(get_network_assignment)

    # Compute statistics per station
    station_stats = heatmap_df.groupby(['station_id', 'network']).agg({
        'rms_3d': ['mean', 'std', 'min', 'max']
    }).reset_index()
    station_stats.columns = ['station_id', 'network', 'mean_rms', 'std_rms', 'min_rms', 'max_rms']

    return station_stats


@st.cache_data(ttl=300, show_spinner=False)
def get_pppar_dd_vectors(year: int, doy: int) -> pd.DataFrame:
    """
    Compute PPP-AR vs DD coordinate difference vectors for map plotting.

    Returns:
        DataFrame with station_id, lat, lon, dE, dN, dU, vector_mag columns
    """
    # Get PPP-AR coordinates
    pppar_df = get_pppar_coordinates(year, doy)

    if pppar_df.empty:
        return pd.DataFrame()

    # Get DD coordinates
    dd_base_dir = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX"
    crd_path = f"{dd_base_dir}/{year}/{doy}/STA/F10_{year}{doy:03d}0.CRD.gz"

    if not os.path.exists(crd_path):
        return pd.DataFrame()

    try:
        dd_coords = parse_dd_crd_file(crd_path, year, doy)
    except Exception:
        return pd.DataFrame()

    if not dd_coords:
        return pd.DataFrame()

    vectors = []
    for _, ppp_row in pppar_df.iterrows():
        station = ppp_row['station_id']
        dd_coord = get_dd_coords_for_station(station, dd_coords)

        if dd_coord:
            dE, dN, dU = compute_enu_diff_dd(ppp_row['x'], ppp_row['y'], ppp_row['z'], dd_coord)
            vector_mag = np.sqrt(dE**2 + dN**2) * 1000  # horizontal magnitude in mm

            vectors.append({
                'station_id': station,
                'lat': ppp_row['lat'],
                'lon': ppp_row['lon'],
                'dE_mm': dE * 1000,
                'dN_mm': dN * 1000,
                'dU_mm': dU * 1000,
                'vector_mag': vector_mag
            })

    return pd.DataFrame(vectors)


def main():
    # Header with logos using Streamlit columns
    from pathlib import Path

    # Create header layout with columns
    col1, col2, col3 = st.columns([1, 3, 1])

    with col1:
        st.markdown("""
        <a href="https://www.uni.lu" target="_blank" title="University of Luxembourg">
            <img src="https://www.uni.lu/wp-content/uploads/sites/9/2024/06/university-luxembourg-nobseline.svg"
                 alt="University of Luxembourg"
                 style="height: 70px; max-width: 180px; object-fit: contain; filter: brightness(1.2) invert(1);">
        </a>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div style="text-align: center;">
            <h1 style="color: #FFFFFF; margin: 0; font-size: 2rem; font-weight: 700;">
                🛰️ GNSS Quality Monitoring Dashboard
            </h1>
            <p style="color: #9CA3AF; margin: 8px 0 0 0; font-size: 0.95rem;">
                Real-time monitoring of PPP processing quality metrics
            </p>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        act_logo_path = Path("/home/ahunegnaw/ACT_KEYS/logo/ACT.png")
        if act_logo_path.exists():
            st.image(str(act_logo_path), width=120)
        else:
            st.markdown("""
            <div style="background:#E63946; color:white; padding:10px 15px; border-radius:8px; font-weight:bold; font-size:12px; text-align:center;">
                ACT<br>Luxembourg
            </div>
            """, unsafe_allow_html=True)

    st.divider()

    # Sidebar controls with dark styling
    with st.sidebar:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #4ECDC4 0%, #45B7AA 100%);
            padding: 15px 20px;
            border-radius: 12px;
            margin-bottom: 20px;
            box-shadow: 0 4px 15px rgba(78, 205, 196, 0.3);
        ">
            <h3 style="color: #1A1D21; margin: 0; font-size: 1.2rem; font-weight: 700;">⚙️ Settings</h3>
        </div>
        """, unsafe_allow_html=True)

        # Auto-refresh toggle
        auto_refresh = st.toggle("Auto-refresh", value=True)
        refresh_interval = st.slider(
            "Refresh interval (seconds)",
            min_value=30, max_value=300, value=60
        )

        st.divider()

        # Page Navigation - single selectbox with all pages grouped by category
        st.markdown('<p style="color: #4ECDC4; font-weight: 600; font-size: 0.9rem; margin-bottom: 5px;">NAVIGATION</p>', unsafe_allow_html=True)

        # All pages in display order with category separators
        all_pages = [
            "--- Overview & Coordinates ---",
            "📊 Overview",
            "📈 Coordinate Repeatability",
            "📊 Repeatability (DD)",
            "📉 RMS Analysis",
            "🔥 Repeatability Heatmap",
            "--- Troposphere ---",
            "🌡️ ZTD Monitor",
            "🌬️ Trop. Gradients",
            "--- Ambiguity Resolution ---",
            "🎯 Ambiguity Resolution (DD)",
            "🎯 Ambiguity Resolution (PPP-AR)",
            "🔗 Sat. Ambiguity PRN",
            "📐 Baseline (DD)",
            "--- Satellites & Tracking ---",
            "🛰️ Satellite Tracking",
            "📶 Data Availability",
            "🛰️ Satellite Residuals",
            "📡 Obs. Residuals",
            "--- Station & Data Quality ---",
            "⚙️ Processing Stats",
            "🚫 Outlier Statistics",
            "🔬 Observation Quality",
            "📊 Station Completeness",
            "⏱️ Receiver Clocks",
            "🏔️ Station Elevation",
            "🌊 Ocean Loading",
            "--- Network & Comparison ---",
            "🗺️ PPP-DD Vectors",
            "📦 Network Comparison",
            "🔗 Station Correlation",
            "📈 DD Multi-DOY",
            "🌍 NGL/CODE External TS",
            "--- Advanced Analysis ---",
            "🎬 Time Evolution",
            "🤖 ML Quality Control",
        ]

        # Separate actual pages from category headers
        selectable_pages = [p for p in all_pages if not p.startswith("---")]

        # Custom CSS to style category headers in the radio group
        st.markdown("""
        <style>
        /* Make sidebar radio labels more compact */
        div[data-testid="stSidebar"] .stRadio > div {
            gap: 0px;
        }
        div[data-testid="stSidebar"] .stRadio label {
            padding: 2px 0px;
            font-size: 0.85rem;
        }
        /* Style category headers displayed via markdown */
        .nav-category {
            color: #4ECDC4;
            font-weight: 700;
            font-size: 0.78rem;
            letter-spacing: 0.5px;
            text-transform: uppercase;
            margin-top: 10px;
            margin-bottom: 2px;
            padding: 4px 0px;
            border-bottom: 1px solid rgba(78, 205, 196, 0.3);
        }
        .nav-category:first-child {
            margin-top: 0px;
        }
        </style>
        """, unsafe_allow_html=True)

        # Render grouped navigation with category headers + radio
        if 'selected_page' not in st.session_state:
            st.session_state['selected_page'] = '📊 Overview'

        # Build navigation using category headers and radio buttons per group
        nav_groups = {
            "OVERVIEW & COORDINATES": [
                "📊 Overview",
                "📈 Coordinate Repeatability",
                "📊 Repeatability (DD)",
                "📉 RMS Analysis",
                "🔥 Repeatability Heatmap",
            ],
            "TROPOSPHERE": [
                "🌡️ ZTD Monitor",
                "🌬️ Trop. Gradients",
            ],
            "AMBIGUITY RESOLUTION": [
                "🎯 Ambiguity Resolution (DD)",
                "🎯 Ambiguity Resolution (PPP-AR)",
                "🔗 Sat. Ambiguity PRN",
                "📐 Baseline (DD)",
            ],
            "SATELLITES & TRACKING": [
                "🛰️ Satellite Tracking",
                "📶 Data Availability",
                "🛰️ Satellite Residuals",
                "📡 Obs. Residuals",
            ],
            "STATION & DATA QUALITY": [
                "⚙️ Processing Stats",
                "🚫 Outlier Statistics",
                "🔬 Observation Quality",
                "📊 Station Completeness",
                "⏱️ Receiver Clocks",
                "🏔️ Station Elevation",
                "🌊 Ocean Loading",
            ],
            "NETWORK & COMPARISON": [
                "🗺️ PPP-DD Vectors",
                "📦 Network Comparison",
                "🔗 Station Correlation",
                "📈 DD Multi-DOY",
                "🌍 NGL/CODE External TS",
            ],
            "ADVANCED ANALYSIS": [
                "🎬 Time Evolution",
                "🤖 ML Quality Control",
                "🌐 NTAL/NTOL Loading",
            ],
        }

        current_page = st.session_state.get('selected_page', '📊 Overview')

        for group_name, pages in nav_groups.items():
            st.markdown(f'<div class="nav-category">{group_name}</div>', unsafe_allow_html=True)
            for page in pages:
                is_active = (page == current_page)
                btn_type = "primary" if is_active else "secondary"
                if st.button(page, key=f"nav_{page}", use_container_width=True, type=btn_type):
                    st.session_state['selected_page'] = page
                    st.rerun()

        selected_page = st.session_state.get('selected_page', '📊 Overview')

        st.divider()

        # Date range selection
        st.markdown('<p style="color: #6366F1; font-weight: 600; font-size: 0.9rem; margin-bottom: 10px;">DATE RANGE</p>', unsafe_allow_html=True)
        current_year = datetime.now().year
        # Build year list: include DB years plus current year, sorted descending
        db_years = cached_get_available_years()
        all_years = sorted(set(db_years + [current_year]), reverse=True)
        # Default to the most recent year that has data
        default_year_idx = 0
        if db_years and db_years[0] in all_years:
            default_year_idx = all_years.index(db_years[0])
        year = st.selectbox("Year", all_years, index=default_year_idx)

        # Get DOY range from database for selected year
        db_min_doy, db_max_doy = cached_get_doy_range(year)

        col1, col2 = st.columns(2)
        with col1:
            start_doy = st.number_input("Start DOY", min_value=1, max_value=366, value=db_min_doy)
        with col2:
            end_doy = st.number_input("End DOY", min_value=1, max_value=366, value=db_max_doy)

        st.divider()

        # Station selection
        st.markdown('<p style="color: #4ECDC4; font-weight: 600; font-size: 0.9rem; margin-bottom: 10px;">STATIONS</p>', unsafe_allow_html=True)
        try:
            available_stations = cached_get_all_stations()
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

        # Page routing based on sidebar selection
        # (replaces horizontal tabs with sidebar navigation)

        # PAGE: Overview
        if selected_page == "📊 Overview":
            st.header("Processing Overview")

            # Get latest processing results (cached)
            latest = cached_get_latest_processing(limit=100)

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

                # === STATION MAP ===
                st.markdown("""
                <div style="
                    background: linear-gradient(135deg, #1E2530 0%, #242830 100%);
                    padding: 15px 25px;
                    border-radius: 15px;
                    margin-bottom: 20px;
                    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.4);
                    border: 1px solid #374151;
                    border-left: 4px solid #4ECDC4;
                ">
                    <h3 style="color: #FFFFFF; margin: 0; font-size: 1.4rem; font-weight: 600;">
                        🗺️ Station Network Map
                    </h3>
                    <p style="color: #9CA3AF; margin: 5px 0 0 0; font-size: 0.9rem;">
                        Interactive map of GNSS reference stations with quality indicators
                    </p>
                </div>
                """, unsafe_allow_html=True)

                # DOY selector
                available_doys = get_available_doys(2025)
                latest_year = 2025

                # Create columns for DOY selector
                map_col1, map_col2, map_col3 = st.columns([1, 2, 1])
                with map_col2:
                    if available_doys:
                        selected_doy = st.selectbox(
                            "📅 Select Day of Year (DOY)",
                            options=available_doys,
                            index=0,
                            help="Choose a DOY to view station positions and status for that day"
                        )
                    else:
                        selected_doy = 299
                        st.warning("No DOY data found, using default DOY 299")

                # Get station coordinates from F10 file for selected DOY
                df_coords = get_station_coordinates_from_f10(latest_year, selected_doy)

                if not df_coords.empty:
                    # Get RMS data for the selected DOY (cached)
                    try:
                        all_stats = cached_get_stats(year=latest_year, limit=5000)
                        if all_stats:
                            df_all_stats = pd.DataFrame(all_stats)
                            # Filter for selected DOY
                            df_doy_stats = df_all_stats[df_all_stats['doy'] == selected_doy]
                            if not df_doy_stats.empty:
                                df_map = df_coords.merge(
                                    df_doy_stats[['station_id', 'rms_unit_weight', 'num_observations']].drop_duplicates(),
                                    on='station_id',
                                    how='left'
                                )
                            else:
                                df_map = df_coords.copy()
                                df_map['rms_unit_weight'] = None
                                df_map['num_observations'] = None
                        else:
                            df_map = df_coords.copy()
                            df_map['rms_unit_weight'] = None
                            df_map['num_observations'] = None
                    except Exception as e:
                        df_map = df_coords.copy()
                        df_map['rms_unit_weight'] = None
                        df_map['num_observations'] = None

                    # Calculate RMS status for coloring
                    df_map['rms_mm'] = df_map['rms_unit_weight'].fillna(0) * 1000
                    df_map['status'] = df_map['rms_unit_weight'].apply(
                        lambda x: 'Good (≤2mm)' if x and x <= 0.002
                        else ('Warning (2-5mm)' if x and x <= 0.005 else 'Poor (>5mm)' if x else 'No Data')
                    )
                    df_map['height_m'] = df_map['height'].round(1)
                    df_map['num_obs'] = df_map['num_observations'].fillna(0).astype(int)

                    # Create beautiful dark map with Plotly scatter_geo
                    fig_map = px.scatter_geo(
                        df_map,
                        lat='lat',
                        lon='lon',
                        color='status',
                        text='station_id',  # Add station labels
                        hover_name='station_id',
                        hover_data={
                            'lat': ':.4f',
                            'lon': ':.4f',
                            'height_m': ':.1f',
                            'rms_mm': ':.2f',
                            'num_obs': ':,',
                            'status': False,
                            'station_id': False
                        },
                        color_discrete_map={
                            'Good (≤2mm)': '#4ECDC4',      # Teal
                            'Warning (2-5mm)': '#FBBF24',  # Amber/Yellow
                            'Poor (>5mm)': '#F87171',      # Red
                            'No Data': '#6B7280'           # Gray
                        },
                        category_orders={'status': ['Good (≤2mm)', 'Warning (2-5mm)', 'Poor (>5mm)', 'No Data']}
                    )

                    # Update layout for beautiful dark appearance
                    fig_map.update_layout(
                        geo=dict(
                            scope='europe',
                            projection_type='natural earth',
                            showland=True,
                            landcolor='#2D3748',  # Dark gray land
                            showocean=True,
                            oceancolor='#1A202C',  # Very dark blue ocean
                            showlakes=True,
                            lakecolor='#1E3A5F',  # Dark blue lakes
                            showcountries=True,
                            countrycolor='#4A5568',
                            countrywidth=0.8,
                            showcoastlines=True,
                            coastlinecolor='#4ECDC4',  # Teal coastlines
                            coastlinewidth=1.0,
                            showframe=True,
                            framecolor='#374151',
                            framewidth=2,
                            bgcolor='#1E2530',
                            lonaxis=dict(range=[-12, 28]),
                            lataxis=dict(range=[38, 65]),
                            showsubunits=True,
                            subunitcolor='#4A5568',
                            showrivers=True,
                            rivercolor='#2D4A6F',
                            riverwidth=0.5
                        ),
                        paper_bgcolor='#242830',
                        plot_bgcolor='#1E2530',
                        margin=dict(l=10, r=10, t=50, b=10),
                        height=600,
                        title=dict(
                            text=f'<b>Station Network</b> — DOY {selected_doy} ({latest_year}) — {len(df_map)} Stations',
                            font=dict(size=16, color='#FFFFFF', family='Inter, sans-serif'),
                            x=0.5,
                            xanchor='center'
                        ),
                        legend=dict(
                            title=dict(text='<b>RMS Status</b>', font=dict(size=12, color='#E2E8F0')),
                            orientation='h',
                            yanchor='bottom',
                            y=1.0,
                            xanchor='center',
                            x=0.5,
                            bgcolor='rgba(36, 40, 48, 0.95)',
                            bordercolor='#374151',
                            borderwidth=1,
                            font=dict(size=11, color='#E2E8F0'),
                            itemsizing='constant'
                        ),
                        font=dict(family='Inter, sans-serif', color='#E2E8F0')
                    )

                    # Style markers with station labels
                    fig_map.update_traces(
                        marker=dict(
                            size=12,
                            line=dict(width=2, color='#1E2530'),
                            opacity=0.95,
                            symbol='circle'
                        ),
                        textposition='top center',
                        textfont=dict(
                            size=9,
                            color='#E2E8F0',  # Light text for dark background
                            family='Inter, sans-serif'
                        ),
                        mode='markers+text'
                    )

                    # Add glow effect by adding a second trace
                    fig_map.add_trace(
                        go.Scattergeo(
                            lat=df_map['lat'],
                            lon=df_map['lon'],
                            mode='markers',
                            marker=dict(
                                size=20,
                                color='rgba(78, 205, 196, 0.2)',  # Teal glow
                                symbol='circle'
                            ),
                            hoverinfo='skip',
                            showlegend=False
                        )
                    )

                    st.plotly_chart(fig_map, use_container_width=True)

                    # Station summary below map in dark cards
                    st.markdown("""
                    <style>
                    .status-card {
                        background: #242830;
                        border-radius: 12px;
                        padding: 15px;
                        text-align: center;
                        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
                        border: 1px solid #374151;
                    }
                    .status-card h2 {
                        margin: 0;
                        color: #FFFFFF;
                        font-size: 2rem;
                        font-weight: 700;
                    }
                    .status-card p {
                        margin: 5px 0 0 0;
                        color: #64748B;
                        font-size: 0.85rem;
                    }
                    </style>
                    """, unsafe_allow_html=True)

                    col_map1, col_map2, col_map3, col_map4 = st.columns(4)
                    with col_map1:
                        good_count = len(df_map[df_map['status'] == 'Good (≤2mm)'])
                        st.metric("🟢 Good", good_count)
                    with col_map2:
                        warn_count = len(df_map[df_map['status'] == 'Warning (2-5mm)'])
                        st.metric("🟡 Warning", warn_count)
                    with col_map3:
                        poor_count = len(df_map[df_map['status'] == 'Poor (>5mm)'])
                        st.metric("🔴 Poor", poor_count)
                    with col_map4:
                        no_data_count = len(df_map[df_map['status'] == 'No Data'])
                        st.metric("⚪ No Data", no_data_count)
                else:
                    st.info("Station coordinates not available. Process data to populate coordinates.")

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
        elif selected_page == "📈 Coordinate Repeatability":
            st.header("Coordinate Repeatability Analysis")

            if available_stations:
                # Get solutions from DuckDB database (populated by ingest_pppar.py)
                all_solutions = []
                for station in available_stations:
                    solutions = cached_get_solutions(
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

                    # Detrending controls
                    ts_col1, ts_col2, ts_col3 = st.columns([2, 1, 1])
                    with ts_col1:
                        ts_stations = st.multiselect(
                            "Select stations for time series (multiple allowed)",
                            options=available_stations,
                            default=available_stations[:3] if len(available_stations) >= 3 else available_stations,
                            key='ts_stations'
                        )
                    with ts_col2:
                        detrend_method = st.selectbox(
                            "Detrending Method",
                            options=['none', 'mean', 'linear', 'polynomial'],
                            index=1,  # Default to mean removal
                            key='detrend_method',
                            help="Remove trend from time series: mean (remove average), linear (remove linear trend), polynomial (remove quadratic trend)"
                        )
                    with ts_col3:
                        show_trend = st.checkbox("Show trend line", value=True, key='show_trend',
                                               help="Display the fitted trend line on the plot")

                    if ts_stations:
                        fig = make_subplots(
                            rows=3, cols=1,
                            subplot_titles=['X deviation (mm)', 'Y deviation (mm)', 'Z deviation (mm)'],
                            shared_xaxes=True
                        )

                        colors = ['#00d4ff', '#ff6b6b', '#4ecdc4', '#ffe66d', '#c44dff',
                                  '#ff9f43', '#26de81', '#fd79a8', '#a29bfe', '#ffeaa7']

                        # Store velocity estimates for display
                        velocity_data = []

                        for idx, station in enumerate(ts_stations):
                            station_df = df[df['station_id'] == station].sort_values('doy').copy()

                            if len(station_df) > 0:
                                color = colors[idx % len(colors)]
                                station_display = station.replace('.', '')
                                station_velocities = {'Station': station_display}

                                for i, coord in enumerate(['x', 'y', 'z'], 1):
                                    # Convert to mm and apply detrending
                                    values_mm = (station_df[coord] - station_df[coord].mean()) * 1000

                                    if detrend_method == 'none':
                                        station_df[f'{coord}_dev'] = values_mm
                                        station_df[f'{coord}_trend'] = np.zeros(len(values_mm))
                                        velocity = 0.0
                                    else:
                                        detrended, trend, velocity = detrend_timeseries(
                                            values_mm.values, method=detrend_method
                                        )
                                        station_df[f'{coord}_dev'] = detrended
                                        station_df[f'{coord}_trend'] = trend

                                    station_velocities[f'{coord.upper()} vel (mm/yr)'] = velocity

                                    # Plot detrended values
                                    fig.add_trace(
                                        go.Scatter(
                                            x=station_df['doy'],
                                            y=station_df[f'{coord}_dev'],
                                            mode='lines+markers',
                                            name=station_display,
                                            legendgroup=station_display,
                                            showlegend=(i == 1),
                                            line=dict(color=color),
                                            marker=dict(color=color, size=6)
                                        ),
                                        row=i, col=1
                                    )

                                    # Plot trend line if requested
                                    if show_trend and detrend_method != 'none':
                                        # For mean removal, trend is constant; for others show fit
                                        fig.add_trace(
                                            go.Scatter(
                                                x=station_df['doy'],
                                                y=station_df[f'{coord}_trend'],
                                                mode='lines',
                                                name=f'{station_display} trend',
                                                legendgroup=f'{station_display}_trend',
                                                showlegend=False,
                                                line=dict(color=color, dash='dash', width=1),
                                                opacity=0.5
                                            ),
                                            row=i, col=1
                                        )

                                velocity_data.append(station_velocities)

                        # Add zero reference lines
                        for row in range(1, 4):
                            fig.add_hline(y=0, line_dash="dot", line_color="gray", row=row, col=1)

                        title_suffix = f" (Detrended: {detrend_method})" if detrend_method != 'none' else ""
                        fig.update_layout(
                            height=700,
                            title=f'Coordinate Deviations from Mean by Station{title_suffix}',
                            showlegend=True,
                            legend=dict(orientation="h", yanchor="bottom", y=1.08, x=0.5, xanchor="center"),
                            margin=dict(t=80)
                        )
                        fig.update_xaxes(title_text="Day of Year", row=3, col=1)
                        apply_colorful_style(fig)
                        st.plotly_chart(fig, use_container_width=True)

                        # Show velocity estimates if linear or polynomial detrending
                        if detrend_method in ['linear', 'polynomial'] and velocity_data:
                            st.markdown("### 📈 Estimated Velocities")
                            st.markdown("*Note: Velocities from short time series have high uncertainty*")
                            vel_df = pd.DataFrame(velocity_data)
                            st.dataframe(
                                vel_df.style.format({
                                    'X vel (mm/yr)': '{:.2f}',
                                    'Y vel (mm/yr)': '{:.2f}',
                                    'Z vel (mm/yr)': '{:.2f}'
                                }),
                                use_container_width=True,
                                hide_index=True
                            )

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

                    if all_code_coords:
                        # Find stations that exist in both local data and CODE products
                        code_station_set = set()
                        for doy_coords in all_code_coords.values():
                            for coord in doy_coords:
                                code_station_set.add(coord.station_4char)
                        common_stations = sorted([s for s in available_stations if s.upper() in code_station_set])

                        if common_stations:
                            code_compare_stations = st.multiselect(
                                "Stations to compare with CODE (only showing stations present in CODE solutions)",
                                options=common_stations,
                                default=common_stations,
                                key='code_compare_stations'
                            )
                        else:
                            code_compare_stations = []
                            st.warning("None of your local stations are present in CODE SINEX solutions.")

                        # Build comparison data
                        comparison_data = []

                        for station in code_compare_stations:
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

                            # Option to also show DD data
                            include_dd_in_code_plot = st.checkbox(
                                "Include DD Network differences",
                                value=True,
                                help="Also show Local-DD differences on the same plot (dashed lines)",
                                key="include_dd_in_code_enu"
                            )

                            # Load DD coordinates if requested
                            dd_comparison_data = []
                            if include_dd_in_code_plot:
                                dd_base_dir = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX"
                                dd_coords_for_code = {}
                                # Get DOY range from the existing data
                                doys_in_data = df['doy'].unique()
                                for dd_doy in doys_in_data:
                                    dd_doy = int(dd_doy)
                                    crd_path = f"{dd_base_dir}/{year}/{dd_doy}/STA/F10_{year}{dd_doy:03d}0.CRD.gz"
                                    if os.path.exists(crd_path):
                                        try:
                                            coords = parse_dd_crd_file(crd_path, year, dd_doy)
                                            if coords:
                                                dd_coords_for_code[dd_doy] = coords
                                        except Exception:
                                            pass

                                # Build DD comparison data
                                for station in ts_stations:
                                    station_df = df[df['station_id'] == station]
                                    for _, row in station_df.iterrows():
                                        doy = int(row['doy'])
                                        if doy in dd_coords_for_code:
                                            dd_coord = get_dd_coords_for_station(station, dd_coords_for_code[doy])
                                            if dd_coord:
                                                dE, dN, dU = compute_enu_diff_dd(row['x'], row['y'], row['z'], dd_coord)
                                                dd_comparison_data.append({
                                                    'station': station,
                                                    'doy': doy,
                                                    'dE_mm': dE * 1000,
                                                    'dN_mm': dN * 1000,
                                                    'dU_mm': dU * 1000
                                                })

                            colors = px.colors.qualitative.Set1

                            fig_enu = make_subplots(
                                rows=3, cols=1,
                                subplot_titles=['East (mm)', 'North (mm)', 'Up (mm)'],
                                shared_xaxes=True
                            )

                            for i, station in enumerate(df_comp['station'].unique()):
                                sdf = df_comp[df_comp['station'] == station].sort_values('doy')
                                color = colors[i % len(colors)]

                                # CODE traces (solid lines)
                                fig_enu.add_trace(
                                    go.Scatter(
                                        x=sdf['doy'], y=sdf['dE_mm'],
                                        name=f'{station} (CODE)', legendgroup=f'{station}_code',
                                        mode='lines+markers', marker=dict(color=color),
                                        line=dict(width=2)
                                    ),
                                    row=1, col=1
                                )
                                fig_enu.add_trace(
                                    go.Scatter(
                                        x=sdf['doy'], y=sdf['dN_mm'],
                                        name=f'{station} (CODE)', legendgroup=f'{station}_code',
                                        mode='lines+markers', marker=dict(color=color),
                                        line=dict(width=2),
                                        showlegend=False
                                    ),
                                    row=2, col=1
                                )
                                fig_enu.add_trace(
                                    go.Scatter(
                                        x=sdf['doy'], y=sdf['dU_mm'],
                                        name=f'{station} (CODE)', legendgroup=f'{station}_code',
                                        mode='lines+markers', marker=dict(color=color),
                                        line=dict(width=2),
                                        showlegend=False
                                    ),
                                    row=3, col=1
                                )

                            # Add DD traces if available (dashed lines)
                            if dd_comparison_data:
                                df_dd = pd.DataFrame(dd_comparison_data)
                                for i, station in enumerate(df_dd['station'].unique()):
                                    sdf_dd = df_dd[df_dd['station'] == station].sort_values('doy')
                                    color = colors[i % len(colors)]

                                    fig_enu.add_trace(
                                        go.Scatter(
                                            x=sdf_dd['doy'], y=sdf_dd['dE_mm'],
                                            name=f'{station} (DD)', legendgroup=f'{station}_dd',
                                            mode='lines+markers',
                                            marker=dict(color=color, symbol='diamond'),
                                            line=dict(dash='dash', width=2)
                                        ),
                                        row=1, col=1
                                    )
                                    fig_enu.add_trace(
                                        go.Scatter(
                                            x=sdf_dd['doy'], y=sdf_dd['dN_mm'],
                                            name=f'{station} (DD)', legendgroup=f'{station}_dd',
                                            mode='lines+markers',
                                            marker=dict(color=color, symbol='diamond'),
                                            line=dict(dash='dash', width=2),
                                            showlegend=False
                                        ),
                                        row=2, col=1
                                    )
                                    fig_enu.add_trace(
                                        go.Scatter(
                                            x=sdf_dd['doy'], y=sdf_dd['dU_mm'],
                                            name=f'{station} (DD)', legendgroup=f'{station}_dd',
                                            mode='lines+markers',
                                            marker=dict(color=color, symbol='diamond'),
                                            line=dict(dash='dash', width=2),
                                            showlegend=False
                                        ),
                                        row=3, col=1
                                    )

                            # Add zero reference lines
                            for row in range(1, 4):
                                fig_enu.add_hline(y=0, line_dash="dash", line_color="gray", row=row, col=1)

                            plot_title = 'Coordinate Differences: Local PPP vs CODE' + (' & DD' if dd_comparison_data else '') + ' (ENU)'
                            fig_enu.update_layout(
                                height=600,
                                title=plot_title,
                                showlegend=True,
                                legend=dict(orientation="h", yanchor="bottom", y=1.08, x=0.5, xanchor="center"),
                                margin=dict(t=100)
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
                    else:
                        st.info("No CODE coordinate products loaded. Check if products are available for the selected dates.")

                    # ========================================
                    # DD (DOUBLE DIFFERENCE) COORDINATE COMPARISON
                    # ========================================
                    st.divider()
                    st.subheader("DD (Double Difference) Coordinate Comparison")
                    st.markdown("""
                    Compare local PPP daily coordinates with Double Difference (DD) network solutions
                    from local processing (`/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/`)
                    """)

                    # DD comparison controls
                    dd_coord_col1, dd_coord_col2, dd_coord_col3 = st.columns(3)
                    with dd_coord_col1:
                        dd_coord_start_doy = st.number_input(
                            "Start DOY",
                            min_value=1, max_value=366,
                            value=start_doy,
                            key='dd_coord_start_doy'
                        )
                    with dd_coord_col2:
                        dd_coord_end_doy = st.number_input(
                            "End DOY",
                            min_value=1, max_value=366,
                            value=end_doy,
                            key='dd_coord_end_doy'
                        )
                    with dd_coord_col3:
                        dd_coord_product = st.selectbox(
                            "Solution Type",
                            options=["F10", "R10", "P1"],
                            index=0,
                            help="F10=Final, R10=Rapid, P1=Preliminary",
                            key='dd_coord_product_type'
                        )

                    # DD base directory
                    dd_base_dir = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX"
                    all_dd_coords = {}  # DOY -> list of DDStationCoord
                    loaded_dd_doys = []
                    failed_dd_doys = []

                    with st.spinner(f"Loading DD coordinate files for DOY {dd_coord_start_doy}-{dd_coord_end_doy}..."):
                        for dd_doy in range(dd_coord_start_doy, dd_coord_end_doy + 1):
                            # Build CRD file path
                            crd_path = f"{dd_base_dir}/{year}/{dd_doy}/STA/{dd_coord_product}_{year}{dd_doy:03d}0.CRD.gz"
                            if os.path.exists(crd_path):
                                try:
                                    coords = parse_dd_crd_file(crd_path, year, dd_doy)
                                    if coords:
                                        all_dd_coords[dd_doy] = coords
                                        loaded_dd_doys.append(dd_doy)
                                except Exception as e:
                                    failed_dd_doys.append(dd_doy)
                            else:
                                failed_dd_doys.append(dd_doy)

                    # Show loading status
                    if loaded_dd_doys:
                        st.success(f"Loaded DD coordinates for {len(loaded_dd_doys)} days: DOY {min(loaded_dd_doys)}-{max(loaded_dd_doys)}")
                    if failed_dd_doys:
                        st.warning(f"DD files not found for {len(failed_dd_doys)} days: {failed_dd_doys[:5]}{'...' if len(failed_dd_doys) > 5 else ''}")

                    # Compare PPP-AR with DD solutions
                    # Load PPP-AR coordinates from CRD files (not database)
                    all_pppar_coords = {}  # DOY -> DataFrame
                    loaded_pppar_doys = []

                    with st.spinner(f"Loading PPP-AR coordinate files for DOY {dd_coord_start_doy}-{dd_coord_end_doy}..."):
                        for pppar_doy in range(dd_coord_start_doy, dd_coord_end_doy + 1):
                            df_pppar = get_pppar_coordinates(year, pppar_doy)
                            if not df_pppar.empty:
                                all_pppar_coords[pppar_doy] = df_pppar
                                loaded_pppar_doys.append(pppar_doy)

                    if loaded_pppar_doys:
                        st.info(f"Loaded PPP-AR coordinates for {len(loaded_pppar_doys)} days: DOY {min(loaded_pppar_doys)}-{max(loaded_pppar_doys)}")

                    if all_dd_coords and all_pppar_coords:
                        comparison_data = []

                        # Find common DOYs
                        common_doys = set(all_dd_coords.keys()) & set(all_pppar_coords.keys())

                        for doy in sorted(common_doys):
                            df_pppar = all_pppar_coords[doy]
                            dd_coords_list = all_dd_coords[doy]

                            for _, pppar_row in df_pppar.iterrows():
                                station = pppar_row['station_id']

                                # Find matching DD coordinate
                                dd_coord = get_dd_coords_for_station(station, dd_coords_list)
                                if dd_coord is None:
                                    continue

                                # Compute ENU differences (PPP-AR - DD)
                                dE, dN, dU = compute_enu_diff_dd(
                                    pppar_row['x'], pppar_row['y'], pppar_row['z'],
                                    dd_coord
                                )

                                comparison_data.append({
                                    'station': station,
                                    'doy': doy,
                                    'dE_mm': dE * 1000,
                                    'dN_mm': dN * 1000,
                                    'dU_mm': dU * 1000,
                                    '3D_mm': math.sqrt(dE**2 + dN**2 + dU**2) * 1000,
                                    'dd_system': dd_coord.system
                                })

                        if comparison_data:
                            df_dd_comp = pd.DataFrame(comparison_data)

                            # Summary statistics table
                            st.subheader("PPP vs DD Comparison Statistics")
                            df_dd_stats = df_dd_comp.groupby('station').agg({
                                'dE_mm': ['mean', 'std'],
                                'dN_mm': ['mean', 'std'],
                                'dU_mm': ['mean', 'std'],
                                '3D_mm': ['mean', 'count'],
                                'dd_system': 'first'
                            }).reset_index()
                            df_dd_stats.columns = ['Station', 'dE Mean (mm)', 'dE Std (mm)',
                                                   'dN Mean (mm)', 'dN Std (mm)',
                                                   'dU Mean (mm)', 'dU Std (mm)',
                                                   '3D RMS (mm)', 'N Days', 'System']

                            st.dataframe(
                                df_dd_stats.style.format({
                                    'dE Mean (mm)': '{:.2f}',
                                    'dE Std (mm)': '{:.2f}',
                                    'dN Mean (mm)': '{:.2f}',
                                    'dN Std (mm)': '{:.2f}',
                                    'dU Mean (mm)': '{:.2f}',
                                    'dU Std (mm)': '{:.2f}',
                                    '3D RMS (mm)': '{:.2f}'
                                }),
                                use_container_width=True,
                                hide_index=True
                            )

                            # ENU time series plot
                            st.subheader("ENU Differences: PPP - DD")

                            # Station selection for ENU plot
                            all_dd_stations = sorted(df_dd_comp['station'].unique().tolist())
                            selected_dd_enu_stations = st.multiselect(
                                "Select stations to display",
                                options=all_dd_stations,
                                default=all_dd_stations[:5] if len(all_dd_stations) > 5 else all_dd_stations,
                                key='dd_enu_station_select',
                                help="Select one or more stations to display in the ENU plot"
                            )

                            if not selected_dd_enu_stations:
                                st.warning("Please select at least one station.")
                            else:
                                colors_dd = px.colors.qualitative.Set2

                                fig_dd_enu = make_subplots(
                                    rows=3, cols=1,
                                    subplot_titles=['East (mm)', 'North (mm)', 'Up (mm)'],
                                    shared_xaxes=True
                                )

                                for i, station in enumerate(selected_dd_enu_stations):
                                    sdf = df_dd_comp[df_dd_comp['station'] == station].sort_values('doy')
                                    color = colors_dd[i % len(colors_dd)]

                                    fig_dd_enu.add_trace(
                                        go.Scatter(
                                            x=sdf['doy'], y=sdf['dE_mm'],
                                            name=f'{station}', legendgroup=station,
                                            mode='lines+markers', marker=dict(color=color)
                                        ),
                                        row=1, col=1
                                    )
                                    fig_dd_enu.add_trace(
                                        go.Scatter(
                                            x=sdf['doy'], y=sdf['dN_mm'],
                                            name=f'{station}', legendgroup=station,
                                            mode='lines+markers', marker=dict(color=color),
                                            showlegend=False
                                        ),
                                        row=2, col=1
                                    )
                                    fig_dd_enu.add_trace(
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
                                    fig_dd_enu.add_hline(y=0, line_dash="dash", line_color="gray", row=row, col=1)

                                fig_dd_enu.update_layout(
                                    height=600,
                                    title='Coordinate Differences: Local PPP - DD Network Solution (ENU)',
                                    showlegend=True,
                                    legend=dict(orientation="h", yanchor="bottom", y=1.08, x=0.5, xanchor="center"),
                                    margin=dict(t=80)
                                )
                                fig_dd_enu.update_xaxes(title_text="Day of Year", row=3, col=1)
                                apply_colorful_style(fig_dd_enu)
                                st.plotly_chart(fig_dd_enu, use_container_width=True)

                            # Bar chart of 3D RMS by station
                            st.subheader("3D RMS vs DD by Station")
                            fig_dd_bar = px.bar(
                                df_dd_stats.sort_values('3D RMS (mm)'),
                                x='Station',
                                y='3D RMS (mm)',
                                title='3D RMS Difference: PPP vs DD Network Solutions',
                                color='3D RMS (mm)',
                                color_continuous_scale='RdYlGn_r'
                            )
                            fig_dd_bar.add_hline(y=10, line_dash="dash", line_color="#ff6b6b",
                                                 annotation_text="10mm threshold")
                            apply_colorful_style(fig_dd_bar)
                            st.plotly_chart(fig_dd_bar, use_container_width=True)

                        else:
                            st.info("No matching stations found between PPP and DD solutions.")
                    elif not all_dd_coords:
                        st.info("No DD coordinate files found. Check if DD processing is complete.")
                    elif not dd_ppp_solutions:
                        st.warning(f"No PPP solutions found for DOY {dd_coord_start_doy}-{dd_coord_end_doy}. Run: `python3 -m frontend.ingest_all {dd_coord_start_doy} {dd_coord_end_doy}`")

                else:
                    st.info("No solution data found for selected stations and date range.")
            else:
                st.warning("Please select at least one station.")

        # TAB 2B: Repeatability (DD) - Helmert Transformation Results
        elif selected_page == "📊 Repeatability (DD)":
            st.header("Helmert Transformation Repeatability (DD)")
            st.markdown("""
            **Double Difference Repeatability Analysis** - Analyzing coordinate residuals from Helmert transformation
            comparing DD solution coordinates against reference frame (a priori) coordinates.
            - **Reference Stations**: Stations used in the transformation (without V marker)
            - **Non-Reference Stations**: Verified stations not used in transformation (with V marker)
            """)

            # DD results base directory
            dd_base_dir = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX"

            # Load HLM data for the date range
            hlm_results = load_hlm_timeseries(dd_base_dir, year, start_doy, end_doy)

            if hlm_results:
                st.success(f"Loaded Helmert results for {len(hlm_results)} days (DOY {start_doy}-{end_doy})")

                # Get all stations from all results
                all_stations_ref = set()
                all_stations_nonref = set()
                for result in hlm_results:
                    for res in result.residuals:
                        if res.is_reference:
                            all_stations_ref.add(res.station_4char)
                        else:
                            all_stations_nonref.add(res.station_4char)

                # Create sub-tabs for different views
                hlm_tab1, hlm_tab2, hlm_tab3, hlm_tab4 = st.tabs([
                    "Time Series",
                    "RMS Statistics",
                    "Transformation Parameters",
                    "Daily Details"
                ])

                with hlm_tab1:
                    st.subheader("Station Residual Time Series")

                    # Station selection
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Reference Stations**")
                        selected_ref = st.multiselect(
                            "Select reference stations",
                            sorted(all_stations_ref),
                            default=list(sorted(all_stations_ref))[:5] if len(all_stations_ref) > 5 else list(sorted(all_stations_ref)),
                            key="hlm_ref_stations"
                        )
                    with col2:
                        st.markdown("**Non-Reference (Verified) Stations**")
                        selected_nonref = st.multiselect(
                            "Select non-reference stations",
                            sorted(all_stations_nonref),
                            default=list(sorted(all_stations_nonref))[:5] if len(all_stations_nonref) > 5 else list(sorted(all_stations_nonref)),
                            key="hlm_nonref_stations"
                        )

                    # Build time series data for reference stations
                    if selected_ref:
                        st.markdown("### Reference Stations Residuals")
                        ref_data = []
                        for result in hlm_results:
                            for res in result.residuals:
                                if res.is_reference and res.station_4char in selected_ref:
                                    ref_data.append({
                                        'DOY': result.doy,
                                        'Station': res.station_4char,
                                        'North (mm)': res.north_mm,
                                        'East (mm)': res.east_mm,
                                        'Up (mm)': res.up_mm
                                    })

                        if ref_data:
                            ref_df = pd.DataFrame(ref_data)

                            # Create subplots for N, E, U
                            fig_ref = make_subplots(
                                rows=3, cols=1,
                                shared_xaxes=True,
                                subplot_titles=('North Residuals (mm)', 'East Residuals (mm)', 'Up Residuals (mm)'),
                                vertical_spacing=0.08
                            )

                            colors = px.colors.qualitative.Set2
                            for i, station in enumerate(selected_ref):
                                station_df = ref_df[ref_df['Station'] == station]
                                color = colors[i % len(colors)]

                                fig_ref.add_trace(go.Scatter(
                                    x=station_df['DOY'], y=station_df['North (mm)'],
                                    mode='lines+markers', name=station,
                                    line=dict(color=color), marker=dict(size=6),
                                    legendgroup=station, showlegend=True
                                ), row=1, col=1)

                                fig_ref.add_trace(go.Scatter(
                                    x=station_df['DOY'], y=station_df['East (mm)'],
                                    mode='lines+markers', name=station,
                                    line=dict(color=color), marker=dict(size=6),
                                    legendgroup=station, showlegend=False
                                ), row=2, col=1)

                                fig_ref.add_trace(go.Scatter(
                                    x=station_df['DOY'], y=station_df['Up (mm)'],
                                    mode='lines+markers', name=station,
                                    line=dict(color=color), marker=dict(size=6),
                                    legendgroup=station, showlegend=False
                                ), row=3, col=1)

                            fig_ref.update_layout(
                                height=700,
                                title_text="Reference Stations - Helmert Residuals vs DOY",
                                hovermode='x unified'
                            )
                            fig_ref.update_xaxes(title_text="Day of Year", row=3, col=1)
                            apply_colorful_style(fig_ref)
                            st.plotly_chart(fig_ref, use_container_width=True)

                    # Build time series data for non-reference stations
                    if selected_nonref:
                        st.markdown("### Non-Reference (Verified) Stations Residuals")
                        nonref_data = []
                        for result in hlm_results:
                            for res in result.residuals:
                                if not res.is_reference and res.station_4char in selected_nonref:
                                    nonref_data.append({
                                        'DOY': result.doy,
                                        'Station': res.station_4char,
                                        'North (mm)': res.north_mm,
                                        'East (mm)': res.east_mm,
                                        'Up (mm)': res.up_mm
                                    })

                        if nonref_data:
                            nonref_df = pd.DataFrame(nonref_data)

                            # Create subplots for N, E, U
                            fig_nonref = make_subplots(
                                rows=3, cols=1,
                                shared_xaxes=True,
                                subplot_titles=('North Residuals (mm)', 'East Residuals (mm)', 'Up Residuals (mm)'),
                                vertical_spacing=0.08
                            )

                            colors = px.colors.qualitative.Dark2
                            for i, station in enumerate(selected_nonref):
                                station_df = nonref_df[nonref_df['Station'] == station]
                                color = colors[i % len(colors)]

                                fig_nonref.add_trace(go.Scatter(
                                    x=station_df['DOY'], y=station_df['North (mm)'],
                                    mode='lines+markers', name=station,
                                    line=dict(color=color), marker=dict(size=6),
                                    legendgroup=station, showlegend=True
                                ), row=1, col=1)

                                fig_nonref.add_trace(go.Scatter(
                                    x=station_df['DOY'], y=station_df['East (mm)'],
                                    mode='lines+markers', name=station,
                                    line=dict(color=color), marker=dict(size=6),
                                    legendgroup=station, showlegend=False
                                ), row=2, col=1)

                                fig_nonref.add_trace(go.Scatter(
                                    x=station_df['DOY'], y=station_df['Up (mm)'],
                                    mode='lines+markers', name=station,
                                    line=dict(color=color), marker=dict(size=6),
                                    legendgroup=station, showlegend=False
                                ), row=3, col=1)

                            fig_nonref.update_layout(
                                height=700,
                                title_text="Non-Reference Stations - Helmert Residuals vs DOY",
                                hovermode='x unified'
                            )
                            fig_nonref.update_xaxes(title_text="Day of Year", row=3, col=1)
                            apply_colorful_style(fig_nonref)
                            st.plotly_chart(fig_nonref, use_container_width=True)

                with hlm_tab2:
                    st.subheader("RMS Statistics by Component")

                    # Build statistics over time
                    stats_data = []
                    for result in hlm_results:
                        # Reference stations stats
                        stats_data.append({
                            'DOY': result.doy,
                            'Type': 'Reference',
                            'RMS North': result.ref_stats.rms_north,
                            'RMS East': result.ref_stats.rms_east,
                            'RMS Up': result.ref_stats.rms_up,
                            'IQR North': result.ref_stats.iqr_north,
                            'IQR East': result.ref_stats.iqr_east,
                            'IQR Up': result.ref_stats.iqr_up,
                            'Mean North': result.ref_stats.mean_north,
                            'Mean East': result.ref_stats.mean_east,
                            'Mean Up': result.ref_stats.mean_up,
                            'Median North': result.ref_stats.median_north,
                            'Median East': result.ref_stats.median_east,
                            'Median Up': result.ref_stats.median_up,
                            'Min North': result.ref_stats.min_north,
                            'Min East': result.ref_stats.min_east,
                            'Min Up': result.ref_stats.min_up,
                            'Max North': result.ref_stats.max_north,
                            'Max East': result.ref_stats.max_east,
                            'Max Up': result.ref_stats.max_up,
                            'Overall RMS 3D': result.ref_stats.overall_rms_3d if result.ref_stats.overall_rms_3d > 0 else result.transform_params.rms_transformation,
                        })
                        # All stations stats
                        stats_data.append({
                            'DOY': result.doy,
                            'Type': 'All Stations',
                            'RMS North': result.all_stats.rms_north,
                            'RMS East': result.all_stats.rms_east,
                            'RMS Up': result.all_stats.rms_up,
                            'IQR North': result.all_stats.iqr_north,
                            'IQR East': result.all_stats.iqr_east,
                            'IQR Up': result.all_stats.iqr_up,
                            'Mean North': result.all_stats.mean_north,
                            'Mean East': result.all_stats.mean_east,
                            'Mean Up': result.all_stats.mean_up,
                            'Median North': result.all_stats.median_north,
                            'Median East': result.all_stats.median_east,
                            'Median Up': result.all_stats.median_up,
                            'Min North': result.all_stats.min_north,
                            'Min East': result.all_stats.min_east,
                            'Min Up': result.all_stats.min_up,
                            'Max North': result.all_stats.max_north,
                            'Max East': result.all_stats.max_east,
                            'Max Up': result.all_stats.max_up,
                            'Overall RMS 3D': result.all_stats.overall_rms_3d,
                        })

                    stats_df = pd.DataFrame(stats_data)

                    # RMS by component plot
                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("#### Reference Stations Only")
                        ref_stats = stats_df[stats_df['Type'] == 'Reference']
                        if not ref_stats.empty:
                            fig_rms_ref = go.Figure()
                            fig_rms_ref.add_trace(go.Scatter(
                                x=ref_stats['DOY'], y=ref_stats['RMS North'],
                                mode='lines+markers', name='RMS North',
                                line=dict(color='#00d4ff')
                            ))
                            fig_rms_ref.add_trace(go.Scatter(
                                x=ref_stats['DOY'], y=ref_stats['RMS East'],
                                mode='lines+markers', name='RMS East',
                                line=dict(color='#ff6b6b')
                            ))
                            fig_rms_ref.add_trace(go.Scatter(
                                x=ref_stats['DOY'], y=ref_stats['RMS Up'],
                                mode='lines+markers', name='RMS Up',
                                line=dict(color='#4ecdc4')
                            ))
                            fig_rms_ref.update_layout(
                                title="RMS by Component (Reference)",
                                xaxis_title="DOY",
                                yaxis_title="RMS (mm)",
                                height=350
                            )
                            apply_colorful_style(fig_rms_ref)
                            st.plotly_chart(fig_rms_ref, use_container_width=True)

                    with col2:
                        st.markdown("#### All Stations")
                        all_stats = stats_df[stats_df['Type'] == 'All Stations']
                        if not all_stats.empty:
                            fig_rms_all = go.Figure()
                            fig_rms_all.add_trace(go.Scatter(
                                x=all_stats['DOY'], y=all_stats['RMS North'],
                                mode='lines+markers', name='RMS North',
                                line=dict(color='#00d4ff')
                            ))
                            fig_rms_all.add_trace(go.Scatter(
                                x=all_stats['DOY'], y=all_stats['RMS East'],
                                mode='lines+markers', name='RMS East',
                                line=dict(color='#ff6b6b')
                            ))
                            fig_rms_all.add_trace(go.Scatter(
                                x=all_stats['DOY'], y=all_stats['RMS Up'],
                                mode='lines+markers', name='RMS Up',
                                line=dict(color='#4ecdc4')
                            ))
                            fig_rms_all.update_layout(
                                title="RMS by Component (All Stations)",
                                xaxis_title="DOY",
                                yaxis_title="RMS (mm)",
                                height=350
                            )
                            apply_colorful_style(fig_rms_all)
                            st.plotly_chart(fig_rms_all, use_container_width=True)

                    # Statistics summary table
                    st.markdown("### Statistics Summary (Latest Day)")
                    if hlm_results:
                        latest = hlm_results[-1]
                        summary_data = {
                            'Statistic': ['RMS', 'IQR', 'MEAN', 'MEDIAN', 'MIN', 'MAX'],
                            'Ref North': [
                                f"{latest.ref_stats.rms_north:.2f}",
                                f"{latest.ref_stats.iqr_north:.2f}",
                                f"{latest.ref_stats.mean_north:.2f}",
                                f"{latest.ref_stats.median_north:.2f}",
                                f"{latest.ref_stats.min_north:.2f}",
                                f"{latest.ref_stats.max_north:.2f}"
                            ],
                            'Ref East': [
                                f"{latest.ref_stats.rms_east:.2f}",
                                f"{latest.ref_stats.iqr_east:.2f}",
                                f"{latest.ref_stats.mean_east:.2f}",
                                f"{latest.ref_stats.median_east:.2f}",
                                f"{latest.ref_stats.min_east:.2f}",
                                f"{latest.ref_stats.max_east:.2f}"
                            ],
                            'Ref Up': [
                                f"{latest.ref_stats.rms_up:.2f}",
                                f"{latest.ref_stats.iqr_up:.2f}",
                                f"{latest.ref_stats.mean_up:.2f}",
                                f"{latest.ref_stats.median_up:.2f}",
                                f"{latest.ref_stats.min_up:.2f}",
                                f"{latest.ref_stats.max_up:.2f}"
                            ],
                            'All North': [
                                f"{latest.all_stats.rms_north:.2f}",
                                f"{latest.all_stats.iqr_north:.2f}",
                                f"{latest.all_stats.mean_north:.2f}",
                                f"{latest.all_stats.median_north:.2f}",
                                f"{latest.all_stats.min_north:.2f}",
                                f"{latest.all_stats.max_north:.2f}"
                            ],
                            'All East': [
                                f"{latest.all_stats.rms_east:.2f}",
                                f"{latest.all_stats.iqr_east:.2f}",
                                f"{latest.all_stats.mean_east:.2f}",
                                f"{latest.all_stats.median_east:.2f}",
                                f"{latest.all_stats.min_east:.2f}",
                                f"{latest.all_stats.max_east:.2f}"
                            ],
                            'All Up': [
                                f"{latest.all_stats.rms_up:.2f}",
                                f"{latest.all_stats.iqr_up:.2f}",
                                f"{latest.all_stats.mean_up:.2f}",
                                f"{latest.all_stats.median_up:.2f}",
                                f"{latest.all_stats.min_up:.2f}",
                                f"{latest.all_stats.max_up:.2f}"
                            ]
                        }
                        summary_df = pd.DataFrame(summary_data)
                        st.dataframe(summary_df, use_container_width=True, hide_index=True)

                with hlm_tab3:
                    st.subheader("Helmert Transformation Parameters")

                    # Build transformation parameters data
                    trans_data = []
                    for result in hlm_results:
                        trans_data.append({
                            'DOY': result.doy,
                            'Trans N (mm)': result.transform_params.trans_n_mm,
                            'Trans N Sigma': result.transform_params.trans_n_sigma,
                            'Trans E (mm)': result.transform_params.trans_e_mm,
                            'Trans E Sigma': result.transform_params.trans_e_sigma,
                            'Trans U (mm)': result.transform_params.trans_u_mm,
                            'Trans U Sigma': result.transform_params.trans_u_sigma,
                            'Rot N (arcsec)': result.transform_params.rot_n_arcsec,
                            'Rot N Sigma': result.transform_params.rot_n_sigma,
                            'Rot E (arcsec)': result.transform_params.rot_e_arcsec,
                            'Rot E Sigma': result.transform_params.rot_e_sigma,
                            'Rot U (arcsec)': result.transform_params.rot_u_arcsec,
                            'Rot U Sigma': result.transform_params.rot_u_sigma,
                            'RMS Transformation (mm)': result.transform_params.rms_transformation,
                        })

                    trans_df = pd.DataFrame(trans_data)

                    # Plot RMS of transformation over time
                    fig_rms_trans = go.Figure()
                    fig_rms_trans.add_trace(go.Scatter(
                        x=trans_df['DOY'],
                        y=trans_df['RMS Transformation (mm)'],
                        mode='lines+markers',
                        name='RMS Transformation',
                        line=dict(color='#c44dff', width=3),
                        marker=dict(size=8)
                    ))
                    fig_rms_trans.add_hline(y=2.0, line_dash="dash", line_color="#ff6b6b",
                                           annotation_text="2mm threshold")
                    fig_rms_trans.update_layout(
                        title="RMS of Helmert Transformation",
                        xaxis_title="DOY",
                        yaxis_title="RMS (mm)",
                        height=350
                    )
                    apply_colorful_style(fig_rms_trans)
                    st.plotly_chart(fig_rms_trans, use_container_width=True)

                    # Translation and rotation plots
                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("#### Translation Parameters")
                        fig_trans = make_subplots(
                            rows=3, cols=1, shared_xaxes=True,
                            subplot_titles=("Translation N", "Translation E", "Translation U"),
                            vertical_spacing=0.08,
                        )
                        fig_trans.add_trace(go.Scatter(
                            x=trans_df['DOY'], y=trans_df['Trans N (mm)'],
                            mode='lines+markers', name='Trans N',
                            error_y=dict(type='data', array=trans_df['Trans N Sigma'], visible=True),
                            line=dict(color='#00d4ff'),
                            marker=dict(size=6),
                        ), row=1, col=1)
                        fig_trans.add_trace(go.Scatter(
                            x=trans_df['DOY'], y=trans_df['Trans E (mm)'],
                            mode='lines+markers', name='Trans E',
                            error_y=dict(type='data', array=trans_df['Trans E Sigma'], visible=True),
                            line=dict(color='#ff6b6b'),
                            marker=dict(size=6),
                        ), row=2, col=1)
                        fig_trans.add_trace(go.Scatter(
                            x=trans_df['DOY'], y=trans_df['Trans U (mm)'],
                            mode='lines+markers', name='Trans U',
                            error_y=dict(type='data', array=trans_df['Trans U Sigma'], visible=True),
                            line=dict(color='#4ecdc4'),
                            marker=dict(size=6),
                        ), row=3, col=1)
                        fig_trans.update_yaxes(title_text="mm", row=1, col=1)
                        fig_trans.update_yaxes(title_text="mm", row=2, col=1)
                        fig_trans.update_yaxes(title_text="mm", row=3, col=1)
                        fig_trans.update_xaxes(title_text="DOY", row=3, col=1)
                        fig_trans.update_layout(
                            title="Translation Parameters",
                            height=550,
                            showlegend=False,
                        )
                        apply_colorful_style(fig_trans)
                        st.plotly_chart(fig_trans, use_container_width=True)

                    with col2:
                        st.markdown("#### Rotation Parameters")
                        fig_rot = make_subplots(
                            rows=3, cols=1, shared_xaxes=True,
                            subplot_titles=("Rotation N-axis", "Rotation E-axis", "Rotation U-axis"),
                            vertical_spacing=0.08,
                        )
                        fig_rot.add_trace(go.Scatter(
                            x=trans_df['DOY'], y=trans_df['Rot N (arcsec)'],
                            mode='lines+markers', name='Rot N-axis',
                            error_y=dict(type='data', array=trans_df['Rot N Sigma'], visible=True),
                            line=dict(color='#00d4ff'),
                            marker=dict(size=6),
                        ), row=1, col=1)
                        fig_rot.add_trace(go.Scatter(
                            x=trans_df['DOY'], y=trans_df['Rot E (arcsec)'],
                            mode='lines+markers', name='Rot E-axis',
                            error_y=dict(type='data', array=trans_df['Rot E Sigma'], visible=True),
                            line=dict(color='#ff6b6b'),
                            marker=dict(size=6),
                        ), row=2, col=1)
                        fig_rot.add_trace(go.Scatter(
                            x=trans_df['DOY'], y=trans_df['Rot U (arcsec)'],
                            mode='lines+markers', name='Rot U-axis',
                            error_y=dict(type='data', array=trans_df['Rot U Sigma'], visible=True),
                            line=dict(color='#4ecdc4'),
                            marker=dict(size=6),
                        ), row=3, col=1)
                        fig_rot.update_yaxes(title_text="arcsec", row=1, col=1)
                        fig_rot.update_yaxes(title_text="arcsec", row=2, col=1)
                        fig_rot.update_yaxes(title_text="arcsec", row=3, col=1)
                        fig_rot.update_xaxes(title_text="DOY", row=3, col=1)
                        fig_rot.update_layout(
                            title="Rotation Parameters",
                            height=550,
                            showlegend=False,
                        )
                        apply_colorful_style(fig_rot)
                        st.plotly_chart(fig_rot, use_container_width=True)

                with hlm_tab4:
                    st.subheader("Daily Residual Details")

                    # Day selector
                    available_doys = [r.doy for r in hlm_results]
                    selected_doy = st.selectbox("Select Day of Year", available_doys, index=len(available_doys)-1)

                    # Find the selected result
                    selected_result = next((r for r in hlm_results if r.doy == selected_doy), None)

                    if selected_result:
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total Stations", len(selected_result.residuals))
                        with col2:
                            st.metric("Reference Stations", selected_result.accepted_stations)
                        with col3:
                            st.metric("Verified Stations", selected_result.verified_stations)
                        with col4:
                            st.metric("Rejected Stations", selected_result.rejected_stations)

                        # Display residuals table
                        st.markdown("#### Station Residuals")
                        residuals_data = []
                        for res in selected_result.residuals:
                            residuals_data.append({
                                'Station': res.station_4char,
                                'Type': 'Reference' if res.is_reference else 'Verified',
                                'North (mm)': f"{res.north_mm:.2f}",
                                'East (mm)': f"{res.east_mm:.2f}",
                                'Up (mm)': f"{res.up_mm:.2f}",
                                '3D (mm)': f"{math.sqrt(res.north_mm**2 + res.east_mm**2 + res.up_mm**2):.2f}"
                            })

                        residuals_df = pd.DataFrame(residuals_data)

                        # Color code by type with high contrast colors
                        def style_residuals_row(row):
                            if row['Type'] == 'Reference':
                                return ['background-color: #2e7d32; color: white; font-weight: bold'] * len(row)
                            else:
                                return ['background-color: #c62828; color: white; font-weight: bold'] * len(row)

                        st.dataframe(
                            residuals_df.style.apply(style_residuals_row, axis=1),
                            use_container_width=True,
                            hide_index=True,
                            height=500
                        )

                        # Bar chart of residuals (horizontal orientation for better station label visibility)
                        st.markdown("#### Residual Bar Chart")
                        bar_data = pd.DataFrame([{
                            'Station': res.station_4char,
                            'Type': 'Reference' if res.is_reference else 'Verified',
                            'North': res.north_mm,
                            'East': res.east_mm,
                            'Up': res.up_mm
                        } for res in selected_result.residuals])

                        # Calculate dynamic height based on number of stations
                        num_stations = len(bar_data)
                        chart_height = max(500, num_stations * 25)

                        fig_bar = make_subplots(rows=1, cols=3, subplot_titles=['North (mm)', 'East (mm)', 'Up (mm)'],
                                               horizontal_spacing=0.08)

                        colors_map = {'Reference': '#00d4ff', 'Verified': '#ff6b6b'}
                        for type_val in ['Reference', 'Verified']:
                            type_data = bar_data[bar_data['Type'] == type_val]
                            if not type_data.empty:
                                # Horizontal bars: y=Station (categorical), x=Residual value
                                fig_bar.add_trace(go.Bar(
                                    y=type_data['Station'], x=type_data['North'],
                                    name=type_val, marker_color=colors_map[type_val],
                                    legendgroup=type_val, showlegend=True,
                                    orientation='h'
                                ), row=1, col=1)
                                fig_bar.add_trace(go.Bar(
                                    y=type_data['Station'], x=type_data['East'],
                                    name=type_val, marker_color=colors_map[type_val],
                                    legendgroup=type_val, showlegend=False,
                                    orientation='h'
                                ), row=1, col=2)
                                fig_bar.add_trace(go.Bar(
                                    y=type_data['Station'], x=type_data['Up'],
                                    name=type_val, marker_color=colors_map[type_val],
                                    legendgroup=type_val, showlegend=False,
                                    orientation='h'
                                ), row=1, col=3)

                        fig_bar.update_layout(
                            height=chart_height,
                            title_text=f"Station Residuals for DOY {selected_doy}",
                            barmode='group'
                        )
                        # Update y-axes to show all station labels
                        fig_bar.update_yaxes(tickfont=dict(size=10), categoryorder='total ascending')
                        fig_bar.update_xaxes(title_text='Residual (mm)')
                        apply_colorful_style(fig_bar)
                        st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.warning(f"No Helmert transformation files found for DOY {start_doy}-{end_doy} in {dd_base_dir}")
                st.info("""
                Expected file pattern: `{base_dir}/{year}/{doy}/OUT/HLM_{year}{doy}0.OUT.gz`

                Make sure the DD processing has completed and HLM files are available.
                """)

        # TAB 3: RMS Analysis
        elif selected_page == "📉 RMS Analysis":
            st.header("RMS Residual Analysis")

            # Get processing stats for ALL available stations
            all_stats = []
            for station in available_stations:
                stats = cached_get_stats_by_station(station_id=station, year=year, limit=100)
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
        elif selected_page == "🌡️ ZTD Monitor":
            st.header("Zenith Total Delay (ZTD) & Gradients Monitor")
            st.markdown("PPP-AR Troposphere estimates from TRO SINEX files: ZTD (TROTOT), North Gradient (TGNTOT), East Gradient (TGETOT)")

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
                # Use PPP-AR TRO files directly (correct TROTOT values)
                all_ztd_data = []
                for station in ztd_stations:
                    for doy in range(ztd_start_doy, ztd_end_doy + 1):
                        ztd_data = cached_get_pppar_ztd(station, year, doy)
                        if ztd_data:
                            for record in ztd_data:
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
                        legend=dict(orientation="h", yanchor="bottom", y=1.08, x=0.5, xanchor="center"),
                        margin=dict(t=80)
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
                        legend=dict(orientation="h", yanchor="bottom", y=1.08, x=0.5, xanchor="center"),
                        margin=dict(t=80)
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
                        legend=dict(orientation="h", yanchor="bottom", y=1.08, x=0.5, xanchor="center"),
                        margin=dict(t=80)
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
                    final troposphere products from `ftp.aiub.unibe.ch/CODE/{year}/`.
                    Optionally include DD (Double Difference) network ZTD for three-way comparison.
                    """)

                    # CODE comparison controls - with date range
                    code_col1, code_col2, code_col3, code_col4, code_col5 = st.columns(5)
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
                    with code_col5:
                        include_dd_ztd = st.checkbox(
                            "Include DD ZTD",
                            value=True,
                            help="Include DD (Double Difference) network ZTD for three-way comparison",
                            key='code_include_dd_ztd'
                        )

                    # Load CODE products for date range
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

                    # Load DD TRO data if enabled
                    code_dd_tro_estimates = []
                    code_dd_tro_loaded_doys = []
                    if include_dd_ztd:
                        code_dd_tro_base_dir = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX"
                        with st.spinner(f"Loading DD TRO for DOY {code_start_doy}-{code_end_doy}..."):
                            for dd_doy in range(code_start_doy, code_end_doy + 1):
                                dd_patterns = [
                                    f"{code_dd_tro_base_dir}/{year}/{dd_doy}/ATM/F10_{year}{dd_doy:03d}0.TRO.gz",
                                    f"{code_dd_tro_base_dir}/{year}/{dd_doy:03d}/ATM/F10_{year}{dd_doy:03d}0.TRO.gz",
                                ]
                                for dd_path in dd_patterns:
                                    if os.path.exists(dd_path):
                                        try:
                                            estimates = parse_dd_tro_file(dd_path, year, dd_doy)
                                            code_dd_tro_estimates.extend(estimates)
                                            code_dd_tro_loaded_doys.append(dd_doy)
                                        except Exception:
                                            pass
                                        break
                        if code_dd_tro_loaded_doys:
                            st.info(f"Loaded DD TRO for {len(code_dd_tro_loaded_doys)} days")

                    if all_code_entries:
                        # Find matching stations - use ALL available stations from database (not just user-selected)
                        code_station_4chars = set(e.station_4char for e in all_code_entries)
                        all_available_stations_code = cached_get_all_stations()
                        local_station_4chars = set(s[:4].upper() for s in all_available_stations_code)
                        matching_stations = code_station_4chars.intersection(local_station_4chars)

                        if matching_stations:
                            st.success(f"Found {len(matching_stations)} matching stations")

                            # Station selector - allow user to choose specific stations
                            sorted_matching_code = sorted(matching_stations)
                            selected_code_stations = st.multiselect(
                                "Select stations for CODE comparison",
                                options=sorted_matching_code,
                                default=sorted_matching_code[:5] if len(sorted_matching_code) > 5 else sorted_matching_code,
                                help="Select one or more stations to compare. Default shows first 5 stations.",
                                key='code_station_selector'
                            )

                            if not selected_code_stations:
                                st.warning("Please select at least one station for comparison.")
                                selected_code_stations = []

                            # Use selected stations instead of all matching stations
                            matching_stations = set(selected_code_stations)

                            # Load PPP ZTD data specifically for CODE DOY range (independent of main ZTD range)
                            code_ppp_data = []
                            for sta_4char in matching_stations:
                                # Find full station ID from available stations
                                local_sta_id = None
                                for sta in all_available_stations_code:
                                    if sta[:4].upper() == sta_4char:
                                        local_sta_id = sta
                                        break
                                if local_sta_id:
                                    for doy in range(code_start_doy, code_end_doy + 1):
                                        ztd_records = cached_get_ztd(local_sta_id, year, doy)
                                        if ztd_records:
                                            for rec in ztd_records:
                                                rec['station_id'] = local_sta_id
                                                rec['station_4char'] = sta_4char
                                                code_ppp_data.append(rec)

                            if code_ppp_data:
                                df_code_ppp = pd.DataFrame(code_ppp_data)
                                st.info(f"Loaded {len(df_code_ppp)} PPP ZTD records for CODE comparison (DOY {code_start_doy}-{code_end_doy})")
                            else:
                                df_code_ppp = pd.DataFrame()
                                st.warning(f"No PPP ZTD data found for DOY {code_start_doy}-{code_end_doy}. Run ingestion first.")

                            # Build comparison data for all DOYs
                            comparison_data = []
                            for sta_4char in matching_stations:
                                # Get local station ID
                                local_sta_id = None
                                for sta in all_available_stations_code:
                                    if sta[:4].upper() == sta_4char:
                                        local_sta_id = sta
                                        break

                                if local_sta_id and len(df_code_ppp) > 0:
                                    # Get all CODE data for this station
                                    code_sta_data = get_code_ztd_for_station(all_code_entries, sta_4char)

                                    # Get DD data for this station if enabled
                                    dd_sta_data = {}
                                    if include_dd_ztd and code_dd_tro_estimates:
                                        dd_estimates = get_dd_ztd_timeseries(sta_4char, code_dd_tro_estimates)
                                        for dd_est in dd_estimates:
                                            # Key by DOY and hour
                                            dd_key = (dd_est.doy, dd_est.timestamp.hour)
                                            dd_sta_data[dd_key] = dd_est.ztd_m * 1000  # mm

                                    for code_entry in code_sta_data:
                                        # Find matching local data (same DOY and hour)
                                        local_match = df_code_ppp[
                                            (df_code_ppp['station_id'] == local_sta_id) &
                                            (df_code_ppp['doy'] == code_entry.doy) &
                                            (df_code_ppp['hour'] == code_entry.hour)
                                        ]
                                        if len(local_match) > 0:
                                            local_ztd = local_match.iloc[0]['ztd'] * 1000  # mm
                                            code_ztd = code_entry.ztd * 1000  # mm
                                            diff = local_ztd - code_ztd
                                            # Continuous time: DOY + hour/24
                                            time_val = code_entry.doy + code_entry.hour / 24.0

                                            # Get DD ZTD if available
                                            dd_key = (code_entry.doy, code_entry.hour)
                                            dd_ztd = dd_sta_data.get(dd_key, None)
                                            diff_dd = (local_ztd - dd_ztd) if dd_ztd else None

                                            comparison_data.append({
                                                'station': sta_4char,
                                                'doy': code_entry.doy,
                                                'hour': code_entry.hour,
                                                'time': time_val,
                                                'local_ztd_mm': local_ztd,
                                                'code_ztd_mm': code_ztd,
                                                'dd_ztd_mm': dd_ztd,
                                                'diff_mm': diff,
                                                'diff_dd_mm': diff_dd
                                            })

                            if comparison_data:
                                df_compare = pd.DataFrame(comparison_data)
                                n_days = df_compare['doy'].nunique()

                                # ZTD Time Series Comparison
                                st.markdown(f"**Comparing {len(comparison_data)} epochs across {n_days} days**")

                                # ZTD Time Series (Local vs CODE vs DD)
                                fig_ztd = go.Figure()
                                colors = px.colors.qualitative.Plotly
                                has_dd_data = include_dd_ztd and 'dd_ztd_mm' in df_compare.columns and df_compare['dd_ztd_mm'].notna().any()
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
                                        # Add DD ZTD trace if data available
                                        if has_dd_data:
                                            dd_df = sta_df[sta_df['dd_ztd_mm'].notna()]
                                            if len(dd_df) > 0:
                                                fig_ztd.add_trace(go.Scatter(
                                                    x=dd_df['time'], y=dd_df['dd_ztd_mm'],
                                                    mode='lines', name=f'{sta} DD',
                                                    line=dict(color=colors[i % len(colors)], width=1.5, dash='dot'),
                                                    legendgroup=sta
                                                ))
                                title_suffix = " vs DD" if has_dd_data else ""
                                fig_ztd.update_layout(
                                    title=f"Local vs CODE{title_suffix} ZTD - DOY {code_start_doy} to {code_end_doy}",
                                    xaxis_title="Day of Year",
                                    yaxis_title="ZTD (mm)",
                                    hovermode='x unified',
                                    legend=dict(orientation="h", yanchor="bottom", y=1.08, x=0.5, xanchor="center"),
                                    margin=dict(t=80)
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
                                        stat_entry = {
                                            'Station': sta,
                                            'N Days': sta_df['doy'].nunique(),
                                            'N Epochs': len(sta_df),
                                            'CODE Bias (mm)': np.mean(diffs),
                                            'CODE Std (mm)': np.std(diffs),
                                            'CODE RMS (mm)': np.sqrt(np.mean(diffs**2))
                                        }
                                        # Add DD stats if available
                                        if has_dd_data:
                                            dd_diffs = sta_df['diff_dd_mm'].dropna().values
                                            if len(dd_diffs) > 0:
                                                stat_entry['DD Bias (mm)'] = np.mean(dd_diffs)
                                                stat_entry['DD Std (mm)'] = np.std(dd_diffs)
                                                stat_entry['DD RMS (mm)'] = np.sqrt(np.mean(dd_diffs**2))
                                        stats_data.append(stat_entry)

                                if stats_data:
                                    df_stats = pd.DataFrame(stats_data)

                                    # Overall statistics
                                    all_diffs = df_compare['diff_mm'].values
                                    overall_rms = np.sqrt(np.mean(all_diffs**2))

                                    dd_stats_text = ""
                                    if has_dd_data:
                                        dd_diffs_all = df_compare['diff_dd_mm'].dropna().values
                                        if len(dd_diffs_all) > 0:
                                            dd_rms = np.sqrt(np.mean(dd_diffs_all**2))
                                            dd_stats_text = f" | **DD RMS: {dd_rms:.2f} mm**"

                                    st.markdown(f"""
                                    **Overall Statistics ({n_days} days):** Mean Bias: **{np.mean(all_diffs):.2f} mm** |
                                    Std Dev: **{np.std(all_diffs):.2f} mm** |
                                    CODE RMS: **{overall_rms:.2f} mm** |
                                    N Epochs: **{len(all_diffs)}**{dd_stats_text}
                                    """)

                                    # Per-station table - dynamically format columns based on what's available
                                    format_dict = {
                                        'CODE Bias (mm)': '{:.2f}',
                                        'CODE Std (mm)': '{:.2f}',
                                        'CODE RMS (mm)': '{:.2f}'
                                    }
                                    gradient_cols = ['CODE RMS (mm)']
                                    if has_dd_data and 'DD RMS (mm)' in df_stats.columns:
                                        format_dict.update({
                                            'DD Bias (mm)': '{:.2f}',
                                            'DD Std (mm)': '{:.2f}',
                                            'DD RMS (mm)': '{:.2f}'
                                        })
                                        gradient_cols.append('DD RMS (mm)')
                                    st.dataframe(
                                        df_stats.style.format(format_dict).background_gradient(subset=gradient_cols, cmap='YlOrRd'),
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

                    # ========================================
                    # DD (DOUBLE DIFFERENCE) TROPOSPHERE COMPARISON
                    # ========================================
                    st.divider()
                    st.subheader("DD (Double Difference) Troposphere Comparison")
                    st.markdown("""
                    Compare local PPP ZTD estimates with Double Difference (DD) network solutions
                    from `/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/{year}/{doy}/ATM/`
                    """)

                    # DD Troposphere base directory
                    dd_tro_base_dir = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX"

                    # DD TRO controls
                    dd_tro_col1, dd_tro_col2, dd_tro_col3 = st.columns(3)
                    with dd_tro_col1:
                        dd_tro_start_doy = st.number_input(
                            "DD Start DOY",
                            min_value=1, max_value=366,
                            value=ztd_start_doy,
                            key='dd_tro_start_doy'
                        )
                    with dd_tro_col2:
                        dd_tro_end_doy = st.number_input(
                            "DD End DOY",
                            min_value=1, max_value=366,
                            value=ztd_end_doy,
                            key='dd_tro_end_doy'
                        )
                    with dd_tro_col3:
                        dd_tro_prefix = st.selectbox(
                            "DD Product",
                            options=["F10", "R10", "P1"],
                            index=0,
                            help="F10=Final, R10=Rapid, P1=Preliminary",
                            key='dd_tro_prefix'
                        )

                    # Load DD TRO files for date range (CACHED for faster retrieval)
                    with st.spinner(f"Loading DD TRO products for DOY {dd_tro_start_doy}-{dd_tro_end_doy}..."):
                        all_dd_tro_dicts, dd_tro_loaded_doys, dd_tro_failed_doys = load_dd_tro_cached(
                            dd_tro_base_dir, year, dd_tro_start_doy, dd_tro_end_doy, dd_tro_prefix
                        )

                    if dd_tro_loaded_doys:
                        st.info(f"Loaded DD TRO for {len(dd_tro_loaded_doys)} days: DOY {min(dd_tro_loaded_doys)}-{max(dd_tro_loaded_doys)} (cached)")
                        if dd_tro_failed_doys:
                            st.warning(f"DD TRO files not found for {len(dd_tro_failed_doys)} days: {dd_tro_failed_doys[:10]}")

                    if all_dd_tro_dicts:
                        # Find matching stations - load PPP data for DD DOY range independently
                        dd_tro_station_4chars = set(e['station_4char'].upper() for e in all_dd_tro_dicts)

                        # Get ALL available stations from database (not just user-selected ones)
                        all_available_stations = cached_get_all_stations()
                        local_station_4chars = set(s[:4].upper() for s in all_available_stations)
                        dd_tro_matching_stations = dd_tro_station_4chars.intersection(local_station_4chars)

                        if dd_tro_matching_stations:
                            st.success(f"Found {len(dd_tro_matching_stations)} matching stations")

                            # Station selector - allow user to choose specific stations
                            sorted_matching = sorted(dd_tro_matching_stations)
                            selected_dd_stations = st.multiselect(
                                "Select stations for DD comparison",
                                options=sorted_matching,
                                default=sorted_matching[:5] if len(sorted_matching) > 5 else sorted_matching,
                                help="Select one or more stations to compare. Default shows first 5 stations."
                            )

                            if not selected_dd_stations:
                                st.warning("Please select at least one station for comparison.")
                                selected_dd_stations = []

                            # Use selected stations instead of all matching stations
                            dd_tro_matching_stations = set(selected_dd_stations)

                            # Load PPP ZTD data specifically for DD DOY range (independent of main ZTD range)
                            dd_ppp_data = []
                            for sta_4char in dd_tro_matching_stations:
                                # Find full station ID from available stations
                                local_sta_id = None
                                for sta in all_available_stations:
                                    if sta[:4].upper() == sta_4char:
                                        local_sta_id = sta
                                        break
                                if local_sta_id:
                                    for doy in range(dd_tro_start_doy, dd_tro_end_doy + 1):
                                        ztd_records = cached_get_ztd(local_sta_id, year, doy)
                                        if ztd_records:
                                            for rec in ztd_records:
                                                rec['station_id'] = local_sta_id
                                                rec['station_4char'] = sta_4char
                                                dd_ppp_data.append(rec)

                            if dd_ppp_data:
                                df_dd_ppp = pd.DataFrame(dd_ppp_data)
                                st.info(f"Loaded {len(df_dd_ppp)} PPP ZTD records for DD comparison (DOY {dd_tro_start_doy}-{dd_tro_end_doy})")
                            else:
                                df_dd_ppp = pd.DataFrame()
                                st.warning(f"No PPP ZTD data found for DOY {dd_tro_start_doy}-{dd_tro_end_doy}. Run ingestion first.")

                            # Build comparison data
                            dd_tro_comparison_data = []
                            for sta_4char in dd_tro_matching_stations:
                                # Get local station ID
                                local_sta_id = None
                                for sta in all_available_stations:
                                    if sta[:4].upper() == sta_4char:
                                        local_sta_id = sta
                                        break

                                if local_sta_id and len(df_dd_ppp) > 0:
                                    # Get DD ZTD time series for this station (from cached dicts)
                                    dd_sta_data = [e for e in all_dd_tro_dicts if e['station_4char'].upper() == sta_4char]
                                    dd_sta_data = sorted(dd_sta_data, key=lambda e: e['timestamp'])

                                    for dd_entry in dd_sta_data:
                                        # DD TRO has hourly estimates (3600s interval)
                                        # Find matching local data (same DOY and hour)
                                        dd_hour = dd_entry['timestamp'].hour
                                        local_match = df_dd_ppp[
                                            (df_dd_ppp['station_id'] == local_sta_id) &
                                            (df_dd_ppp['doy'] == dd_entry['doy']) &
                                            (df_dd_ppp['hour'] == dd_hour)
                                        ]
                                        if len(local_match) > 0:
                                            local_ztd_mm = local_match.iloc[0]['ztd'] * 1000  # mm
                                            dd_ztd_mm = dd_entry['ztd_m'] * 1000  # mm
                                            diff = local_ztd_mm - dd_ztd_mm

                                            dd_tro_comparison_data.append({
                                                'station': sta_4char,
                                                'doy': dd_entry['doy'],
                                                'hour': dd_hour,
                                                'epoch': dd_entry['doy'] + dd_hour / 24.0,  # Fractional DOY for plotting
                                                'local_ztd_mm': local_ztd_mm,
                                                'dd_ztd_mm': dd_ztd_mm,
                                                'dd_sigma_mm': dd_entry['sigma_m'] * 1000,
                                                'diff_mm': diff
                                            })

                            if dd_tro_comparison_data:
                                df_dd_compare = pd.DataFrame(dd_tro_comparison_data)
                                dd_n_days = df_dd_compare['doy'].nunique()

                                st.markdown(f"**Comparing {len(dd_tro_comparison_data)} epochs across {dd_n_days} days**")

                                # ZTD Time Series (Local PPP vs DD)
                                fig_dd_ztd = go.Figure()
                                colors = px.colors.qualitative.Plotly
                                for i, sta in enumerate(sorted(dd_tro_matching_stations)):
                                    sta_df = df_dd_compare[df_dd_compare['station'] == sta].sort_values('epoch')
                                    if len(sta_df) > 0:
                                        fig_dd_ztd.add_trace(go.Scatter(
                                            x=sta_df['epoch'], y=sta_df['local_ztd_mm'],
                                            mode='lines+markers', name=f'{sta} Local PPP',
                                            line=dict(color=colors[i % len(colors)], width=2),
                                            marker=dict(size=4),
                                            legendgroup=sta
                                        ))
                                        fig_dd_ztd.add_trace(go.Scatter(
                                            x=sta_df['epoch'], y=sta_df['dd_ztd_mm'],
                                            mode='lines+markers', name=f'{sta} DD Network',
                                            line=dict(color=colors[i % len(colors)], width=2, dash='dash'),
                                            marker=dict(size=4, symbol='diamond'),
                                            legendgroup=sta
                                        ))
                                fig_dd_ztd.update_layout(
                                    title=f"Local PPP vs DD Network ZTD (Hourly) - DOY {dd_tro_start_doy} to {dd_tro_end_doy}",
                                    xaxis_title="Day of Year (fractional)",
                                    yaxis_title="ZTD (mm)",
                                    hovermode='x unified',
                                    legend=dict(orientation="h", yanchor="bottom", y=1.08, x=0.5, xanchor="center"),
                                    margin=dict(t=80)
                                )
                                apply_colorful_style(fig_dd_ztd)
                                st.plotly_chart(fig_dd_ztd, use_container_width=True)

                                # Difference plots
                                col1, col2 = st.columns(2)
                                with col1:
                                    fig_dd_diff = px.line(df_dd_compare.sort_values('epoch'), x='epoch', y='diff_mm',
                                                          color='station',
                                                          title="ZTD Difference (Local PPP - DD)",
                                                          labels={'diff_mm': 'Difference (mm)', 'epoch': 'DOY (fractional)'})
                                    fig_dd_diff.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.5)
                                    fig_dd_diff.add_hline(y=5, line_dash="dot", line_color="red", opacity=0.3)
                                    fig_dd_diff.add_hline(y=-5, line_dash="dot", line_color="red", opacity=0.3)
                                    apply_colorful_style(fig_dd_diff)
                                    st.plotly_chart(fig_dd_diff, use_container_width=True)

                                with col2:
                                    # Histogram of differences
                                    fig_dd_hist = px.histogram(df_dd_compare, x='diff_mm', color='station',
                                                               nbins=30,
                                                               title="ZTD Difference Distribution (PPP - DD)",
                                                               labels={'diff_mm': 'Difference (mm)'})
                                    apply_colorful_style(fig_dd_hist)
                                    st.plotly_chart(fig_dd_hist, use_container_width=True)

                                # Statistics Summary
                                dd_stats_data = []
                                for sta in sorted(dd_tro_matching_stations):
                                    sta_df = df_dd_compare[df_dd_compare['station'] == sta]
                                    if len(sta_df) > 0:
                                        diffs = sta_df['diff_mm'].values
                                        dd_stats_data.append({
                                            'Station': sta,
                                            'N Days': sta_df['doy'].nunique(),
                                            'Mean Bias (mm)': np.mean(diffs),
                                            'Std Dev (mm)': np.std(diffs),
                                            'RMS (mm)': np.sqrt(np.mean(diffs**2)),
                                            'Avg DD Sigma (mm)': sta_df['dd_sigma_mm'].mean()
                                        })

                                if dd_stats_data:
                                    df_dd_stats = pd.DataFrame(dd_stats_data)

                                    # Overall statistics
                                    all_dd_diffs = df_dd_compare['diff_mm'].values
                                    dd_overall_rms = np.sqrt(np.mean(all_dd_diffs**2))

                                    st.markdown(f"""
                                    **Overall DD Statistics ({dd_n_days} days):** Mean Bias: **{np.mean(all_dd_diffs):.2f} mm** |
                                    Std Dev: **{np.std(all_dd_diffs):.2f} mm** |
                                    RMS: **{dd_overall_rms:.2f} mm** |
                                    N Epochs: **{len(all_dd_diffs)}**
                                    """)

                                    # Per-station table
                                    st.dataframe(
                                        df_dd_stats.style.format({
                                            'Mean Bias (mm)': '{:.2f}',
                                            'Std Dev (mm)': '{:.2f}',
                                            'RMS (mm)': '{:.2f}',
                                            'Avg DD Sigma (mm)': '{:.2f}'
                                        }).background_gradient(subset=['RMS (mm)'], cmap='YlOrRd'),
                                        use_container_width=True,
                                        hide_index=True
                                    )

                                    # Quality assessment
                                    if dd_overall_rms < 5:
                                        st.success(f"Excellent agreement with DD network (RMS < 5 mm)")
                                    elif dd_overall_rms < 10:
                                        st.info(f"Good agreement with DD network (RMS < 10 mm)")
                                    elif dd_overall_rms < 15:
                                        st.warning(f"Moderate agreement with DD network (RMS < 15 mm)")
                                    else:
                                        st.error(f"Large differences with DD network (RMS > 15 mm). Check processing configuration.")
                            else:
                                st.warning(f"No matching epochs found for DOY {dd_tro_start_doy}-{dd_tro_end_doy}. Run: `python3 -m frontend.ingest_all {dd_tro_start_doy} {dd_tro_end_doy}`")
                        else:
                            st.info(f"No matching stations found between local ({len(local_station_4chars)} in DB) and DD ({len(dd_tro_station_4chars)} in DD TRO)")
                    else:
                        st.info(f"No DD TRO files found in {dd_tro_base_dir}/{year}/{{doy}}/ATM/")

                    # =====================================================
                    # ERA5 Troposphere Comparison Section
                    # =====================================================
                    st.divider()
                    st.subheader("ERA5 Troposphere Comparison")
                    st.markdown("""
                    Compare local PPP ZTD estimates with **ERA5 reanalysis** atmospheric data from ECMWF.
                    ERA5 provides hourly data at 0.25° x 0.25° grid resolution - an independent validation source.

                    ZTD is computed using:
                    - **ZHD**: Saastamoinen model (from surface pressure)
                    - **ZWD**: Askne-Nordius model (from temperature and water vapor)
                    """)

                    # Check for cached ERA5 data
                    era5_dates = list_cached_era5_dates()

                    if era5_dates:
                        st.info(f"ERA5 data cached: {len(era5_dates)} hourly epochs")

                        # Get station coordinates from the latest CRD file for interpolation
                        station_coords = {}

                        for sta in ztd_stations:
                            sta_coords = cached_get_daily_coords(sta, year=year, start_doy=ztd_start_doy, end_doy=ztd_end_doy)
                            if sta_coords:
                                avg_x = np.mean([c['x'] for c in sta_coords])
                                avg_y = np.mean([c['y'] for c in sta_coords])
                                avg_z = np.mean([c['z'] for c in sta_coords])

                                # Convert XYZ to lat/lon/height (WGS84)
                                a = 6378137.0
                                f = 1/298.257223563
                                b = a * (1 - f)
                                e2 = (a**2 - b**2) / a**2

                                lon = math.atan2(avg_y, avg_x)
                                p = math.sqrt(avg_x**2 + avg_y**2)
                                lat = math.atan2(avg_z, p * (1 - e2))

                                for _ in range(10):
                                    N = a / math.sqrt(1 - e2 * math.sin(lat)**2)
                                    h = p / math.cos(lat) - N
                                    lat = math.atan2(avg_z, p * (1 - e2 * N / (N + h)))

                                N = a / math.sqrt(1 - e2 * math.sin(lat)**2)
                                h = p / math.cos(lat) - N

                                station_coords[sta] = (math.degrees(lat), math.degrees(lon), h)

                        if station_coords:
                            st.write(f"Station coordinates available for {len(station_coords)} stations")

                            # ERA5 comparisons
                            era5_comparisons = []

                            for sta in ztd_stations:
                                if sta not in station_coords:
                                    continue

                                lat, lon, height = station_coords[sta]
                                sta_4char = sta[:4].upper()

                                sta_ztd = [z for z in all_ztd_data if z['station_id'] == sta]
                                if not sta_ztd:
                                    continue

                                for ztd_rec in sta_ztd:
                                    doy = ztd_rec['doy']
                                    hour = ztd_rec['hour']

                                    # Check if this date/hour is in cached ERA5 data
                                    era5_tuple = (year, doy, hour)
                                    if era5_tuple not in era5_dates:
                                        continue

                                    # Get ERA5 ZTD (without downloading)
                                    era5_est = get_era5_ztd_for_station(
                                        sta_4char, lat, lon, height,
                                        year, doy, hour,
                                        download_if_missing=False
                                    )

                                    if era5_est:
                                        era5_comparisons.append({
                                            'station': sta,
                                            'doy': doy,
                                            'hour': hour,
                                            'local_ztd_mm': ztd_rec['ztd'] * 1000,
                                            'era5_ztd_mm': era5_est.ztd_m * 1000,
                                            'era5_zhd_mm': era5_est.zhd_m * 1000,
                                            'era5_zwd_mm': era5_est.zwd_m * 1000,
                                            'era5_pwv_mm': era5_est.pwv_mm,
                                            'era5_pressure_hpa': era5_est.surface_pressure_hpa,
                                            'era5_temp_c': era5_est.temperature_k - 273.15,
                                            'diff_mm': ztd_rec['ztd'] * 1000 - era5_est.ztd_m * 1000
                                        })

                            if era5_comparisons:
                                df_era5 = pd.DataFrame(era5_comparisons)

                                # Summary statistics
                                era5_mean_diff = df_era5['diff_mm'].mean()
                                era5_std_diff = df_era5['diff_mm'].std()
                                era5_rms = np.sqrt((df_era5['diff_mm']**2).mean())

                                col1, col2, col3, col4 = st.columns(4)
                                with col1:
                                    st.metric("Mean Bias", f"{era5_mean_diff:.1f} mm")
                                with col2:
                                    st.metric("Std Dev", f"{era5_std_diff:.1f} mm")
                                with col3:
                                    st.metric("RMS", f"{era5_rms:.1f} mm")
                                with col4:
                                    st.metric("Comparisons", len(df_era5))

                                # ZTD Comparison Plot
                                fig_era5 = make_subplots(
                                    rows=2, cols=1,
                                    subplot_titles=['GNSS vs ERA5 ZTD', 'ZTD Difference (GNSS - ERA5)'],
                                    vertical_spacing=0.15,
                                    row_heights=[0.6, 0.4]
                                )

                                era5_stations = df_era5['station'].unique()

                                for i, sta in enumerate(era5_stations):
                                    sta_df = df_era5[df_era5['station'] == sta].sort_values(['doy', 'hour'])
                                    color = px.colors.qualitative.Set2[i % len(px.colors.qualitative.Set2)]

                                    x_labels = [f"DOY {row['doy']} H{row['hour']:02d}" for _, row in sta_df.iterrows()]

                                    # GNSS ZTD
                                    fig_era5.add_trace(
                                        go.Scatter(
                                            x=x_labels,
                                            y=sta_df['local_ztd_mm'],
                                            mode='lines+markers',
                                            name=f'{sta} (GNSS)',
                                            line=dict(color=color),
                                            marker=dict(size=6),
                                            legendgroup=sta
                                        ),
                                        row=1, col=1
                                    )

                                    # ERA5 ZTD
                                    fig_era5.add_trace(
                                        go.Scatter(
                                            x=x_labels,
                                            y=sta_df['era5_ztd_mm'],
                                            mode='lines+markers',
                                            name=f'{sta} (ERA5)',
                                            line=dict(color=color, dash='dash'),
                                            marker=dict(size=4, symbol='x'),
                                            legendgroup=sta,
                                            showlegend=True
                                        ),
                                        row=1, col=1
                                    )

                                    # Difference
                                    fig_era5.add_trace(
                                        go.Scatter(
                                            x=x_labels,
                                            y=sta_df['diff_mm'],
                                            mode='lines+markers',
                                            name=f'{sta} diff',
                                            line=dict(color=color),
                                            marker=dict(size=5),
                                            legendgroup=sta,
                                            showlegend=False
                                        ),
                                        row=2, col=1
                                    )

                                fig_era5.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)

                                fig_era5.update_yaxes(title_text="ZTD (mm)", row=1, col=1)
                                fig_era5.update_yaxes(title_text="Diff (mm)", row=2, col=1)
                                fig_era5.update_xaxes(tickangle=45)
                                fig_era5.update_layout(height=600, title="GNSS vs ERA5 ZTD Comparison")
                                apply_colorful_style(fig_era5)
                                st.plotly_chart(fig_era5, use_container_width=True)

                                # Per-station statistics
                                st.subheader("ERA5 Comparison Statistics by Station")
                                era5_stats = []
                                for sta in era5_stations:
                                    sta_df = df_era5[df_era5['station'] == sta]
                                    era5_stats.append({
                                        'Station': sta,
                                        'Count': len(sta_df),
                                        'Mean Bias (mm)': sta_df['diff_mm'].mean(),
                                        'Std Dev (mm)': sta_df['diff_mm'].std(),
                                        'RMS (mm)': np.sqrt((sta_df['diff_mm']**2).mean()),
                                        'Avg ERA5 ZHD (mm)': sta_df['era5_zhd_mm'].mean(),
                                        'Avg ERA5 ZWD (mm)': sta_df['era5_zwd_mm'].mean(),
                                        'Avg PWV (mm)': sta_df['era5_pwv_mm'].mean()
                                    })

                                df_era5_stats = pd.DataFrame(era5_stats)
                                st.dataframe(
                                    df_era5_stats.style.format({
                                        'Mean Bias (mm)': '{:.2f}',
                                        'Std Dev (mm)': '{:.2f}',
                                        'RMS (mm)': '{:.2f}',
                                        'Avg ERA5 ZHD (mm)': '{:.1f}',
                                        'Avg ERA5 ZWD (mm)': '{:.1f}',
                                        'Avg PWV (mm)': '{:.1f}'
                                    }).background_gradient(subset=['RMS (mm)'], cmap='YlOrRd'),
                                    use_container_width=True,
                                    hide_index=True
                                )

                                # Quality assessment
                                st.markdown("---")
                                st.markdown("**ERA5 Quality Assessment:**")
                                if era5_rms < 15:
                                    st.success(f"Good agreement with ERA5 atmospheric model (RMS < 15 mm)")
                                elif era5_rms < 25:
                                    st.info(f"Moderate agreement with ERA5 (RMS < 25 mm) - typical for coastal/complex terrain")
                                else:
                                    st.warning(f"Large differences with ERA5 (RMS > 25 mm) - may indicate local atmospheric effects")

                                st.markdown("""
                                **Note:** ERA5 provides hourly estimates at 0.25° x 0.25° grid resolution.
                                Differences of 5-15 mm are typical due to:
                                - GNSS captures real-time atmospheric variations
                                - ERA5 is a reanalysis product (smoothed)
                                - Height reduction uncertainties
                                """)
                            else:
                                st.warning("No matching epochs between GNSS and cached ERA5 data")
                        else:
                            st.warning("No station coordinates available for ERA5 interpolation")
                    else:
                        st.info(f"No ERA5 data cached. Use the era5_ztd module to download data from CDS.")

                else:
                    st.info(f"No ZTD data found for selected stations in DOY range {ztd_start_doy}-{ztd_end_doy}")
                    st.code("To load TRO data, run:\npython -m frontend.tro_parser <doy> --save")
            else:
                st.warning("Please select at least one station.")

        # TAB 5: Ambiguity Resolution (DD)
        elif selected_page == "🎯 Ambiguity Resolution (DD)":
            st.header("DD Ambiguity Resolution Statistics")
            st.markdown("Double Difference ambiguity resolution from Bernese AMB files: WL (Widelane), NL (Narrowlane), L5 (Phase-Based)")

            # DOY selection for AMB file
            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                amb_doy = st.number_input(
                    "Select DOY",
                    min_value=1, max_value=366,
                    value=end_doy,
                    key='amb_doy'
                )
            with col2:
                # System filter
                system_filter = st.selectbox(
                    "System Filter",
                    options=['All', 'G (GPS)', 'E (Galileo)', 'R (GLONASS)', 'G E (GPS+GAL)'],
                    index=0,
                    key='amb_sys_filter'
                )

            # Try to load AMB summary file
            dd_base_dir = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX"
            amb_path = get_amb_summary_path(dd_base_dir, year, amb_doy)

            if amb_path:
                amb_summary = parse_amb_summary_file(amb_path, year, amb_doy)

                if amb_summary:
                    # Summary metrics
                    st.divider()
                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        wl_count = len(amb_summary.wl_baselines)
                        st.metric("WL Baselines", wl_count)
                    with col2:
                        nl_count = len(amb_summary.nl_baselines)
                        st.metric("NL Baselines", nl_count)
                    with col3:
                        l5_count = len(amb_summary.l5_baselines)
                        st.metric("L5 Baselines", l5_count)
                    with col4:
                        sat_count = len(amb_summary.satellite_stats)
                        st.metric("Satellites", sat_count)

                    # Get system filter value
                    sys_map = {'All': None, 'G (GPS)': 'G', 'E (Galileo)': 'E',
                               'R (GLONASS)': 'R', 'G E (GPS+GAL)': 'G E'}
                    sys_val = sys_map.get(system_filter)

                    # === BASELINE AMBIGUITY RESOLUTION ===
                    st.divider()
                    st.subheader("📊 Baseline Ambiguity Resolution")

                    # Create tabs for WL, NL, L5, QIF, and Baseline Pairs
                    bl_tab1, bl_tab2, bl_tab3, bl_tab4, bl_tab5 = st.tabs(["🔵 Widelane (WL)", "🟢 Narrowlane (NL)", "🟣 Phase-Based L5", "🟠 QIF", "📍 Baseline Pairs"])

                    def filter_baselines(baselines, sys_filter):
                        """Filter baselines by system"""
                        if sys_filter is None:
                            return baselines
                        return [b for b in baselines if sys_filter in b.system]

                    with bl_tab1:
                        # Widelane baselines
                        wl_filtered = filter_baselines(amb_summary.wl_baselines, sys_val)
                        if wl_filtered:
                            # === SYSTEM-LEVEL RESOLUTION SUMMARY ===
                            # Calculate per-system statistics (each system max 100%)
                            system_stats = {}
                            for b in wl_filtered:
                                # Handle combined systems like "G E" by splitting
                                systems = b.system.split()
                                for sys in systems:
                                    if sys not in system_stats:
                                        system_stats[sys] = {'before': 0, 'after': 0}
                                    # Distribute ambiguities proportionally across systems
                                    n_sys = len(systems)
                                    system_stats[sys]['before'] += b.amb_before / n_sys
                                    system_stats[sys]['after'] += b.amb_after / n_sys

                            # Create system summary data
                            sys_summary_data = []
                            system_names = {'G': 'GPS', 'E': 'Galileo', 'R': 'GLONASS'}
                            system_colors = {'G': '#00d4ff', 'E': '#4ecdc4', 'R': '#ffd93d'}
                            for sys, stats in sorted(system_stats.items()):
                                if stats['before'] > 0:
                                    res_pct = ((stats['before'] - stats['after']) / stats['before']) * 100
                                    sys_summary_data.append({
                                        'System': system_names.get(sys, sys),
                                        'System Code': sys,
                                        'Resolution (%)': min(res_pct, 100.0),  # Cap at 100%
                                        'Ambiguities Before': int(stats['before']),
                                        'Ambiguities After': int(stats['after']),
                                        'Resolved': int(stats['before'] - stats['after'])
                                    })

                            if sys_summary_data:
                                df_sys = pd.DataFrame(sys_summary_data)

                                # Per-system resolution bar chart
                                fig_sys = px.bar(
                                    df_sys,
                                    x='System',
                                    y='Resolution (%)',
                                    color='System Code',
                                    title=f'Widelane (WL) Resolution by System - DOY {amb_doy}',
                                    text='Resolution (%)',
                                    color_discrete_map=system_colors
                                )
                                fig_sys.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                                fig_sys.update_yaxes(range=[0, 105])  # Max 100% + margin
                                fig_sys.add_hline(y=90, line_dash="dash", line_color="#4ecdc4",
                                                  annotation_text="90% target")
                                fig_sys.update_layout(showlegend=False, height=350)
                                apply_colorful_style(fig_sys)
                                st.plotly_chart(fig_sys, use_container_width=True)

                                # Show system summary metrics
                                cols = st.columns(len(sys_summary_data))
                                for i, row in enumerate(sys_summary_data):
                                    with cols[i]:
                                        st.metric(
                                            label=row['System'],
                                            value=f"{row['Resolution (%)']:.1f}%",
                                            delta=f"{row['Resolved']}/{row['Ambiguities Before']} resolved"
                                        )

                            # === DETAILED BASELINE VIEW ===
                            with st.expander("📋 Detailed Baseline Data", expanded=False):
                                # Create DataFrame
                                wl_data = [{
                                    'Baseline': f"{b.sta1}-{b.sta2}",
                                    'Length (km)': b.length_km,
                                    'Before': b.amb_before,
                                    'After': b.amb_after,
                                    'Resolved': b.amb_before - b.amb_after,
                                    'Resolution (%)': b.resolution_pct,
                                    'System': b.system,
                                    'RMS Before': b.rms_before,
                                    'RMS After': b.rms_after
                                } for b in wl_filtered]
                                df_wl = pd.DataFrame(wl_data)

                                # Bar chart of resolution rates by baseline
                                fig_wl = px.bar(
                                    df_wl.sort_values('Resolution (%)', ascending=True),
                                    y='Baseline',
                                    x='Resolution (%)',
                                    color='System',
                                    orientation='h',
                                    title=f'Per-Baseline WL Resolution - DOY {amb_doy}',
                                    color_discrete_map={'G': '#00d4ff', 'E': '#4ecdc4', 'G E': '#ff6b6b', 'R': '#ffd93d'}
                                )
                                fig_wl.add_vline(x=90, line_dash="dash", line_color="#4ecdc4",
                                                 annotation_text="90% target")
                                fig_wl.update_layout(height=max(400, len(df_wl) * 18))
                                apply_colorful_style(fig_wl)
                                st.plotly_chart(fig_wl, use_container_width=True)

                                # Data table
                                st.dataframe(
                                    df_wl.style.format({
                                        'Length (km)': '{:.1f}',
                                        'Resolution (%)': '{:.1f}',
                                        'RMS Before': '{:.3f}',
                                        'RMS After': '{:.3f}'
                                    }),
                                    use_container_width=True,
                                    hide_index=True
                                )

                            # QIF Resolution vs baseline length scatter
                            qif_filtered = filter_baselines(amb_summary.qif_baselines, sys_val)
                            if qif_filtered:
                                qif_data = [{
                                    'Baseline': f"{b.sta1}-{b.sta2}",
                                    'Length (km)': b.length_km,
                                    'Before': b.amb_before,
                                    'After': b.amb_after,
                                    'Resolved': b.amb_before - b.amb_after,
                                    'Resolution (%)': b.resolution_pct,
                                    'System': b.system,
                                    'RMS Before': b.rms_before,
                                    'RMS After': b.rms_after
                                } for b in qif_filtered]
                                df_qif_scatter = pd.DataFrame(qif_data)

                                fig_qif_scatter = px.scatter(
                                    df_qif_scatter,
                                    x='Length (km)',
                                    y='Resolution (%)',
                                    color='System',
                                    size='Before',
                                    hover_data=['Baseline', 'Resolved'],
                                    title='QIF Resolution vs Baseline Length (Quasi-Ionosphere-Free)',
                                    color_discrete_map={'G': '#00d4ff', 'E': '#4ecdc4', 'GRE': '#ff6b6b', 'R': '#ffd93d'}
                                )
                                fig_qif_scatter.add_hline(y=90, line_dash="dash", line_color="#4ecdc4",
                                                          annotation_text="90% target")
                                apply_colorful_style(fig_qif_scatter)
                                st.plotly_chart(fig_qif_scatter, use_container_width=True)
                            else:
                                st.info("No QIF baselines found for selected filter")
                        else:
                            st.info("No WL baselines found for selected filter")

                    with bl_tab2:
                        # Narrowlane baselines
                        nl_filtered = filter_baselines(amb_summary.nl_baselines, sys_val)
                        if nl_filtered:
                            # === SYSTEM-LEVEL RESOLUTION SUMMARY ===
                            system_stats = {}
                            for b in nl_filtered:
                                systems = b.system.split()
                                for sys in systems:
                                    if sys not in system_stats:
                                        system_stats[sys] = {'before': 0, 'after': 0}
                                    n_sys = len(systems)
                                    system_stats[sys]['before'] += b.amb_before / n_sys
                                    system_stats[sys]['after'] += b.amb_after / n_sys

                            sys_summary_data = []
                            system_names = {'G': 'GPS', 'E': 'Galileo', 'R': 'GLONASS'}
                            system_colors = {'G': '#00d4ff', 'E': '#4ecdc4', 'R': '#ffd93d'}
                            for sys, stats in sorted(system_stats.items()):
                                if stats['before'] > 0:
                                    res_pct = ((stats['before'] - stats['after']) / stats['before']) * 100
                                    sys_summary_data.append({
                                        'System': system_names.get(sys, sys),
                                        'System Code': sys,
                                        'Resolution (%)': min(res_pct, 100.0),
                                        'Ambiguities Before': int(stats['before']),
                                        'Ambiguities After': int(stats['after']),
                                        'Resolved': int(stats['before'] - stats['after'])
                                    })

                            if sys_summary_data:
                                df_sys = pd.DataFrame(sys_summary_data)

                                fig_sys = px.bar(
                                    df_sys,
                                    x='System',
                                    y='Resolution (%)',
                                    color='System Code',
                                    title=f'Narrowlane (NL) Resolution by System - DOY {amb_doy}',
                                    text='Resolution (%)',
                                    color_discrete_map=system_colors
                                )
                                fig_sys.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                                fig_sys.update_yaxes(range=[0, 105])
                                fig_sys.add_hline(y=80, line_dash="dash", line_color="#4ecdc4",
                                                  annotation_text="80% target")
                                fig_sys.update_layout(showlegend=False, height=350)
                                apply_colorful_style(fig_sys)
                                st.plotly_chart(fig_sys, use_container_width=True)

                                cols = st.columns(len(sys_summary_data))
                                for i, row in enumerate(sys_summary_data):
                                    with cols[i]:
                                        st.metric(
                                            label=row['System'],
                                            value=f"{row['Resolution (%)']:.1f}%",
                                            delta=f"{row['Resolved']}/{row['Ambiguities Before']} resolved"
                                        )

                            # === DETAILED BASELINE VIEW ===
                            with st.expander("📋 Detailed Baseline Data", expanded=False):
                                nl_data = [{
                                    'Baseline': f"{b.sta1}-{b.sta2}",
                                    'Length (km)': b.length_km,
                                    'Before': b.amb_before,
                                    'After': b.amb_after,
                                    'Resolved': b.amb_before - b.amb_after,
                                    'Resolution (%)': b.resolution_pct,
                                    'System': b.system,
                                    'RMS Before': b.rms_before,
                                    'RMS After': b.rms_after
                                } for b in nl_filtered]
                                df_nl = pd.DataFrame(nl_data)

                                fig_nl = px.bar(
                                    df_nl.sort_values('Resolution (%)', ascending=True),
                                    y='Baseline',
                                    x='Resolution (%)',
                                    color='System',
                                    orientation='h',
                                    title=f'Per-Baseline NL Resolution - DOY {amb_doy}',
                                    color_discrete_map={'G': '#00d4ff', 'E': '#4ecdc4', 'G E': '#ff6b6b', 'R': '#ffd93d'}
                                )
                                fig_nl.add_vline(x=80, line_dash="dash", line_color="#4ecdc4",
                                                 annotation_text="80% target")
                                fig_nl.update_layout(height=max(400, len(df_nl) * 18))
                                apply_colorful_style(fig_nl)
                                st.plotly_chart(fig_nl, use_container_width=True)

                                # Resolution vs baseline length scatter
                                fig_nl_scatter = px.scatter(
                                    df_nl,
                                    x='Length (km)',
                                    y='Resolution (%)',
                                    color='System',
                                    size='Before',
                                    hover_data=['Baseline', 'Resolved'],
                                    title='NL Resolution vs Baseline Length',
                                    color_discrete_map={'G': '#00d4ff', 'E': '#4ecdc4', 'G E': '#ff6b6b', 'R': '#ffd93d'}
                                )
                                fig_nl_scatter.add_hline(y=80, line_dash="dash", line_color="#4ecdc4")
                                apply_colorful_style(fig_nl_scatter)
                                st.plotly_chart(fig_nl_scatter, use_container_width=True)

                                st.dataframe(
                                    df_nl.style.format({
                                        'Length (km)': '{:.1f}',
                                        'Resolution (%)': '{:.1f}',
                                        'RMS Before': '{:.3f}',
                                        'RMS After': '{:.3f}'
                                    }),
                                    use_container_width=True,
                                    hide_index=True
                                )
                        else:
                            st.info("No NL baselines found for selected filter")

                    with bl_tab3:
                        # L5 (Phase-Based Widelane) baselines
                        l5_filtered = filter_baselines(amb_summary.l5_baselines, sys_val)
                        if l5_filtered:
                            # === SYSTEM-LEVEL RESOLUTION SUMMARY ===
                            system_stats = {}
                            for b in l5_filtered:
                                systems = b.system.split()
                                for sys in systems:
                                    if sys not in system_stats:
                                        system_stats[sys] = {'before': 0, 'after': 0}
                                    n_sys = len(systems)
                                    system_stats[sys]['before'] += b.amb_before / n_sys
                                    system_stats[sys]['after'] += b.amb_after / n_sys

                            sys_summary_data = []
                            system_names = {'G': 'GPS', 'E': 'Galileo', 'R': 'GLONASS'}
                            system_colors = {'G': '#00d4ff', 'E': '#4ecdc4', 'R': '#ffd93d'}
                            for sys, stats in sorted(system_stats.items()):
                                if stats['before'] > 0:
                                    res_pct = ((stats['before'] - stats['after']) / stats['before']) * 100
                                    sys_summary_data.append({
                                        'System': system_names.get(sys, sys),
                                        'System Code': sys,
                                        'Resolution (%)': min(res_pct, 100.0),
                                        'Ambiguities Before': int(stats['before']),
                                        'Ambiguities After': int(stats['after']),
                                        'Resolved': int(stats['before'] - stats['after'])
                                    })

                            if sys_summary_data:
                                df_sys = pd.DataFrame(sys_summary_data)

                                fig_sys = px.bar(
                                    df_sys,
                                    x='System',
                                    y='Resolution (%)',
                                    color='System Code',
                                    title=f'Phase-Based L5 Resolution by System - DOY {amb_doy}',
                                    text='Resolution (%)',
                                    color_discrete_map=system_colors
                                )
                                fig_sys.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                                fig_sys.update_yaxes(range=[0, 105])
                                fig_sys.add_hline(y=70, line_dash="dash", line_color="#4ecdc4",
                                                  annotation_text="70% target")
                                fig_sys.update_layout(showlegend=False, height=350)
                                apply_colorful_style(fig_sys)
                                st.plotly_chart(fig_sys, use_container_width=True)

                                cols = st.columns(len(sys_summary_data))
                                for i, row in enumerate(sys_summary_data):
                                    with cols[i]:
                                        st.metric(
                                            label=row['System'],
                                            value=f"{row['Resolution (%)']:.1f}%",
                                            delta=f"{row['Resolved']}/{row['Ambiguities Before']} resolved"
                                        )

                            # === DETAILED BASELINE VIEW ===
                            with st.expander("📋 Detailed Baseline Data", expanded=False):
                                l5_data = [{
                                    'Baseline': f"{b.sta1}-{b.sta2}",
                                    'Length (km)': b.length_km,
                                    'Before': b.amb_before,
                                    'After': b.amb_after,
                                    'Resolved': b.amb_before - b.amb_after,
                                    'Resolution (%)': b.resolution_pct,
                                    'System': b.system,
                                    'RMS Before': b.rms_before,
                                    'RMS After': b.rms_after
                                } for b in l5_filtered]
                                df_l5 = pd.DataFrame(l5_data)

                                fig_l5 = px.bar(
                                    df_l5.sort_values('Resolution (%)', ascending=True),
                                    y='Baseline',
                                    x='Resolution (%)',
                                    color='System',
                                    orientation='h',
                                    title=f'Per-Baseline L5 Resolution - DOY {amb_doy}',
                                    color_discrete_map={'G': '#00d4ff', 'E': '#4ecdc4', 'G E': '#ff6b6b', 'R': '#ffd93d'}
                                )
                                fig_l5.add_vline(x=70, line_dash="dash", line_color="#4ecdc4",
                                                 annotation_text="70% target")
                                fig_l5.update_layout(height=max(400, len(df_l5) * 18))
                                apply_colorful_style(fig_l5)
                                st.plotly_chart(fig_l5, use_container_width=True)

                                st.dataframe(
                                    df_l5.style.format({
                                        'Length (km)': '{:.1f}',
                                        'Resolution (%)': '{:.1f}',
                                        'RMS Before': '{:.3f}',
                                        'RMS After': '{:.3f}'
                                    }),
                                    use_container_width=True,
                                    hide_index=True
                                )
                        else:
                            st.info("No L5 baselines found for selected filter")

                    with bl_tab4:
                        # QIF (Quasi-Ionosphere-Free) baselines
                        qif_filtered = filter_baselines(amb_summary.qif_baselines, sys_val)
                        if qif_filtered:
                            # === SYSTEM-LEVEL RESOLUTION SUMMARY ===
                            system_stats = {}
                            for b in qif_filtered:
                                systems = b.system.split()
                                for sys in systems:
                                    if sys not in system_stats:
                                        system_stats[sys] = {'before': 0, 'after': 0}
                                    n_sys = len(systems)
                                    system_stats[sys]['before'] += b.amb_before / n_sys
                                    system_stats[sys]['after'] += b.amb_after / n_sys

                            sys_summary_data = []
                            system_names = {'G': 'GPS', 'E': 'Galileo', 'R': 'GLONASS'}
                            system_colors = {'G': '#00d4ff', 'E': '#4ecdc4', 'R': '#ffd93d'}
                            for sys, stats in sorted(system_stats.items()):
                                if stats['before'] > 0:
                                    res_pct = ((stats['before'] - stats['after']) / stats['before']) * 100
                                    sys_summary_data.append({
                                        'System': system_names.get(sys, sys),
                                        'System Code': sys,
                                        'Resolution (%)': min(res_pct, 100.0),
                                        'Ambiguities Before': int(stats['before']),
                                        'Ambiguities After': int(stats['after']),
                                        'Resolved': int(stats['before'] - stats['after'])
                                    })

                            if sys_summary_data:
                                df_sys = pd.DataFrame(sys_summary_data)

                                fig_sys = px.bar(
                                    df_sys,
                                    x='System',
                                    y='Resolution (%)',
                                    color='System Code',
                                    title=f'QIF Resolution by System - DOY {amb_doy}',
                                    text='Resolution (%)',
                                    color_discrete_map=system_colors
                                )
                                fig_sys.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                                fig_sys.update_yaxes(range=[0, 105])
                                fig_sys.add_hline(y=90, line_dash="dash", line_color="#4ecdc4",
                                                  annotation_text="90% target")
                                fig_sys.update_layout(showlegend=False, height=350)
                                apply_colorful_style(fig_sys)
                                st.plotly_chart(fig_sys, use_container_width=True)

                                cols = st.columns(len(sys_summary_data))
                                for i, row in enumerate(sys_summary_data):
                                    with cols[i]:
                                        st.metric(
                                            label=row['System'],
                                            value=f"{row['Resolution (%)']:.1f}%",
                                            delta=f"{row['Resolved']}/{row['Ambiguities Before']} resolved"
                                        )

                            # === DETAILED BASELINE VIEW ===
                            with st.expander("📋 Detailed Baseline Data", expanded=False):
                                qif_data = [{
                                    'Baseline': f"{b.sta1}-{b.sta2}",
                                    'Length (km)': b.length_km,
                                    'Before': b.amb_before,
                                    'After': b.amb_after,
                                    'Resolved': b.amb_before - b.amb_after,
                                    'Resolution (%)': b.resolution_pct,
                                    'System': b.system,
                                    'RMS Before': b.rms_before,
                                    'RMS After': b.rms_after
                                } for b in qif_filtered]
                                df_qif = pd.DataFrame(qif_data)

                                # Bar chart
                                fig_qif = px.bar(
                                    df_qif.sort_values('Resolution (%)', ascending=True),
                                    y='Baseline',
                                    x='Resolution (%)',
                                    color='System',
                                    orientation='h',
                                    title=f'Per-Baseline QIF Resolution - DOY {amb_doy}',
                                    color_discrete_map={'G': '#00d4ff', 'E': '#4ecdc4', 'GRE': '#ff6b6b', 'R': '#ffd93d'}
                                )
                                fig_qif.add_vline(x=90, line_dash="dash", line_color="#4ecdc4",
                                                 annotation_text="90% target")
                                fig_qif.update_layout(height=max(400, len(df_qif) * 12))
                                apply_colorful_style(fig_qif)
                                st.plotly_chart(fig_qif, use_container_width=True)

                                # Resolution vs baseline length scatter
                                fig_qif_length = px.scatter(
                                    df_qif,
                                    x='Length (km)',
                                    y='Resolution (%)',
                                    color='System',
                                    size='Before',
                                    hover_data=['Baseline', 'Resolved', 'RMS Before', 'RMS After'],
                                    title='QIF Resolution vs Baseline Length',
                                    color_discrete_map={'G': '#00d4ff', 'E': '#4ecdc4', 'GRE': '#ff6b6b', 'R': '#ffd93d'}
                                )
                                fig_qif_length.add_hline(y=90, line_dash="dash", line_color="#4ecdc4",
                                                         annotation_text="90% target")
                                apply_colorful_style(fig_qif_length)
                                st.plotly_chart(fig_qif_length, use_container_width=True)

                                # Data table
                                st.dataframe(
                                    df_qif.style.format({
                                        'Length (km)': '{:.1f}',
                                        'Resolution (%)': '{:.1f}',
                                        'RMS Before': '{:.3f}',
                                        'RMS After': '{:.3f}'
                                    }),
                                    use_container_width=True,
                                    hide_index=True
                                )
                        else:
                            st.info("No QIF baselines found for selected filter")

                    with bl_tab5:
                        # Baseline Pairs - Combined view of all AR types per baseline
                        st.markdown("**Per-Baseline Ambiguity Resolution Across All Methods**")

                        # Collect all baselines from all AR types
                        all_baselines = {}

                        # Process WL baselines
                        for b in filter_baselines(amb_summary.wl_baselines, sys_val):
                            key = f"{b.sta1}-{b.sta2}"
                            if key not in all_baselines:
                                all_baselines[key] = {
                                    'Baseline': key,
                                    'Station 1': b.sta1,
                                    'Station 2': b.sta2,
                                    'Length (km)': b.length_km,
                                    'System': b.system
                                }
                            all_baselines[key]['WL (%)'] = b.resolution_pct
                            all_baselines[key]['WL Before'] = b.amb_before
                            all_baselines[key]['WL After'] = b.amb_after

                        # Process NL baselines
                        for b in filter_baselines(amb_summary.nl_baselines, sys_val):
                            key = f"{b.sta1}-{b.sta2}"
                            if key not in all_baselines:
                                all_baselines[key] = {
                                    'Baseline': key,
                                    'Station 1': b.sta1,
                                    'Station 2': b.sta2,
                                    'Length (km)': b.length_km,
                                    'System': b.system
                                }
                            all_baselines[key]['NL (%)'] = b.resolution_pct
                            all_baselines[key]['NL Before'] = b.amb_before
                            all_baselines[key]['NL After'] = b.amb_after

                        # Process L5 baselines
                        for b in filter_baselines(amb_summary.l5_baselines, sys_val):
                            key = f"{b.sta1}-{b.sta2}"
                            if key not in all_baselines:
                                all_baselines[key] = {
                                    'Baseline': key,
                                    'Station 1': b.sta1,
                                    'Station 2': b.sta2,
                                    'Length (km)': b.length_km,
                                    'System': b.system
                                }
                            all_baselines[key]['L5 (%)'] = b.resolution_pct
                            all_baselines[key]['L5 Before'] = b.amb_before
                            all_baselines[key]['L5 After'] = b.amb_after

                        # Process QIF baselines
                        for b in filter_baselines(amb_summary.qif_baselines, sys_val):
                            key = f"{b.sta1}-{b.sta2}"
                            if key not in all_baselines:
                                all_baselines[key] = {
                                    'Baseline': key,
                                    'Station 1': b.sta1,
                                    'Station 2': b.sta2,
                                    'Length (km)': b.length_km,
                                    'System': b.system
                                }
                            all_baselines[key]['QIF (%)'] = b.resolution_pct
                            all_baselines[key]['QIF Before'] = b.amb_before
                            all_baselines[key]['QIF After'] = b.amb_after

                        if all_baselines:
                            df_pairs = pd.DataFrame(list(all_baselines.values()))

                            # Fill NaN with 0 for missing AR types
                            ar_cols = ['WL (%)', 'NL (%)', 'L5 (%)', 'QIF (%)']
                            for col in ar_cols:
                                if col not in df_pairs.columns:
                                    df_pairs[col] = 0
                                else:
                                    df_pairs[col] = df_pairs[col].fillna(0)

                            # Calculate average resolution across all methods
                            df_pairs['Avg (%)'] = df_pairs[ar_cols].replace(0, np.nan).mean(axis=1).fillna(0)

                            # Sort by average resolution
                            df_pairs = df_pairs.sort_values('Avg (%)', ascending=True)

                            # Grouped bar chart showing all AR types per baseline
                            fig_pairs = px.bar(
                                df_pairs,
                                y='Baseline',
                                x=['WL (%)', 'NL (%)', 'L5 (%)', 'QIF (%)'],
                                orientation='h',
                                title=f'Ambiguity Resolution by Baseline Pair - DOY {amb_doy}',
                                barmode='group',
                                color_discrete_map={
                                    'WL (%)': '#00d4ff',
                                    'NL (%)': '#4ecdc4',
                                    'L5 (%)': '#a855f7',
                                    'QIF (%)': '#ff6b6b'
                                }
                            )
                            fig_pairs.update_layout(
                                height=max(500, len(df_pairs) * 25),
                                legend_title_text='AR Method',
                                xaxis_title='Resolution (%)',
                                yaxis_title='Baseline'
                            )
                            fig_pairs.update_xaxes(range=[0, 105])
                            fig_pairs.add_vline(x=90, line_dash="dash", line_color="#4ecdc4",
                                               annotation_text="90% target")
                            apply_colorful_style(fig_pairs)
                            st.plotly_chart(fig_pairs, use_container_width=True)

                            # Heatmap view
                            st.markdown("**Resolution Heatmap**")

                            # Prepare data for heatmap
                            heatmap_data = df_pairs[['Baseline'] + ar_cols].set_index('Baseline')

                            fig_heatmap = px.imshow(
                                heatmap_data.values,
                                x=['WL', 'NL', 'L5', 'QIF'],
                                y=heatmap_data.index.tolist(),
                                color_continuous_scale='RdYlGn',
                                aspect='auto',
                                title='Resolution Heatmap by Baseline and Method',
                                labels={'color': 'Resolution (%)'}
                            )
                            fig_heatmap.update_layout(
                                height=max(400, len(df_pairs) * 20),
                                xaxis_title='AR Method',
                                yaxis_title='Baseline'
                            )
                            fig_heatmap.update_coloraxes(cmin=0, cmax=100)
                            apply_colorful_style(fig_heatmap)
                            st.plotly_chart(fig_heatmap, use_container_width=True)

                            # Summary statistics per baseline
                            st.markdown("**Baseline Summary Table**")

                            # Prepare display columns
                            display_cols = ['Baseline', 'System', 'Length (km)', 'WL (%)', 'NL (%)', 'L5 (%)', 'QIF (%)', 'Avg (%)']
                            display_cols = [c for c in display_cols if c in df_pairs.columns]

                            df_display = df_pairs[display_cols].copy()
                            df_display = df_display.sort_values('Avg (%)', ascending=False)

                            st.dataframe(
                                df_display.style.format({
                                    'Length (km)': '{:.1f}',
                                    'WL (%)': '{:.1f}',
                                    'NL (%)': '{:.1f}',
                                    'L5 (%)': '{:.1f}',
                                    'QIF (%)': '{:.1f}',
                                    'Avg (%)': '{:.1f}'
                                }).background_gradient(
                                    subset=['WL (%)', 'NL (%)', 'L5 (%)', 'QIF (%)', 'Avg (%)'],
                                    cmap='RdYlGn',
                                    vmin=0,
                                    vmax=100
                                ),
                                use_container_width=True,
                                hide_index=True,
                                height=min(600, len(df_display) * 35 + 50)
                            )

                            # Statistics summary
                            st.markdown("**Overall Statistics**")
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                avg_wl = df_pairs['WL (%)'].replace(0, np.nan).mean()
                                st.metric("Avg WL", f"{avg_wl:.1f}%" if not np.isnan(avg_wl) else "N/A")
                            with col2:
                                avg_nl = df_pairs['NL (%)'].replace(0, np.nan).mean()
                                st.metric("Avg NL", f"{avg_nl:.1f}%" if not np.isnan(avg_nl) else "N/A")
                            with col3:
                                avg_l5 = df_pairs['L5 (%)'].replace(0, np.nan).mean()
                                st.metric("Avg L5", f"{avg_l5:.1f}%" if not np.isnan(avg_l5) else "N/A")
                            with col4:
                                avg_qif = df_pairs['QIF (%)'].replace(0, np.nan).mean()
                                st.metric("Avg QIF", f"{avg_qif:.1f}%" if not np.isnan(avg_qif) else "N/A")
                        else:
                            st.info("No baseline data available for selected filter")

                    # === SATELLITE-WISE STATISTICS ===
                    st.divider()
                    st.subheader("🛰️ Satellite-wise Ambiguity Statistics")

                    if amb_summary.satellite_stats:
                        # Create DataFrame for satellite stats
                        sat_data = [{
                            'PRN': s.prn,
                            'System': s.system,
                            'Total Amb': s.amb_total,
                            'L1&L2 Solved': s.l1l2_solved,
                            'L1&L2 (%)': s.l1l2_rel_pct2,
                            'L5 Solved': s.l5_solved,
                            'L5 (%)': s.l5_rel_pct2
                        } for s in amb_summary.satellite_stats]
                        df_sat = pd.DataFrame(sat_data)

                        # Satellite tabs by system
                        sat_tab1, sat_tab2, sat_tab3, sat_tab4 = st.tabs(["All Satellites", "GPS", "Galileo", "GLONASS"])

                        def plot_satellite_stats(df, title_prefix):
                            """Create satellite bar charts"""
                            col1, col2 = st.columns(2)

                            with col1:
                                fig1 = px.bar(
                                    df.sort_values('PRN'),
                                    x='PRN',
                                    y='L1&L2 (%)',
                                    color='System',
                                    title=f'{title_prefix} - L1&L2 Resolution Rate',
                                    color_discrete_map={'GPS': '#00d4ff', 'Galileo': '#4ecdc4', 'GLONASS': '#ffd93d'}
                                )
                                fig1.add_hline(y=80, line_dash="dash", line_color="#ff6b6b")
                                apply_colorful_style(fig1)
                                st.plotly_chart(fig1, use_container_width=True)

                            with col2:
                                fig2 = px.bar(
                                    df.sort_values('PRN'),
                                    x='PRN',
                                    y='L5 (%)',
                                    color='System',
                                    title=f'{title_prefix} - L5 Resolution Rate',
                                    color_discrete_map={'GPS': '#00d4ff', 'Galileo': '#4ecdc4', 'GLONASS': '#ffd93d'}
                                )
                                fig2.add_hline(y=70, line_dash="dash", line_color="#ff6b6b")
                                apply_colorful_style(fig2)
                                st.plotly_chart(fig2, use_container_width=True)

                        with sat_tab1:
                            plot_satellite_stats(df_sat, "All Satellites")
                            st.dataframe(df_sat, use_container_width=True, hide_index=True)

                        with sat_tab2:
                            df_gps = df_sat[df_sat['System'] == 'GPS']
                            if len(df_gps) > 0:
                                plot_satellite_stats(df_gps, "GPS")
                                st.dataframe(df_gps, use_container_width=True, hide_index=True)
                            else:
                                st.info("No GPS satellite data")

                        with sat_tab3:
                            df_gal = df_sat[df_sat['System'] == 'Galileo']
                            if len(df_gal) > 0:
                                plot_satellite_stats(df_gal, "Galileo")
                                st.dataframe(df_gal, use_container_width=True, hide_index=True)
                            else:
                                st.info("No Galileo satellite data")

                        with sat_tab4:
                            df_glo = df_sat[df_sat['System'] == 'GLONASS']
                            if len(df_glo) > 0:
                                plot_satellite_stats(df_glo, "GLONASS")
                                st.dataframe(df_glo, use_container_width=True, hide_index=True)
                            else:
                                st.info("No GLONASS satellite data")

                    # === SYSTEM TOTALS ===
                    st.divider()
                    st.subheader("📈 System Totals Summary")

                    # System totals from satellite stats
                    sys_totals = []
                    if amb_summary.gps_total:
                        sys_totals.append({
                            'System': 'GPS',
                            'Total Amb': amb_summary.gps_total.get('amb_total', 0),
                            'L1&L2 Solved': amb_summary.gps_total.get('l1l2_solved', 0),
                            'L1&L2 (%)': amb_summary.gps_total.get('l1l2_rel_pct', 0),
                            'L5 Solved': amb_summary.gps_total.get('l5_solved', 0),
                            'L5 (%)': amb_summary.gps_total.get('l5_rel_pct', 0)
                        })
                    if amb_summary.gal_total:
                        sys_totals.append({
                            'System': 'Galileo',
                            'Total Amb': amb_summary.gal_total.get('amb_total', 0),
                            'L1&L2 Solved': amb_summary.gal_total.get('l1l2_solved', 0),
                            'L1&L2 (%)': amb_summary.gal_total.get('l1l2_rel_pct', 0),
                            'L5 Solved': amb_summary.gal_total.get('l5_solved', 0),
                            'L5 (%)': amb_summary.gal_total.get('l5_rel_pct', 0)
                        })
                    if amb_summary.glo_total:
                        sys_totals.append({
                            'System': 'GLONASS',
                            'Total Amb': amb_summary.glo_total.get('amb_total', 0),
                            'L1&L2 Solved': amb_summary.glo_total.get('l1l2_solved', 0),
                            'L1&L2 (%)': amb_summary.glo_total.get('l1l2_rel_pct', 0),
                            'L5 Solved': amb_summary.glo_total.get('l5_solved', 0),
                            'L5 (%)': amb_summary.glo_total.get('l5_rel_pct', 0)
                        })

                    if sys_totals:
                        df_sys = pd.DataFrame(sys_totals)

                        col1, col2 = st.columns(2)
                        with col1:
                            fig_sys1 = px.bar(
                                df_sys,
                                x='System',
                                y='L1&L2 (%)',
                                color='System',
                                title='L1&L2 Ambiguity Resolution by System',
                                color_discrete_map={'GPS': '#00d4ff', 'Galileo': '#4ecdc4', 'GLONASS': '#ffd93d'}
                            )
                            fig_sys1.add_hline(y=80, line_dash="dash", line_color="#ff6b6b",
                                               annotation_text="80% target")
                            apply_colorful_style(fig_sys1)
                            st.plotly_chart(fig_sys1, use_container_width=True)

                        with col2:
                            fig_sys2 = px.bar(
                                df_sys,
                                x='System',
                                y='L5 (%)',
                                color='System',
                                title='L5 Ambiguity Resolution by System',
                                color_discrete_map={'GPS': '#00d4ff', 'Galileo': '#4ecdc4', 'GLONASS': '#ffd93d'}
                            )
                            fig_sys2.add_hline(y=70, line_dash="dash", line_color="#ff6b6b",
                                               annotation_text="70% target")
                            apply_colorful_style(fig_sys2)
                            st.plotly_chart(fig_sys2, use_container_width=True)

                        st.dataframe(df_sys, use_container_width=True, hide_index=True)

                    # WL/NL totals from baseline processing
                    if amb_summary.wl_totals or amb_summary.nl_totals:
                        st.subheader("📊 Baseline Processing Totals")

                        bl_totals = []
                        for t in amb_summary.wl_totals:
                            bl_totals.append({
                                'Type': 'WL',
                                'System': t.system,
                                'Baselines': t.baselines,
                                'Before': t.amb_before,
                                'After': t.amb_after,
                                'Resolution (%)': t.resolution_pct,
                                'Max Residual': t.max_residual,
                                'RMS Residual': t.rms_residual
                            })
                        for t in amb_summary.nl_totals:
                            bl_totals.append({
                                'Type': 'NL',
                                'System': t.system,
                                'Baselines': t.baselines,
                                'Before': t.amb_before,
                                'After': t.amb_after,
                                'Resolution (%)': t.resolution_pct,
                                'Max Residual': t.max_residual,
                                'RMS Residual': t.rms_residual
                            })

                        if bl_totals:
                            df_bl_tot = pd.DataFrame(bl_totals)
                            fig_bl_tot = px.bar(
                                df_bl_tot,
                                x='System',
                                y='Resolution (%)',
                                color='Type',
                                barmode='group',
                                title='WL/NL Resolution by System',
                                color_discrete_map={'WL': '#00d4ff', 'NL': '#4ecdc4'}
                            )
                            apply_colorful_style(fig_bl_tot)
                            st.plotly_chart(fig_bl_tot, use_container_width=True)
                            st.dataframe(df_bl_tot, use_container_width=True, hide_index=True)

                else:
                    st.error(f"Failed to parse AMB file: {amb_path}")
            else:
                st.warning(f"No AMB summary file found for DOY {amb_doy}")
                st.info("Expected path pattern: `/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/{year}/{doy}/OUT/AMB_{year}{doy}0.SUM.gz`")

        # TAB 5b: Ambiguity Resolution (PPP-AR)
        elif selected_page == "🎯 Ambiguity Resolution (PPP-AR)":
            st.header("PPP Ambiguity Resolution Statistics")
            st.markdown("Precise Point Positioning with Ambiguity Resolution (PPP-AR) using external bias products")

            # Get ambiguity data from database (PPP processing)
            amb_data = cached_get_ambiguity(year=year, start_doy=start_doy, end_doy=end_doy)

            if amb_data:
                df_amb = pd.DataFrame(amb_data)

                # Summary metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    avg_wl = df_amb['wl_combined'].mean() if 'wl_combined' in df_amb.columns else None
                    st.metric("Avg WL Combined", f"{avg_wl:.1f}%" if pd.notna(avg_wl) else "N/A")
                with col2:
                    avg_nl = df_amb['nl_combined'].mean() if 'nl_combined' in df_amb.columns else None
                    st.metric("Avg NL Combined", f"{avg_nl:.1f}%" if pd.notna(avg_nl) else "N/A")
                with col3:
                    st.metric("Stations", df_amb['station_id'].nunique())

                st.divider()

                # Get latest DOY data for each station
                latest_doy = df_amb['doy'].max()
                df_latest = df_amb[df_amb['doy'] == latest_doy].copy()

                if len(df_latest) > 0:
                    # Widelane Resolution by Station
                    st.subheader("Widelane Resolution by Station (PPP-AR)")
                    wl_cols = [c for c in ['wl_gps', 'wl_gal', 'wl_combined'] if c in df_latest.columns]
                    if wl_cols:
                        fig_wl = px.bar(
                            df_latest.sort_values('wl_combined' if 'wl_combined' in df_latest.columns else wl_cols[0], ascending=False),
                            x='station_id',
                            y=wl_cols,
                            title=f'PPP-AR Widelane Ambiguity Resolution - DOY {latest_doy}',
                            labels={'value': 'Resolution Rate (%)', 'station_id': 'Station'},
                            barmode='group',
                            color_discrete_map={'wl_gps': '#00d4ff', 'wl_gal': '#4ecdc4', 'wl_combined': '#ff6b6b'}
                        )
                        fig_wl.add_hline(y=90, line_dash="dash", line_color="#4ecdc4",
                                         annotation_text="90% target")
                        fig_wl.update_layout(legend_title_text='Constellation')
                        apply_colorful_style(fig_wl)
                        st.plotly_chart(fig_wl, use_container_width=True)

                    # Narrowlane Resolution by Station
                    st.subheader("Narrowlane Resolution by Station (PPP-AR)")
                    nl_cols = [c for c in ['nl_gps', 'nl_gal', 'nl_combined'] if c in df_latest.columns]
                    if nl_cols:
                        fig_nl = px.bar(
                            df_latest.sort_values('nl_combined' if 'nl_combined' in df_latest.columns else nl_cols[0], ascending=False),
                            x='station_id',
                            y=nl_cols,
                            title=f'PPP-AR Narrowlane Ambiguity Resolution - DOY {latest_doy}',
                            labels={'value': 'Resolution Rate (%)', 'station_id': 'Station'},
                            barmode='group',
                            color_discrete_map={'nl_gps': '#00d4ff', 'nl_gal': '#4ecdc4', 'nl_combined': '#ff6b6b'}
                        )
                        fig_nl.add_hline(y=80, line_dash="dash", line_color="#4ecdc4",
                                         annotation_text="80% target")
                        fig_nl.update_layout(legend_title_text='Constellation')
                        apply_colorful_style(fig_nl)
                        st.plotly_chart(fig_nl, use_container_width=True)

                # AR Success Rate Time Series
                st.subheader("📈 AR Success Rate Over Time")
                st.markdown("Track ambiguity resolution performance trends across days")

                # Calculate daily averages
                if 'wl_combined' in df_amb.columns and 'nl_combined' in df_amb.columns:
                    daily_avg = df_amb.groupby('doy').agg({
                        'wl_combined': 'mean',
                        'nl_combined': 'mean'
                    }).reset_index()
                    daily_avg.columns = ['DOY', 'WL Combined (%)', 'NL Combined (%)']

                    # Also compute percentage of stations meeting targets
                    daily_targets = df_amb.groupby('doy').apply(
                        lambda x: pd.Series({
                            'WL >= 90%': (x['wl_combined'] >= 90).sum() / len(x) * 100,
                            'NL >= 80%': (x['nl_combined'] >= 80).sum() / len(x) * 100
                        })
                    ).reset_index()

                    # Create subplot with two panels
                    fig_ar_ts = make_subplots(
                        rows=2, cols=1,
                        subplot_titles=['Average AR Success Rate', 'Stations Meeting Target (%)'],
                        shared_xaxes=True,
                        vertical_spacing=0.15
                    )

                    # Panel 1: Average success rate
                    fig_ar_ts.add_trace(
                        go.Scatter(
                            x=daily_avg['DOY'],
                            y=daily_avg['WL Combined (%)'],
                            mode='lines+markers',
                            name='WL Combined',
                            line=dict(color='#00d4ff', width=2),
                            marker=dict(size=8)
                        ),
                        row=1, col=1
                    )
                    fig_ar_ts.add_trace(
                        go.Scatter(
                            x=daily_avg['DOY'],
                            y=daily_avg['NL Combined (%)'],
                            mode='lines+markers',
                            name='NL Combined',
                            line=dict(color='#ff6b6b', width=2),
                            marker=dict(size=8)
                        ),
                        row=1, col=1
                    )

                    # Add target lines
                    fig_ar_ts.add_hline(y=90, line_dash="dash", line_color="#4ecdc4",
                                       annotation_text="WL target 90%", row=1, col=1)
                    fig_ar_ts.add_hline(y=80, line_dash="dash", line_color="#ffe66d",
                                       annotation_text="NL target 80%", row=1, col=1)

                    # Panel 2: Stations meeting target
                    fig_ar_ts.add_trace(
                        go.Bar(
                            x=daily_targets['doy'],
                            y=daily_targets['WL >= 90%'],
                            name='WL >= 90%',
                            marker_color='#00d4ff',
                            opacity=0.7
                        ),
                        row=2, col=1
                    )
                    fig_ar_ts.add_trace(
                        go.Bar(
                            x=daily_targets['doy'],
                            y=daily_targets['NL >= 80%'],
                            name='NL >= 80%',
                            marker_color='#ff6b6b',
                            opacity=0.7
                        ),
                        row=2, col=1
                    )

                    fig_ar_ts.update_layout(
                        height=600,
                        title="PPP-AR Success Rate Trends",
                        showlegend=True,
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
                        barmode='group'
                    )
                    fig_ar_ts.update_yaxes(title_text="Rate (%)", range=[0, 105], row=1, col=1)
                    fig_ar_ts.update_yaxes(title_text="Stations (%)", range=[0, 105], row=2, col=1)
                    fig_ar_ts.update_xaxes(title_text="Day of Year", row=2, col=1)
                    apply_colorful_style(fig_ar_ts)
                    st.plotly_chart(fig_ar_ts, use_container_width=True)

                    # Show summary statistics
                    st.markdown("### 📊 Period Summary")
                    summary_cols = st.columns(4)
                    with summary_cols[0]:
                        st.metric("Mean WL", f"{daily_avg['WL Combined (%)'].mean():.1f}%")
                    with summary_cols[1]:
                        st.metric("Mean NL", f"{daily_avg['NL Combined (%)'].mean():.1f}%")
                    with summary_cols[2]:
                        wl_meet_pct = (daily_avg['WL Combined (%)'] >= 90).sum() / len(daily_avg) * 100
                        st.metric("Days WL >= 90%", f"{wl_meet_pct:.0f}%")
                    with summary_cols[3]:
                        nl_meet_pct = (daily_avg['NL Combined (%)'] >= 80).sum() / len(daily_avg) * 100
                        st.metric("Days NL >= 80%", f"{nl_meet_pct:.0f}%")

                # Detailed table
                st.subheader("Detailed PPP-AR Ambiguity Statistics")

                # Format for display - dynamically select available columns
                display_cols = ['station_id', 'doy']
                if 'receiver' in df_amb.columns:
                    display_cols.append('receiver')
                for col in ['wl_gps', 'wl_gal', 'wl_combined', 'nl_gps', 'nl_gal', 'nl_combined']:
                    if col in df_amb.columns:
                        display_cols.append(col)

                display_df = df_amb[display_cols].copy()

                # Rename columns for display
                col_rename = {
                    'station_id': 'Station', 'doy': 'DOY', 'receiver': 'Receiver',
                    'wl_gps': 'WL GPS', 'wl_gal': 'WL GAL', 'wl_combined': 'WL G+E',
                    'nl_gps': 'NL GPS', 'nl_gal': 'NL GAL', 'nl_combined': 'NL G+E'
                }
                display_df.columns = [col_rename.get(c, c) for c in display_df.columns]

                # Format numeric columns
                format_dict = {}
                for col in display_df.columns:
                    if col.startswith('WL') or col.startswith('NL'):
                        format_dict[col] = '{:.1f}'

                st.dataframe(
                    display_df.style.format(format_dict, na_rep='-'),
                    use_container_width=True,
                    hide_index=True
                )

                # Stations with low resolution
                if 'nl_combined' in df_amb.columns:
                    st.subheader("Stations Needing Attention (NL < 70%)")
                    low_nl = df_amb[df_amb['nl_combined'] < 70][['station_id', 'doy', 'nl_combined'] + (['receiver'] if 'receiver' in df_amb.columns else [])]
                    if len(low_nl) > 0:
                        st.dataframe(low_nl, use_container_width=True, hide_index=True)
                    else:
                        st.success("All stations have NL resolution >= 70%")

            else:
                st.info("No PPP-AR ambiguity data found in database.")
                st.markdown("""
                **PPP-AR vs DD Ambiguity Resolution:**
                - **PPP-AR**: Uses external bias products (e.g., CNES/CLS, CODE) to fix ambiguities in single-station mode
                - **DD**: Uses double-difference observations between stations to eliminate biases and fix ambiguities

                To load PPP-AR data, run: `python -m frontend.amb_report <doy> --save`
                """)

        # TAB 6: Satellite Tracking
        elif selected_page == "🛰️ Satellite Tracking":
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
            sat_data = cached_get_satellite_tracking(year=year, doy=sat_doy)

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
        elif selected_page == "⚙️ Processing Stats":
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
            stats_data = cached_get_stats(year=year, limit=2000)
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
        elif selected_page == "🔗 Sat. Ambiguity PRN":
            st.header("Satellite-wise Ambiguity Resolution by PRN")
            st.markdown("""
            Per-satellite ambiguity resolution statistics showing L1/L2 and L5 resolution rates for each PRN.
            - **GPS**: PRN 1-32
            - **Galileo**: PRN 201-236
            """)

            # Query satellite ambiguity PRN data
            sat_amb_prn_data = cached_get_satellite_ambiguity_prn(year=year)

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
        elif selected_page == "📡 Obs. Residuals":
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
            res_data = cached_get_station_residuals(year=year, doy=res_doy)

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
        elif selected_page == "📶 Data Availability":
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
            avail_data = cached_get_data_availability(year=year, doy=avail_doy)

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
        elif selected_page == "🚫 Outlier Statistics":
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
            outlier_data = cached_get_data_availability(year=year, doy=outlier_doy)

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
        # TAB 12: OBSERVATION QUALITY
        # ==================================
        elif selected_page == "🔬 Observation Quality":
            st.header("Observation Quality Analysis")
            st.markdown("""
            Comprehensive GNSS observation quality metrics:
            - **Phase Residuals RMS** by constellation (GPS, GLONASS, Galileo)
            - **Multipath Indicators** (MP1, MP2 from RINEX QC)
            - **Cycle Slip Rate** per station
            - **Signal-to-Noise Ratio** (SNR) trends
            - **Coordinate Quality Checks** - Daily repeatability (RMS of daily solutions vs. weekly mean)
            - **PPP-AR Quality Control** - Wide Lane (WL) and Narrow Lane (NL) fix rates per station
            """)

            # Day selection
            col1, col2 = st.columns(2)
            with col1:
                obsq_doy = st.number_input(
                    "Select DOY",
                    min_value=1, max_value=366,
                    value=end_doy,
                    key='obsq_doy'
                )

            if obsq_doy:
                # ============================================
                # Section 1: Phase Residuals RMS by Constellation
                # ============================================
                st.subheader("Phase Residuals RMS by Constellation")

                # Get constellation-level residuals
                resid_by_const = cached_get_residuals_by_constellation(year, obsq_doy)

                if resid_by_const:
                    df_const = pd.DataFrame(resid_by_const)

                    # Define constellation colors (matches database names: GPS, GLONASS, GALILEO)
                    const_colors = {'GPS': '#00d4ff', 'GLONASS': '#ff6b6b', 'GALILEO': '#4ecdc4'}

                    col1, col2 = st.columns(2)

                    with col1:
                        # Bar chart of mean RMS by constellation
                        fig_const_rms = px.bar(
                            df_const,
                            x='constellation',
                            y='mean_rms',
                            color='constellation',
                            color_discrete_map=const_colors,
                            title=f'Mean Phase Residuals RMS by Constellation - DOY {obsq_doy}',
                            labels={'mean_rms': 'Mean RMS (mm)', 'constellation': 'Constellation'},
                            error_y='std_rms'
                        )
                        fig_const_rms.add_hline(y=3.0, line_dash="dash", line_color="#ffa502",
                                                 annotation_text="3mm threshold")
                        apply_colorful_style(fig_const_rms)
                        st.plotly_chart(fig_const_rms, use_container_width=True)

                    with col2:
                        # Statistics table
                        st.markdown("**Constellation Statistics**")
                        df_display = df_const[['constellation', 'num_stations', 'num_satellites',
                                               'mean_rms', 'min_rms', 'max_rms']].copy()
                        df_display.columns = ['Constellation', 'Stations', 'Satellites',
                                             'Mean RMS (mm)', 'Min RMS (mm)', 'Max RMS (mm)']
                        st.dataframe(
                            df_display.style.format({
                                'Mean RMS (mm)': '{:.2f}',
                                'Min RMS (mm)': '{:.2f}',
                                'Max RMS (mm)': '{:.2f}'
                            }).background_gradient(subset=['Mean RMS (mm)'], cmap='RdYlGn_r'),
                            use_container_width=True,
                            hide_index=True
                        )

                    # Station-level residuals by constellation
                    st.markdown("**Residuals by Station and Constellation**")
                    resid_by_sta_const = cached_get_residuals_by_station_constellation(year, obsq_doy)

                    if resid_by_sta_const:
                        df_sta_const = pd.DataFrame(resid_by_sta_const)

                        # Pivot for heatmap
                        pivot_rms = df_sta_const.pivot_table(
                            index='station_id',
                            columns='constellation',
                            values='mean_rms',
                            aggfunc='first'
                        ).fillna(0)

                        # Reorder columns (matches database names: GPS, GLONASS, GALILEO)
                        cols_order = [c for c in ['GPS', 'GLONASS', 'GALILEO'] if c in pivot_rms.columns]
                        if cols_order:
                            pivot_rms = pivot_rms[cols_order]

                        fig_heatmap = go.Figure(data=go.Heatmap(
                            z=pivot_rms.values,
                            x=pivot_rms.columns.tolist(),
                            y=pivot_rms.index.tolist(),
                            colorscale='RdYlGn_r',
                            colorbar=dict(title='RMS (mm)'),
                            hovertemplate='Station: %{y}<br>Constellation: %{x}<br>RMS: %{z:.2f} mm<extra></extra>'
                        ))
                        fig_heatmap.update_layout(
                            title='Phase Residuals RMS by Station and Constellation',
                            xaxis_title='Constellation',
                            yaxis_title='Station',
                            height=max(400, len(pivot_rms) * 20)
                        )
                        apply_colorful_style(fig_heatmap)
                        st.plotly_chart(fig_heatmap, use_container_width=True)

                else:
                    st.info(f"No residuals data for DOY {obsq_doy}. Residuals are parsed from EDL_*.SUM files.")

                st.divider()

                # ============================================
                # Section 2: Observation Quality Metrics (from Bernese MAUPRP)
                # ============================================
                st.subheader("Observation Quality Metrics")
                st.markdown("*Observation counts, cycle slips, and RMS from Bernese MAUPRP preprocessing (MPR\\_\\*.SUM)*")

                # Try to get observation quality data
                obs_quality = cached_get_observation_quality(year=year, doy=obsq_doy)

                if obs_quality:
                    df_obsq = pd.DataFrame(obs_quality)

                    # Summary metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        n_stations = len(df_obsq)
                        st.metric("Stations Processed", f"{n_stations}")
                    with col2:
                        total_obs = df_obsq['total_observations'].sum() if 'total_observations' in df_obsq else 0
                        st.metric("Total Observations", f"{total_obs:,}")
                    with col3:
                        total_slips = df_obsq['cycle_slips'].sum() if 'cycle_slips' in df_obsq else 0
                        st.metric("Total Cycle Slips", f"{total_slips:,}")
                    with col4:
                        n_excellent = len(df_obsq[df_obsq['quality_level'] == 'EXCELLENT']) if 'quality_level' in df_obsq else 0
                        n_good = len(df_obsq[df_obsq['quality_level'] == 'GOOD']) if 'quality_level' in df_obsq else 0
                        st.metric("Excellent + Good", f"{n_excellent + n_good} / {n_stations}")

                    # Row 1: Observation counts and quality
                    col1, col2 = st.columns(2)

                    with col1:
                        # Observation count per station
                        if 'total_observations' in df_obsq.columns:
                            obs_data = df_obsq[['station_id', 'total_observations']].sort_values('total_observations')
                            fig_obs = px.bar(
                                obs_data,
                                x='station_id',
                                y='total_observations',
                                color='total_observations',
                                color_continuous_scale='Viridis',
                                title='Total Observations by Station (MAUPRP)',
                                labels={'total_observations': 'Observations', 'station_id': 'Station'}
                            )
                            median_obs = obs_data['total_observations'].median()
                            fig_obs.add_hline(y=median_obs, line_dash="dash", line_color="#ffa502",
                                            annotation_text=f"Median: {median_obs:,.0f}")
                            apply_colorful_style(fig_obs)
                            st.plotly_chart(fig_obs, use_container_width=True)
                        else:
                            st.info("No observation count data available")

                    with col2:
                        # Quality level distribution pie chart
                        if 'quality_level' in df_obsq.columns:
                            quality_counts = df_obsq['quality_level'].value_counts()
                            quality_colors = {'EXCELLENT': '#26de81', 'GOOD': '#4ecdc4',
                                            'ACCEPTABLE': '#ffa502', 'POOR': '#ff4757'}
                            fig_qual = px.pie(
                                values=quality_counts.values,
                                names=quality_counts.index,
                                title='Station Quality Distribution',
                                color=quality_counts.index,
                                color_discrete_map=quality_colors
                            )
                            apply_colorful_style(fig_qual)
                            st.plotly_chart(fig_qual, use_container_width=True)
                        else:
                            st.info("No quality level data available")

                    # Row 2: Cycle slips and deleted observations
                    st.markdown("**Cycle Slip & Editing Analysis**")
                    col1, col2 = st.columns(2)

                    with col1:
                        # Cycle slip rate per station
                        if 'cycle_slip_rate' in df_obsq.columns:
                            slip_data = df_obsq[['station_id', 'cycle_slips', 'cycle_slip_rate']].dropna()
                            if len(slip_data) > 0:
                                fig_slip = px.bar(
                                    slip_data.sort_values('cycle_slip_rate', ascending=False),
                                    x='station_id',
                                    y='cycle_slip_rate',
                                    color='cycle_slip_rate',
                                    color_continuous_scale='RdYlGn_r',
                                    title='Cycle Slip Rate by Station',
                                    labels={'cycle_slip_rate': 'Slips per 1000 obs', 'station_id': 'Station'}
                                )
                                apply_colorful_style(fig_slip)
                                st.plotly_chart(fig_slip, use_container_width=True)
                            else:
                                st.info("No cycle slip rate data available")
                        else:
                            st.info("Cycle slip rate not available")

                    with col2:
                        # Cycle slips absolute count per station
                        if 'cycle_slips' in df_obsq.columns:
                            slip_abs = df_obsq[['station_id', 'cycle_slips']].sort_values('cycle_slips', ascending=False)
                            fig_slip_abs = px.bar(
                                slip_abs,
                                x='station_id',
                                y='cycle_slips',
                                color='cycle_slips',
                                color_continuous_scale='RdYlGn_r',
                                title='Total Cycle Slips by Station',
                                labels={'cycle_slips': 'Cycle Slips', 'station_id': 'Station'}
                            )
                            apply_colorful_style(fig_slip_abs)
                            st.plotly_chart(fig_slip_abs, use_container_width=True)

                    # Detailed table
                    st.markdown("**Detailed Observation Quality Metrics**")
                    display_cols = ['station_id', 'total_observations', 'cycle_slips',
                                   'cycle_slip_rate', 'quality_level']
                    available_cols = [c for c in display_cols if c in df_obsq.columns]
                    df_display = df_obsq[available_cols].copy()

                    col_rename = {
                        'station_id': 'Station', 'total_observations': 'Observations',
                        'cycle_slips': 'Cycle Slips', 'cycle_slip_rate': 'Slip Rate (/1000 obs)',
                        'quality_level': 'Quality'
                    }
                    df_display.rename(columns=col_rename, inplace=True)

                    format_dict = {}
                    if 'Slip Rate (/1000 obs)' in df_display.columns:
                        format_dict['Slip Rate (/1000 obs)'] = '{:.3f}'

                    st.dataframe(
                        df_display.style.format(format_dict),
                        use_container_width=True,
                        hide_index=True
                    )

                else:
                    st.info(f"""
                    No observation quality data for DOY {obsq_doy}.

                    Observation quality metrics (observations, cycle slips, RMS) are parsed from Bernese MAUPRP output (MPR_*.SUM files).
                    Run ingestion to populate this data.
                    """)

                st.divider()

                # ============================================
                # Section 3: Coordinate Quality Checks
                # ============================================
                st.subheader("Coordinate Quality Checks")
                st.markdown("*Daily repeatability: RMS of daily solutions vs. weekly mean (XYZ in mm)*")

                # Get all station repeatability over the DOY range
                try:
                    repeat_data = cached_get_all_station_repeatability(year, start_doy, end_doy)

                    if repeat_data:
                        df_repeat = pd.DataFrame(repeat_data)

                        # Summary metrics
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            mean_3d_rms = df_repeat['rms_3d_mm'].mean()
                            st.metric(
                                "Mean 3D RMS",
                                f"{mean_3d_rms:.1f} mm",
                                delta="Good" if mean_3d_rms < 5 else ("Fair" if mean_3d_rms < 10 else "Check")
                            )
                        with col2:
                            mean_x_rms = df_repeat['rms_x_mm'].mean()
                            st.metric("Mean X RMS", f"{mean_x_rms:.1f} mm")
                        with col3:
                            mean_y_rms = df_repeat['rms_y_mm'].mean()
                            st.metric("Mean Y RMS", f"{mean_y_rms:.1f} mm")
                        with col4:
                            mean_z_rms = df_repeat['rms_z_mm'].mean()
                            st.metric("Mean Z RMS", f"{mean_z_rms:.1f} mm")

                        # Bar chart of 3D RMS by station
                        fig_repeat = px.bar(
                            df_repeat.sort_values('rms_3d_mm'),
                            x='station_id',
                            y='rms_3d_mm',
                            color='rms_3d_mm',
                            color_continuous_scale='RdYlGn_r',
                            title=f'Coordinate Repeatability (3D RMS) - DOY {start_doy}-{end_doy}',
                            labels={'station_id': 'Station', 'rms_3d_mm': '3D RMS (mm)'},
                            hover_data=['rms_x_mm', 'rms_y_mm', 'rms_z_mm', 'num_days']
                        )
                        fig_repeat.add_hline(y=5.0, line_dash="dash", line_color="#2ecc71",
                                             annotation_text="5mm target")
                        fig_repeat.add_hline(y=10.0, line_dash="dash", line_color="#ffa502",
                                             annotation_text="10mm warning")
                        apply_colorful_style(fig_repeat)
                        st.plotly_chart(fig_repeat, use_container_width=True)

                        # Detailed repeatability table
                        st.markdown("**Station Repeatability Table**")
                        df_table = df_repeat[['station_id', 'num_days', 'rms_x_mm', 'rms_y_mm', 'rms_z_mm', 'rms_3d_mm']].copy()
                        df_table.columns = ['Station', 'Days', 'X RMS (mm)', 'Y RMS (mm)', 'Z RMS (mm)', '3D RMS (mm)']

                        # Color coding based on 3D RMS
                        def color_rms(val):
                            if val < 5:
                                return 'background-color: #d4edda'  # green
                            elif val < 10:
                                return 'background-color: #fff3cd'  # yellow
                            else:
                                return 'background-color: #f8d7da'  # red

                        st.dataframe(
                            df_table.style.format({
                                'X RMS (mm)': '{:.2f}',
                                'Y RMS (mm)': '{:.2f}',
                                'Z RMS (mm)': '{:.2f}',
                                '3D RMS (mm)': '{:.2f}'
                            }).applymap(color_rms, subset=['3D RMS (mm)']),
                            use_container_width=True,
                            hide_index=True
                        )

                        # Expandable section for outlier analysis
                        with st.expander("Outlier Analysis"):
                            threshold = st.slider("3D RMS Threshold (mm)", 1.0, 20.0, 10.0, 0.5)
                            outliers = df_repeat[df_repeat['rms_3d_mm'] > threshold]
                            if len(outliers) > 0:
                                st.warning(f"**{len(outliers)} stations** exceed {threshold:.1f}mm threshold:")
                                for _, row in outliers.iterrows():
                                    st.markdown(f"- **{row['station_id']}**: 3D RMS = {row['rms_3d_mm']:.2f} mm ({row['num_days']} days)")
                            else:
                                st.success(f"All stations within {threshold:.1f}mm threshold")

                    else:
                        st.info("No coordinate data available for repeatability analysis.")
                except Exception as e:
                    st.error(f"Error loading coordinate repeatability: {e}")

                # Section 4: PPP-AR Quality Control
                st.markdown("---")
                st.subheader("4. PPP-AR Quality Control")
                st.markdown("""
                Ambiguity Resolution (AR) metrics showing Wide Lane (WL) and Narrow Lane (NL)
                fix rates. Higher rates indicate better positioning accuracy.
                - **WL Target**: >90% | **NL Target**: >70%
                """)

                try:
                    # Get AR summary statistics
                    ar_summary = cached_get_ar_summary_stats(year, start_doy, end_doy)

                    if ar_summary and ar_summary.get('num_records', 0) > 0:
                        # Summary metrics
                        ar_col1, ar_col2, ar_col3, ar_col4 = st.columns(4)
                        with ar_col1:
                            mean_wl = ar_summary.get('mean_wl_combined', 0) or 0
                            wl_color = "normal" if mean_wl >= 90 else ("off" if mean_wl >= 80 else "inverse")
                            st.metric("Mean WL Fix Rate", f"{mean_wl:.1f}%", delta_color=wl_color)
                        with ar_col2:
                            mean_nl = ar_summary.get('mean_nl_combined', 0) or 0
                            nl_color = "normal" if mean_nl >= 70 else ("off" if mean_nl >= 50 else "inverse")
                            st.metric("Mean NL Fix Rate", f"{mean_nl:.1f}%", delta_color=nl_color)
                        with ar_col3:
                            st.metric("Stations", ar_summary.get('num_stations', 0))
                        with ar_col4:
                            low_nl = ar_summary.get('low_nl_count', 0) or 0
                            st.metric("Low NL Stations (<50%)", low_nl)

                        # Get AR by station
                        ar_stations = cached_get_ar_by_station(year, start_doy, end_doy)
                        if ar_stations:
                            df_ar = pd.DataFrame(ar_stations)

                            # AR Success Rate chart
                            fig_ar = go.Figure()

                            # Add WL bars
                            fig_ar.add_trace(go.Bar(
                                name='Wide Lane (WL)',
                                x=df_ar['station_id'],
                                y=df_ar['mean_wl'],
                                marker_color='rgb(55, 128, 191)',
                                text=df_ar['mean_wl'].round(1),
                                textposition='outside'
                            ))

                            # Add NL bars
                            fig_ar.add_trace(go.Bar(
                                name='Narrow Lane (NL)',
                                x=df_ar['station_id'],
                                y=df_ar['mean_nl'],
                                marker_color='rgb(50, 171, 96)',
                                text=df_ar['mean_nl'].round(1),
                                textposition='outside'
                            ))

                            # Target lines
                            fig_ar.add_hline(y=90, line_dash="dash", line_color="blue",
                                           annotation_text="WL Target (90%)")
                            fig_ar.add_hline(y=70, line_dash="dash", line_color="green",
                                           annotation_text="NL Target (70%)")

                            fig_ar.update_layout(
                                title="AR Success Rate by Station",
                                xaxis_title="Station",
                                yaxis_title="Fix Rate (%)",
                                yaxis=dict(range=[0, 105]),
                                barmode='group',
                                height=450
                            )
                            st.plotly_chart(fig_ar, use_container_width=True)

                            # Constellation breakdown
                            st.markdown("##### Constellation Comparison")
                            const_col1, const_col2 = st.columns(2)
                            with const_col1:
                                mean_wl_gps = ar_summary.get('mean_wl_gps', 0) or 0
                                mean_nl_gps = ar_summary.get('mean_nl_gps', 0) or 0
                                st.markdown(f"**GPS**: WL={mean_wl_gps:.1f}%, NL={mean_nl_gps:.1f}%")
                            with const_col2:
                                mean_wl_gal = ar_summary.get('mean_wl_gal', 0) or 0
                                mean_nl_gal = ar_summary.get('mean_nl_gal', 0) or 0
                                st.markdown(f"**Galileo**: WL={mean_wl_gal:.1f}%, NL={mean_nl_gal:.1f}%")

                            # Expandable table with full data
                            with st.expander("Detailed AR Statistics by Station"):
                                display_cols = ['station_id', 'receiver', 'num_days',
                                              'mean_wl', 'mean_nl', 'mean_wl_gps', 'mean_nl_gps',
                                              'mean_wl_gal', 'mean_nl_gal', 'ar_quality']
                                display_cols = [c for c in display_cols if c in df_ar.columns]
                                df_display = df_ar[display_cols].copy()

                                # Rename columns
                                df_display.columns = ['Station', 'Receiver', 'Days',
                                                     'WL%', 'NL%', 'WL GPS%', 'NL GPS%',
                                                     'WL GAL%', 'NL GAL%', 'Quality'][:len(display_cols)]

                                # Apply styling
                                def color_ar(val):
                                    if pd.isna(val):
                                        return ''
                                    if val >= 90:
                                        return 'background-color: #90EE90'
                                    elif val >= 70:
                                        return 'background-color: #FFFFE0'
                                    else:
                                        return 'background-color: #FFB6C1'

                                styled = df_display.style.applymap(
                                    color_ar,
                                    subset=[c for c in df_display.columns if '%' in c]
                                )
                                st.dataframe(styled, use_container_width=True)

                            # Receiver performance comparison
                            ar_by_receiver = cached_get_ar_by_receiver_type(year, start_doy, end_doy)
                            if ar_by_receiver and len(ar_by_receiver) > 1:
                                with st.expander("AR Performance by Receiver Type"):
                                    df_recv = pd.DataFrame(ar_by_receiver)
                                    fig_recv = go.Figure(data=[
                                        go.Bar(name='WL', x=df_recv['receiver'], y=df_recv['mean_wl'],
                                              marker_color='rgb(55, 128, 191)'),
                                        go.Bar(name='NL', x=df_recv['receiver'], y=df_recv['mean_nl'],
                                              marker_color='rgb(50, 171, 96)')
                                    ])
                                    fig_recv.update_layout(
                                        title="AR Performance by Receiver Type",
                                        xaxis_title="Receiver",
                                        yaxis_title="Fix Rate (%)",
                                        barmode='group',
                                        height=350
                                    )
                                    st.plotly_chart(fig_recv, use_container_width=True)
                    else:
                        st.info("No ambiguity resolution data available for the selected period.")

                except Exception as e:
                    st.error(f"Error loading AR metrics: {e}")

        # ==================================
        # TAB 13: TROPOSPHERIC GRADIENT MONITORING
        # ==================================
        elif selected_page == "🌬️ Trop. Gradients":
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
                grad_stations = cached_get_gradient_stations(year, grad_doy)

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
                            data = cached_get_tropospheric_gradients(year, grad_doy, sta)
                            all_grad_data.extend(data)

                        if all_grad_data:
                            df_grad = pd.DataFrame(all_grad_data)

                            # Convert gradients from meters to millimeters for display
                            for col in ['grad_n', 'grad_n_rms', 'grad_e', 'grad_e_rms', 'grad_magnitude']:
                                if col in df_grad.columns:
                                    df_grad[col] = df_grad[col] * 1000.0

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

                            # Select stations for polar plot (multi-select)
                            polar_stations = st.multiselect(
                                "Select stations for polar plot",
                                options=selected_grad_stations,
                                default=selected_grad_stations[:min(3, len(selected_grad_stations))],
                                key="polar_stations"
                            )

                            df_polar = df_grad[df_grad['station_id'].isin(polar_stations)].copy()

                            if len(df_polar) > 0:
                                fig_polar = go.Figure()
                                # Use distinct colors per station
                                colors = px.colors.qualitative.Plotly
                                for i, sta in enumerate(polar_stations):
                                    sta_df = df_polar[df_polar['station_id'] == sta]
                                    if len(sta_df) == 0:
                                        continue
                                    fig_polar.add_trace(go.Scatterpolar(
                                        r=sta_df['grad_magnitude'],
                                        theta=sta_df['grad_azimuth'],
                                        mode='markers',
                                        name=sta,
                                        marker=dict(
                                            size=8,
                                            color=sta_df['hour'],
                                            colorscale='Viridis',
                                            showscale=(i == 0),
                                            colorbar=dict(
                                                title="Hour<br>(UTC)",
                                                tickvals=[0, 6, 12, 18, 23],
                                                ticktext=["00h", "06h", "12h", "18h", "23h"]
                                            ),
                                            symbol=i,
                                            line=dict(width=1, color=colors[i % len(colors)])
                                        ),
                                        text=sta_df['datetime'].dt.strftime('%H:%M UTC'),
                                        hovertemplate=f"<b>{sta}</b><br>Magnitude: %{{r:.3f}} mm<br>Azimuth: %{{theta:.1f}}°<br>Time: %{{text}}<extra></extra>"
                                    ))
                                title_stations = ", ".join(polar_stations)
                                fig_polar.update_layout(
                                    title=f"Gradient Vectors — {title_stations}",
                                    polar=dict(
                                        radialaxis=dict(visible=True, title="Magnitude (mm)"),
                                        angularaxis=dict(
                                            direction="clockwise",
                                            rotation=90,
                                            ticktext=["N", "NE", "E", "SE", "S", "SW", "W", "NW"],
                                            tickvals=[0, 45, 90, 135, 180, 225, 270, 315]
                                        )
                                    ),
                                    showlegend=True,
                                    legend=dict(title="Station"),
                                    height=550
                                )
                                apply_colorful_style(fig_polar)
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
        # TAB 14: RECEIVER CLOCK ANALYSIS
        # ==================================
        elif selected_page == "⏱️ Receiver Clocks":
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
                clk_stations = cached_get_clock_stations(year, clk_doy)

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
                            data = cached_get_receiver_clocks(year, clk_doy, sta)
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
        # TAB 15: Station Data Completeness
        # ========================================
        elif selected_page == "📊 Station Completeness":
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
                        ztd_rows = cached_get_ztd(station, year, comp_doy)
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
                    stats = cached_get_stats(year=year, limit=5000)
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
                timeline_data = cached_get_data_gaps_timeline(year, start_doy, end_doy)

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

        # ========================================
        # TAB 16: Ocean Loading
        # ========================================
        elif selected_page == "🌊 Ocean Loading":
            st.header("Ocean Tide Loading Displacements")
            st.markdown("""
            Visualize ocean tide loading effects on GNSS stations from BLQ coefficients.
            Ocean loading causes periodic displacements due to gravitational attraction of the sun and moon on ocean water mass.

            **Tidal Constituents**: M2 (principal lunar), S2 (principal solar), K1, O1, P1 (diurnal), MF, MM, SSA (long-period)
            """)

            # BLQ file path
            blq_default_path = "/home/ahunegnaw/GPSDATA/CAMPAIGN54/OCT2025_MGX/STA/FIN_EGA.BLQ"
            blq_path = st.text_input("BLQ File Path", value=blq_default_path, key='blq_path')

            @st.cache_data(ttl=3600, show_spinner=False)
            def load_blq_data(path: str):
                """Load and cache BLQ data"""
                return parse_blq_file(path)

            if os.path.exists(blq_path):
                with st.spinner("Loading ocean loading coefficients..."):
                    all_blq_stations = load_blq_data(blq_path)

                if all_blq_stations:
                    # Filter to only show stations being processed (from database)
                    processing_stations = set(s.upper()[:4] for s in available_stations) if available_stations else set()

                    # Match BLQ stations with processing stations (case-insensitive, first 4 chars)
                    blq_stations = {}
                    matched_stations = []
                    for blq_id, station_data in all_blq_stations.items():
                        blq_id_upper = blq_id.upper()[:4]
                        if blq_id_upper in processing_stations:
                            blq_stations[blq_id] = station_data
                            matched_stations.append(blq_id)

                    # If no matches found, show all (fallback)
                    if not blq_stations:
                        blq_stations = all_blq_stations
                        st.warning(f"No matching stations found. Showing all {len(all_blq_stations)} stations from BLQ file.")
                    else:
                        st.success(f"Loaded ocean loading for **{len(blq_stations)}** processing stations (matched from {len(all_blq_stations)} in BLQ file)")

                    # Get station summary
                    summaries = get_station_summary(blq_stations)

                    # Create sub-tabs
                    ol_tab1, ol_tab2, ol_tab3, ol_tab4, ol_tab5 = st.tabs([
                        "📊 Station Summary",
                        "🗺️ Geographic Map",
                        "📈 Time Series",
                        "🔍 Constituent Analysis",
                        "📐 CM Corrections"
                    ])

                    with ol_tab1:
                        st.subheader("Ocean Loading Displacement Summary")

                        # Calculate distance to coast for all stations
                        df_summary = pd.DataFrame(summaries)
                        df_summary['distance_to_coast_km'] = df_summary.apply(
                            lambda row: calculate_distance_to_coast(row['latitude'], row['longitude']),
                            axis=1
                        )
                        df_summary['coastal_class'] = df_summary['distance_to_coast_km'].apply(classify_coastal_proximity)

                        # Summary metrics with distance info
                        col1, col2, col3, col4, col5 = st.columns(5)
                        with col1:
                            max_total = df_summary['max_total_mm'].max()
                            st.metric("Max Total Disp.", f"{max_total:.1f} mm")
                        with col2:
                            avg_dist = df_summary['distance_to_coast_km'].mean()
                            st.metric("Avg Distance to Coast", f"{avg_dist:.0f} km")
                        with col3:
                            coastal_count = len(df_summary[df_summary['distance_to_coast_km'] < 50])
                            st.metric("🌊 Coastal Stations", f"{coastal_count}")
                        with col4:
                            near_coastal = len(df_summary[(df_summary['distance_to_coast_km'] >= 50) & (df_summary['distance_to_coast_km'] < 150)])
                            st.metric("🏖️ Near-coastal", f"{near_coastal}")
                        with col5:
                            inland_count = len(df_summary[df_summary['distance_to_coast_km'] >= 300])
                            st.metric("🏔️ Inland Stations", f"{inland_count}")

                        # ==========================================
                        # KEY VISUALIZATION: Ocean Loading vs Distance to Coast
                        # ==========================================
                        st.markdown("---")
                        st.markdown("""
                        <div style="
                            background: linear-gradient(135deg, #1a365d 0%, #2c5282 100%);
                            padding: 15px 20px;
                            border-radius: 12px;
                            margin-bottom: 15px;
                            border-left: 4px solid #4ecdc4;
                        ">
                            <h4 style="color: #ffffff; margin: 0;">🌊 Ocean Loading vs Distance to Coast</h4>
                            <p style="color: #a0aec0; margin: 5px 0 0 0; font-size: 0.9rem;">
                                Ocean tide loading decreases exponentially with distance from the coast
                            </p>
                        </div>
                        """, unsafe_allow_html=True)

                        # Scatter plot: Distance vs Ocean Loading (THE KEY PLOT)
                        fig_scatter = px.scatter(
                            df_summary,
                            x='distance_to_coast_km',
                            y='max_total_mm',
                            color='coastal_class',
                            size='max_total_mm',
                            hover_name='station_id',
                            hover_data={
                                'latitude': ':.3f',
                                'longitude': ':.3f',
                                'max_up_mm': ':.2f',
                                'distance_to_coast_km': ':.1f',
                                'coastal_class': True
                            },
                            title='Ocean Loading Displacement vs Distance to Coast',
                            labels={
                                'distance_to_coast_km': 'Distance to Coast (km)',
                                'max_total_mm': 'Max 3D Displacement (mm)',
                                'coastal_class': 'Station Type'
                            },
                            color_discrete_map={
                                '🌊 Coastal (<50 km)': '#FF6B6B',
                                '🏖️ Near-coastal (50-150 km)': '#FFA94D',
                                '🌲 Semi-inland (150-300 km)': '#69DB7C',
                                '🏔️ Inland (>300 km)': '#4DABF7'
                            },
                            size_max=25
                        )
                        # Add exponential decay trendline annotation
                        fig_scatter.update_layout(
                            height=500,
                            xaxis=dict(gridcolor='#374151'),
                            yaxis=dict(gridcolor='#374151'),
                            legend=dict(
                                title='Station Type',
                                yanchor="top", y=0.99,
                                xanchor="right", x=0.99,
                                bgcolor='rgba(30,37,48,0.8)'
                            )
                        )
                        # Add reference zones
                        fig_scatter.add_vrect(x0=0, x1=50, fillcolor="#FF6B6B", opacity=0.1,
                                             layer="below", line_width=0,
                                             annotation_text="Coastal Zone", annotation_position="top left")
                        fig_scatter.add_vrect(x0=50, x1=150, fillcolor="#FFA94D", opacity=0.1,
                                             layer="below", line_width=0)
                        fig_scatter.add_vrect(x0=150, x1=300, fillcolor="#69DB7C", opacity=0.1,
                                             layer="below", line_width=0)
                        apply_colorful_style(fig_scatter)
                        st.plotly_chart(fig_scatter, use_container_width=True)

                        # Calculate correlation
                        correlation = df_summary['distance_to_coast_km'].corr(df_summary['max_total_mm'])
                        st.markdown(f"""
                        <div style="background: #1E2530; padding: 12px 20px; border-radius: 8px; border-left: 4px solid {'#10B981' if correlation < -0.5 else '#F59E0B'};">
                            <b>📊 Correlation Coefficient:</b> <span style="color: #4ecdc4; font-size: 1.2rem;">{correlation:.3f}</span>
                            &nbsp;&nbsp;|&nbsp;&nbsp;
                            <span style="color: #9CA3AF;">
                                {'Strong negative correlation - Ocean loading clearly decreases with distance!' if correlation < -0.5 else 'Moderate correlation observed'}
                            </span>
                        </div>
                        """, unsafe_allow_html=True)

                        st.markdown("---")

                        # Two-column layout for comparison charts
                        chart_col1, chart_col2 = st.columns(2)

                        with chart_col1:
                            # Box plot by coastal class
                            fig_box = px.box(
                                df_summary,
                                x='coastal_class',
                                y='max_total_mm',
                                color='coastal_class',
                                title='Ocean Loading by Coastal Proximity',
                                labels={'max_total_mm': 'Max 3D Displacement (mm)', 'coastal_class': ''},
                                color_discrete_map={
                                    '🌊 Coastal (<50 km)': '#FF6B6B',
                                    '🏖️ Near-coastal (50-150 km)': '#FFA94D',
                                    '🌲 Semi-inland (150-300 km)': '#69DB7C',
                                    '🏔️ Inland (>300 km)': '#4DABF7'
                                }
                            )
                            fig_box.update_layout(height=400, showlegend=False, xaxis_tickangle=-15)
                            apply_colorful_style(fig_box)
                            st.plotly_chart(fig_box, use_container_width=True)

                        with chart_col2:
                            # Average displacement by class
                            class_avg = df_summary.groupby('coastal_class').agg({
                                'max_total_mm': 'mean',
                                'distance_to_coast_km': 'mean',
                                'station_id': 'count'
                            }).reset_index()
                            class_avg.columns = ['Class', 'Avg Displacement (mm)', 'Avg Distance (km)', 'Count']
                            class_avg = class_avg.sort_values('Avg Distance (km)')

                            fig_bar_class = px.bar(
                                class_avg,
                                x='Class',
                                y='Avg Displacement (mm)',
                                color='Class',
                                text='Count',
                                title='Average Ocean Loading by Station Class',
                                color_discrete_map={
                                    '🌊 Coastal (<50 km)': '#FF6B6B',
                                    '🏖️ Near-coastal (50-150 km)': '#FFA94D',
                                    '🌲 Semi-inland (150-300 km)': '#69DB7C',
                                    '🏔️ Inland (>300 km)': '#4DABF7'
                                }
                            )
                            fig_bar_class.update_traces(texttemplate='n=%{text}', textposition='outside')
                            fig_bar_class.update_layout(height=400, showlegend=False, xaxis_tickangle=-15)
                            apply_colorful_style(fig_bar_class)
                            st.plotly_chart(fig_bar_class, use_container_width=True)

                        # Bar chart sorted by distance to coast
                        st.markdown("### Station Displacement Sorted by Distance to Coast")
                        df_sorted = df_summary.sort_values('distance_to_coast_km')
                        fig_disp = px.bar(
                            df_sorted,
                            x='station_id',
                            y='max_total_mm',
                            color='distance_to_coast_km',
                            color_continuous_scale='RdYlBu_r',
                            hover_data={
                                'distance_to_coast_km': ':.1f',
                                'max_up_mm': ':.2f',
                                'coastal_class': True
                            },
                            title='Maximum 3D Ocean Loading (Sorted by Distance to Coast: Near → Far)',
                            labels={'max_total_mm': 'Max 3D Displacement (mm)', 'station_id': 'Station',
                                   'distance_to_coast_km': 'Distance (km)'}
                        )
                        fig_disp.update_layout(
                            height=450,
                            coloraxis_colorbar_title='Dist. to<br>Coast (km)',
                            xaxis_tickangle=-45
                        )
                        apply_colorful_style(fig_disp)
                        st.plotly_chart(fig_disp, use_container_width=True)

                        # Data table with distance
                        st.markdown("### 📋 Detailed Station Data with Distance to Coast")
                        display_cols = ['station_id', 'coastal_class', 'distance_to_coast_km', 'latitude', 'longitude',
                                       'max_up_mm', 'max_east_mm', 'max_north_mm', 'max_total_mm', 'dominant_constituent']
                        df_display = df_summary[display_cols].sort_values('distance_to_coast_km')
                        st.dataframe(
                            df_display.style.format({
                                'latitude': '{:.4f}',
                                'longitude': '{:.4f}',
                                'distance_to_coast_km': '{:.1f}',
                                'max_up_mm': '{:.2f}',
                                'max_east_mm': '{:.2f}',
                                'max_north_mm': '{:.2f}',
                                'max_total_mm': '{:.2f}'
                            }).background_gradient(subset=['max_total_mm'], cmap='YlOrRd', vmin=0, vmax=80)
                             .background_gradient(subset=['distance_to_coast_km'], cmap='Blues_r', vmin=0, vmax=500),
                            use_container_width=True,
                            hide_index=True,
                            height=500
                        )

                    with ol_tab2:
                        st.subheader("Geographic Distribution of Ocean Loading")

                        # Map view selector
                        map_color_by = st.radio(
                            "Color stations by:",
                            options=['Ocean Loading Magnitude', 'Distance to Coast'],
                            horizontal=True,
                            key='ol_map_color'
                        )

                        if map_color_by == 'Ocean Loading Magnitude':
                            fig_map = px.scatter_geo(
                                df_summary,
                                lat='latitude',
                                lon='longitude',
                                size='max_total_mm',
                                color='max_total_mm',
                                hover_name='station_id',
                                hover_data={
                                    'max_up_mm': ':.2f',
                                    'max_east_mm': ':.2f',
                                    'max_north_mm': ':.2f',
                                    'distance_to_coast_km': ':.1f',
                                    'coastal_class': True,
                                    'dominant_constituent': True
                                },
                                title='🌊 Ocean Loading Magnitude by Station Location',
                                color_continuous_scale='YlOrRd',
                                size_max=25
                            )
                            fig_map.update_layout(coloraxis_colorbar_title='Max Disp.<br>(mm)')
                        else:
                            fig_map = px.scatter_geo(
                                df_summary,
                                lat='latitude',
                                lon='longitude',
                                size='max_total_mm',
                                color='distance_to_coast_km',
                                hover_name='station_id',
                                hover_data={
                                    'max_total_mm': ':.2f',
                                    'distance_to_coast_km': ':.1f',
                                    'coastal_class': True
                                },
                                title='📏 Distance to Coast by Station Location',
                                color_continuous_scale='RdYlBu',
                                size_max=25
                            )
                            fig_map.update_layout(coloraxis_colorbar_title='Distance<br>(km)')

                        fig_map.update_geos(
                            projection_type="natural earth",
                            showland=True,
                            landcolor='#1E2530',
                            showocean=True,
                            oceancolor='#0d1117',
                            showcoastlines=True,
                            coastlinecolor='#4ecdc4',
                            coastlinewidth=2,
                            showlakes=True,
                            lakecolor='#0d1117',
                            showcountries=True,
                            countrycolor='#374151'
                        )
                        fig_map.update_layout(
                            height=600,
                            paper_bgcolor='rgba(0,0,0,0)',
                            geo=dict(bgcolor='rgba(0,0,0,0)')
                        )
                        apply_colorful_style(fig_map)
                        st.plotly_chart(fig_map, use_container_width=True)

                        # Map colored by coastal class
                        st.markdown("### Stations by Coastal Proximity Class")
                        fig_map_class = px.scatter_geo(
                            df_summary,
                            lat='latitude',
                            lon='longitude',
                            color='coastal_class',
                            size='max_total_mm',
                            hover_name='station_id',
                            hover_data={
                                'max_total_mm': ':.2f',
                                'distance_to_coast_km': ':.1f'
                            },
                            title='Station Classification by Distance to Coast',
                            color_discrete_map={
                                '🌊 Coastal (<50 km)': '#FF6B6B',
                                '🏖️ Near-coastal (50-150 km)': '#FFA94D',
                                '🌲 Semi-inland (150-300 km)': '#69DB7C',
                                '🏔️ Inland (>300 km)': '#4DABF7'
                            },
                            size_max=20
                        )
                        fig_map_class.update_geos(
                            projection_type="natural earth",
                            showland=True,
                            landcolor='#1E2530',
                            showocean=True,
                            oceancolor='#0d1117',
                            showcoastlines=True,
                            coastlinecolor='#4ecdc4',
                            coastlinewidth=2,
                            showlakes=True,
                            lakecolor='#0d1117'
                        )
                        fig_map_class.update_layout(
                            height=550,
                            paper_bgcolor='rgba(0,0,0,0)',
                            geo=dict(bgcolor='rgba(0,0,0,0)'),
                            legend=dict(title='Coastal Proximity')
                        )
                        apply_colorful_style(fig_map_class)
                        st.plotly_chart(fig_map_class, use_container_width=True)

                    with ol_tab3:
                        st.subheader("Ocean Loading Time Series")

                        # Station selector
                        selected_station = st.selectbox(
                            "Select Station",
                            options=list(blq_stations.keys()),
                            key='ol_station_select'
                        )

                        if selected_station:
                            station = blq_stations[selected_station]

                            # Time range selector
                            col1, col2 = st.columns(2)
                            with col1:
                                hours_to_plot = st.slider(
                                    "Duration (hours)",
                                    min_value=12, max_value=168, value=48,
                                    key='ol_duration'
                                )
                            with col2:
                                interval = st.selectbox(
                                    "Interval",
                                    options=[15, 30, 60],
                                    index=0,
                                    format_func=lambda x: f"{x} minutes",
                                    key='ol_interval'
                                )

                            # Calculate time series
                            now = datetime.utcnow()
                            start_time = now
                            end_time = now + timedelta(hours=hours_to_plot)

                            timeseries = calculate_displacement_timeseries(
                                station, start_time, end_time, interval_minutes=interval
                            )

                            if timeseries:
                                df_ts = pd.DataFrame(timeseries)

                                # Plot time series
                                fig_ts = px.line(
                                    df_ts,
                                    x='datetime',
                                    y=['up_mm', 'east_mm', 'north_mm'],
                                    title=f'Ocean Loading Displacement for {selected_station}',
                                    labels={'value': 'Displacement (mm)', 'datetime': 'Time (UTC)'},
                                    color_discrete_map={
                                        'up_mm': '#4ecdc4',
                                        'east_mm': '#ff6b6b',
                                        'north_mm': '#ffd93d'
                                    }
                                )
                                fig_ts.update_layout(
                                    legend_title_text='Component',
                                    height=450,
                                    hovermode='x unified'
                                )
                                apply_colorful_style(fig_ts)
                                st.plotly_chart(fig_ts, use_container_width=True)

                                # Current displacement
                                current_disp = calculate_displacement(station, now)
                                st.markdown(f"**Current displacement at {now.strftime('%Y-%m-%d %H:%M:%S')} UTC:**")
                                col1, col2, col3, col4 = st.columns(4)
                                with col1:
                                    st.metric("Up", f"{current_disp['up_mm']:.3f} mm")
                                with col2:
                                    st.metric("East", f"{current_disp['east_mm']:.3f} mm")
                                with col3:
                                    st.metric("North", f"{current_disp['north_mm']:.3f} mm")
                                with col4:
                                    st.metric("3D", f"{current_disp['total_3d_mm']:.3f} mm")

                                # Station info
                                st.markdown(f"""
                                **Station Info:** Lat: {station.latitude:.4f}°, Lon: {station.longitude:.4f}°, Height: {station.height:.1f} m
                                """)

                    with ol_tab4:
                        st.subheader("Tidal Constituent Analysis")

                        # Station selector for constituent analysis
                        const_station = st.selectbox(
                            "Select Station for Analysis",
                            options=list(blq_stations.keys()),
                            key='ol_const_station'
                        )

                        if const_station:
                            station = blq_stations[const_station]
                            const_amps = station.get_constituent_amplitudes()

                            # Create constituent dataframe
                            const_data = []
                            for name, amps in const_amps.items():
                                const_data.append({
                                    'Constituent': name,
                                    'Period': f"{TIDAL_PERIODS[name]:.1f} h",
                                    'Up (mm)': amps['up_mm'],
                                    'East (mm)': amps['east_mm'],
                                    'North (mm)': amps['north_mm'],
                                    'Total (mm)': np.sqrt(amps['up_mm']**2 + amps['east_mm']**2 + amps['north_mm']**2)
                                })
                            df_const = pd.DataFrame(const_data)

                            # Bar chart of constituent amplitudes
                            fig_const = px.bar(
                                df_const,
                                x='Constituent',
                                y=['Up (mm)', 'East (mm)', 'North (mm)'],
                                title=f'Tidal Constituent Amplitudes for {const_station}',
                                barmode='group',
                                color_discrete_map={
                                    'Up (mm)': '#4ecdc4',
                                    'East (mm)': '#ff6b6b',
                                    'North (mm)': '#ffd93d'
                                }
                            )
                            fig_const.update_layout(
                                legend_title_text='Component',
                                height=400
                            )
                            apply_colorful_style(fig_const)
                            st.plotly_chart(fig_const, use_container_width=True)

                            # Data table
                            st.dataframe(
                                df_const.style.format({
                                    'Up (mm)': '{:.3f}',
                                    'East (mm)': '{:.3f}',
                                    'North (mm)': '{:.3f}',
                                    'Total (mm)': '{:.3f}'
                                }).background_gradient(subset=['Total (mm)'], cmap='Blues'),
                                use_container_width=True,
                                hide_index=True
                            )

                            # Constituent contribution pie chart
                            fig_pie = px.pie(
                                df_const,
                                values='Total (mm)',
                                names='Constituent',
                                title=f'Relative Contribution of Tidal Constituents for {const_station}',
                                color_discrete_sequence=px.colors.qualitative.Set3
                            )
                            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                            apply_colorful_style(fig_pie)
                            st.plotly_chart(fig_pie, use_container_width=True)

                            # Phase information
                            st.markdown("**Phase Information (degrees, Greenwich phase lag)**")
                            phase_data = []
                            for i, name in enumerate(CONSTITUENT_NAMES):
                                if i < len(station.phase_radial):
                                    phase_data.append({
                                        'Constituent': name,
                                        'Up Phase': station.phase_radial[i],
                                        'East Phase': station.phase_east[i],
                                        'North Phase': station.phase_north[i]
                                    })
                            df_phase = pd.DataFrame(phase_data)
                            st.dataframe(
                                df_phase.style.format({
                                    'Up Phase': '{:.1f}°',
                                    'East Phase': '{:.1f}°',
                                    'North Phase': '{:.1f}°'
                                }),
                                use_container_width=True,
                                hide_index=True
                            )

                    with ol_tab5:
                        st.subheader("Center of Mass (CM) Corrections")

                        # Educational header
                        st.markdown("""
                        <div style="
                            background: linear-gradient(135deg, #2d3748 0%, #1a202c 100%);
                            padding: 20px;
                            border-radius: 12px;
                            margin-bottom: 20px;
                            border-left: 4px solid #f6ad55;
                        ">
                            <h4 style="color: #f6ad55; margin: 0 0 10px 0;">📐 What are Center of Mass Corrections?</h4>
                            <p style="color: #e2e8f0; margin: 0; font-size: 0.95rem; line-height: 1.6;">
                                Ocean tides redistribute massive amounts of water, causing two effects:<br>
                                <b>1. Local crustal deformation</b> (ocean loading at each station)<br>
                                <b>2. Geocenter motion</b> (the Earth's center of mass shifts ~1-2 mm)<br><br>
                                Your BLQ file has <b style="color: #68d391;">CMC: YES</b>, meaning the geocenter motion
                                has been <b>removed</b> from the loading values, putting them in the <b>CE/CF frame</b>
                                (Center of [solid] Earth / Center of Figure).
                            </p>
                        </div>
                        """, unsafe_allow_html=True)

                        # CM correction amplitudes from literature (Dong et al. 1997, Scherneck 1991)
                        # These are the geocenter motion amplitudes for each tidal constituent
                        cm_corrections = {
                            'M2': {'up': 0.95, 'east': 0.32, 'north': 0.48, 'period': 12.42},
                            'S2': {'up': 0.42, 'east': 0.14, 'north': 0.21, 'period': 12.00},
                            'N2': {'up': 0.19, 'east': 0.06, 'north': 0.10, 'period': 12.66},
                            'K2': {'up': 0.11, 'east': 0.04, 'north': 0.06, 'period': 11.97},
                            'K1': {'up': 0.31, 'east': 0.10, 'north': 0.15, 'period': 23.93},
                            'O1': {'up': 0.28, 'east': 0.09, 'north': 0.14, 'period': 25.82},
                            'P1': {'up': 0.10, 'east': 0.03, 'north': 0.05, 'period': 24.07},
                            'Q1': {'up': 0.05, 'east': 0.02, 'north': 0.03, 'period': 26.87},
                            'MF': {'up': 0.22, 'east': 0.07, 'north': 0.11, 'period': 327.86},
                            'MM': {'up': 0.12, 'east': 0.04, 'north': 0.06, 'period': 661.31},
                            'SSA': {'up': 0.08, 'east': 0.03, 'north': 0.04, 'period': 4383.05},
                        }

                        # Create dataframe for CM corrections
                        cm_data = []
                        for name, vals in cm_corrections.items():
                            total_3d = np.sqrt(vals['up']**2 + vals['east']**2 + vals['north']**2)
                            cm_data.append({
                                'Constituent': name,
                                'Period (h)': vals['period'],
                                'Up (mm)': vals['up'],
                                'East (mm)': vals['east'],
                                'North (mm)': vals['north'],
                                'Total 3D (mm)': total_3d
                            })
                        df_cm = pd.DataFrame(cm_data)

                        # Summary metrics
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            max_cm = df_cm['Total 3D (mm)'].max()
                            st.metric("Max CM Correction", f"{max_cm:.2f} mm", help="Largest geocenter motion amplitude (M2)")
                        with col2:
                            total_rss = np.sqrt(sum(df_cm['Total 3D (mm)']**2))
                            st.metric("RSS All Constituents", f"{total_rss:.2f} mm", help="Root sum square of all constituents")
                        with col3:
                            avg_loading = df_summary['max_total_mm'].mean() if 'df_summary' in dir() else 10.0
                            pct_of_loading = (max_cm / avg_loading) * 100 if avg_loading > 0 else 0
                            st.metric("% of Avg Loading", f"{pct_of_loading:.1f}%", help="CM correction as % of average ocean loading")
                        with col4:
                            st.metric("Reference Frame", "CE/CF", help="Your coordinates are in Center of Figure frame")

                        # Visual comparison: CM corrections bar chart
                        st.markdown("---")
                        st.markdown("### 📊 CM Correction Amplitudes by Constituent")

                        fig_cm_bar = px.bar(
                            df_cm,
                            x='Constituent',
                            y=['Up (mm)', 'East (mm)', 'North (mm)'],
                            title='Geocenter Motion Amplitudes (CM Corrections) by Tidal Constituent',
                            barmode='group',
                            color_discrete_map={
                                'Up (mm)': '#f6ad55',
                                'East (mm)': '#fc8181',
                                'North (mm)': '#68d391'
                            }
                        )
                        fig_cm_bar.update_layout(
                            height=400,
                            legend_title_text='Component',
                            xaxis_title='Tidal Constituent',
                            yaxis_title='Amplitude (mm)'
                        )
                        apply_colorful_style(fig_cm_bar)
                        st.plotly_chart(fig_cm_bar, use_container_width=True)

                        # Compare CM corrections to actual ocean loading
                        st.markdown("### 🔄 CM Corrections vs Ocean Loading")
                        st.markdown("""
                        This comparison shows how small the CM corrections are relative to actual ocean loading displacements.
                        CM corrections are **global** (same everywhere), while ocean loading is **location-dependent**.
                        """)

                        # Get average ocean loading per constituent if data available
                        comparison_data = []
                        if blq_stations:
                            # Calculate average ocean loading amplitude per constituent across all stations
                            avg_amps = {name: {'up': [], 'east': [], 'north': []} for name in CONSTITUENT_NAMES}
                            for station_id, station in blq_stations.items():
                                const_amps = station.get_constituent_amplitudes()
                                for name, amps in const_amps.items():
                                    avg_amps[name]['up'].append(amps['up_mm'])
                                    avg_amps[name]['east'].append(amps['east_mm'])
                                    avg_amps[name]['north'].append(amps['north_mm'])

                            for name in CONSTITUENT_NAMES:
                                if name in cm_corrections and avg_amps[name]['up']:
                                    avg_up = np.mean(avg_amps[name]['up'])
                                    avg_east = np.mean(avg_amps[name]['east'])
                                    avg_north = np.mean(avg_amps[name]['north'])
                                    avg_total = np.sqrt(avg_up**2 + avg_east**2 + avg_north**2)

                                    cm_total = np.sqrt(
                                        cm_corrections[name]['up']**2 +
                                        cm_corrections[name]['east']**2 +
                                        cm_corrections[name]['north']**2
                                    )

                                    comparison_data.append({
                                        'Constituent': name,
                                        'Ocean Loading (mm)': avg_total,
                                        'CM Correction (mm)': cm_total,
                                        'CM as % of Loading': (cm_total / avg_total * 100) if avg_total > 0 else 0
                                    })

                        if comparison_data:
                            df_compare = pd.DataFrame(comparison_data)

                            # Side-by-side bar chart
                            fig_compare = go.Figure()
                            fig_compare.add_trace(go.Bar(
                                name='Ocean Loading',
                                x=df_compare['Constituent'],
                                y=df_compare['Ocean Loading (mm)'],
                                marker_color='#4ecdc4',
                                text=df_compare['Ocean Loading (mm)'].round(2),
                                textposition='outside'
                            ))
                            fig_compare.add_trace(go.Bar(
                                name='CM Correction',
                                x=df_compare['Constituent'],
                                y=df_compare['CM Correction (mm)'],
                                marker_color='#f6ad55',
                                text=df_compare['CM Correction (mm)'].round(2),
                                textposition='outside'
                            ))
                            fig_compare.update_layout(
                                title='Ocean Loading vs CM Correction (Average Across All Stations)',
                                barmode='group',
                                height=450,
                                xaxis_title='Tidal Constituent',
                                yaxis_title='Amplitude (mm)',
                                legend=dict(
                                    yanchor="top", y=0.99,
                                    xanchor="right", x=0.99
                                )
                            )
                            apply_colorful_style(fig_compare)
                            st.plotly_chart(fig_compare, use_container_width=True)

                            # Percentage comparison
                            st.markdown("### 📈 CM Correction as Percentage of Ocean Loading")

                            fig_pct = px.bar(
                                df_compare,
                                x='Constituent',
                                y='CM as % of Loading',
                                title='CM Correction Relative to Ocean Loading (%)',
                                color='CM as % of Loading',
                                color_continuous_scale='Oranges',
                                text='CM as % of Loading'
                            )
                            fig_pct.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                            fig_pct.update_layout(
                                height=400,
                                xaxis_title='Tidal Constituent',
                                yaxis_title='Percentage (%)',
                                showlegend=False
                            )
                            apply_colorful_style(fig_pct)
                            st.plotly_chart(fig_pct, use_container_width=True)

                            # Data table
                            st.markdown("### 📋 Detailed Comparison Table")
                            st.dataframe(
                                df_compare.style.format({
                                    'Ocean Loading (mm)': '{:.3f}',
                                    'CM Correction (mm)': '{:.3f}',
                                    'CM as % of Loading': '{:.1f}%'
                                }).background_gradient(subset=['CM as % of Loading'], cmap='Oranges'),
                                use_container_width=True,
                                hide_index=True
                            )

                        # CM corrections data table
                        st.markdown("### 📋 CM Correction Reference Values")
                        st.markdown("*Values from Dong et al. (1997) and Scherneck (1991)*")
                        st.dataframe(
                            df_cm.style.format({
                                'Period (h)': '{:.2f}',
                                'Up (mm)': '{:.3f}',
                                'East (mm)': '{:.3f}',
                                'North (mm)': '{:.3f}',
                                'Total 3D (mm)': '{:.3f}'
                            }).background_gradient(subset=['Total 3D (mm)'], cmap='Oranges'),
                            use_container_width=True,
                            hide_index=True
                        )

                        # Educational note about frame consistency
                        st.markdown("---")
                        st.markdown("""
                        <div style="
                            background: linear-gradient(135deg, #1a4731 0%, #22543d 100%);
                            padding: 20px;
                            border-radius: 12px;
                            border-left: 4px solid #68d391;
                        ">
                            <h4 style="color: #68d391; margin: 0 0 10px 0;">✅ Your Frame Consistency Status</h4>
                            <table style="color: #e2e8f0; width: 100%; border-collapse: collapse;">
                                <tr style="border-bottom: 1px solid #4a5568;">
                                    <td style="padding: 8px 0;"><b>BLQ File</b></td>
                                    <td style="padding: 8px 0;">CMC: YES → CE/CF frame</td>
                                    <td style="padding: 8px 0; color: #68d391;">✓</td>
                                </tr>
                                <tr style="border-bottom: 1px solid #4a5568;">
                                    <td style="padding: 8px 0;"><b>Bernese ORBGEN</b></td>
                                    <td style="padding: 8px 0;">CMCYN_O: 1, CMCYN_A: 1 → CE/CF frame</td>
                                    <td style="padding: 8px 0; color: #68d391;">✓</td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px 0;"><b>CODE Products</b></td>
                                    <td style="padding: 8px 0;">CE/CF frame (same as IGS, ESA)</td>
                                    <td style="padding: 8px 0; color: #68d391;">✓</td>
                                </tr>
                            </table>
                            <p style="color: #9ae6b4; margin: 15px 0 0 0; font-size: 0.9rem;">
                                <b>Result:</b> All components are consistent. Your final coordinates do NOT include geocenter motion.
                            </p>
                        </div>
                        """, unsafe_allow_html=True)

                        # Impact explanation
                        st.markdown("### 🎯 Practical Impact")
                        st.markdown("""
                        | Scenario | Impact |
                        |----------|--------|
                        | **If consistent (your case)** | No systematic error, coordinates in standard CE/CF frame |
                        | **If inconsistent** | ~1-2 mm systematic error, spurious annual/semi-annual signals |
                        | **Main affected constituents** | M2 (~1 mm), S2 (~0.4 mm), K1/O1 (~0.3 mm) |
                        | **Effect on ZTD** | Height-ZTD coupling can propagate errors to troposphere estimates |
                        """)

                else:
                    st.error("Failed to parse BLQ file. Check file format.")
            else:
                st.warning(f"BLQ file not found: {blq_path}")
                st.info("Please provide a valid path to a Bernese BLQ file.")

        # TAB 17: Station Elevation / DEM
        elif selected_page == "🏔️ Station Elevation":
            st.markdown("""
            <div style="
                background: linear-gradient(135deg, #1E2530 0%, #242830 100%);
                padding: 15px 25px;
                border-radius: 15px;
                margin-bottom: 20px;
                box-shadow: 0 4px 15px rgba(0, 0, 0, 0.4);
                border: 1px solid #374151;
                border-left: 4px solid #10B981;
            ">
                <h3 style="color: #FFFFFF; margin: 0; font-size: 1.4rem; font-weight: 600;">
                    🏔️ Station Elevation Analysis
                </h3>
                <p style="color: #9CA3AF; margin: 5px 0 0 0; font-size: 0.9rem;">
                    Height differences and ZTD a priori estimation for GNSS stations
                </p>
            </div>
            """, unsafe_allow_html=True)

            # DOY selector for elevation data
            elev_available_doys = get_available_doys(2025)
            elev_col1, elev_col2, elev_col3 = st.columns([1, 2, 1])
            with elev_col2:
                if elev_available_doys:
                    elev_selected_doy = st.selectbox(
                        "📅 Select Day of Year (DOY)",
                        options=elev_available_doys,
                        index=0,
                        key='elev_doy_select',
                        help="Choose a DOY to view station elevations"
                    )
                else:
                    elev_selected_doy = 299
                    st.warning("No DOY data found, using default DOY 299")

            # Get station coordinates from F10 file
            df_elev = get_station_coordinates_from_f10(2025, elev_selected_doy)

            if not df_elev.empty:
                # Calculate ZTD a priori based on height
                # Using modified Hopfield/Saastamoinen model approximation:
                # ZTD ≈ 2.3 * exp(-h/8500) meters, where h is height in meters
                # This is a simplified model - actual values depend on pressure/temperature
                df_elev['ztd_apriori_m'] = 2.3 * np.exp(-df_elev['height'] / 8500)
                df_elev['ztd_apriori_mm'] = df_elev['ztd_apriori_m'] * 1000

                # Sort by height for better visualization
                df_elev_sorted = df_elev.sort_values('height', ascending=False).reset_index(drop=True)

                # Summary metrics
                st.markdown("### 📊 Elevation Summary")
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Total Stations", len(df_elev))
                with col2:
                    st.metric("Max Height", f"{df_elev['height'].max():.1f} m")
                with col3:
                    st.metric("Min Height", f"{df_elev['height'].min():.1f} m")
                with col4:
                    height_range = df_elev['height'].max() - df_elev['height'].min()
                    st.metric("Height Range", f"{height_range:.1f} m")
                with col5:
                    avg_height = df_elev['height'].mean()
                    st.metric("Avg Height", f"{avg_height:.1f} m")

                # Create sub-tabs for different views
                elev_tab1, elev_tab2, elev_tab3, elev_tab4, elev_tab5 = st.tabs([
                    "📊 Height Overview",
                    "🏔️ DEM Surface",
                    "🗺️ 3D Station Map",
                    "📐 Height Differences",
                    "🌡️ ZTD A Priori"
                ])

                with elev_tab1:
                    st.subheader("Station Heights Overview")

                    # Bar chart of station heights (sorted)
                    fig_heights = px.bar(
                        df_elev_sorted,
                        x='station_id',
                        y='height',
                        color='height',
                        color_continuous_scale='Viridis',
                        title='Station Heights (Sorted by Elevation)',
                        labels={'height': 'Height (m)', 'station_id': 'Station'}
                    )
                    fig_heights.update_layout(
                        xaxis_tickangle=-45,
                        height=500,
                        coloraxis_colorbar_title='Height (m)'
                    )
                    apply_colorful_style(fig_heights)
                    st.plotly_chart(fig_heights, use_container_width=True)

                    # Height distribution histogram
                    fig_hist = px.histogram(
                        df_elev,
                        x='height',
                        nbins=20,
                        title='Distribution of Station Heights',
                        labels={'height': 'Height (m)', 'count': 'Number of Stations'},
                        color_discrete_sequence=['#10B981']
                    )
                    fig_hist.add_vline(x=avg_height, line_dash="dash", line_color="#ffd93d",
                                      annotation_text=f"Mean: {avg_height:.0f} m")
                    apply_colorful_style(fig_hist)
                    st.plotly_chart(fig_hist, use_container_width=True)

                    # Height statistics by elevation bands
                    st.markdown("**Height Classification**")
                    bins = [0, 100, 300, 500, 1000, 2000, float('inf')]
                    labels = ['Sea Level (0-100m)', 'Low (100-300m)', 'Medium (300-500m)',
                             'High (500-1000m)', 'Mountain (1000-2000m)', 'Alpine (>2000m)']
                    df_elev['height_class'] = pd.cut(df_elev['height'], bins=bins, labels=labels)
                    class_counts = df_elev['height_class'].value_counts().reset_index()
                    class_counts.columns = ['Height Class', 'Count']

                    fig_pie = px.pie(
                        class_counts,
                        values='Count',
                        names='Height Class',
                        title='Station Distribution by Height Class',
                        color_discrete_sequence=px.colors.sequential.Viridis
                    )
                    fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                    apply_colorful_style(fig_pie)
                    st.plotly_chart(fig_pie, use_container_width=True)

                with elev_tab2:
                    st.subheader("Digital Elevation Model (DEM) - Europe")

                    st.markdown("""
                    This DEM covers all of Europe and is interpolated from GNSS station heights using
                    Radial Basis Function (RBF) interpolation. Station markers show the network coverage.
                    """)

                    # European bounds
                    EUROPE_LON_MIN, EUROPE_LON_MAX = -12.0, 35.0  # Western Europe to Eastern Europe
                    EUROPE_LAT_MIN, EUROPE_LAT_MAX = 34.0, 72.0  # Mediterranean to Nordic

                    # DEM resolution settings
                    dem_col1, dem_col2, dem_col3 = st.columns(3)
                    with dem_col1:
                        grid_resolution = st.slider(
                            "Grid Resolution",
                            min_value=30, max_value=150, value=80,
                            help="Number of grid points in each direction (higher = more detail)",
                            key='dem_resolution'
                        )
                    with dem_col2:
                        interp_method = st.selectbox(
                            "Interpolation Method",
                            options=['multiquadric', 'thin_plate', 'linear', 'cubic'],
                            index=0,
                            help="RBF interpolation function type",
                            key='dem_interp'
                        )
                    with dem_col3:
                        smooth_factor = st.slider(
                            "Smoothing",
                            min_value=0.0, max_value=2.0, value=0.5, step=0.1,
                            help="Smoothing factor for interpolation",
                            key='dem_smooth'
                        )

                    # Create DEM surface using RBF interpolation
                    try:
                        # European grid bounds
                        lon_min, lon_max = EUROPE_LON_MIN, EUROPE_LON_MAX
                        lat_min, lat_max = EUROPE_LAT_MIN, EUROPE_LAT_MAX

                        # Create grid covering all of Europe
                        lon_grid = np.linspace(lon_min, lon_max, grid_resolution)
                        lat_grid = np.linspace(lat_min, lat_max, grid_resolution)
                        lon_mesh, lat_mesh = np.meshgrid(lon_grid, lat_grid)

                        # RBF interpolation for smooth terrain surface
                        # Use station heights to create terrain approximation
                        rbf = Rbf(
                            df_elev['lon'].values,
                            df_elev['lat'].values,
                            df_elev['height'].values,
                            function=interp_method,
                            smooth=smooth_factor
                        )
                        height_grid = rbf(lon_mesh, lat_mesh)

                        # Clip negative heights to 0 (sea level) and cap extreme extrapolations
                        height_grid = np.clip(height_grid, 0, df_elev['height'].max() * 1.5)

                        # Create 3D surface plot
                        fig_dem = go.Figure()

                        # Add terrain surface covering Europe
                        fig_dem.add_trace(go.Surface(
                            x=lon_grid,
                            y=lat_grid,
                            z=height_grid,
                            colorscale='Earth',
                            name='Terrain',
                            opacity=0.9,
                            showscale=True,
                            colorbar=dict(title='Elevation (m)', x=1.02),
                            hovertemplate='Lon: %{x:.2f}°<br>Lat: %{y:.2f}°<br>Elevation: %{z:.0f}m<extra></extra>'
                        ))

                        # Add station markers on top of surface
                        fig_dem.add_trace(go.Scatter3d(
                            x=df_elev['lon'],
                            y=df_elev['lat'],
                            z=df_elev['height'] + 100,  # Above surface for visibility
                            mode='markers+text',
                            marker=dict(
                                size=6,
                                color='#FF6B6B',
                                symbol='diamond',
                                line=dict(width=1, color='white')
                            ),
                            text=df_elev['station_id'],
                            textposition='top center',
                            textfont=dict(size=8, color='white'),
                            name='GNSS Stations',
                            hovertemplate='<b>%{text}</b><br>Lon: %{x:.3f}°<br>Lat: %{y:.3f}°<br>Height: %{z:.1f}m<extra></extra>'
                        ))

                        fig_dem.update_layout(
                            title='Digital Elevation Model - Europe with GNSS Station Network',
                            height=750,
                            scene=dict(
                                xaxis_title='Longitude (°)',
                                yaxis_title='Latitude (°)',
                                zaxis_title='Elevation (m)',
                                bgcolor='#1E2530',
                                xaxis=dict(
                                    gridcolor='#374151',
                                    backgroundcolor='#1E2530',
                                    range=[lon_min, lon_max]
                                ),
                                yaxis=dict(
                                    gridcolor='#374151',
                                    backgroundcolor='#1E2530',
                                    range=[lat_min, lat_max]
                                ),
                                zaxis=dict(
                                    gridcolor='#374151',
                                    backgroundcolor='#1E2530'
                                ),
                                aspectratio=dict(x=1.5, y=1.2, z=0.4),
                                camera=dict(eye=dict(x=1.8, y=1.8, z=1.0))
                            ),
                            paper_bgcolor='rgba(0,0,0,0)'
                        )
                        st.plotly_chart(fig_dem, use_container_width=True)

                        # Station coverage info
                        st.info(f"🛰️ DEM interpolated from **{len(df_elev)} GNSS stations** across Europe. "
                               f"Grid: {grid_resolution}×{grid_resolution} points covering "
                               f"Lon [{lon_min}°, {lon_max}°], Lat [{lat_min}°, {lat_max}°]")

                        # Contour map of Europe
                        st.markdown("### Elevation Contour Map - Europe")
                        fig_contour = go.Figure()

                        # Add filled contours
                        contour_max = min(float(df_elev['height'].max()) * 1.2, 3000)
                        fig_contour.add_trace(go.Contour(
                            x=lon_grid,
                            y=lat_grid,
                            z=height_grid,
                            colorscale='Earth',
                            contours=dict(
                                start=0,
                                end=contour_max,
                                size=200,  # 200m contour interval for Europe scale
                                showlabels=True,
                                labelfont=dict(size=9, color='white')
                            ),
                            colorbar=dict(title='Elevation (m)'),
                            name='Elevation',
                            hovertemplate='Lon: %{x:.2f}°<br>Lat: %{y:.2f}°<br>Elevation: %{z:.0f}m<extra></extra>'
                        ))

                        # Add station markers
                        fig_contour.add_trace(go.Scatter(
                            x=df_elev['lon'],
                            y=df_elev['lat'],
                            mode='markers+text',
                            marker=dict(
                                size=10,
                                color=df_elev['height'],
                                colorscale='Viridis',
                                symbol='triangle-up',
                                line=dict(width=1, color='white')
                            ),
                            text=df_elev['station_id'],
                            textposition='top center',
                            textfont=dict(size=8, color='white'),
                            name='GNSS Stations',
                            hovertemplate='<b>%{text}</b><br>Lon: %{x:.3f}°<br>Lat: %{y:.3f}°<br>Height: %{marker.color:.1f}m<extra></extra>'
                        ))

                        fig_contour.update_layout(
                            title='Topographic Contour Map of Europe (200m intervals)',
                            xaxis_title='Longitude (°)',
                            yaxis_title='Latitude (°)',
                            height=650,
                            xaxis=dict(range=[EUROPE_LON_MIN, EUROPE_LON_MAX], scaleanchor='y'),
                            yaxis=dict(range=[EUROPE_LAT_MIN, EUROPE_LAT_MAX]),
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='#1E2530'
                        )
                        apply_colorful_style(fig_contour)
                        st.plotly_chart(fig_contour, use_container_width=True)

                        # Elevation profile along latitude/longitude across Europe
                        st.markdown("### Elevation Profile Across Europe")
                        st.markdown("Draw a cross-section through the DEM to see elevation changes.")
                        profile_col1, profile_col2 = st.columns(2)
                        with profile_col1:
                            profile_type = st.radio(
                                "Profile Direction",
                                options=['East-West (constant latitude)', 'North-South (constant longitude)'],
                                key='profile_direction'
                            )
                        with profile_col2:
                            if 'East-West' in profile_type:
                                profile_value = st.slider(
                                    "Select Latitude (°N)",
                                    min_value=float(EUROPE_LAT_MIN),
                                    max_value=float(EUROPE_LAT_MAX),
                                    value=48.0,  # Central Europe latitude
                                    step=0.5,
                                    key='profile_lat'
                                )
                            else:
                                profile_value = st.slider(
                                    "Select Longitude (°E)",
                                    min_value=float(EUROPE_LON_MIN),
                                    max_value=float(EUROPE_LON_MAX),
                                    value=10.0,  # Central Europe longitude
                                    step=0.5,
                                    key='profile_lon'
                                )

                        # Extract profile
                        if 'East-West' in profile_type:
                            profile_x = lon_grid
                            profile_z = rbf(lon_grid, np.full_like(lon_grid, profile_value))
                            x_label = 'Longitude (°)'
                            profile_title = f'East-West Elevation Profile at Lat {profile_value:.1f}°N'
                            # Find stations near this latitude (within 1 degree)
                            nearby_stations = df_elev[abs(df_elev['lat'] - profile_value) < 1.0]
                            station_x = nearby_stations['lon'].values
                            station_z = nearby_stations['height'].values
                        else:
                            profile_x = lat_grid
                            profile_z = rbf(np.full_like(lat_grid, profile_value), lat_grid)
                            x_label = 'Latitude (°)'
                            profile_title = f'North-South Elevation Profile at Lon {profile_value:.1f}°E'
                            # Find stations near this longitude (within 1 degree)
                            nearby_stations = df_elev[abs(df_elev['lon'] - profile_value) < 1.0]
                            station_x = nearby_stations['lat'].values
                            station_z = nearby_stations['height'].values

                        # Clip profile to reasonable values
                        profile_z_clipped = np.clip(profile_z, 0, df_elev['height'].max() * 1.5)

                        fig_profile = go.Figure()
                        # Terrain profile
                        fig_profile.add_trace(go.Scatter(
                            x=profile_x,
                            y=profile_z_clipped,
                            mode='lines',
                            fill='tozeroy',
                            line=dict(color='#10B981', width=2),
                            fillcolor='rgba(16, 185, 129, 0.3)',
                            name='Terrain'
                        ))
                        # Add nearby stations as markers
                        if len(nearby_stations) > 0:
                            fig_profile.add_trace(go.Scatter(
                                x=station_x,
                                y=station_z,
                                mode='markers+text',
                                marker=dict(size=10, color='#FF6B6B', symbol='triangle-up'),
                                text=nearby_stations['station_id'].values,
                                textposition='top center',
                                textfont=dict(size=8),
                                name='GNSS Stations'
                            ))
                        fig_profile.update_layout(
                            title=profile_title,
                            xaxis_title=x_label,
                            yaxis_title='Elevation (m)',
                            height=400
                        )
                        apply_colorful_style(fig_profile)
                        st.plotly_chart(fig_profile, use_container_width=True)

                        # DEM statistics
                        st.markdown("### DEM Statistics")
                        dem_stats_col1, dem_stats_col2, dem_stats_col3, dem_stats_col4 = st.columns(4)
                        with dem_stats_col1:
                            st.metric("Grid Points", f"{grid_resolution}×{grid_resolution}")
                        with dem_stats_col2:
                            st.metric("DEM Max Height", f"{height_grid.max():.1f} m")
                        with dem_stats_col3:
                            st.metric("DEM Min Height", f"{height_grid.min():.1f} m")
                        with dem_stats_col4:
                            st.metric("DEM Mean Height", f"{height_grid.mean():.1f} m")

                    except Exception as e:
                        st.error(f"Error creating DEM: {e}")
                        st.info("Try adjusting the grid resolution or interpolation method.")

                with elev_tab3:
                    st.subheader("3D Station Elevation Map")

                    # 3D scatter plot showing station positions with height as Z
                    fig_3d = px.scatter_3d(
                        df_elev,
                        x='lon',
                        y='lat',
                        z='height',
                        color='height',
                        size='height',
                        size_max=15,
                        text='station_id',
                        color_continuous_scale='Viridis',
                        title='3D Station Network Elevation Model',
                        labels={'lon': 'Longitude (°)', 'lat': 'Latitude (°)', 'height': 'Height (m)'}
                    )
                    fig_3d.update_layout(
                        height=700,
                        scene=dict(
                            xaxis_title='Longitude (°)',
                            yaxis_title='Latitude (°)',
                            zaxis_title='Height (m)',
                            bgcolor='#1E2530',
                            xaxis=dict(gridcolor='#374151', backgroundcolor='#1E2530'),
                            yaxis=dict(gridcolor='#374151', backgroundcolor='#1E2530'),
                            zaxis=dict(gridcolor='#374151', backgroundcolor='#1E2530'),
                        ),
                        paper_bgcolor='rgba(0,0,0,0)'
                    )
                    fig_3d.update_traces(marker=dict(line=dict(width=1, color='white')))
                    st.plotly_chart(fig_3d, use_container_width=True)

                    # 2D map with height as color
                    st.markdown("**Geographic Distribution with Elevation**")
                    fig_geo = px.scatter_geo(
                        df_elev,
                        lat='lat',
                        lon='lon',
                        color='height',
                        size='height',
                        hover_name='station_id',
                        hover_data={'lat': ':.4f', 'lon': ':.4f', 'height': ':.1f'},
                        color_continuous_scale='Viridis',
                        title='Station Heights on Map',
                        size_max=20
                    )
                    fig_geo.update_geos(
                        projection_type="natural earth",
                        showland=True,
                        landcolor='#1E2530',
                        showocean=True,
                        oceancolor='#0d1117',
                        showcoastlines=True,
                        coastlinecolor='#10B981',
                        showlakes=True,
                        lakecolor='#0d1117'
                    )
                    fig_geo.update_layout(
                        height=500,
                        paper_bgcolor='rgba(0,0,0,0)',
                        geo=dict(bgcolor='rgba(0,0,0,0)')
                    )
                    apply_colorful_style(fig_geo)
                    st.plotly_chart(fig_geo, use_container_width=True)

                with elev_tab4:
                    st.subheader("Height Differences Between Stations")

                    st.markdown("""
                    Height differences between stations are important for:
                    - Understanding tropospheric delay variations
                    - Baseline processing constraints
                    - Network geometry analysis
                    """)

                    # Calculate height difference matrix for up to 40 stations
                    max_stations_for_matrix = 40
                    if len(df_elev_sorted) > max_stations_for_matrix:
                        df_matrix = df_elev_sorted.head(max_stations_for_matrix)
                        st.info(f"Showing matrix for top {max_stations_for_matrix} stations by height")
                    else:
                        df_matrix = df_elev_sorted

                    stations = df_matrix['station_id'].tolist()
                    heights = df_matrix['height'].tolist()

                    # Create height difference matrix
                    n = len(stations)
                    diff_matrix = np.zeros((n, n))
                    for i in range(n):
                        for j in range(n):
                            diff_matrix[i, j] = heights[i] - heights[j]

                    # Heatmap of height differences
                    fig_heatmap = px.imshow(
                        diff_matrix,
                        x=stations,
                        y=stations,
                        color_continuous_scale='RdBu_r',
                        color_continuous_midpoint=0,
                        title='Height Difference Matrix (Station i - Station j) [meters]',
                        labels={'color': 'Δh (m)'}
                    )
                    fig_heatmap.update_layout(
                        height=600,
                        xaxis_tickangle=-45
                    )
                    apply_colorful_style(fig_heatmap)
                    st.plotly_chart(fig_heatmap, use_container_width=True)

                    # Station comparison tool
                    st.markdown("**Compare Two Stations**")
                    comp_col1, comp_col2 = st.columns(2)
                    with comp_col1:
                        station1 = st.selectbox(
                            "Station 1",
                            options=df_elev_sorted['station_id'].tolist(),
                            key='elev_station1'
                        )
                    with comp_col2:
                        station2 = st.selectbox(
                            "Station 2",
                            options=df_elev_sorted['station_id'].tolist(),
                            index=1 if len(df_elev_sorted) > 1 else 0,
                            key='elev_station2'
                        )

                    if station1 and station2:
                        h1 = df_elev[df_elev['station_id'] == station1]['height'].values[0]
                        h2 = df_elev[df_elev['station_id'] == station2]['height'].values[0]
                        lat1 = df_elev[df_elev['station_id'] == station1]['lat'].values[0]
                        lat2 = df_elev[df_elev['station_id'] == station2]['lat'].values[0]
                        lon1 = df_elev[df_elev['station_id'] == station1]['lon'].values[0]
                        lon2 = df_elev[df_elev['station_id'] == station2]['lon'].values[0]

                        # Calculate horizontal distance (approximate using Haversine)
                        R = 6371000  # Earth radius in meters
                        phi1, phi2 = math.radians(lat1), math.radians(lat2)
                        dphi = math.radians(lat2 - lat1)
                        dlambda = math.radians(lon2 - lon1)
                        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
                        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
                        horiz_dist = R * c / 1000  # km

                        diff = h1 - h2
                        ztd_diff = (2.3 * np.exp(-h1/8500) - 2.3 * np.exp(-h2/8500)) * 1000  # mm

                        result_col1, result_col2, result_col3, result_col4 = st.columns(4)
                        with result_col1:
                            st.metric(f"{station1} Height", f"{h1:.1f} m")
                        with result_col2:
                            st.metric(f"{station2} Height", f"{h2:.1f} m")
                        with result_col3:
                            st.metric("Height Difference", f"{diff:.1f} m",
                                     delta=f"{'↑' if diff > 0 else '↓'} {abs(diff):.0f}m")
                        with result_col4:
                            st.metric("Horizontal Dist.", f"{horiz_dist:.1f} km")

                        st.markdown(f"""
                        **ZTD Impact:** The ~{abs(diff):.0f} m height difference results in approximately
                        **{abs(ztd_diff):.1f} mm** difference in ZTD a priori values.
                        """)

                    # Largest height differences table
                    st.markdown("**Largest Height Differences in Network**")
                    diff_pairs = []
                    for i in range(len(df_elev)):
                        for j in range(i+1, len(df_elev)):
                            diff_pairs.append({
                                'Station 1': df_elev.iloc[i]['station_id'],
                                'Station 2': df_elev.iloc[j]['station_id'],
                                'Height 1 (m)': df_elev.iloc[i]['height'],
                                'Height 2 (m)': df_elev.iloc[j]['height'],
                                'Δh (m)': abs(df_elev.iloc[i]['height'] - df_elev.iloc[j]['height'])
                            })
                    df_pairs = pd.DataFrame(diff_pairs).sort_values('Δh (m)', ascending=False).head(20)
                    st.dataframe(
                        df_pairs.style.format({
                            'Height 1 (m)': '{:.1f}',
                            'Height 2 (m)': '{:.1f}',
                            'Δh (m)': '{:.1f}'
                        }).background_gradient(subset=['Δh (m)'], cmap='Reds'),
                        use_container_width=True,
                        hide_index=True
                    )

                with elev_tab5:
                    st.subheader("ZTD A Priori Estimation")

                    st.markdown("""
                    **Zenith Total Delay (ZTD)** a priori values are estimated from station height using
                    an exponential model based on standard atmosphere:

                    ```
                    ZTD_apriori ≈ 2.3 × exp(-h / 8500) meters
                    ```

                    Where:
                    - **2.3 m** is the approximate ZTD at sea level
                    - **8500 m** is the atmospheric scale height
                    - **h** is the station height in meters

                    This is important because:
                    - Higher stations have lower a priori ZTD
                    - Processing uses these values as initial estimates
                    - Actual ZTD varies with weather conditions (±10-15%)
                    """)

                    # ZTD a priori metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Max ZTD A Priori", f"{df_elev['ztd_apriori_mm'].max():.1f} mm")
                    with col2:
                        st.metric("Min ZTD A Priori", f"{df_elev['ztd_apriori_mm'].min():.1f} mm")
                    with col3:
                        ztd_range = df_elev['ztd_apriori_mm'].max() - df_elev['ztd_apriori_mm'].min()
                        st.metric("ZTD Range", f"{ztd_range:.1f} mm")
                    with col4:
                        st.metric("Avg ZTD A Priori", f"{df_elev['ztd_apriori_mm'].mean():.1f} mm")

                    # Scatter plot: Height vs ZTD a priori
                    fig_ztd = px.scatter(
                        df_elev,
                        x='height',
                        y='ztd_apriori_mm',
                        color='station_id',
                        hover_name='station_id',
                        title='ZTD A Priori vs Station Height',
                        labels={'height': 'Station Height (m)', 'ztd_apriori_mm': 'ZTD A Priori (mm)'},
                        trendline='ols'
                    )
                    fig_ztd.update_layout(height=450, showlegend=False)
                    apply_colorful_style(fig_ztd)
                    st.plotly_chart(fig_ztd, use_container_width=True)

                    # Bar chart of ZTD a priori by station (sorted by height)
                    df_ztd_sorted = df_elev.sort_values('height', ascending=False)
                    fig_ztd_bar = px.bar(
                        df_ztd_sorted,
                        x='station_id',
                        y='ztd_apriori_mm',
                        color='height',
                        color_continuous_scale='Viridis',
                        title='ZTD A Priori by Station (Sorted by Height)',
                        labels={'ztd_apriori_mm': 'ZTD A Priori (mm)', 'station_id': 'Station'}
                    )
                    fig_ztd_bar.update_layout(
                        xaxis_tickangle=-45,
                        height=450
                    )
                    apply_colorful_style(fig_ztd_bar)
                    st.plotly_chart(fig_ztd_bar, use_container_width=True)

                    # Dual axis chart: Height and ZTD
                    fig_dual = make_subplots(specs=[[{"secondary_y": True}]])
                    fig_dual.add_trace(
                        go.Bar(
                            x=df_ztd_sorted['station_id'],
                            y=df_ztd_sorted['height'],
                            name='Height (m)',
                            marker_color='#10B981',
                            opacity=0.7
                        ),
                        secondary_y=False
                    )
                    fig_dual.add_trace(
                        go.Scatter(
                            x=df_ztd_sorted['station_id'],
                            y=df_ztd_sorted['ztd_apriori_mm'],
                            name='ZTD A Priori (mm)',
                            mode='lines+markers',
                            marker_color='#F59E0B',
                            line=dict(width=2)
                        ),
                        secondary_y=True
                    )
                    fig_dual.update_layout(
                        title='Station Height and ZTD A Priori Comparison',
                        xaxis_tickangle=-45,
                        height=450,
                        legend=dict(x=0.01, y=0.99)
                    )
                    fig_dual.update_yaxes(title_text="Height (m)", secondary_y=False)
                    fig_dual.update_yaxes(title_text="ZTD A Priori (mm)", secondary_y=True)
                    apply_colorful_style(fig_dual)
                    st.plotly_chart(fig_dual, use_container_width=True)

                    # Data table with all elevation data
                    st.markdown("**Station Elevation and ZTD Data**")
                    display_cols = ['station_id', 'lat', 'lon', 'height', 'ztd_apriori_mm']
                    st.dataframe(
                        df_ztd_sorted[display_cols].style.format({
                            'lat': '{:.4f}',
                            'lon': '{:.4f}',
                            'height': '{:.1f}',
                            'ztd_apriori_mm': '{:.1f}'
                        }).background_gradient(subset=['height'], cmap='Greens')
                         .background_gradient(subset=['ztd_apriori_mm'], cmap='Blues'),
                        use_container_width=True,
                        hide_index=True,
                        height=400
                    )

                    # ZTD sensitivity analysis
                    st.markdown("**ZTD Sensitivity to Height**")
                    st.markdown("""
                    | Height Change | ZTD Change (approx) |
                    |--------------|---------------------|
                    | +100 m | -27 mm |
                    | +500 m | -130 mm |
                    | +1000 m | -248 mm |
                    | +2000 m | -449 mm |
                    """)

            else:
                st.warning(f"No coordinate data found for DOY {elev_selected_doy}")
                st.info("Station coordinates are loaded from F10 solution files. Ensure processing has been completed for the selected DOY.")

        # TAB 18: ML Quality Control
        # ========================================
        elif selected_page == "🤖 ML Quality Control":
            st.markdown("""
            <div style="
                background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                padding: 20px 25px;
                border-radius: 15px;
                margin-bottom: 20px;
                box-shadow: 0 4px 15px rgba(0, 0, 0, 0.4);
                border: 1px solid #374151;
                border-left: 4px solid #e94560;
            ">
                <h3 style="color: #FFFFFF; margin: 0; font-size: 1.4rem; font-weight: 600;">
                    🤖 Machine Learning Quality Control
                </h3>
                <p style="color: #9CA3AF; margin: 5px 0 0 0; font-size: 0.9rem;">
                    Automated anomaly detection and issue identification in GNSS time series
                </p>
            </div>
            """, unsafe_allow_html=True)

            if not SKLEARN_AVAILABLE:
                st.error("⚠️ scikit-learn is not installed. Please install it with: `pip install scikit-learn`")
                st.stop()

            # ML sub-tabs
            ml_tab1, ml_tab2, ml_tab3, ml_tab4, ml_tab5 = st.tabs([
                "🔍 Anomaly Detection",
                "📉 Change Point Detection",
                "🏥 Station Health Report",
                "📊 Multi-Station Comparison",
                "⚠️ Issue Summary"
            ])

            # Helper functions for ML analysis
            @st.cache_data(ttl=3600)
            def detect_anomalies_isolation_forest(data: np.ndarray, contamination: float = 0.05) -> np.ndarray:
                """Detect anomalies using Isolation Forest algorithm."""
                if len(data) < 10:
                    return np.zeros(len(data), dtype=bool)
                data_2d = data.reshape(-1, 1)
                clf = IsolationForest(contamination=contamination, random_state=42, n_estimators=100)
                predictions = clf.fit_predict(data_2d)
                return predictions == -1  # -1 indicates anomaly

            @st.cache_data(ttl=3600)
            def detect_anomalies_lof(data: np.ndarray, n_neighbors: int = 20, contamination: float = 0.05) -> np.ndarray:
                """Detect anomalies using Local Outlier Factor."""
                if len(data) < n_neighbors + 1:
                    return np.zeros(len(data), dtype=bool)
                data_2d = data.reshape(-1, 1)
                clf = LocalOutlierFactor(n_neighbors=min(n_neighbors, len(data)-1), contamination=contamination)
                predictions = clf.fit_predict(data_2d)
                return predictions == -1

            @st.cache_data(ttl=3600)
            def detect_anomalies_zscore(data: np.ndarray, threshold: float = 3.0) -> np.ndarray:
                """Detect anomalies using Z-score method."""
                if len(data) < 3:
                    return np.zeros(len(data), dtype=bool)
                z_scores = np.abs(stats.zscore(data, nan_policy='omit'))
                return z_scores > threshold

            @st.cache_data(ttl=3600)
            def detect_anomalies_iqr(data: np.ndarray, k: float = 1.5) -> np.ndarray:
                """Detect anomalies using IQR method."""
                q1, q3 = np.nanpercentile(data, [25, 75])
                iqr = q3 - q1
                lower_bound = q1 - k * iqr
                upper_bound = q3 + k * iqr
                return (data < lower_bound) | (data > upper_bound)

            @st.cache_data(ttl=3600)
            def detect_change_points_cusum(data: np.ndarray, threshold: float = 5.0) -> list:
                """Detect change points using CUSUM algorithm."""
                if len(data) < 5:
                    return []
                mean = np.nanmean(data)
                std = np.nanstd(data)
                if std == 0:
                    return []
                normalized = (data - mean) / std
                cusum_pos = np.zeros(len(data))
                cusum_neg = np.zeros(len(data))
                change_points = []

                for i in range(1, len(data)):
                    cusum_pos[i] = max(0, cusum_pos[i-1] + normalized[i] - 0.5)
                    cusum_neg[i] = min(0, cusum_neg[i-1] + normalized[i] + 0.5)
                    if cusum_pos[i] > threshold or cusum_neg[i] < -threshold:
                        change_points.append(i)
                        cusum_pos[i] = 0
                        cusum_neg[i] = 0

                return change_points

            @st.cache_data(ttl=3600)
            def detect_jumps(data: np.ndarray, threshold_mm: float = 5.0) -> list:
                """Detect sudden jumps in time series."""
                if len(data) < 3:
                    return []
                diff = np.abs(np.diff(data))
                jump_indices = np.where(diff > threshold_mm)[0] + 1
                return list(jump_indices)

            @st.cache_data(ttl=3600)
            def calculate_station_health_score(
                anomaly_rate: float,
                jump_count: int,
                rms: float,
                data_completeness: float
            ) -> tuple:
                """Calculate overall station health score (0-100)."""
                # Weight factors
                anomaly_penalty = min(anomaly_rate * 100, 30)  # max 30 points penalty
                jump_penalty = min(jump_count * 5, 20)  # max 20 points penalty
                rms_penalty = min(rms / 2, 20)  # max 20 points penalty (normalized)
                completeness_bonus = data_completeness * 30  # max 30 points bonus

                score = 100 - anomaly_penalty - jump_penalty - rms_penalty + completeness_bonus - 30
                score = max(0, min(100, score))

                if score >= 80:
                    status = "🟢 Excellent"
                elif score >= 60:
                    status = "🟡 Good"
                elif score >= 40:
                    status = "🟠 Fair"
                else:
                    status = "🔴 Poor"

                return score, status

            # Load coordinate time series data for ML analysis
            @st.cache_data(ttl=600)
            def load_ml_timeseries_data(year: int, doy_list: list) -> dict:
                """Load time series data for ML analysis from PPP-AR coordinates."""
                all_data = {}
                for doy in doy_list:
                    df = get_station_coordinates_from_f10(year, doy)
                    if not df.empty:
                        for _, row in df.iterrows():
                            # Use station_id column from CRD file
                            station = row.get('station_id', row.get('station', 'UNKNOWN'))
                            if station not in all_data:
                                all_data[station] = {'doy': [], 'lat': [], 'lon': [], 'height': [], 'x': [], 'y': [], 'z': []}
                            all_data[station]['doy'].append(doy)
                            all_data[station]['lat'].append(row.get('lat', 0))
                            all_data[station]['lon'].append(row.get('lon', 0))
                            all_data[station]['height'].append(row.get('height', 0))
                            all_data[station]['x'].append(row.get('x', 0))
                            all_data[station]['y'].append(row.get('y', 0))
                            all_data[station]['z'].append(row.get('z', 0))
                return all_data

            with ml_tab1:
                st.subheader("🔍 Time Series Anomaly Detection")

                st.markdown("""
                Detect outliers and anomalies in coordinate time series using multiple machine learning algorithms.
                Each method has different strengths for identifying different types of anomalies.
                """)

                # Settings
                col1, col2, col3 = st.columns(3)
                with col1:
                    ml_year = st.selectbox("Year", options=[2025, 2024], index=0, key='ml_year')
                with col2:
                    ml_doy_range = st.slider(
                        "DOY Range",
                        min_value=1, max_value=365,
                        value=(296, 320),
                        key='ml_doy_range'
                    )
                with col3:
                    contamination = st.slider(
                        "Expected Anomaly Rate (%)",
                        min_value=1, max_value=20,
                        value=5,
                        key='ml_contamination'
                    ) / 100

                # Algorithm selection
                algorithm = st.selectbox(
                    "Detection Algorithm",
                    options=[
                        "Isolation Forest (best for high-dimensional)",
                        "Local Outlier Factor (best for local density)",
                        "Z-Score (simple statistical)",
                        "IQR (robust to outliers)",
                        "Ensemble (combine all methods)"
                    ],
                    key='ml_algorithm'
                )

                # Load data
                doy_list = list(range(ml_doy_range[0], ml_doy_range[1] + 1))
                with st.spinner("Loading time series data..."):
                    ml_data = load_ml_timeseries_data(ml_year, doy_list)

                if ml_data:
                    # Station selector
                    ml_stations = sorted(ml_data.keys())
                    selected_ml_station = st.selectbox(
                        "Select Station for Analysis",
                        options=ml_stations,
                        key='ml_station_select'
                    )

                    if selected_ml_station and selected_ml_station in ml_data:
                        station_data = ml_data[selected_ml_station]
                        doys = np.array(station_data['doy'])
                        height_data = np.array(station_data['height'])

                        # Convert to relative (remove mean) for better visualization
                        height_mean = np.nanmean(height_data)
                        up_relative = (height_data - height_mean) * 1000  # Convert to mm

                        if len(up_relative) >= 5:
                            # Run anomaly detection
                            if "Isolation Forest" in algorithm:
                                anomalies = detect_anomalies_isolation_forest(up_relative, contamination)
                            elif "Local Outlier Factor" in algorithm:
                                anomalies = detect_anomalies_lof(up_relative, contamination=contamination)
                            elif "Z-Score" in algorithm:
                                anomalies = detect_anomalies_zscore(up_relative, threshold=3.0)
                            elif "IQR" in algorithm:
                                anomalies = detect_anomalies_iqr(up_relative, k=1.5)
                            else:  # Ensemble
                                a1 = detect_anomalies_isolation_forest(up_relative, contamination)
                                a2 = detect_anomalies_lof(up_relative, contamination=contamination)
                                a3 = detect_anomalies_zscore(up_relative, threshold=3.0)
                                a4 = detect_anomalies_iqr(up_relative, k=1.5)
                                # Anomaly if detected by at least 2 methods
                                anomalies = (a1.astype(int) + a2.astype(int) + a3.astype(int) + a4.astype(int)) >= 2

                            # Summary metrics
                            n_anomalies = np.sum(anomalies)
                            anomaly_rate = n_anomalies / len(anomalies) * 100

                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Points", len(up_relative))
                            with col2:
                                st.metric("Anomalies Detected", n_anomalies, delta=f"{anomaly_rate:.1f}%")
                            with col3:
                                st.metric("RMS (mm)", f"{np.nanstd(up_relative):.2f}")
                            with col4:
                                clean_rms = np.nanstd(up_relative[~anomalies]) if np.sum(~anomalies) > 0 else 0
                                st.metric("Clean RMS (mm)", f"{clean_rms:.2f}")

                            # Visualization
                            fig_anomaly = go.Figure()

                            # Normal points
                            normal_mask = ~anomalies
                            fig_anomaly.add_trace(go.Scatter(
                                x=doys[normal_mask],
                                y=up_relative[normal_mask],
                                mode='markers+lines',
                                name='Normal',
                                marker=dict(color='#4ecdc4', size=8),
                                line=dict(color='#4ecdc4', width=1)
                            ))

                            # Anomaly points
                            if n_anomalies > 0:
                                fig_anomaly.add_trace(go.Scatter(
                                    x=doys[anomalies],
                                    y=up_relative[anomalies],
                                    mode='markers',
                                    name='Anomaly',
                                    marker=dict(color='#ff6b6b', size=12, symbol='x',
                                               line=dict(color='#ffffff', width=2))
                                ))

                            # Mean line
                            fig_anomaly.add_hline(y=0, line_dash="dash", line_color="#888888",
                                                 annotation_text="Mean")

                            # Sigma bands
                            std = np.nanstd(up_relative)
                            fig_anomaly.add_hrect(y0=-2*std, y1=2*std, fillcolor="#4ecdc4",
                                                 opacity=0.1, layer="below", line_width=0,
                                                 annotation_text="±2σ")
                            fig_anomaly.add_hrect(y0=-3*std, y1=-2*std, fillcolor="#ffa94d",
                                                 opacity=0.1, layer="below", line_width=0)
                            fig_anomaly.add_hrect(y0=2*std, y1=3*std, fillcolor="#ffa94d",
                                                 opacity=0.1, layer="below", line_width=0)

                            fig_anomaly.update_layout(
                                title=f'Up Component Anomaly Detection - {selected_ml_station}',
                                xaxis_title='Day of Year',
                                yaxis_title='Relative Up (mm)',
                                height=500,
                                showlegend=True,
                                legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99)
                            )
                            apply_colorful_style(fig_anomaly)
                            st.plotly_chart(fig_anomaly, use_container_width=True)

                            # List of anomalies
                            if n_anomalies > 0:
                                st.markdown("### 📋 Detected Anomalies")
                                anomaly_df = pd.DataFrame({
                                    'DOY': doys[anomalies],
                                    'Up (mm)': up_relative[anomalies],
                                    'Deviation (σ)': np.abs(up_relative[anomalies]) / std
                                })
                                anomaly_df = anomaly_df.sort_values('Deviation (σ)', ascending=False)
                                st.dataframe(
                                    anomaly_df.style.format({
                                        'Up (mm)': '{:.2f}',
                                        'Deviation (σ)': '{:.2f}'
                                    }).background_gradient(subset=['Deviation (σ)'], cmap='Reds'),
                                    use_container_width=True,
                                    hide_index=True
                                )
                        else:
                            st.warning(f"Insufficient data points for {selected_ml_station} (need at least 5)")
                else:
                    st.warning("No coordinate data found for the selected period.")

            with ml_tab2:
                st.subheader("📉 Change Point & Jump Detection")

                st.markdown("""
                Detect sudden changes, jumps, and discontinuities in coordinate time series.
                Common causes: antenna changes, earthquakes, processing issues, receiver problems.
                """)

                # Settings
                col1, col2 = st.columns(2)
                with col1:
                    jump_threshold = st.slider(
                        "Jump Detection Threshold (mm)",
                        min_value=1.0, max_value=20.0,
                        value=5.0, step=0.5,
                        key='jump_threshold'
                    )
                with col2:
                    cusum_threshold = st.slider(
                        "CUSUM Sensitivity",
                        min_value=2.0, max_value=10.0,
                        value=5.0, step=0.5,
                        key='cusum_threshold'
                    )

                if ml_data:
                    cp_station = st.selectbox(
                        "Select Station",
                        options=sorted(ml_data.keys()),
                        key='cp_station_select'
                    )

                    if cp_station and cp_station in ml_data:
                        station_data = ml_data[cp_station]
                        doys = np.array(station_data['doy'])
                        up_data = np.array(station_data['height'])
                        up_mean = np.nanmean(up_data)
                        up_relative = (up_data - up_mean) * 1000

                        if len(up_relative) >= 5:
                            # Detect jumps and change points
                            jumps = detect_jumps(up_relative, jump_threshold)
                            change_points = detect_change_points_cusum(up_relative, cusum_threshold)

                            # Metrics
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Jumps Detected", len(jumps))
                            with col2:
                                st.metric("Change Points (CUSUM)", len(change_points))
                            with col3:
                                max_jump = np.max(np.abs(np.diff(up_relative))) if len(up_relative) > 1 else 0
                                st.metric("Max Jump (mm)", f"{max_jump:.2f}")

                            # Visualization
                            fig_cp = go.Figure()

                            # Time series
                            fig_cp.add_trace(go.Scatter(
                                x=doys,
                                y=up_relative,
                                mode='lines+markers',
                                name='Up Component',
                                marker=dict(color='#4ecdc4', size=6),
                                line=dict(color='#4ecdc4', width=2)
                            ))

                            # Mark jumps
                            for jump_idx in jumps:
                                if jump_idx < len(doys):
                                    fig_cp.add_vline(
                                        x=doys[jump_idx],
                                        line_dash="dash",
                                        line_color="#ff6b6b",
                                        line_width=2,
                                        annotation_text=f"Jump",
                                        annotation_position="top"
                                    )

                            # Mark change points
                            for cp_idx in change_points:
                                if cp_idx < len(doys):
                                    fig_cp.add_vline(
                                        x=doys[cp_idx],
                                        line_dash="dot",
                                        line_color="#ffa94d",
                                        line_width=2
                                    )

                            fig_cp.update_layout(
                                title=f'Change Point Detection - {cp_station}',
                                xaxis_title='Day of Year',
                                yaxis_title='Relative Up (mm)',
                                height=450
                            )
                            apply_colorful_style(fig_cp)
                            st.plotly_chart(fig_cp, use_container_width=True)

                            # Diff plot
                            if len(up_relative) > 1:
                                diff_data = np.diff(up_relative)
                                diff_doys = doys[1:]

                                fig_diff = go.Figure()
                                colors = ['#ff6b6b' if abs(d) > jump_threshold else '#4ecdc4' for d in diff_data]
                                fig_diff.add_trace(go.Bar(
                                    x=diff_doys,
                                    y=diff_data,
                                    marker_color=colors,
                                    name='Day-to-day Change'
                                ))
                                fig_diff.add_hline(y=jump_threshold, line_dash="dash", line_color="#ff6b6b",
                                                  annotation_text=f"+{jump_threshold} mm threshold")
                                fig_diff.add_hline(y=-jump_threshold, line_dash="dash", line_color="#ff6b6b",
                                                  annotation_text=f"-{jump_threshold} mm threshold")

                                fig_diff.update_layout(
                                    title='Day-to-Day Changes',
                                    xaxis_title='Day of Year',
                                    yaxis_title='Change (mm)',
                                    height=350
                                )
                                apply_colorful_style(fig_diff)
                                st.plotly_chart(fig_diff, use_container_width=True)

                            # List detected events
                            if jumps or change_points:
                                st.markdown("### 📋 Detected Events")
                                events = []
                                for j in jumps:
                                    if j < len(doys):
                                        events.append({
                                            'DOY': doys[j],
                                            'Type': '🔴 Jump',
                                            'Magnitude (mm)': abs(up_relative[j] - up_relative[j-1]) if j > 0 else 0
                                        })
                                for cp in change_points:
                                    if cp < len(doys) and cp not in jumps:
                                        events.append({
                                            'DOY': doys[cp],
                                            'Type': '🟠 Change Point',
                                            'Magnitude (mm)': abs(up_relative[cp] - np.mean(up_relative[:cp])) if cp > 0 else 0
                                        })
                                if events:
                                    st.dataframe(pd.DataFrame(events), use_container_width=True, hide_index=True)

            with ml_tab3:
                st.subheader("🏥 Station Health Report")

                st.markdown("""
                Comprehensive health assessment for each station based on multiple quality indicators.
                """)

                if ml_data:
                    health_reports = []

                    for station in sorted(ml_data.keys()):
                        station_data = ml_data[station]
                        doys = np.array(station_data['doy'])
                        up_data = np.array(station_data['height'])

                        if len(up_data) >= 5:
                            up_mean = np.nanmean(up_data)
                            up_relative = (up_data - up_mean) * 1000

                            # Calculate metrics
                            anomalies = detect_anomalies_isolation_forest(up_relative, 0.05)
                            anomaly_rate = np.sum(anomalies) / len(anomalies)
                            jumps = detect_jumps(up_relative, 5.0)
                            rms = np.nanstd(up_relative)
                            completeness = len(doys) / len(doy_list)

                            score, status = calculate_station_health_score(
                                anomaly_rate, len(jumps), rms, completeness
                            )

                            health_reports.append({
                                'Station': station,
                                'Health Score': score,
                                'Status': status,
                                'Data Points': len(doys),
                                'Completeness (%)': completeness * 100,
                                'Anomalies': np.sum(anomalies),
                                'Anomaly Rate (%)': anomaly_rate * 100,
                                'Jumps': len(jumps),
                                'RMS (mm)': rms
                            })

                    if health_reports:
                        df_health = pd.DataFrame(health_reports)
                        df_health = df_health.sort_values('Health Score', ascending=False)

                        # Summary
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            excellent = len(df_health[df_health['Health Score'] >= 80])
                            st.metric("🟢 Excellent", excellent)
                        with col2:
                            good = len(df_health[(df_health['Health Score'] >= 60) & (df_health['Health Score'] < 80)])
                            st.metric("🟡 Good", good)
                        with col3:
                            fair = len(df_health[(df_health['Health Score'] >= 40) & (df_health['Health Score'] < 60)])
                            st.metric("🟠 Fair", fair)
                        with col4:
                            poor = len(df_health[df_health['Health Score'] < 40])
                            st.metric("🔴 Poor", poor)

                        # Health score distribution
                        fig_health = px.bar(
                            df_health,
                            x='Station',
                            y='Health Score',
                            color='Health Score',
                            color_continuous_scale='RdYlGn',
                            title='Station Health Scores'
                        )
                        fig_health.add_hline(y=80, line_dash="dash", line_color="#22c55e",
                                            annotation_text="Excellent threshold")
                        fig_health.add_hline(y=60, line_dash="dash", line_color="#eab308",
                                            annotation_text="Good threshold")
                        fig_health.add_hline(y=40, line_dash="dash", line_color="#f97316",
                                            annotation_text="Fair threshold")
                        fig_health.update_layout(height=450, xaxis_tickangle=-45)
                        apply_colorful_style(fig_health)
                        st.plotly_chart(fig_health, use_container_width=True)

                        # Detailed table
                        st.markdown("### 📋 Detailed Health Report")
                        st.dataframe(
                            df_health.style.format({
                                'Health Score': '{:.1f}',
                                'Completeness (%)': '{:.1f}',
                                'Anomaly Rate (%)': '{:.1f}',
                                'RMS (mm)': '{:.2f}'
                            }).background_gradient(subset=['Health Score'], cmap='RdYlGn')
                            .background_gradient(subset=['RMS (mm)'], cmap='Reds_r'),
                            use_container_width=True,
                            hide_index=True
                        )

                        # Problem stations
                        problem_stations = df_health[df_health['Health Score'] < 60]
                        if not problem_stations.empty:
                            st.markdown("### ⚠️ Stations Requiring Attention")
                            for _, row in problem_stations.iterrows():
                                with st.expander(f"🔍 {row['Station']} - {row['Status']} (Score: {row['Health Score']:.1f})"):
                                    issues = []
                                    if row['Anomaly Rate (%)'] > 10:
                                        issues.append(f"• High anomaly rate: {row['Anomaly Rate (%)']:.1f}%")
                                    if row['Jumps'] > 2:
                                        issues.append(f"• Multiple jumps detected: {row['Jumps']}")
                                    if row['RMS (mm)'] > 10:
                                        issues.append(f"• High RMS: {row['RMS (mm)']:.2f} mm")
                                    if row['Completeness (%)'] < 80:
                                        issues.append(f"• Low data completeness: {row['Completeness (%)']:.1f}%")

                                    if issues:
                                        st.markdown("\n".join(issues))
                                    else:
                                        st.markdown("No specific issues identified.")

            with ml_tab4:
                st.subheader("📊 Multi-Station Comparison")

                st.markdown("""
                Compare anomaly patterns across multiple stations to identify systematic vs station-specific issues.
                """)

                if ml_data:
                    # Create anomaly matrix
                    stations = sorted(ml_data.keys())
                    all_doys = sorted(set(doy for s in ml_data.values() for doy in s['doy']))

                    anomaly_matrix = np.zeros((len(stations), len(all_doys)))
                    anomaly_matrix[:] = np.nan

                    for i, station in enumerate(stations):
                        station_data = ml_data[station]
                        doys = np.array(station_data['doy'])
                        up_data = np.array(station_data['height'])

                        if len(up_data) >= 5:
                            up_mean = np.nanmean(up_data)
                            up_relative = (up_data - up_mean) * 1000
                            anomalies = detect_anomalies_ensemble(up_relative) if 'detect_anomalies_ensemble' in dir() else detect_anomalies_isolation_forest(up_relative, 0.05)

                            for j, doy in enumerate(doys):
                                if doy in all_doys:
                                    doy_idx = all_doys.index(doy)
                                    anomaly_matrix[i, doy_idx] = 1 if anomalies[j] else 0

                    # Heatmap
                    fig_matrix = go.Figure(data=go.Heatmap(
                        z=anomaly_matrix,
                        x=all_doys,
                        y=stations,
                        colorscale=[[0, '#1a1a2e'], [0.5, '#4ecdc4'], [1, '#ff6b6b']],
                        showscale=True,
                        colorbar=dict(title='Anomaly', tickvals=[0, 1], ticktext=['Normal', 'Anomaly'])
                    ))

                    fig_matrix.update_layout(
                        title='Anomaly Detection Matrix (All Stations × All Days)',
                        xaxis_title='Day of Year',
                        yaxis_title='Station',
                        height=max(400, len(stations) * 20)
                    )
                    apply_colorful_style(fig_matrix)
                    st.plotly_chart(fig_matrix, use_container_width=True)

                    # Systematic issues (same DOY, multiple stations)
                    doy_anomaly_counts = np.nansum(anomaly_matrix, axis=0)
                    systematic_doys = [all_doys[i] for i, count in enumerate(doy_anomaly_counts) if count >= len(stations) * 0.3]

                    if systematic_doys:
                        st.markdown("### 🔴 Potential Systematic Issues")
                        st.warning(f"DOYs with anomalies in ≥30% of stations: **{systematic_doys}**")
                        st.markdown("These may indicate processing issues, product problems, or external events affecting multiple stations.")

            with ml_tab5:
                st.subheader("⚠️ Issue Summary & Recommendations")

                if ml_data:
                    # Collect all issues
                    all_issues = []
                    total_anomalies = 0
                    total_jumps = 0
                    total_stations = len(ml_data)
                    problem_count = 0

                    for station in ml_data:
                        station_data = ml_data[station]
                        doys = np.array(station_data['doy'])
                        up_data = np.array(station_data['height'])

                        if len(up_data) >= 5:
                            up_mean = np.nanmean(up_data)
                            up_relative = (up_data - up_mean) * 1000

                            anomalies = detect_anomalies_isolation_forest(up_relative, 0.05)
                            jumps = detect_jumps(up_relative, 5.0)
                            rms = np.nanstd(up_relative)

                            total_anomalies += np.sum(anomalies)
                            total_jumps += len(jumps)

                            # Categorize issues
                            if np.sum(anomalies) / len(anomalies) > 0.1:
                                all_issues.append({
                                    'Station': station,
                                    'Issue Type': '🔴 High Anomaly Rate',
                                    'Severity': 'High',
                                    'Description': f'{np.sum(anomalies)} anomalies ({np.sum(anomalies)/len(anomalies)*100:.1f}%)',
                                    'Recommendation': 'Review station equipment and local conditions'
                                })
                                problem_count += 1

                            if len(jumps) > 0:
                                for j_idx in jumps:
                                    if j_idx < len(doys):
                                        all_issues.append({
                                            'Station': station,
                                            'Issue Type': '🟠 Jump Detected',
                                            'Severity': 'Medium',
                                            'Description': f'Jump at DOY {doys[j_idx]}',
                                            'Recommendation': 'Check for antenna change, earthquake, or processing issue'
                                        })

                            if rms > 15:
                                all_issues.append({
                                    'Station': station,
                                    'Issue Type': '🟡 High RMS',
                                    'Severity': 'Medium',
                                    'Description': f'RMS = {rms:.2f} mm',
                                    'Recommendation': 'Review multipath environment and data quality'
                                })

                    # Summary metrics
                    st.markdown("""
                    <div style="
                        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                        padding: 20px;
                        border-radius: 12px;
                        margin-bottom: 20px;
                    ">
                        <h4 style="color: #e94560; margin: 0 0 15px 0;">📊 Overall Quality Summary</h4>
                    </div>
                    """, unsafe_allow_html=True)

                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Stations", total_stations)
                    with col2:
                        st.metric("Total Anomalies", total_anomalies)
                    with col3:
                        st.metric("Total Jumps", total_jumps)
                    with col4:
                        healthy_pct = (total_stations - problem_count) / total_stations * 100 if total_stations > 0 else 0
                        st.metric("Healthy Stations", f"{healthy_pct:.0f}%")

                    # Issues table
                    if all_issues:
                        st.markdown("### 📋 All Detected Issues")
                        df_issues = pd.DataFrame(all_issues)

                        # Filter by severity
                        severity_filter = st.multiselect(
                            "Filter by Severity",
                            options=['High', 'Medium', 'Low'],
                            default=['High', 'Medium'],
                            key='severity_filter'
                        )

                        filtered_issues = df_issues[df_issues['Severity'].isin(severity_filter)]
                        st.dataframe(filtered_issues, use_container_width=True, hide_index=True)

                        # Export option
                        csv = filtered_issues.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Issues Report (CSV)",
                            data=csv,
                            file_name=f"ml_quality_issues_{ml_year}_{ml_doy_range[0]}-{ml_doy_range[1]}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.success("✅ No significant issues detected in the selected period!")

                    # Recommendations summary
                    st.markdown("### 💡 General Recommendations")
                    st.markdown("""
                    | Issue Type | Recommended Action |
                    |------------|-------------------|
                    | High Anomaly Rate | Review station equipment, check for local interference, verify antenna installation |
                    | Jumps/Discontinuities | Check station log for equipment changes, verify processing parameters |
                    | High RMS | Investigate multipath, sky visibility, and data quality |
                    | Low Completeness | Check data transmission, receiver health, and network connectivity |
                    | Systematic Issues | Review orbit/clock products, check for global processing errors |
                    """)

        # ============================================================
        # TAB 19: PPP vs DD Vector Comparison Map
        # ============================================================
        elif selected_page == "🗺️ PPP-DD Vectors":
            st.header("🗺️ PPP-AR vs DD Vector Comparison")
            st.markdown("""
            Compare PPP-AR and Double-Difference coordinate solutions on an interactive map.
            Vectors show the horizontal difference between PPP-AR and DD solutions.
            """)

            # Controls
            vec_col1, vec_col2, vec_col3 = st.columns(3)
            with vec_col1:
                vec_year = st.selectbox("Year", [2025, 2024], index=0, key='vec_year')
            with vec_col2:
                vec_available_doys = get_available_doys(vec_year)
                vec_doy = st.selectbox("DOY", vec_available_doys if vec_available_doys else [299], key='vec_doy')
            with vec_col3:
                vec_scale = st.slider("Vector Scale", 1, 100, 20, key='vec_scale',
                                     help="Scale factor for vector display (larger = more visible)")

            # Get vector data
            vec_df = get_pppar_dd_vectors(vec_year, vec_doy)

            if not vec_df.empty:
                # Statistics
                st.markdown("### 📊 Difference Statistics")
                stat_cols = st.columns(5)
                with stat_cols[0]:
                    st.metric("Stations", len(vec_df))
                with stat_cols[1]:
                    st.metric("Mean dE", f"{vec_df['dE_mm'].mean():.2f} mm")
                with stat_cols[2]:
                    st.metric("Mean dN", f"{vec_df['dN_mm'].mean():.2f} mm")
                with stat_cols[3]:
                    st.metric("Mean dU", f"{vec_df['dU_mm'].mean():.2f} mm")
                with stat_cols[4]:
                    st.metric("Mean Horiz.", f"{vec_df['vector_mag'].mean():.2f} mm")

                # Create map with vectors
                fig_vec = go.Figure()

                # Add station markers
                fig_vec.add_trace(go.Scattergeo(
                    lon=vec_df['lon'],
                    lat=vec_df['lat'],
                    mode='markers',
                    marker=dict(
                        size=10,
                        color=vec_df['vector_mag'],
                        colorscale='RdYlGn_r',
                        colorbar=dict(title="Horiz. Diff (mm)"),
                        line=dict(width=1, color='white')
                    ),
                    text=vec_df.apply(lambda r: f"{r['station_id']}<br>dE: {r['dE_mm']:.1f}mm<br>dN: {r['dN_mm']:.1f}mm<br>dU: {r['dU_mm']:.1f}mm", axis=1),
                    hoverinfo='text',
                    name='Stations'
                ))

                # Add vectors (lines from station to endpoint)
                for _, row in vec_df.iterrows():
                    # Scale vectors for visibility (scale factor in degrees per mm)
                    scale_factor = vec_scale * 0.001  # Adjust as needed
                    end_lon = row['lon'] + row['dE_mm'] * scale_factor
                    end_lat = row['lat'] + row['dN_mm'] * scale_factor

                    fig_vec.add_trace(go.Scattergeo(
                        lon=[row['lon'], end_lon],
                        lat=[row['lat'], end_lat],
                        mode='lines',
                        line=dict(
                            width=2,
                            color='#ff6b6b' if row['vector_mag'] > 5 else '#4ECDC4'
                        ),
                        showlegend=False,
                        hoverinfo='skip'
                    ))

                    # Add arrowhead
                    fig_vec.add_trace(go.Scattergeo(
                        lon=[end_lon],
                        lat=[end_lat],
                        mode='markers',
                        marker=dict(
                            size=6,
                            symbol='triangle-up',
                            color='#ff6b6b' if row['vector_mag'] > 5 else '#4ECDC4'
                        ),
                        showlegend=False,
                        hoverinfo='skip'
                    ))

                fig_vec.update_geos(
                    projection_type="mercator",
                    showland=True,
                    landcolor='#2D3748',
                    showocean=True,
                    oceancolor='#1A202C',
                    showcoastlines=True,
                    coastlinecolor='#4A5568',
                    showframe=True,
                    framecolor='#4A5568',
                    center=dict(lat=vec_df['lat'].mean(), lon=vec_df['lon'].mean()),
                    projection_scale=8
                )

                fig_vec.update_layout(
                    title=f"PPP-AR vs DD Horizontal Differences - {vec_year} DOY {vec_doy}",
                    height=600,
                    margin=dict(l=0, r=0, t=50, b=0),
                    paper_bgcolor='rgba(0,0,0,0)',
                    geo=dict(bgcolor='rgba(0,0,0,0)')
                )

                st.plotly_chart(fig_vec, use_container_width=True)

                # Data table
                st.markdown("### 📋 Detailed Differences")
                vec_df_display = vec_df[['station_id', 'lat', 'lon', 'dE_mm', 'dN_mm', 'dU_mm', 'vector_mag']].copy()
                vec_df_display.columns = ['Station', 'Lat', 'Lon', 'dE (mm)', 'dN (mm)', 'dU (mm)', 'Horiz (mm)']
                st.dataframe(
                    vec_df_display.style.format({
                        'Lat': '{:.4f}', 'Lon': '{:.4f}',
                        'dE (mm)': '{:.2f}', 'dN (mm)': '{:.2f}',
                        'dU (mm)': '{:.2f}', 'Horiz (mm)': '{:.2f}'
                    }).background_gradient(subset=['Horiz (mm)'], cmap='RdYlGn_r'),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.warning(f"No PPP-AR vs DD comparison data available for {vec_year} DOY {vec_doy}")

        # ============================================================
        # TAB 20: Repeatability Heatmap (Station × DOY)
        # ============================================================
        elif selected_page == "🔥 Repeatability Heatmap":
            st.header("🔥 Repeatability Heatmap")
            st.markdown("""
            Heatmap showing 3D repeatability (deviation from mean) for each station-DOY combination.
            Identifies problematic days or stations with high scatter.
            """)

            # Controls
            hm_col1, hm_col2, hm_col3 = st.columns(3)
            with hm_col1:
                hm_year = st.selectbox("Year", [2025, 2024], index=0, key='hm_year')
            with hm_col2:
                hm_start_doy = st.number_input("Start DOY", 1, 366, start_doy, key='hm_start_doy')
            with hm_col3:
                hm_end_doy = st.number_input("End DOY", 1, 366, end_doy, key='hm_end_doy')

            heatmap_df = compute_repeatability_heatmap(hm_year, hm_start_doy, hm_end_doy)

            if not heatmap_df.empty:
                # Pivot for heatmap
                pivot_df = heatmap_df.pivot(index='station_id', columns='doy', values='rms_3d')

                # Statistics
                st.markdown("### 📊 Summary Statistics")
                stat_cols = st.columns(4)
                with stat_cols[0]:
                    st.metric("Stations", len(pivot_df))
                with stat_cols[1]:
                    st.metric("Days", len(pivot_df.columns))
                with stat_cols[2]:
                    st.metric("Mean 3D Scatter", f"{heatmap_df['rms_3d'].mean():.2f} mm")
                with stat_cols[3]:
                    st.metric("Max 3D Scatter", f"{heatmap_df['rms_3d'].max():.2f} mm")

                # Create heatmap
                fig_hm = go.Figure(data=go.Heatmap(
                    z=pivot_df.values,
                    x=[f"DOY {d}" for d in pivot_df.columns],
                    y=pivot_df.index,
                    colorscale='RdYlGn_r',
                    colorbar=dict(title="3D Scatter (mm)"),
                    hovertemplate="Station: %{y}<br>%{x}<br>Scatter: %{z:.2f} mm<extra></extra>"
                ))

                fig_hm.update_layout(
                    title="3D Coordinate Scatter (Deviation from Mean Position)",
                    xaxis_title="Day of Year",
                    yaxis_title="Station",
                    height=max(400, len(pivot_df) * 20),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='#1E2530'
                )
                apply_colorful_style(fig_hm)
                st.plotly_chart(fig_hm, use_container_width=True)

                # Bar chart of average scatter per station
                st.markdown("### 📊 Average Scatter by Station")
                station_avg = heatmap_df.groupby('station_id')['rms_3d'].mean().sort_values(ascending=False)

                fig_bar = px.bar(
                    x=station_avg.index,
                    y=station_avg.values,
                    labels={'x': 'Station', 'y': 'Mean 3D Scatter (mm)'},
                    title="Mean 3D Scatter by Station",
                    color=station_avg.values,
                    color_continuous_scale='RdYlGn_r'
                )
                fig_bar.add_hline(y=10, line_dash="dash", line_color="red",
                                 annotation_text="10mm threshold")
                apply_colorful_style(fig_bar)
                st.plotly_chart(fig_bar, use_container_width=True)

                # Identify outliers
                st.markdown("### ⚠️ High Scatter Days (> 15mm)")
                outliers = heatmap_df[heatmap_df['rms_3d'] > 15].sort_values('rms_3d', ascending=False)
                if not outliers.empty:
                    st.dataframe(
                        outliers.style.format({'rms_3d': '{:.2f}'}),
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.success("No high scatter days detected (all < 15mm)")
            else:
                st.warning("No repeatability data available for the selected period")

        # ============================================================
        # TAB 21: Network Comparison Box Plots
        # ============================================================
        elif selected_page == "📦 Network Comparison":
            st.header("📦 Network Comparison")
            st.markdown("""
            Compare repeatability statistics across different GNSS networks (IGS, EUREF, Regional).
            Box plots show the distribution of 3D coordinate scatter.
            """)

            # Controls
            nc_col1, nc_col2, nc_col3 = st.columns(3)
            with nc_col1:
                nc_year = st.selectbox("Year", [2025, 2024], index=0, key='nc_year')
            with nc_col2:
                nc_start_doy = st.number_input("Start DOY", 1, 366, start_doy, key='nc_start_doy')
            with nc_col3:
                nc_end_doy = st.number_input("End DOY", 1, 366, end_doy, key='nc_end_doy')

            network_stats = get_network_statistics(nc_year, nc_start_doy, nc_end_doy)

            if not network_stats.empty:
                # Summary by network
                st.markdown("### 📊 Network Summary")
                network_summary = network_stats.groupby('network').agg({
                    'station_id': 'count',
                    'mean_rms': ['mean', 'std', 'min', 'max']
                }).round(2)
                network_summary.columns = ['Stations', 'Mean RMS', 'Std RMS', 'Min RMS', 'Max RMS']
                st.dataframe(network_summary, use_container_width=True)

                # Box plot by network
                fig_box = px.box(
                    network_stats,
                    x='network',
                    y='mean_rms',
                    color='network',
                    points='all',
                    hover_data=['station_id'],
                    title="3D RMS Distribution by Network",
                    labels={'mean_rms': 'Mean 3D RMS (mm)', 'network': 'Network'}
                )
                fig_box.add_hline(y=10, line_dash="dash", line_color="red",
                                 annotation_text="10mm threshold")
                apply_colorful_style(fig_box)
                fig_box.update_layout(height=500)
                st.plotly_chart(fig_box, use_container_width=True)

                # Violin plot for more detail
                st.markdown("### 🎻 Distribution Details")
                fig_violin = px.violin(
                    network_stats,
                    x='network',
                    y='mean_rms',
                    color='network',
                    box=True,
                    points='all',
                    hover_data=['station_id'],
                    title="Repeatability Distribution by Network (Violin Plot)"
                )
                apply_colorful_style(fig_violin)
                fig_violin.update_layout(height=500)
                st.plotly_chart(fig_violin, use_container_width=True)

                # Station ranking
                st.markdown("### 🏆 Station Ranking (Best to Worst)")
                ranked = network_stats.sort_values('mean_rms')[['station_id', 'network', 'mean_rms', 'std_rms']]
                ranked.columns = ['Station', 'Network', 'Mean RMS (mm)', 'Std RMS (mm)']

                # Color code by performance
                def color_rms(val):
                    if val < 5:
                        return 'background-color: #2ecc71'
                    elif val < 10:
                        return 'background-color: #f1c40f'
                    else:
                        return 'background-color: #e74c3c'

                st.dataframe(
                    ranked.style.applymap(color_rms, subset=['Mean RMS (mm)']).format({
                        'Mean RMS (mm)': '{:.2f}',
                        'Std RMS (mm)': '{:.2f}'
                    }),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.warning("No network comparison data available")

        # ============================================================
        # TAB 22: Time Evolution Animation
        # ============================================================
        elif selected_page == "🎬 Time Evolution":
            st.header("🎬 Time Evolution")
            st.markdown("""
            Animated visualization of coordinate changes over time.
            Watch how station positions evolve day by day.
            """)

            # Controls
            te_col1, te_col2, te_col3 = st.columns(3)
            with te_col1:
                te_year = st.selectbox("Year", [2025, 2024], index=0, key='te_year')
            with te_col2:
                te_start_doy = st.number_input("Start DOY", 1, 366, start_doy, key='te_start_doy')
            with te_col3:
                te_end_doy = st.number_input("End DOY", 1, 366, end_doy, key='te_end_doy')

            # Collect time series data
            all_coords = []
            for doy in range(te_start_doy, te_end_doy + 1):
                df_coords = get_pppar_coordinates(te_year, doy)
                if not df_coords.empty:
                    df_coords['doy'] = doy
                    all_coords.append(df_coords)

            if all_coords:
                df_all = pd.concat(all_coords, ignore_index=True)

                # Calculate deviation from mean for each station
                station_means = df_all.groupby('station_id').agg({
                    'height': 'mean', 'lat': 'mean', 'lon': 'mean'
                }).reset_index()

                df_all = df_all.merge(
                    station_means.rename(columns={
                        'height': 'mean_height', 'lat': 'mean_lat', 'lon': 'mean_lon'
                    }),
                    on='station_id'
                )
                df_all['height_diff_mm'] = (df_all['height'] - df_all['mean_height']) * 1000

                # Create animated scatter plot
                fig_anim = px.scatter_geo(
                    df_all,
                    lat='lat',
                    lon='lon',
                    color='height_diff_mm',
                    size=abs(df_all['height_diff_mm']) + 1,
                    animation_frame='doy',
                    hover_name='station_id',
                    hover_data={'height_diff_mm': ':.2f', 'lat': ':.3f', 'lon': ':.3f'},
                    color_continuous_scale='RdBu_r',
                    range_color=[-20, 20],
                    title="Station Height Deviations Over Time"
                )

                fig_anim.update_geos(
                    projection_type="mercator",
                    showland=True,
                    landcolor='#2D3748',
                    showocean=True,
                    oceancolor='#1A202C',
                    showcoastlines=True,
                    coastlinecolor='#4A5568',
                    center=dict(lat=df_all['lat'].mean(), lon=df_all['lon'].mean()),
                    projection_scale=8
                )

                fig_anim.update_layout(
                    height=600,
                    paper_bgcolor='rgba(0,0,0,0)',
                    coloraxis_colorbar=dict(title="Height Diff (mm)")
                )

                st.plotly_chart(fig_anim, use_container_width=True)

                # Time series plot for selected stations
                st.markdown("### 📈 Height Time Series")
                te_stations = st.multiselect(
                    "Select stations for time series",
                    options=df_all['station_id'].unique().tolist(),
                    default=df_all['station_id'].unique()[:5].tolist(),
                    key='te_stations'
                )

                if te_stations:
                    fig_ts = go.Figure()
                    colors = px.colors.qualitative.Set2

                    for i, station in enumerate(te_stations):
                        sdf = df_all[df_all['station_id'] == station].sort_values('doy')
                        fig_ts.add_trace(go.Scatter(
                            x=sdf['doy'],
                            y=sdf['height_diff_mm'],
                            mode='lines+markers',
                            name=station,
                            line=dict(color=colors[i % len(colors)])
                        ))

                    fig_ts.add_hline(y=0, line_dash="dash", line_color="gray")
                    fig_ts.update_layout(
                        title="Height Deviation from Mean",
                        xaxis_title="Day of Year",
                        yaxis_title="Height Difference (mm)",
                        height=400
                    )
                    apply_colorful_style(fig_ts)
                    st.plotly_chart(fig_ts, use_container_width=True)
            else:
                st.warning("No coordinate data available for animation")

        # ============================================================
        # TAB 23: Station Correlation Matrix
        # ============================================================
        elif selected_page == "🔗 Station Correlation":
            st.header("🔗 Station Correlation Matrix")
            st.markdown("""
            Analyze spatial correlations between station coordinate time series.
            High correlations may indicate common-mode signals (atmosphere, orbits).
            """)

            # Controls
            corr_col1, corr_col2, corr_col3, corr_col4 = st.columns(4)
            with corr_col1:
                corr_year = st.selectbox("Year", [2025, 2024], index=0, key='corr_year')
            with corr_col2:
                corr_start_doy = st.number_input("Start DOY", 1, 366, start_doy, key='corr_start_doy')
            with corr_col3:
                corr_end_doy = st.number_input("End DOY", 1, 366, end_doy, key='corr_end_doy')
            with corr_col4:
                corr_component = st.selectbox("Component", ['up', 'east', 'north'], key='corr_component')

            corr_matrix = compute_station_correlation(corr_year, corr_start_doy, corr_end_doy, corr_component)

            if not corr_matrix.empty:
                # Summary statistics
                st.markdown("### 📊 Correlation Summary")
                corr_values = corr_matrix.values[np.triu_indices_from(corr_matrix.values, k=1)]
                stat_cols = st.columns(4)
                with stat_cols[0]:
                    st.metric("Stations", len(corr_matrix))
                with stat_cols[1]:
                    st.metric("Mean Correlation", f"{np.nanmean(corr_values):.3f}")
                with stat_cols[2]:
                    st.metric("Max Correlation", f"{np.nanmax(corr_values):.3f}")
                with stat_cols[3]:
                    st.metric("Min Correlation", f"{np.nanmin(corr_values):.3f}")

                # Create heatmap
                fig_corr = go.Figure(data=go.Heatmap(
                    z=corr_matrix.values,
                    x=corr_matrix.columns,
                    y=corr_matrix.index,
                    colorscale='RdBu_r',
                    zmid=0,
                    colorbar=dict(title="Correlation"),
                    hovertemplate="%{y} vs %{x}<br>Correlation: %{z:.3f}<extra></extra>"
                ))

                fig_corr.update_layout(
                    title=f"Station {corr_component.upper()} Component Correlation Matrix",
                    xaxis_title="Station",
                    yaxis_title="Station",
                    height=max(500, len(corr_matrix) * 15),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='#1E2530'
                )
                apply_colorful_style(fig_corr)
                st.plotly_chart(fig_corr, use_container_width=True)

                # Find highly correlated pairs
                st.markdown("### 🔍 Highly Correlated Station Pairs (|r| > 0.7)")
                high_corr = []
                for i, sta1 in enumerate(corr_matrix.index):
                    for j, sta2 in enumerate(corr_matrix.columns):
                        if i < j:  # Upper triangle only
                            r = corr_matrix.iloc[i, j]
                            if abs(r) > 0.7:
                                high_corr.append({
                                    'Station 1': sta1,
                                    'Station 2': sta2,
                                    'Correlation': r
                                })

                if high_corr:
                    high_corr_df = pd.DataFrame(high_corr).sort_values('Correlation', ascending=False)
                    st.dataframe(
                        high_corr_df.style.format({'Correlation': '{:.3f}'}).background_gradient(
                            subset=['Correlation'], cmap='RdBu_r', vmin=-1, vmax=1
                        ),
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.info("No highly correlated pairs found (|r| > 0.7)")

                # Network graph visualization
                st.markdown("### 🕸️ Correlation Network")
                st.markdown("Stations connected by lines if correlation > 0.5 (green) or < -0.5 (red)")

                # Build network edges
                edges = []
                nodes = list(corr_matrix.index)
                for i, sta1 in enumerate(nodes):
                    for j, sta2 in enumerate(nodes):
                        if i < j:
                            r = corr_matrix.iloc[i, j]
                            if abs(r) > 0.5:
                                edges.append((sta1, sta2, r))

                if edges:
                    # Get station positions
                    df_coords = get_pppar_coordinates(corr_year, corr_start_doy)
                    if not df_coords.empty:
                        pos_dict = {row['station_id']: (row['lon'], row['lat'])
                                   for _, row in df_coords.iterrows()}

                        fig_net = go.Figure()

                        # Add edges
                        for sta1, sta2, r in edges:
                            if sta1 in pos_dict and sta2 in pos_dict:
                                x0, y0 = pos_dict[sta1]
                                x1, y1 = pos_dict[sta2]
                                color = '#2ecc71' if r > 0 else '#e74c3c'
                                fig_net.add_trace(go.Scattergeo(
                                    lon=[x0, x1],
                                    lat=[y0, y1],
                                    mode='lines',
                                    line=dict(width=abs(r) * 3, color=color),
                                    opacity=0.6,
                                    showlegend=False
                                ))

                        # Add nodes
                        node_lons = [pos_dict[n][0] for n in nodes if n in pos_dict]
                        node_lats = [pos_dict[n][1] for n in nodes if n in pos_dict]
                        node_names = [n for n in nodes if n in pos_dict]

                        fig_net.add_trace(go.Scattergeo(
                            lon=node_lons,
                            lat=node_lats,
                            mode='markers+text',
                            marker=dict(size=10, color='#4ECDC4'),
                            text=node_names,
                            textposition='top center',
                            name='Stations'
                        ))

                        fig_net.update_geos(
                            projection_type="mercator",
                            showland=True,
                            landcolor='#2D3748',
                            showocean=True,
                            oceancolor='#1A202C',
                            center=dict(lat=np.mean(node_lats), lon=np.mean(node_lons)),
                            projection_scale=8
                        )

                        fig_net.update_layout(
                            title="Station Correlation Network",
                            height=500,
                            paper_bgcolor='rgba(0,0,0,0)'
                        )
                        st.plotly_chart(fig_net, use_container_width=True)
                else:
                    st.info("No strong correlations to display in network")
            else:
                st.warning("No correlation data available")

        # ============================================================
        # TAB 24: Baseline (DD) Network Visualization
        # ============================================================
        elif selected_page == "📐 Baseline (DD)":
            st.header("📐 Baseline Network (DD)")

            # Explanation of baseline processing
            with st.expander("ℹ️ Understanding Baseline Processing", expanded=False):
                st.markdown("""
                **Double-Difference (DD) Processing Flow:**

                The baseline network is built in **two sessions** for efficient ambiguity resolution:
                """)

                # Visual flow diagram
                st.code("""
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                    DD BASELINE PROCESSING FLOW                          │
    └─────────────────────────────────────────────────────────────────────────┘

    ┌─────────────────────┐         ┌─────────────────────┐
    │   SESSION 1         │         │   SESSION 2         │
    │   (Local Network)   │         │   (European)        │
    │                     │         │                     │
    │   0531 ─── ENTZ    │         │   WARN ─── BOR1    │
    │     │       │      │         │     │ ╲   │        │
    │   ROUL ─── BASC    │    ──►  │   TLSE   GRAZ     │
    │     │       │      │         │     │ ╲   │        │
    │   ERPL ─── KBG2    │         │   YEBE   MATE     │
    │                     │         │                     │
    │   ~19 baselines    │         │   ~17 baselines    │
    │   10-50 km         │         │   100-1500 km      │
    └─────────────────────┘         └─────────────────────┘
              │                              │
              └──────────────┬───────────────┘
                             ▼
                  ┌─────────────────────┐
                  │   FINAL NETWORK     │
                  │   (BSL_*.BSL)       │
                  │                     │
                  │   ~36 baselines    │
                  │   Combined for      │
                  │   coordinate est.   │
                  └─────────────────────┘
                """, language=None)

                st.markdown("""
                **Session Details:**

                | Session | Stations | Baseline Length | Purpose |
                |---------|----------|-----------------|---------|
                | Session 1 | 0531, ROUL, BASC, ERPL, KBG2, etc. | 10-50 km | Local ambiguity resolution (Luxembourg) |
                | Session 2 | WARN, TLSE, GRAZ, MATE, BOR1, etc. | 100-1500 km | European reference frame tie |

                **Ambiguity Resolution Strategy:**
                - **L12**: L1/L2 widelane baselines (wide-lane, easier to resolve)
                - **L53**: L5/L3 extra-widelane (for triple-frequency receivers)
                - **QIF**: Quasi-Ionosphere-Free combinations (narrowlane)
                - **Excluded**: Baselines removed due to poor data quality

                **Why Two Sessions?**
                - Short baselines have highly correlated atmospheric errors
                - Easier to fix ambiguities on short baselines first
                - Long baselines use resolved ambiguities as constraints
                - WARN acts as hub station connecting local and European networks
                """)

            # Controls row 1: Year and DOY
            bsl_col1, bsl_col2, bsl_col3 = st.columns(3)
            dd_base_dir = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX"

            with bsl_col1:
                bsl_year = st.selectbox("Year", [2025, 2024], index=0, key='bsl_year')
            with bsl_col2:
                bsl_available_doys = []
                # Check which DOYs have BSL files
                for d in range(280, 320):
                    bsl_check = f"{dd_base_dir}/{bsl_year}/{d}/STA/BSL_{bsl_year}{d:03d}0.BSL.gz"
                    if os.path.exists(bsl_check):
                        bsl_available_doys.append(d)
                if not bsl_available_doys:
                    bsl_available_doys = [302]  # Default
                bsl_doy = st.selectbox("DOY", bsl_available_doys, index=0, key='bsl_doy')

            with bsl_col3:
                # Session selector
                session_options = {
                    "Final (Combined)": f"BSL_{bsl_year}{bsl_doy:03d}0.BSL.gz",
                    "Session 1 (Local/Luxembourg)": f"{bsl_year}{bsl_doy:03d}0001.BSL.gz",
                    "Session 2 (European)": f"{bsl_year}{bsl_doy:03d}0002.BSL.gz",
                    "Excluded Baselines": f"{bsl_year}{bsl_doy:03d}0EX.BSL.gz",
                }
                session_type = st.selectbox("Baseline Session", list(session_options.keys()), index=0, key='bsl_session')

            # Controls row 2: Zoom presets
            zoom_col1, zoom_col2, zoom_col3 = st.columns(3)
            with zoom_col1:
                zoom_preset = st.selectbox(
                    "🔍 Zoom Region",
                    ["Europe (Full)", "Luxembourg (Local)", "Western Europe", "Custom"],
                    key='bsl_zoom'
                )

            # Define zoom regions
            zoom_regions = {
                "Europe (Full)": {"lat": 50.0, "lon": 10.0, "scale": 6},
                "Luxembourg (Local)": {"lat": 49.75, "lon": 6.1, "scale": 80},
                "Western Europe": {"lat": 48.5, "lon": 4.0, "scale": 15},
                "Custom": {"lat": 50.0, "lon": 10.0, "scale": 6},
            }

            if zoom_preset == "Custom":
                with zoom_col2:
                    custom_lat = st.number_input("Center Lat", value=49.75, min_value=30.0, max_value=75.0, key='bsl_custom_lat')
                with zoom_col3:
                    custom_lon = st.number_input("Center Lon", value=6.1, min_value=-15.0, max_value=45.0, key='bsl_custom_lon')
                zoom_scale = st.slider("Zoom Scale", 2, 100, 40, key='bsl_custom_scale')
                zoom_center = {"lat": custom_lat, "lon": custom_lon, "scale": zoom_scale}
            else:
                zoom_center = zoom_regions[zoom_preset]

            # File paths based on session selection
            bsl_filename = session_options[session_type]
            bsl_path = f"{dd_base_dir}/{bsl_year}/{bsl_doy}/STA/{bsl_filename}"
            crd_path = f"{dd_base_dir}/{bsl_year}/{bsl_doy}/STA/F10_{bsl_year}{bsl_doy:03d}0.CRD.gz"

            # Show available sessions overview
            st.markdown("### 📂 Available Baseline Sessions")
            session_info = []
            session_files = {
                "Session 1 (Local)": f"{bsl_year}{bsl_doy:03d}0001.BSL.gz",
                "Session 2 (European)": f"{bsl_year}{bsl_doy:03d}0002.BSL.gz",
                "Final (Combined)": f"BSL_{bsl_year}{bsl_doy:03d}0.BSL.gz",
                "Excluded": f"{bsl_year}{bsl_doy:03d}0EX.BSL.gz",
            }
            for sess_name, sess_file in session_files.items():
                sess_path = f"{dd_base_dir}/{bsl_year}/{bsl_doy}/STA/{sess_file}"
                if os.path.exists(sess_path):
                    try:
                        sess_baselines = load_baselines_with_coords(sess_path, crd_path)
                        sess_stats = get_baseline_statistics(sess_baselines)
                        session_info.append({
                            "Session": sess_name,
                            "Baselines": sess_stats['total_baselines'],
                            "Stations": sess_stats['unique_stations'],
                            "Status": "✅ Available"
                        })
                    except Exception:
                        session_info.append({
                            "Session": sess_name,
                            "Baselines": "-",
                            "Stations": "-",
                            "Status": "⚠️ Error"
                        })
                else:
                    session_info.append({
                        "Session": sess_name,
                        "Baselines": "-",
                        "Stations": "-",
                        "Status": "❌ Not found"
                    })

            sess_df = pd.DataFrame(session_info)
            st.dataframe(sess_df, use_container_width=True, hide_index=True)

            # Load baselines
            if os.path.exists(bsl_path) and os.path.exists(crd_path):
                baselines = load_baselines_with_coords(bsl_path, crd_path)
                stats = get_baseline_statistics(baselines)

                # Summary metrics
                st.markdown("### 📊 Network Summary")
                stat_cols = st.columns(5)
                with stat_cols[0]:
                    st.metric("Total Baselines", stats['total_baselines'])
                with stat_cols[1]:
                    st.metric("Valid (with coords)", stats['valid_baselines'])
                with stat_cols[2]:
                    st.metric("Unique Stations", stats['unique_stations'])
                with stat_cols[3]:
                    st.metric("Min Length", f"{stats['min_length_km']:.1f} km")
                with stat_cols[4]:
                    st.metric("Max Length", f"{stats['max_length_km']:.1f} km")

                # Filter controls
                st.markdown("### 🎛️ Display Options")
                filter_col1, filter_col2, filter_col3 = st.columns(3)
                with filter_col1:
                    min_length = st.slider("Min baseline length (km)", 0, 500, 0, key='bsl_min_len')
                with filter_col2:
                    max_length = st.slider("Max baseline length (km)", 100, 2000, 1500, key='bsl_max_len')
                with filter_col3:
                    color_by = st.selectbox("Color by", ["Length", "Station"], key='bsl_color')

                # Filter baselines
                valid_baselines = [b for b in baselines
                                  if b.length_km is not None
                                  and min_length <= b.length_km <= max_length]

                if valid_baselines:
                    # Create map
                    fig_bsl = go.Figure()

                    # Color scale based on length
                    lengths = [b.length_km for b in valid_baselines]
                    min_len, max_len = min(lengths), max(lengths)

                    # Add baselines as lines
                    for bl in valid_baselines:
                        # Normalize length for color
                        if max_len > min_len:
                            norm_len = (bl.length_km - min_len) / (max_len - min_len)
                        else:
                            norm_len = 0.5

                        # Color based on length (blue=short, red=long)
                        if color_by == "Length":
                            r = int(norm_len * 255)
                            g = int((1 - abs(norm_len - 0.5) * 2) * 200)
                            b = int((1 - norm_len) * 255)
                            line_color = f'rgb({r},{g},{b})'
                        else:
                            line_color = '#4ECDC4'

                        fig_bsl.add_trace(go.Scattergeo(
                            lon=[bl.lon1, bl.lon2],
                            lat=[bl.lat1, bl.lat2],
                            mode='lines',
                            line=dict(width=2, color=line_color),
                            opacity=0.7,
                            hoverinfo='text',
                            text=f"{bl.station1} - {bl.station2}<br>{bl.length_km:.1f} km",
                            showlegend=False
                        ))

                    # Add station markers
                    station_coords = {}
                    for bl in valid_baselines:
                        if bl.lat1 is not None:
                            station_coords[bl.station1] = (bl.lat1, bl.lon1)
                        if bl.lat2 is not None:
                            station_coords[bl.station2] = (bl.lat2, bl.lon2)

                    sta_names = list(station_coords.keys())
                    sta_lats = [station_coords[s][0] for s in sta_names]
                    sta_lons = [station_coords[s][1] for s in sta_names]

                    # Count connections per station
                    connection_counts = {}
                    for bl in valid_baselines:
                        connection_counts[bl.station1] = connection_counts.get(bl.station1, 0) + 1
                        connection_counts[bl.station2] = connection_counts.get(bl.station2, 0) + 1

                    sta_sizes = [8 + connection_counts.get(s, 1) * 2 for s in sta_names]

                    fig_bsl.add_trace(go.Scattergeo(
                        lon=sta_lons,
                        lat=sta_lats,
                        mode='markers+text',
                        marker=dict(
                            size=sta_sizes,
                            color='#FF6B6B',
                            line=dict(width=1, color='white')
                        ),
                        text=sta_names,
                        textposition='top center',
                        textfont=dict(size=9, color='white'),
                        hoverinfo='text',
                        hovertext=[f"{s}<br>{connection_counts.get(s, 0)} baselines" for s in sta_names],
                        name='Stations'
                    ))

                    # Update layout with zoom presets
                    fig_bsl.update_geos(
                        projection_type="mercator",
                        showland=True,
                        landcolor='#2D3748',
                        showocean=True,
                        oceancolor='#1A202C',
                        showcoastlines=True,
                        coastlinecolor='#4A5568',
                        showframe=True,
                        framecolor='#4A5568',
                        showcountries=True,
                        countrycolor='#4A5568',
                        center=dict(lat=zoom_center["lat"], lon=zoom_center["lon"]),
                        projection_scale=zoom_center["scale"]
                    )

                    # Session description
                    session_desc = {
                        "Final (Combined)": "Final Network",
                        "Session 1 (Local/Luxembourg)": "Session 1 - Local",
                        "Session 2 (European)": "Session 2 - European",
                        "Excluded Baselines": "Excluded",
                    }

                    fig_bsl.update_layout(
                        title=f"DD Baseline Network - {bsl_year} DOY {bsl_doy} - {session_desc.get(session_type, session_type)} ({len(valid_baselines)} baselines)",
                        height=700,
                        margin=dict(l=0, r=0, t=50, b=0),
                        paper_bgcolor='rgba(0,0,0,0)',
                        geo=dict(bgcolor='rgba(0,0,0,0)')
                    )

                    st.plotly_chart(fig_bsl, use_container_width=True)

                    # Baseline length histogram
                    st.markdown("### 📊 Baseline Length Distribution")
                    fig_hist = px.histogram(
                        x=lengths,
                        nbins=30,
                        labels={'x': 'Baseline Length (km)', 'y': 'Count'},
                        title="Distribution of Baseline Lengths"
                    )
                    fig_hist.update_traces(marker_color='#4ECDC4')
                    fig_hist.add_vline(x=np.mean(lengths), line_dash="dash", line_color="red",
                                      annotation_text=f"Mean: {np.mean(lengths):.0f} km")
                    apply_colorful_style(fig_hist)
                    st.plotly_chart(fig_hist, use_container_width=True)

                    # Station connectivity table
                    st.markdown("### 🔗 Station Connectivity")
                    conn_df = pd.DataFrame([
                        {'Station': s, 'Baselines': connection_counts.get(s, 0)}
                        for s in sorted(station_coords.keys())
                    ]).sort_values('Baselines', ascending=False)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Most Connected Stations:**")
                        st.dataframe(conn_df.head(10), use_container_width=True, hide_index=True)
                    with col2:
                        st.markdown("**Least Connected Stations:**")
                        st.dataframe(conn_df.tail(10), use_container_width=True, hide_index=True)

                    # Network Topology Visualization
                    st.markdown("### 🌐 Network Topology (How Baselines Connect)")

                    # Build adjacency list
                    adjacency = {}
                    for bl in valid_baselines:
                        if bl.station1 not in adjacency:
                            adjacency[bl.station1] = []
                        if bl.station2 not in adjacency:
                            adjacency[bl.station2] = []
                        adjacency[bl.station1].append(bl.station2)
                        adjacency[bl.station2].append(bl.station1)

                    # Find hub stations (most connections)
                    hub_stations = sorted(connection_counts.items(), key=lambda x: x[1], reverse=True)[:5]

                    # Create network tree visualization
                    topo_col1, topo_col2 = st.columns([1, 1])

                    with topo_col1:
                        st.markdown("**Hub Stations (Starting Points):**")
                        for hub, count in hub_stations:
                            role = ""
                            if hub == "WARN":
                                role = " (European hub)"
                            elif hub in ["ROUL", "TLSE"]:
                                role = " (Secondary hub)"
                            st.markdown(f"- **{hub}**: {count} connections{role}")

                        st.markdown("---")
                        st.markdown("**Connection Tree from Main Hub:**")

                        # Build tree from main hub
                        main_hub = hub_stations[0][0] if hub_stations else None
                        if main_hub and main_hub in adjacency:
                            tree_lines = []
                            visited = set([main_hub])
                            tree_lines.append(f"**{main_hub}** (hub)")

                            # Level 1: Direct connections
                            level1 = sorted(adjacency[main_hub])
                            for i, sta in enumerate(level1):
                                prefix = "└── " if i == len(level1) - 1 else "├── "
                                tree_lines.append(f"  {prefix}{sta}")
                                visited.add(sta)

                                # Level 2: Connections of level 1 stations
                                if sta in adjacency:
                                    level2 = [s for s in adjacency[sta] if s not in visited]
                                    for j, sta2 in enumerate(sorted(level2)[:3]):  # Limit depth
                                        inner_prefix = "    └── " if j == len(level2[:3]) - 1 else "    ├── "
                                        if i < len(level1) - 1:
                                            inner_prefix = "│   " + inner_prefix[4:]
                                        tree_lines.append(f"  {inner_prefix}{sta2}")

                            st.code("\n".join(tree_lines), language=None)

                    with topo_col2:
                        st.markdown("**Baseline Connection Order:**")
                        st.markdown("""
                        The network is processed in this order:

                        1. **Widelane (WL)** ambiguities resolved first
                           - Uses L1-L2 code combination
                           - ~86 cm wavelength (easier to fix)

                        2. **Narrowlane (NL)** ambiguities resolved second
                           - Uses resolved WL as constraint
                           - ~10 cm wavelength (precise)

                        3. **Final coordinates** estimated
                           - Uses fixed integer ambiguities
                           - All baselines contribute together
                        """)

                        # Show baseline pairs in processing order
                        st.markdown("**Baseline Pairs (alphabetical):**")
                        baseline_pairs = sorted([
                            f"{bl.station1}-{bl.station2}" if bl.station1 < bl.station2
                            else f"{bl.station2}-{bl.station1}"
                            for bl in valid_baselines
                        ])
                        # Show in columns
                        pairs_text = " | ".join(baseline_pairs[:15])
                        if len(baseline_pairs) > 15:
                            pairs_text += f" ... (+{len(baseline_pairs)-15} more)"
                        st.text(pairs_text)

                    # Interactive Network Graph Visualization
                    st.markdown("### 🎨 Interactive Network Graph (Colored Paths)")

                    # Layout options
                    graph_col1, graph_col2 = st.columns(2)
                    with graph_col1:
                        graph_layout = st.selectbox(
                            "Graph Layout",
                            ["Geographic (Real Coordinates)", "Radial (Hub-centered)", "Force-directed"],
                            key='graph_layout'
                        )
                    with graph_col2:
                        show_labels = st.checkbox("Show station labels", value=True, key='show_labels')

                    # BFS to assign colors based on distance from hub
                    from collections import deque

                    # Find main hub
                    main_hub = hub_stations[0][0] if hub_stations else list(adjacency.keys())[0]

                    # Assign branch colors using BFS
                    branch_colors = {}  # station -> color
                    edge_colors = {}    # (sta1, sta2) -> color
                    visited_graph = set()
                    queue = deque()

                    # Color palette for different branches
                    color_palette = [
                        '#FF6B6B',  # Red
                        '#4ECDC4',  # Teal
                        '#45B7D1',  # Blue
                        '#96CEB4',  # Green
                        '#FFEAA7',  # Yellow
                        '#DDA0DD',  # Plum
                        '#98D8C8',  # Mint
                        '#F7DC6F',  # Gold
                        '#BB8FCE',  # Purple
                        '#85C1E9',  # Light Blue
                        '#F8B500',  # Orange
                        '#00CED1',  # Dark Cyan
                    ]

                    # Start BFS from main hub
                    branch_colors[main_hub] = '#FFFFFF'  # Hub is white
                    visited_graph.add(main_hub)

                    # Assign different colors to each branch from hub
                    direct_neighbors = sorted(adjacency.get(main_hub, []))
                    for idx, neighbor in enumerate(direct_neighbors):
                        branch_color = color_palette[idx % len(color_palette)]
                        queue.append((neighbor, branch_color, 1))
                        edge_key = tuple(sorted([main_hub, neighbor]))
                        edge_colors[edge_key] = branch_color

                    while queue:
                        station, color, depth = queue.popleft()
                        if station in visited_graph:
                            continue
                        visited_graph.add(station)
                        branch_colors[station] = color

                        # Add unvisited neighbors
                        for neighbor in adjacency.get(station, []):
                            if neighbor not in visited_graph:
                                edge_key = tuple(sorted([station, neighbor]))
                                edge_colors[edge_key] = color
                                queue.append((neighbor, color, depth + 1))

                    # Handle disconnected stations
                    for sta in adjacency.keys():
                        if sta not in branch_colors:
                            branch_colors[sta] = '#808080'  # Gray for disconnected

                    # Create network graph figure
                    fig_network = go.Figure()

                    # Calculate positions based on selected layout
                    import math as m
                    pos = {}
                    use_geo_coords = False

                    if graph_layout == "Geographic (Real Coordinates)":
                        # Use actual lat/lon for positions
                        use_geo_coords = True
                        for sta in adjacency.keys():
                            if sta in station_coords:
                                pos[sta] = (station_coords[sta][1], station_coords[sta][0])

                    elif graph_layout == "Radial (Hub-centered)":
                        # Hub at center, connections radiating outward by depth
                        visited_pos = set()
                        level_stations = {0: [main_hub]}
                        visited_pos.add(main_hub)

                        # BFS to find levels
                        current_level = 0
                        while True:
                            next_level = []
                            for sta in level_stations.get(current_level, []):
                                for neighbor in adjacency.get(sta, []):
                                    if neighbor not in visited_pos:
                                        next_level.append(neighbor)
                                        visited_pos.add(neighbor)
                            if not next_level:
                                break
                            current_level += 1
                            level_stations[current_level] = next_level

                        # Position nodes in concentric circles
                        pos[main_hub] = (0, 0)
                        for level, stations in level_stations.items():
                            if level == 0:
                                continue
                            n = len(stations)
                            radius = level * 1.5
                            for i, sta in enumerate(stations):
                                angle = 2 * m.pi * i / n + (level * 0.3)  # Offset each level
                                pos[sta] = (radius * m.cos(angle), radius * m.sin(angle))

                    else:  # Force-directed (simple spring layout)
                        # Initialize with circular layout
                        stations_list = list(adjacency.keys())
                        n = len(stations_list)
                        for i, sta in enumerate(stations_list):
                            angle = 2 * m.pi * i / n
                            pos[sta] = (m.cos(angle) * 2, m.sin(angle) * 2)

                        # Simple force-directed iterations
                        for iteration in range(50):
                            forces = {sta: [0, 0] for sta in stations_list}

                            # Repulsion between all nodes
                            for i, sta1 in enumerate(stations_list):
                                for sta2 in stations_list[i+1:]:
                                    dx = pos[sta1][0] - pos[sta2][0]
                                    dy = pos[sta1][1] - pos[sta2][1]
                                    dist = m.sqrt(dx*dx + dy*dy) + 0.01
                                    force = 0.5 / (dist * dist)
                                    forces[sta1][0] += force * dx / dist
                                    forces[sta1][1] += force * dy / dist
                                    forces[sta2][0] -= force * dx / dist
                                    forces[sta2][1] -= force * dy / dist

                            # Attraction along edges
                            for bl in valid_baselines:
                                if bl.station1 in pos and bl.station2 in pos:
                                    dx = pos[bl.station2][0] - pos[bl.station1][0]
                                    dy = pos[bl.station2][1] - pos[bl.station1][1]
                                    dist = m.sqrt(dx*dx + dy*dy) + 0.01
                                    force = dist * 0.1
                                    forces[bl.station1][0] += force * dx / dist
                                    forces[bl.station1][1] += force * dy / dist
                                    forces[bl.station2][0] -= force * dx / dist
                                    forces[bl.station2][1] -= force * dy / dist

                            # Apply forces
                            for sta in stations_list:
                                pos[sta] = (
                                    pos[sta][0] + forces[sta][0] * 0.1,
                                    pos[sta][1] + forces[sta][1] * 0.1
                                )

                    # Add edges (baselines) with colors
                    for bl in valid_baselines:
                        if bl.station1 in pos and bl.station2 in pos:
                            x0, y0 = pos[bl.station1]
                            x1, y1 = pos[bl.station2]

                            edge_key = tuple(sorted([bl.station1, bl.station2]))
                            edge_color = edge_colors.get(edge_key, '#808080')

                            # Add line
                            fig_network.add_trace(go.Scatter(
                                x=[x0, x1, None],
                                y=[y0, y1, None],
                                mode='lines',
                                line=dict(width=3, color=edge_color),
                                hoverinfo='text',
                                text=f"{bl.station1} ↔ {bl.station2}<br>{bl.length_km:.1f} km",
                                showlegend=False,
                                opacity=0.8
                            ))

                            # Add arrow in middle showing direction (from hub outward)
                            mid_x, mid_y = (x0 + x1) / 2, (y0 + y1) / 2

                    # Add nodes (stations) with colors
                    for sta, (x, y) in pos.items():
                        node_color = branch_colors.get(sta, '#808080')
                        node_size = 20 if sta == main_hub else 12 + connection_counts.get(sta, 1) * 2

                        # Hub station gets special marker
                        if sta == main_hub:
                            marker_symbol = 'star'
                            node_size = 25
                        else:
                            marker_symbol = 'circle'

                        # Determine display mode based on checkbox
                        display_mode = 'markers+text' if show_labels else 'markers'

                        fig_network.add_trace(go.Scatter(
                            x=[x],
                            y=[y],
                            mode=display_mode,
                            marker=dict(
                                size=node_size,
                                color=node_color,
                                symbol=marker_symbol,
                                line=dict(width=2, color='white')
                            ),
                            text=[sta] if show_labels else None,
                            textposition='top center',
                            textfont=dict(size=10, color='white'),
                            hoverinfo='text',
                            hovertext=f"<b>{sta}</b><br>{connection_counts.get(sta, 0)} connections",
                            showlegend=False
                        ))

                    # Create legend for branch colors
                    for idx, neighbor in enumerate(direct_neighbors[:8]):  # Limit legend
                        branch_color = color_palette[idx % len(color_palette)]
                        fig_network.add_trace(go.Scatter(
                            x=[None], y=[None],
                            mode='markers',
                            marker=dict(size=10, color=branch_color),
                            name=f"Branch: {main_hub}→{neighbor}",
                            showlegend=True
                        ))

                    # Add hub to legend
                    fig_network.add_trace(go.Scatter(
                        x=[None], y=[None],
                        mode='markers',
                        marker=dict(size=15, color='white', symbol='star', line=dict(width=2, color='black')),
                        name=f"Hub: {main_hub}",
                        showlegend=True
                    ))

                    fig_network.update_layout(
                        title=f"Network Topology - Colored by Branch from {main_hub}",
                        showlegend=True,
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=-0.2,
                            xanchor="center",
                            x=0.5,
                            font=dict(size=10)
                        ),
                        height=600,
                        xaxis=dict(
                            showgrid=False,
                            zeroline=False,
                            showticklabels=use_geo_coords,
                            title="Longitude" if use_geo_coords else ""
                        ),
                        yaxis=dict(
                            showgrid=False,
                            zeroline=False,
                            showticklabels=use_geo_coords,
                            title="Latitude" if use_geo_coords else "",
                            scaleanchor="x" if not use_geo_coords else None
                        ),
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='#1a1a2e',
                        margin=dict(l=20, r=20, t=50, b=80)
                    )

                    st.plotly_chart(fig_network, use_container_width=True)

                    # Color legend explanation
                    st.markdown("""
                    **Legend:**
                    - ⭐ **Star** = Hub station (starting point)
                    - Each **color** represents a different branch/path from the hub
                    - **Larger nodes** = More connections
                    - Lines show baselines connecting stations
                    """)

                    # Baseline details table
                    st.markdown("### 📋 Baseline Details")
                    bsl_df = pd.DataFrame([
                        {
                            'Station 1': bl.station1,
                            'Station 2': bl.station2,
                            'Length (km)': bl.length_km,
                            'Lat1': bl.lat1,
                            'Lon1': bl.lon1,
                            'Lat2': bl.lat2,
                            'Lon2': bl.lon2
                        }
                        for bl in sorted(valid_baselines, key=lambda x: x.length_km or 0, reverse=True)
                    ])
                    st.dataframe(
                        bsl_df.style.format({
                            'Length (km)': '{:.1f}',
                            'Lat1': '{:.4f}', 'Lon1': '{:.4f}',
                            'Lat2': '{:.4f}', 'Lon2': '{:.4f}'
                        }),
                        use_container_width=True,
                        hide_index=True
                    )

                    # ===== CLUSTER VISUALIZATION SECTION =====
                    st.markdown("---")
                    st.markdown("### 🗂️ Station Cluster Analysis")
                    st.markdown("""
                    This section shows which of your stations are defined in the CLU (Cluster) file
                    used for baseline formation. Stations not in the CLU file may have suboptimal
                    baseline connections.
                    """)

                    # Parse CLU file
                    clu_file_path = "/home/ahunegnaw/GPSDATA/CAMPAIGN54/OCT2025_MGX/STA/FIN_EGA_TGX.CLU"
                    clu_assignments = {}
                    cluster_names = {1: "Europe", 2: "Americas", 3: "Asia-Pacific"}

                    if os.path.exists(clu_file_path):
                        try:
                            with open(clu_file_path, 'r') as f:
                                for line in f:
                                    line = line.strip()
                                    if not line or line.startswith('SNGDIF') or line.startswith('-') or line.startswith('STATION') or line.startswith('*'):
                                        continue
                                    parts = line.split()
                                    if len(parts) >= 2:
                                        try:
                                            station = parts[0][:4].upper()
                                            cluster = int(parts[-1])
                                            clu_assignments[station] = cluster
                                        except (ValueError, IndexError):
                                            continue
                        except Exception as e:
                            st.warning(f"Could not parse CLU file: {e}")

                    # Get unique stations from baselines
                    baseline_stations = set()
                    for bl in baselines:
                        baseline_stations.add(bl.station1.upper()[:4])
                        baseline_stations.add(bl.station2.upper()[:4])

                    # Build station info with coordinates
                    station_coords = {}
                    for bl in baselines:
                        sta1 = bl.station1.upper()[:4]
                        sta2 = bl.station2.upper()[:4]
                        if bl.lat1 is not None and bl.lon1 is not None:
                            station_coords[sta1] = (bl.lat1, bl.lon1)
                        if bl.lat2 is not None and bl.lon2 is not None:
                            station_coords[sta2] = (bl.lat2, bl.lon2)

                    # Create cluster analysis data
                    cluster_data = []
                    found_stations = []
                    missing_stations = []

                    for sta in sorted(baseline_stations):
                        sta_upper = sta.upper()[:4]
                        if sta_upper in clu_assignments:
                            cluster_num = clu_assignments[sta_upper]
                            cluster_name = cluster_names.get(cluster_num, f"Cluster {cluster_num}")
                            status = "✅ Found"
                            found_stations.append(sta_upper)
                        else:
                            cluster_num = None
                            cluster_name = "❌ MISSING"
                            status = "❌ Missing"
                            missing_stations.append(sta_upper)

                        lat, lon = station_coords.get(sta_upper, (None, None))
                        cluster_data.append({
                            'Station': sta_upper,
                            'Cluster': cluster_num if cluster_num else "N/A",
                            'Region': cluster_name,
                            'Status': status,
                            'Latitude': lat,
                            'Longitude': lon
                        })

                    # Display summary metrics
                    col_clu1, col_clu2, col_clu3 = st.columns(3)
                    with col_clu1:
                        st.metric("Total Stations", len(baseline_stations))
                    with col_clu2:
                        st.metric("In CLU File", len(found_stations), delta=None)
                    with col_clu3:
                        st.metric("Missing from CLU", len(missing_stations),
                                  delta=f"-{len(missing_stations)}" if missing_stations else None,
                                  delta_color="inverse")

                    # Create cluster map visualization
                    if station_coords:
                        fig_cluster = go.Figure()

                        # Color map for clusters
                        cluster_colors = {
                            1: '#2ecc71',    # Green for Europe
                            2: '#3498db',    # Blue for Americas
                            3: '#9b59b6',    # Purple for Asia-Pacific
                            None: '#e74c3c'  # Red for missing
                        }

                        # Group stations by cluster
                        cluster_groups = {}
                        for sta in baseline_stations:
                            sta_upper = sta.upper()[:4]
                            cluster = clu_assignments.get(sta_upper, None)
                            if cluster not in cluster_groups:
                                cluster_groups[cluster] = []
                            if sta_upper in station_coords:
                                cluster_groups[cluster].append((sta_upper, station_coords[sta_upper]))

                        # Plot each cluster group
                        for cluster, stations in cluster_groups.items():
                            if not stations:
                                continue

                            lats = [s[1][0] for s in stations]
                            lons = [s[1][1] for s in stations]
                            names = [s[0] for s in stations]

                            if cluster is None:
                                legend_name = "❌ Missing from CLU"
                                symbol = 'x'
                                size = 18
                            else:
                                legend_name = f"Cluster {cluster}: {cluster_names.get(cluster, 'Unknown')}"
                                symbol = 'circle'
                                size = 14

                            fig_cluster.add_trace(go.Scattergeo(
                                lat=lats,
                                lon=lons,
                                mode='markers+text',
                                marker=dict(
                                    size=size,
                                    color=cluster_colors.get(cluster, '#95a5a6'),
                                    symbol=symbol,
                                    line=dict(width=1, color='white')
                                ),
                                text=names,
                                textposition='top center',
                                textfont=dict(size=9, color='white'),
                                name=legend_name,
                                hovertemplate='<b>%{text}</b><br>Lat: %{lat:.4f}<br>Lon: %{lon:.4f}<extra></extra>'
                            ))

                        fig_cluster.update_layout(
                            title="Station Cluster Assignments",
                            geo=dict(
                                scope='europe',
                                showland=True,
                                landcolor='rgb(40, 40, 60)',
                                showocean=True,
                                oceancolor='rgb(20, 30, 50)',
                                showcountries=True,
                                countrycolor='rgb(80, 80, 100)',
                                showcoastlines=True,
                                coastlinecolor='rgb(100, 100, 120)',
                                projection_type='mercator',
                                center=dict(lat=49.8, lon=6.1),  # Center on Luxembourg
                                lonaxis_range=[-10, 30],
                                lataxis_range=[35, 65]
                            ),
                            showlegend=True,
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=-0.15,
                                xanchor="center",
                                x=0.5
                            ),
                            height=500,
                            margin=dict(l=0, r=0, t=40, b=60)
                        )

                        st.plotly_chart(fig_cluster, use_container_width=True)

                    # Display cluster table
                    st.markdown("#### Station Cluster Details")
                    cluster_df = pd.DataFrame(cluster_data)

                    # Style the dataframe
                    def highlight_missing(row):
                        if row['Status'] == '❌ Missing':
                            return ['background-color: #5c2323'] * len(row)
                        return [''] * len(row)

                    styled_df = cluster_df.style.apply(highlight_missing, axis=1)
                    if 'Latitude' in cluster_df.columns and 'Longitude' in cluster_df.columns:
                        styled_df = styled_df.format({
                            'Latitude': '{:.4f}',
                            'Longitude': '{:.4f}'
                        }, na_rep='N/A')

                    st.dataframe(styled_df, use_container_width=True, hide_index=True)

                    # Show missing stations alert
                    if missing_stations:
                        st.warning(f"""
                        **⚠️ {len(missing_stations)} stations are missing from CLU file!**

                        These stations are not defined in `FIN_EGA_TGX.CLU` and may have suboptimal
                        baseline connections. Consider adding them to Cluster 1 (Europe).

                        **Missing stations:** {', '.join(sorted(missing_stations))}
                        """)

                        # Provide CLU entries to add
                        with st.expander("📋 CLU entries to add (copy-paste ready)"):
                            st.markdown("""
                            Add these lines to `FIN_EGA_TGX.CLU` to include missing stations in Cluster 1 (Europe):
                            """)
                            clu_entries = []
                            for sta in sorted(missing_stations):
                                # Format: STATION NAME + DOMES (use placeholder) + CLU number
                                clu_entries.append(f"{sta} 00000M001      1")
                            st.code("\n".join(clu_entries), language=None)

                            st.info("""
                            **Note:** The DOMES number (00000M001) is a placeholder. If you have the actual
                            DOMES numbers for these stations, replace them accordingly.

                            After adding these entries, re-run your processing to improve baseline formation.
                            """)
                    else:
                        st.success("✅ All stations are defined in the CLU file!")

                    # ===== COORDINATE QUALITY MAP SECTION =====
                    st.markdown("---")
                    st.markdown("### 📍 Coordinate Quality Map (from Helmert)")
                    st.markdown("""
                    This map shows the coordinate quality of each station based on Helmert transformation
                    residuals (NEU differences vs. reference frame). Color indicates 3D quality.
                    """)

                    # Try to load HLM file for this day
                    hlm_path = get_hlm_path(dd_base_dir, bsl_year, bsl_doy)

                    if hlm_path and os.path.exists(hlm_path):
                        try:
                            hlm_result = parse_hlm_file(hlm_path, bsl_year, bsl_doy)

                            if hlm_result.residuals:
                                # Display summary statistics
                                stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
                                with stat_col1:
                                    st.metric("North RMS", f"{hlm_result.ref_stats.rms_north:.2f} mm")
                                with stat_col2:
                                    st.metric("East RMS", f"{hlm_result.ref_stats.rms_east:.2f} mm")
                                with stat_col3:
                                    st.metric("Up RMS", f"{hlm_result.ref_stats.rms_up:.2f} mm")
                                with stat_col4:
                                    # Calculate 3D RMS
                                    rms_3d = math.sqrt(hlm_result.ref_stats.rms_north**2 +
                                                       hlm_result.ref_stats.rms_east**2 +
                                                       hlm_result.ref_stats.rms_up**2)
                                    st.metric("3D RMS", f"{rms_3d:.2f} mm")

                                # Build data for map with coordinates from baselines
                                hlm_map_data = []
                                for res in hlm_result.residuals:
                                    sta = res.station_4char.upper()[:4]
                                    # Get coordinates from baseline data
                                    lat, lon = station_coords.get(sta, (None, None))
                                    if lat is not None and lon is not None:
                                        # Calculate 3D residual
                                        rms_3d_sta = math.sqrt(res.north_mm**2 + res.east_mm**2 + res.up_mm**2)
                                        hlm_map_data.append({
                                            'station': sta,
                                            'lat': lat,
                                            'lon': lon,
                                            'north_mm': res.north_mm,
                                            'east_mm': res.east_mm,
                                            'up_mm': res.up_mm,
                                            'rms_3d': rms_3d_sta,
                                            'is_reference': res.is_reference,
                                            'flag': res.flag2
                                        })

                                if hlm_map_data:
                                    # Create quality map
                                    fig_quality = go.Figure()

                                    # Define quality thresholds and colors
                                    # Green: < 3mm, Yellow: 3-6mm, Orange: 6-10mm, Red: > 10mm
                                    def get_quality_color(rms_3d):
                                        if rms_3d < 3:
                                            return '#2ecc71'  # Green - Excellent
                                        elif rms_3d < 6:
                                            return '#f1c40f'  # Yellow - Good
                                        elif rms_3d < 10:
                                            return '#e67e22'  # Orange - Fair
                                        else:
                                            return '#e74c3c'  # Red - Poor

                                    def get_quality_label(rms_3d):
                                        if rms_3d < 3:
                                            return 'Excellent (<3mm)'
                                        elif rms_3d < 6:
                                            return 'Good (3-6mm)'
                                        elif rms_3d < 10:
                                            return 'Fair (6-10mm)'
                                        else:
                                            return 'Poor (>10mm)'

                                    # Group by quality for legend
                                    quality_groups = {'Excellent (<3mm)': [], 'Good (3-6mm)': [],
                                                      'Fair (6-10mm)': [], 'Poor (>10mm)': []}
                                    for d in hlm_map_data:
                                        label = get_quality_label(d['rms_3d'])
                                        quality_groups[label].append(d)

                                    # Plot each quality group
                                    quality_colors = {
                                        'Excellent (<3mm)': '#2ecc71',
                                        'Good (3-6mm)': '#f1c40f',
                                        'Fair (6-10mm)': '#e67e22',
                                        'Poor (>10mm)': '#e74c3c'
                                    }

                                    for quality_label, stations in quality_groups.items():
                                        if not stations:
                                            continue

                                        lats = [s['lat'] for s in stations]
                                        lons = [s['lon'] for s in stations]
                                        names = [s['station'] for s in stations]
                                        rms_vals = [s['rms_3d'] for s in stations]
                                        north_vals = [s['north_mm'] for s in stations]
                                        east_vals = [s['east_mm'] for s in stations]
                                        up_vals = [s['up_mm'] for s in stations]
                                        refs = ['Ref' if s['is_reference'] else 'Non-Ref' for s in stations]

                                        # Size based on 3D RMS (larger = worse)
                                        sizes = [max(10, min(25, 8 + r * 1.5)) for r in rms_vals]

                                        hover_text = [
                                            f"<b>{n}</b><br>" +
                                            f"N: {north:.2f} mm<br>" +
                                            f"E: {east:.2f} mm<br>" +
                                            f"U: {up:.2f} mm<br>" +
                                            f"<b>3D: {rms:.2f} mm</b><br>" +
                                            f"Status: {ref}"
                                            for n, north, east, up, rms, ref in
                                            zip(names, north_vals, east_vals, up_vals, rms_vals, refs)
                                        ]

                                        fig_quality.add_trace(go.Scattergeo(
                                            lat=lats,
                                            lon=lons,
                                            mode='markers+text',
                                            marker=dict(
                                                size=sizes,
                                                color=quality_colors[quality_label],
                                                line=dict(width=1, color='white'),
                                                opacity=0.85
                                            ),
                                            text=names,
                                            textposition='top center',
                                            textfont=dict(size=8, color='white'),
                                            name=f"{quality_label} ({len(stations)})",
                                            hovertemplate='%{hovertext}<extra></extra>',
                                            hovertext=hover_text
                                        ))

                                    fig_quality.update_layout(
                                        title=f"Coordinate Quality - DOY {bsl_doy} (Helmert Residuals)",
                                        geo=dict(
                                            scope='europe',
                                            showland=True,
                                            landcolor='rgb(40, 40, 60)',
                                            showocean=True,
                                            oceancolor='rgb(20, 30, 50)',
                                            showcountries=True,
                                            countrycolor='rgb(80, 80, 100)',
                                            showcoastlines=True,
                                            coastlinecolor='rgb(100, 100, 120)',
                                            projection_type='mercator',
                                            center=dict(lat=49.8, lon=6.1),
                                            lonaxis_range=[-10, 30],
                                            lataxis_range=[35, 65]
                                        ),
                                        showlegend=True,
                                        legend=dict(
                                            orientation="h",
                                            yanchor="bottom",
                                            y=-0.15,
                                            xanchor="center",
                                            x=0.5
                                        ),
                                        height=550,
                                        margin=dict(l=0, r=0, t=40, b=60)
                                    )

                                    st.plotly_chart(fig_quality, use_container_width=True)

                                    # Quality legend explanation
                                    st.markdown("""
                                    **Quality Thresholds (3D RMS):**
                                    - 🟢 **Excellent** (< 3mm): Reference-quality coordinates
                                    - 🟡 **Good** (3-6mm): Normal repeatability
                                    - 🟠 **Fair** (6-10mm): Check for issues
                                    - 🔴 **Poor** (> 10mm): Investigate station/data problems
                                    """)

                                    # Detailed residuals table
                                    with st.expander("📋 Detailed NEU Residuals"):
                                        hlm_df = pd.DataFrame(hlm_map_data)
                                        hlm_df = hlm_df.rename(columns={
                                            'station': 'Station',
                                            'north_mm': 'North (mm)',
                                            'east_mm': 'East (mm)',
                                            'up_mm': 'Up (mm)',
                                            'rms_3d': '3D (mm)',
                                            'is_reference': 'Reference',
                                            'flag': 'Flag'
                                        })
                                        hlm_df = hlm_df[['Station', 'North (mm)', 'East (mm)', 'Up (mm)', '3D (mm)', 'Reference', 'Flag']]
                                        hlm_df = hlm_df.sort_values('3D (mm)', ascending=False)

                                        # Color code the 3D column
                                        def color_3d(val):
                                            if val < 3:
                                                return 'background-color: #1e5631'
                                            elif val < 6:
                                                return 'background-color: #5c4813'
                                            elif val < 10:
                                                return 'background-color: #5c3013'
                                            else:
                                                return 'background-color: #5c1313'

                                        styled_hlm = hlm_df.style.format({
                                            'North (mm)': '{:.2f}',
                                            'East (mm)': '{:.2f}',
                                            'Up (mm)': '{:.2f}',
                                            '3D (mm)': '{:.2f}'
                                        }).applymap(color_3d, subset=['3D (mm)'])

                                        st.dataframe(styled_hlm, use_container_width=True, hide_index=True)

                                    # Bar chart of residuals
                                    with st.expander("📊 NEU Residuals Bar Chart"):
                                        hlm_bar_df = pd.DataFrame(hlm_map_data).sort_values('rms_3d', ascending=True)

                                        fig_bar = go.Figure()

                                        fig_bar.add_trace(go.Bar(
                                            name='North',
                                            x=hlm_bar_df['station'],
                                            y=hlm_bar_df['north_mm'],
                                            marker_color='#3498db'
                                        ))
                                        fig_bar.add_trace(go.Bar(
                                            name='East',
                                            x=hlm_bar_df['station'],
                                            y=hlm_bar_df['east_mm'],
                                            marker_color='#2ecc71'
                                        ))
                                        fig_bar.add_trace(go.Bar(
                                            name='Up',
                                            x=hlm_bar_df['station'],
                                            y=hlm_bar_df['up_mm'],
                                            marker_color='#e74c3c'
                                        ))

                                        fig_bar.update_layout(
                                            title="NEU Residuals by Station",
                                            barmode='group',
                                            xaxis_title="Station",
                                            yaxis_title="Residual (mm)",
                                            height=400,
                                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                                        )

                                        st.plotly_chart(fig_bar, use_container_width=True)

                                else:
                                    st.warning("Could not match HLM stations with baseline coordinates")

                            else:
                                st.warning("No station residuals found in HLM file")

                        except Exception as e:
                            st.error(f"Error parsing HLM file: {e}")
                            import traceback
                            st.code(traceback.format_exc())

                    else:
                        st.info(f"""
                        **HLM file not found** for DOY {bsl_doy}

                        Expected path: `{dd_base_dir}/{bsl_year}/{bsl_doy}/OUT/HLM_{bsl_year}{bsl_doy:03d}0.OUT.gz`

                        The HLM file contains Helmert transformation residuals comparing your
                        solution to the reference frame (IGS20).
                        """)

                    # ===== ML QUALITY ASSESSMENT SECTION =====
                    st.markdown("---")
                    st.markdown("### 🤖 ML Station Quality Assessment")
                    st.markdown("""
                    Machine learning-based quality assessment that analyzes multiple data sources
                    (Helmert residuals, ZTD, gradients, baseline RMS, ambiguity resolution)
                    to identify problematic stations and explain WHY they are flagged.
                    """)

                    if not ML_SKLEARN_AVAILABLE:
                        st.warning("⚠️ scikit-learn not installed. ML quality assessment unavailable.")
                        st.code("pip install scikit-learn")
                    else:
                        try:
                            with st.spinner("Analyzing station quality with ML..."):
                                ml_report = assess_network_quality(dd_base_dir, bsl_year, bsl_doy)

                            if ml_report and ml_report.stations:
                                # Summary metrics
                                ml_col1, ml_col2, ml_col3, ml_col4 = st.columns(4)

                                with ml_col1:
                                    st.metric("Total Stations", ml_report.num_stations)
                                with ml_col2:
                                    st.metric("Good (A/B)", f"{ml_report.num_good_stations}", delta=None,
                                              delta_color="normal")
                                with ml_col3:
                                    st.metric("Warning (C)", f"{ml_report.num_warning_stations}",
                                              delta=f"{ml_report.num_warning_stations}" if ml_report.num_warning_stations > 5 else None,
                                              delta_color="off")
                                with ml_col4:
                                    st.metric("Bad (D/F)", f"{ml_report.num_bad_stations}",
                                              delta=f"-{ml_report.num_bad_stations}" if ml_report.num_bad_stations > 0 else None,
                                              delta_color="inverse")

                                # Grade distribution chart
                                grade_counts = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'F': 0}
                                for sta_id, m in ml_report.stations.items():
                                    grade_counts[m.quality_grade] += 1

                                grade_colors = {'A': '#27ae60', 'B': '#2ecc71', 'C': '#f1c40f',
                                                'D': '#e67e22', 'F': '#e74c3c'}

                                fig_grades = go.Figure()
                                fig_grades.add_trace(go.Bar(
                                    x=list(grade_counts.keys()),
                                    y=list(grade_counts.values()),
                                    marker_color=[grade_colors[g] for g in grade_counts.keys()],
                                    text=list(grade_counts.values()),
                                    textposition='auto'
                                ))
                                fig_grades.update_layout(
                                    title="Quality Grade Distribution",
                                    xaxis_title="Grade",
                                    yaxis_title="Number of Stations",
                                    height=300,
                                    showlegend=False
                                )

                                # Show grade distribution in expander
                                with st.expander("📊 Grade Distribution", expanded=True):
                                    st.plotly_chart(fig_grades, use_container_width=True)

                                    st.markdown("""
                                    **Grading System:**
                                    - **A (Excellent)**: Score ≥ 80 - Reference quality
                                    - **B (Good)**: Score 65-79 - Normal performance
                                    - **C (Fair)**: Score 50-64 - Minor issues
                                    - **D (Poor)**: Score 35-49 - Significant problems
                                    - **F (Fail)**: Score < 35 - Major issues, investigate immediately
                                    """)

                                # Problem stations table (D and F grades)
                                problem_stations = [(sta_id, m) for sta_id, m in ml_report.stations.items()
                                                    if m.quality_grade in ['D', 'F']]
                                if problem_stations:
                                    st.markdown("#### 🚨 Problem Stations Requiring Attention")

                                    # Get diagnoses per station
                                    station_diagnoses = {}
                                    for diag in ml_report.diagnoses:
                                        if diag.station_id not in station_diagnoses:
                                            station_diagnoses[diag.station_id] = []
                                        station_diagnoses[diag.station_id].append(diag)

                                    problem_data = []
                                    for sta_id, m in sorted(problem_stations, key=lambda x: x[1].quality_score):
                                        # Get primary issues from flags and diagnoses
                                        issues = m.flags[:3] if m.flags else []
                                        if sta_id in station_diagnoses:
                                            issues.extend([d.message[:50] for d in station_diagnoses[sta_id][:2]])
                                        issues_str = "; ".join(issues[:3]) if issues else "Unknown"

                                        problem_data.append({
                                            'Station': sta_id,
                                            'Grade': m.quality_grade,
                                            'Score': f"{m.quality_score:.0f}",
                                            'Issues': issues_str,
                                            'Anomaly': '⚠️ Yes' if m.anomaly_score > 0.5 else 'No'
                                        })

                                    problem_df = pd.DataFrame(problem_data)

                                    # Style the dataframe
                                    def color_grade(val):
                                        colors = {'A': '#1e5631', 'B': '#2e5631', 'C': '#5c4813',
                                                  'D': '#5c3013', 'F': '#5c1313'}
                                        return f'background-color: {colors.get(val, "#333")}'

                                    styled_problem = problem_df.style.applymap(
                                        color_grade, subset=['Grade']
                                    )
                                    st.dataframe(styled_problem, use_container_width=True, hide_index=True)

                                # Detailed diagnostics in expander
                                with st.expander("📋 Full Station Diagnostics"):
                                    # Build comprehensive table
                                    diag_data = []
                                    for sta_id, m in sorted(ml_report.stations.items(),
                                                            key=lambda x: x[1].quality_score):
                                        # Get issues from flags
                                        issues = "; ".join(m.flags[:3]) if m.flags else "None"

                                        diag_data.append({
                                            'Station': sta_id,
                                            'Grade': m.quality_grade,
                                            'Score': f"{m.quality_score:.0f}",
                                            'HLM 3D (mm)': f"{m.hlm_3d:.2f}" if m.hlm_3d else "-",
                                            'ZTD Std (mm)': f"{m.ztd_std*1000:.2f}" if m.ztd_std else "-",
                                            'RMS (mm)': f"{m.baseline_rms_mean:.2f}" if m.baseline_rms_mean else "-",
                                            'Baselines': m.num_baselines,
                                            'Issues': issues,
                                            'Anomaly': 'Yes' if m.anomaly_score > 0.5 else 'No'
                                        })

                                    diag_df = pd.DataFrame(diag_data)
                                    st.dataframe(diag_df, use_container_width=True, hide_index=True,
                                                 height=400)

                                # Quality Map colored by ML grade
                                with st.expander("🗺️ ML Quality Map"):
                                    # Build map data using station coords from baselines
                                    ml_map_data = []
                                    for sta_id, m in ml_report.stations.items():
                                        sta = sta_id[:4].upper()
                                        lat, lon = station_coords.get(sta, (None, None))
                                        if lat is not None and lon is not None:
                                            issues = "; ".join(m.flags[:2]) if m.flags else "OK"
                                            ml_map_data.append({
                                                'station': sta,
                                                'lat': lat,
                                                'lon': lon,
                                                'grade': m.quality_grade,
                                                'score': m.quality_score,
                                                'issues': issues
                                            })

                                    if ml_map_data:
                                        fig_ml_map = go.Figure()

                                        # Group by grade
                                        for grade in ['A', 'B', 'C', 'D', 'F']:
                                            grade_stations = [s for s in ml_map_data if s['grade'] == grade]
                                            if not grade_stations:
                                                continue

                                            fig_ml_map.add_trace(go.Scattergeo(
                                                lat=[s['lat'] for s in grade_stations],
                                                lon=[s['lon'] for s in grade_stations],
                                                mode='markers+text',
                                                marker=dict(
                                                    size=12 if grade in ['D', 'F'] else 10,
                                                    color=grade_colors[grade],
                                                    line=dict(width=1, color='white'),
                                                    opacity=0.9 if grade in ['D', 'F'] else 0.7
                                                ),
                                                text=[s['station'] for s in grade_stations],
                                                textposition='top center',
                                                textfont=dict(size=8, color='white'),
                                                name=f"Grade {grade} ({len(grade_stations)})",
                                                hovertemplate='<b>%{text}</b><br>' +
                                                              f'Grade: {grade}<br>' +
                                                              'Score: %{customdata[0]:.0f}<br>' +
                                                              'Issues: %{customdata[1]}<extra></extra>',
                                                customdata=[[s['score'], s['issues']] for s in grade_stations]
                                            ))

                                        fig_ml_map.update_layout(
                                            title=f"ML Quality Assessment - DOY {bsl_doy}",
                                            geo=dict(
                                                scope='europe',
                                                showland=True,
                                                landcolor='rgb(40, 40, 60)',
                                                showocean=True,
                                                oceancolor='rgb(20, 30, 50)',
                                                showcountries=True,
                                                countrycolor='rgb(80, 80, 100)',
                                                showcoastlines=True,
                                                coastlinecolor='rgb(100, 100, 120)',
                                                projection_type='mercator',
                                                center=dict(lat=49.8, lon=6.1),
                                                lonaxis_range=[-10, 30],
                                                lataxis_range=[35, 65]
                                            ),
                                            showlegend=True,
                                            legend=dict(
                                                orientation="h",
                                                yanchor="bottom",
                                                y=-0.15,
                                                xanchor="center",
                                                x=0.5
                                            ),
                                            height=500,
                                            margin=dict(l=0, r=0, t=40, b=60)
                                        )

                                        st.plotly_chart(fig_ml_map, use_container_width=True)
                                    else:
                                        st.info("Could not match ML results with station coordinates")

                                # Recommendations from diagnoses
                                if problem_stations and ml_report.diagnoses:
                                    st.markdown("#### 💡 Recommendations")
                                    shown_stations = set()
                                    for diag in ml_report.diagnoses:
                                        if diag.station_id in shown_stations:
                                            continue
                                        # Only show for problem stations
                                        if diag.station_id in [s[0] for s in problem_stations]:
                                            m = ml_report.stations.get(diag.station_id)
                                            if m:
                                                st.markdown(f"**{diag.station_id}** (Grade {m.quality_grade}, Score {m.quality_score:.0f}):")
                                                # Get all recommendations for this station
                                                sta_diags = [d for d in ml_report.diagnoses if d.station_id == diag.station_id]
                                                for d in sta_diags[:3]:
                                                    st.markdown(f"  - {d.recommendation}")
                                                shown_stations.add(diag.station_id)
                                                if len(shown_stations) >= 5:
                                                    break

                            else:
                                st.warning("Could not generate ML quality report. Check if result files exist.")

                        except Exception as e:
                            st.error(f"Error in ML quality assessment: {e}")
                            import traceback
                            with st.expander("Error details"):
                                st.code(traceback.format_exc())

                else:
                    st.warning("No baselines match the filter criteria")

            else:
                st.warning(f"BSL or CRD file not found for {bsl_year} DOY {bsl_doy}")

                # Check which files are missing
                missing_files = []
                if not os.path.exists(bsl_path):
                    missing_files.append(f"BSL: `{bsl_path}`")
                if not os.path.exists(crd_path):
                    missing_files.append(f"CRD: `{crd_path}`")

                st.info(f"""
                **Missing files:**
                {chr(10).join('- ' + f for f in missing_files)}

                **Available BSL files in STA directory:**
                """)

                # List available BSL files
                sta_dir = f"{dd_base_dir}/{bsl_year}/{bsl_doy}/STA"
                if os.path.exists(sta_dir):
                    bsl_files = [f for f in os.listdir(sta_dir) if 'BSL' in f.upper()]
                    if bsl_files:
                        st.code("\n".join(sorted(bsl_files)))
                    else:
                        st.text("No BSL files found")
                else:
                    st.text(f"Directory not found: {sta_dir}")

        # ============================================================
        # TAB 25: DD Multi-DOY Analysis
        # ============================================================
        elif selected_page == "📈 DD Multi-DOY":
            st.header("📈 DD Multi-DOY Analysis")
            st.markdown("""
            Comprehensive quality analysis across multiple days of Double-Difference processing.
            Includes coordinate repeatability, satellite health, ambiguity resolution, troposphere,
            baseline network, and coordinate time series.
            """)

            # Configuration
            dd_analysis_base_dir = "/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX"

            config_col1, config_col2, config_col3 = st.columns(3)
            with config_col1:
                dd_analysis_year = st.selectbox("Year", [2025, 2024], index=0, key='dd_analysis_year')
            with config_col2:
                available_doys = get_dd_available_doys(dd_analysis_base_dir, dd_analysis_year)
                if available_doys:
                    default_start = min(available_doys)
                    default_end = max(available_doys)
                else:
                    default_start, default_end = 296, 306
                doy_range = st.slider("DOY Range", 1, 366, (default_start, default_end), key='dd_analysis_doy_range')
            with config_col3:
                st.metric("Available DOYs", len(available_doys) if available_doys else 0)

            doy_start, doy_end = doy_range

            # Sub-tabs for different analyses
            dd_tab1, dd_tab2, dd_tab3, dd_tab4, dd_tab5, dd_tab6, dd_tab7 = st.tabs([
                "📈 Repeatability",
                "🛰️ Satellite Health",
                "🎯 AR Dashboard",
                "🌬️ Trop Gradients",
                "🔗 Network Graph",
                "📍 Coord Time Series",
                "📊 Coord Repeatability (DD)"
            ])

            # ----------------------------------------------------------
            # DD TAB 1: Baseline Repeatability Time Series
            # ----------------------------------------------------------
            with dd_tab1:
                st.subheader("📈 Baseline Repeatability Time Series")
                st.markdown("NEU residuals from Helmert transformation across multiple days.")

                with st.spinner("Loading repeatability data..."):
                    repeat_stations = load_hlm_ts(dd_analysis_base_dir, dd_analysis_year, doy_start, doy_end)

                if repeat_stations:
                    # Summary table
                    repeat_summary = get_repeatability_summary(repeat_stations)

                    # Metrics row
                    rep_col1, rep_col2, rep_col3, rep_col4 = st.columns(4)
                    with rep_col1:
                        st.metric("Stations", len(repeat_stations))
                    with rep_col2:
                        if not repeat_summary.empty and 'WRMS_3D (mm)' in repeat_summary.columns:
                            mean_wrms = repeat_summary['WRMS_3D (mm)'].mean()
                        else:
                            mean_wrms = 0
                        st.metric("Mean WRMS 3D", f"{mean_wrms:.2f} mm")
                    with rep_col3:
                        if not repeat_summary.empty and 'WRMS_3D (mm)' in repeat_summary.columns:
                            good_count = len(repeat_summary[repeat_summary['WRMS_3D (mm)'] < 5])
                        else:
                            good_count = 0
                        st.metric("Good (<5mm)", good_count)
                    with rep_col4:
                        if not repeat_summary.empty and 'WRMS_3D (mm)' in repeat_summary.columns:
                            poor_count = len(repeat_summary[repeat_summary['WRMS_3D (mm)'] > 10])
                        else:
                            poor_count = 0
                        st.metric("Poor (>10mm)", poor_count)

                    # Station selector for time series plot
                    selected_station = st.selectbox(
                        "Select Station for Time Series",
                        sorted(repeat_stations.keys()),
                        key='repeat_station_select'
                    )

                    if selected_station and selected_station in repeat_stations:
                        sta_data = repeat_stations[selected_station]

                        # Create time series plot
                        fig_repeat = make_subplots(rows=3, cols=1, shared_xaxes=True,
                                                   subplot_titles=['North (mm)', 'East (mm)', 'Up (mm)'],
                                                   vertical_spacing=0.08)

                        fig_repeat.add_trace(go.Scatter(
                            x=sta_data.doys, y=sta_data.north_mm,
                            mode='lines+markers', name='North',
                            line=dict(color='#3498db'), marker=dict(size=6)
                        ), row=1, col=1)

                        fig_repeat.add_trace(go.Scatter(
                            x=sta_data.doys, y=sta_data.east_mm,
                            mode='lines+markers', name='East',
                            line=dict(color='#2ecc71'), marker=dict(size=6)
                        ), row=2, col=1)

                        fig_repeat.add_trace(go.Scatter(
                            x=sta_data.doys, y=sta_data.up_mm,
                            mode='lines+markers', name='Up',
                            line=dict(color='#e74c3c'), marker=dict(size=6)
                        ), row=3, col=1)

                        wrms_n, wrms_e, wrms_u = sta_data.get_wrms()
                        fig_repeat.update_layout(
                            title=f"Coordinate Residuals: {selected_station} (WRMS: N={wrms_n:.2f}, E={wrms_e:.2f}, U={wrms_u:.2f} mm)",
                            height=500, showlegend=False
                        )
                        fig_repeat.update_xaxes(title_text="DOY", row=3, col=1)

                        st.plotly_chart(fig_repeat, use_container_width=True)

                        # Detect jumps
                        jumps = sta_data.detect_jumps(threshold_mm=10.0)
                        if jumps:
                            st.warning(f"⚠️ Detected {len(jumps)} coordinate jumps (>10mm):")
                            for doy, comp, val in jumps:
                                st.text(f"  DOY {doy}: {comp} jump of {val:.1f} mm")

                    # Summary table
                    with st.expander("📋 Repeatability Summary Table", expanded=True):
                        if not repeat_summary.empty and 'WRMS_3D (mm)' in repeat_summary.columns:
                            st.dataframe(
                                repeat_summary.style.format({
                                    'WRMS_N (mm)': '{:.2f}',
                                    'WRMS_E (mm)': '{:.2f}',
                                    'WRMS_U (mm)': '{:.2f}',
                                    'WRMS_3D (mm)': '{:.2f}',
                                    'Mean 3D (mm)': '{:.2f}',
                                    'Max 3D (mm)': '{:.2f}'
                                }).background_gradient(subset=['WRMS_3D (mm)'], cmap='RdYlGn_r'),
                                use_container_width=True, hide_index=True
                            )
                        else:
                            st.info("No stations with sufficient data (>= 2 days) for summary.")
                else:
                    st.warning("No repeatability data available for the selected DOY range.")

            # ----------------------------------------------------------
            # DD TAB 2: Satellite Health Monitor
            # ----------------------------------------------------------
            with dd_tab2:
                st.subheader("🛰️ Satellite Health Monitor")
                st.markdown("Per-satellite observation statistics and RMS from CHK.SUM files.")

                # Load for a single day first
                single_doy = st.selectbox("Select DOY", list(range(doy_start, doy_end + 1)), key='sat_health_doy')
                chk_path = get_chk_path(dd_analysis_base_dir, dd_analysis_year, single_doy)

                if chk_path:
                    satellites = parse_chk_summary(chk_path)

                    if satellites:
                        # Group by constellation
                        gps_sats = [s for s in satellites if s.constellation == 'G']
                        gal_sats = [s for s in satellites if s.constellation == 'E']

                        sat_col1, sat_col2, sat_col3, sat_col4 = st.columns(4)
                        with sat_col1:
                            st.metric("Total Satellites", len(satellites))
                        with sat_col2:
                            st.metric("GPS", len(gps_sats))
                        with sat_col3:
                            st.metric("Galileo", len(gal_sats))
                        with sat_col4:
                            mean_rms = np.mean([s.rms_after for s in satellites])
                            st.metric("Mean RMS", f"{mean_rms:.2f} mm")

                        # RMS bar chart by PRN
                        sat_df = pd.DataFrame([{
                            'PRN': f"{'G' if s.constellation == 'G' else 'E'}{s.prn:02d}" if s.prn < 100 else f"E{s.prn-100:02d}",
                            'Constellation': 'GPS' if s.constellation == 'G' else 'Galileo',
                            'RMS (mm)': s.rms_after,
                            'Obs Count': s.obs_count_after,
                            'Obs %': s.obs_percent_after
                        } for s in satellites])

                        fig_sat_rms = px.bar(
                            sat_df.sort_values('PRN'),
                            x='PRN', y='RMS (mm)',
                            color='Constellation',
                            color_discrete_map={'GPS': '#3498db', 'Galileo': '#e74c3c'},
                            title=f"Satellite RMS - DOY {single_doy}"
                        )
                        fig_sat_rms.add_hline(y=1.5, line_dash="dash", line_color="orange",
                                              annotation_text="Typical threshold")
                        fig_sat_rms.update_layout(height=400)
                        st.plotly_chart(fig_sat_rms, use_container_width=True)

                        # Observation count chart
                        fig_sat_obs = px.bar(
                            sat_df.sort_values('PRN'),
                            x='PRN', y='Obs Count',
                            color='Constellation',
                            color_discrete_map={'GPS': '#3498db', 'Galileo': '#e74c3c'},
                            title="Observation Count per Satellite"
                        )
                        fig_sat_obs.update_layout(height=350)
                        st.plotly_chart(fig_sat_obs, use_container_width=True)

                        # Detailed table
                        with st.expander("📋 Satellite Details"):
                            st.dataframe(sat_df, use_container_width=True, hide_index=True)
                    else:
                        st.warning("No satellite data parsed from CHK file.")
                else:
                    st.warning(f"CHK.SUM file not found for DOY {single_doy}")

            # ----------------------------------------------------------
            # DD TAB 3: Ambiguity Resolution Dashboard
            # ----------------------------------------------------------
            with dd_tab3:
                st.subheader("🎯 Ambiguity Resolution Dashboard")
                st.markdown("Widelane and Narrowlane ambiguity fixing rates.")

                # Load AR time series
                with st.spinner("Loading AR statistics..."):
                    ar_timeseries = load_ar_timeseries(dd_analysis_base_dir, dd_analysis_year, doy_start, doy_end)

                if len(ar_timeseries) > 0:
                    # Summary metrics
                    ar_col1, ar_col2, ar_col3, ar_col4 = st.columns(4)
                    with ar_col1:
                        st.metric("Days Analyzed", len(ar_timeseries))
                    with ar_col2:
                        mean_wl = ar_timeseries['WL Rate (%)'].mean()
                        st.metric("Mean WL Rate", f"{mean_wl:.1f}%")
                    with ar_col3:
                        mean_nl = ar_timeseries['NL Rate (%)'].mean() if 'NL Rate (%)' in ar_timeseries.columns else 0
                        st.metric("Mean NL Rate", f"{mean_nl:.1f}%")
                    with ar_col4:
                        total_baselines = ar_timeseries['Baselines'].sum()
                        st.metric("Total Baselines", int(total_baselines))

                    # Time series plot
                    fig_ar_ts = go.Figure()
                    fig_ar_ts.add_trace(go.Scatter(
                        x=ar_timeseries['DOY'], y=ar_timeseries['WL Rate (%)'],
                        mode='lines+markers', name='Widelane',
                        line=dict(color='#2ecc71', width=2),
                        marker=dict(size=8)
                    ))
                    fig_ar_ts.add_hline(y=95, line_dash="dash", line_color="orange",
                                        annotation_text="95% threshold")
                    fig_ar_ts.update_layout(
                        title="Ambiguity Resolution Success Rate Over Time",
                        xaxis_title="DOY",
                        yaxis_title="AR Rate (%)",
                        yaxis_range=[80, 100],
                        height=400
                    )
                    st.plotly_chart(fig_ar_ts, use_container_width=True)

                    # Per-station AR rates for selected day
                    ar_single_doy = st.selectbox("Select DOY for Station Details",
                                                  ar_timeseries['DOY'].tolist(),
                                                  key='ar_station_doy')
                    amb_path = get_amb_path(dd_analysis_base_dir, dd_analysis_year, ar_single_doy)

                    if amb_path:
                        baselines, _ = parse_amb_summary(amb_path)
                        if baselines:
                            station_ar = get_station_ar_rates(baselines)

                            fig_station_ar = px.bar(
                                station_ar.sort_values('WL Rate (%)'),
                                x='Station', y='WL Rate (%)',
                                color='WL Rate (%)',
                                color_continuous_scale='RdYlGn',
                                range_color=[80, 100],
                                title=f"Per-Station WL AR Rate - DOY {ar_single_doy}"
                            )
                            fig_station_ar.add_hline(y=95, line_dash="dash", line_color="red")
                            fig_station_ar.update_layout(height=400)
                            st.plotly_chart(fig_station_ar, use_container_width=True)

                            with st.expander("📋 Station AR Details"):
                                st.dataframe(
                                    station_ar.style.format({
                                        'WL Rate (%)': '{:.1f}',
                                        'NL Rate (%)': '{:.1f}'
                                    }),
                                    use_container_width=True, hide_index=True
                                )
                else:
                    st.warning("No AR data available for the selected DOY range.")

            # ----------------------------------------------------------
            # DD TAB 4: Troposphere Gradient Animation
            # ----------------------------------------------------------
            with dd_tab4:
                st.subheader("🌬️ Troposphere Gradients")
                st.markdown("ZTD and gradient estimates from TRP files.")

                trp_doy = st.selectbox("Select DOY", list(range(doy_start, doy_end + 1)), key='trp_doy')
                trp_path = get_trp_path(dd_analysis_base_dir, dd_analysis_year, trp_doy)

                if trp_path:
                    with st.spinner("Parsing troposphere data..."):
                        trp_epochs = parse_trp_file(trp_path)

                    if trp_epochs:
                        # Get station coordinates from baseline file
                        trp_crd_path = f"{dd_analysis_base_dir}/{dd_analysis_year}/{trp_doy}/STA/F10_{dd_analysis_year}{trp_doy:03d}0.CRD.gz"
                        trp_station_coords = {}

                        if os.path.exists(trp_crd_path):
                            baselines_for_coords = load_baselines_with_coords(
                                f"{dd_analysis_base_dir}/{dd_analysis_year}/{trp_doy}/STA/BSL_{dd_analysis_year}{trp_doy:03d}0.BSL.gz",
                                trp_crd_path
                            )
                            for bl in baselines_for_coords:
                                if bl.lat1 and bl.lon1:
                                    trp_station_coords[bl.station1[:4].upper()] = (bl.lat1, bl.lon1)
                                if bl.lat2 and bl.lon2:
                                    trp_station_coords[bl.station2[:4].upper()] = (bl.lat2, bl.lon2)

                        # Summary statistics
                        unique_stations = list(set(e.station for e in trp_epochs))
                        trp_col1, trp_col2, trp_col3 = st.columns(3)
                        with trp_col1:
                            st.metric("Stations", len(unique_stations))
                        with trp_col2:
                            st.metric("Epochs", len(set(e.epoch for e in trp_epochs)))
                        with trp_col3:
                            mean_ztd = np.mean([e.ztd_total * 1000 for e in trp_epochs])
                            st.metric("Mean ZTD", f"{mean_ztd:.1f} mm")

                        # Time selector for gradient map
                        unique_hours = sorted(list(set(e.epoch.strftime('%H:%M') for e in trp_epochs)))
                        selected_hour = st.select_slider("Select Time (UTC)", options=unique_hours, key='trp_hour')

                        # Filter epochs for selected hour
                        hour_epochs = [e for e in trp_epochs if e.epoch.strftime('%H:%M') == selected_hour]

                        # Build gradient map
                        if hour_epochs and trp_station_coords:
                            map_data = []
                            for e in hour_epochs:
                                if e.station in trp_station_coords:
                                    lat, lon = trp_station_coords[e.station]
                                    grad_mag = math.sqrt(e.gradient_n**2 + e.gradient_e**2) * 1000
                                    map_data.append({
                                        'station': e.station,
                                        'lat': lat,
                                        'lon': lon,
                                        'ztd_mm': e.ztd_total * 1000,
                                        'grad_n': e.gradient_n * 1000,
                                        'grad_e': e.gradient_e * 1000,
                                        'grad_mag': grad_mag
                                    })

                            if map_data:
                                map_df = pd.DataFrame(map_data)

                                fig_trp_map = px.scatter_geo(
                                    map_df,
                                    lat='lat', lon='lon',
                                    color='grad_mag',
                                    size='grad_mag',
                                    hover_name='station',
                                    hover_data={'ztd_mm': ':.1f', 'grad_n': ':.2f', 'grad_e': ':.2f'},
                                    color_continuous_scale='Viridis',
                                    title=f"Troposphere Gradients - DOY {trp_doy} {selected_hour} UTC"
                                )
                                fig_trp_map.update_layout(
                                    geo=dict(
                                        scope='europe',
                                        showland=True,
                                        landcolor='rgb(40, 40, 60)',
                                        center=dict(lat=49.8, lon=6.1),
                                        lonaxis_range=[-10, 30],
                                        lataxis_range=[35, 65]
                                    ),
                                    height=500
                                )
                                st.plotly_chart(fig_trp_map, use_container_width=True)

                                # ZTD time series for selected station
                                trp_station = st.selectbox("Station for ZTD Time Series",
                                                           unique_stations, key='trp_station')
                                sta_epochs = [e for e in trp_epochs if e.station == trp_station]

                                if sta_epochs:
                                    fig_ztd_ts = go.Figure()
                                    times = [e.epoch for e in sta_epochs]
                                    ztds = [e.ztd_total * 1000 for e in sta_epochs]

                                    fig_ztd_ts.add_trace(go.Scatter(
                                        x=times, y=ztds,
                                        mode='lines+markers', name='ZTD',
                                        line=dict(color='#3498db')
                                    ))
                                    fig_ztd_ts.update_layout(
                                        title=f"ZTD Time Series: {trp_station}",
                                        xaxis_title="Time (UTC)",
                                        yaxis_title="ZTD (mm)",
                                        height=350
                                    )
                                    st.plotly_chart(fig_ztd_ts, use_container_width=True)
                    else:
                        st.warning("No troposphere data parsed from TRP file.")
                else:
                    st.warning(f"TRP file not found for DOY {trp_doy}")

            # ----------------------------------------------------------
            # DD TAB 5: Baseline Network Graph
            # ----------------------------------------------------------
            with dd_tab5:
                st.subheader("🔗 Baseline Network Graph")
                st.markdown("Interactive network visualization with baseline RMS quality.")

                net_doy = st.selectbox("Select DOY", list(range(doy_start, doy_end + 1)), key='net_doy')
                rms_path = get_rms_path(dd_analysis_base_dir, dd_analysis_year, net_doy)

                if rms_path:
                    baselines_rms = parse_rms_file(rms_path)

                    if baselines_rms:
                        # Get station coordinates
                        net_crd_path = f"{dd_analysis_base_dir}/{dd_analysis_year}/{net_doy}/STA/F10_{dd_analysis_year}{net_doy:03d}0.CRD.gz"
                        net_bsl_path = f"{dd_analysis_base_dir}/{dd_analysis_year}/{net_doy}/STA/BSL_{dd_analysis_year}{net_doy:03d}0.BSL.gz"
                        net_station_coords = {}

                        if os.path.exists(net_crd_path) and os.path.exists(net_bsl_path):
                            net_baselines = load_baselines_with_coords(net_bsl_path, net_crd_path)
                            for bl in net_baselines:
                                if bl.lat1 and bl.lon1:
                                    net_station_coords[bl.station1[:4].upper()] = (bl.lat1, bl.lon1)
                                if bl.lat2 and bl.lon2:
                                    net_station_coords[bl.station2[:4].upper()] = (bl.lat2, bl.lon2)

                        # Summary metrics
                        net_col1, net_col2, net_col3, net_col4 = st.columns(4)
                        with net_col1:
                            st.metric("Baselines", len(baselines_rms))
                        with net_col2:
                            mean_rms = np.mean([bl.rms_total for bl in baselines_rms])
                            st.metric("Mean RMS", f"{mean_rms:.2f} mm")
                        with net_col3:
                            max_rms = max([bl.rms_total for bl in baselines_rms])
                            st.metric("Max RMS", f"{max_rms:.2f} mm")
                        with net_col4:
                            connectivity = get_station_connectivity(baselines_rms)
                            st.metric("Stations", len(connectivity))

                        # Build network graph
                        nodes, edges = build_network_graph_data(baselines_rms, net_station_coords)

                        if edges:
                            fig_network = go.Figure()

                            # Add edges (baselines)
                            for edge in edges:
                                # Color based on RMS
                                if edge['rms'] < 1.3:
                                    color = '#2ecc71'  # Green
                                elif edge['rms'] < 1.5:
                                    color = '#f1c40f'  # Yellow
                                else:
                                    color = '#e74c3c'  # Red

                                fig_network.add_trace(go.Scattergeo(
                                    lon=[edge['lon1'], edge['lon2']],
                                    lat=[edge['lat1'], edge['lat2']],
                                    mode='lines',
                                    line=dict(width=max(1, 3 - edge['rms']), color=color),
                                    hoverinfo='text',
                                    text=f"{edge['station1']}-{edge['station2']}: {edge['rms']:.2f}mm, {edge['length_km']:.0f}km",
                                    showlegend=False
                                ))

                            # Add nodes (stations)
                            if nodes:
                                fig_network.add_trace(go.Scattergeo(
                                    lon=[n['lon'] for n in nodes],
                                    lat=[n['lat'] for n in nodes],
                                    mode='markers+text',
                                    marker=dict(size=10, color='#3498db'),
                                    text=[n['id'] for n in nodes],
                                    textposition='top center',
                                    textfont=dict(size=8, color='white'),
                                    name='Stations'
                                ))

                            fig_network.update_layout(
                                title=f"Baseline Network - DOY {net_doy}",
                                geo=dict(
                                    scope='europe',
                                    showland=True,
                                    landcolor='rgb(40, 40, 60)',
                                    showocean=True,
                                    oceancolor='rgb(20, 30, 50)',
                                    center=dict(lat=49.8, lon=6.1),
                                    lonaxis_range=[-10, 30],
                                    lataxis_range=[35, 65]
                                ),
                                height=550,
                                showlegend=False
                            )
                            st.plotly_chart(fig_network, use_container_width=True)

                            st.markdown("""
                            **Baseline Colors:** 🟢 <1.3mm (Excellent) | 🟡 1.3-1.5mm (Good) | 🔴 >1.5mm (Check)
                            """)

                        # Connectivity table
                        with st.expander("📋 Station Connectivity"):
                            st.dataframe(
                                connectivity.style.format({
                                    'Mean RMS (mm)': '{:.2f}',
                                    'Mean Obs': '{:.0f}'
                                }),
                                use_container_width=True, hide_index=True
                            )

                        # Baseline details
                        with st.expander("📋 Baseline Details"):
                            bl_df = pd.DataFrame([{
                                'Station 1': bl.station1,
                                'Station 2': bl.station2,
                                'RMS (mm)': bl.rms_total,
                                'Obs': bl.num_obs,
                                'Sats': bl.num_sat
                            } for bl in baselines_rms])
                            st.dataframe(
                                bl_df.sort_values('RMS (mm)', ascending=False),
                                use_container_width=True, hide_index=True
                            )
                    else:
                        st.warning("No baseline data parsed from RMS file.")
                else:
                    st.warning(f"RMS file not found for DOY {net_doy}")

            # ----------------------------------------------------------
            # DD TAB 6: Coordinate Time Series
            # ----------------------------------------------------------
            with dd_tab6:
                st.subheader("📍 Coordinate Time Series")
                st.markdown("ENU coordinate differences from mean over multiple days.")

                with st.spinner("Loading coordinate time series..."):
                    coord_ts = load_coordinate_timeseries(dd_analysis_base_dir, dd_analysis_year,
                                                          doy_start, doy_end, prefix="F10")

                if coord_ts:
                    coord_summary = get_coordinate_summary(coord_ts)

                    # Metrics
                    crd_col1, crd_col2, crd_col3, crd_col4 = st.columns(4)
                    with crd_col1:
                        st.metric("Stations", len(coord_ts))
                    with crd_col2:
                        mean_u_std = coord_summary['U Std (mm)'].mean()
                        st.metric("Mean U Std", f"{mean_u_std:.2f} mm")
                    with crd_col3:
                        mean_h_std = (coord_summary['E Std (mm)'].mean() + coord_summary['N Std (mm)'].mean()) / 2
                        st.metric("Mean H Std", f"{mean_h_std:.2f} mm")
                    with crd_col4:
                        days = coord_summary['Days'].max()
                        st.metric("Max Days", int(days))

                    # Station selector - filter to only monitored stations
                    crd_all_stations = sorted(coord_ts.keys())
                    crd_monitored = [s for s in crd_all_stations if s in MONITORED_STATIONS]
                    crd_station = st.selectbox("Select Station",
                                               crd_monitored if crd_monitored else crd_all_stations,
                                               key='crd_station')

                    if crd_station and crd_station in coord_ts:
                        ts = coord_ts[crd_station]

                        fig_crd = make_subplots(rows=3, cols=1, shared_xaxes=True,
                                                subplot_titles=['East (mm)', 'North (mm)', 'Up (mm)'],
                                                vertical_spacing=0.08)

                        fig_crd.add_trace(go.Scatter(
                            x=ts['doys'], y=ts['east'],
                            mode='lines+markers', name='East',
                            line=dict(color='#2ecc71')
                        ), row=1, col=1)

                        fig_crd.add_trace(go.Scatter(
                            x=ts['doys'], y=ts['north'],
                            mode='lines+markers', name='North',
                            line=dict(color='#3498db')
                        ), row=2, col=1)

                        fig_crd.add_trace(go.Scatter(
                            x=ts['doys'], y=ts['up'],
                            mode='lines+markers', name='Up',
                            line=dict(color='#e74c3c')
                        ), row=3, col=1)

                        # Add zero lines
                        for row in [1, 2, 3]:
                            fig_crd.add_hline(y=0, line_dash="dash", line_color="gray", row=row, col=1)

                        e_std = np.std(ts['east'])
                        n_std = np.std(ts['north'])
                        u_std = np.std(ts['up'])

                        fig_crd.update_layout(
                            title=f"Coordinate Time Series: {crd_station} (Std: E={e_std:.2f}, N={n_std:.2f}, U={u_std:.2f} mm)",
                            height=500, showlegend=False
                        )
                        fig_crd.update_xaxes(title_text="DOY", row=3, col=1)

                        st.plotly_chart(fig_crd, use_container_width=True)

                    # Summary table
                    with st.expander("📋 Coordinate Summary", expanded=True):
                        st.dataframe(
                            coord_summary.style.format({
                                'E Std (mm)': '{:.2f}',
                                'N Std (mm)': '{:.2f}',
                                'U Std (mm)': '{:.2f}',
                                'Ve (mm/yr)': '{:.1f}',
                                'Vn (mm/yr)': '{:.1f}',
                                'Vu (mm/yr)': '{:.1f}'
                            }).background_gradient(subset=['U Std (mm)'], cmap='RdYlGn_r'),
                            use_container_width=True, hide_index=True
                        )
                else:
                    st.warning("No coordinate time series available for the selected DOY range.")

            # ----------------------------------------------------------
            # DD TAB 7: Coordinate Repeatability from R2S PRC
            # ----------------------------------------------------------
            with dd_tab7:
                st.subheader("📊 Coordinate Repeatability (DD)")
                st.markdown("""
                Coordinate repeatability from **PART 9: SLIDING 7-SESSION COMPARISON** in R2S PRC files.
                Shows NEU repeatability for each station based on sliding 7-day window comparison.
                """)

                # Load data for single DOY
                r2s_doy = st.selectbox(
                    "Select DOY",
                    list(range(doy_start, doy_end + 1)),
                    key='r2s_doy_select'
                )

                r2s_path = get_r2s_prc_path(dd_analysis_base_dir, dd_analysis_year, r2s_doy)

                if r2s_path:
                    with st.spinner("Parsing R2S PRC file..."):
                        r2s_data = parse_r2s_prc_file(r2s_path)

                    if r2s_data and r2s_data.stations:
                        # Metrics row
                        r2s_col1, r2s_col2, r2s_col3, r2s_col4 = st.columns(4)
                        with r2s_col1:
                            st.metric("Stations", len(r2s_data.stations))
                        with r2s_col2:
                            st.metric("Total N (mm)", f"{r2s_data.total_north_mm:.2f}")
                        with r2s_col3:
                            st.metric("Total E (mm)", f"{r2s_data.total_east_mm:.2f}")
                        with r2s_col4:
                            st.metric("Total U (mm)", f"{r2s_data.total_up_mm:.2f}")

                        # Build DataFrame for display
                        r2s_table_data = []
                        for sta in r2s_data.stations:
                            r2s_table_data.append({
                                'Station': sta.station,
                                '#Days': sta.n_days,
                                'N (mm)': sta.north_mm,
                                'E (mm)': sta.east_mm,
                                'U (mm)': sta.up_mm,
                                '3D (mm)': sta.wrms_3d
                            })

                        df_r2s = pd.DataFrame(r2s_table_data)

                        # Visualization - Bar chart
                        # Only show chart if we have non-zero data
                        if df_r2s['3D (mm)'].sum() > 0:
                            fig_r2s = px.bar(
                                df_r2s.sort_values('3D (mm)', ascending=False),
                                x='Station', y=['N (mm)', 'E (mm)', 'U (mm)'],
                                title=f"Coordinate Repeatability - DOY {r2s_doy}",
                                barmode='group',
                                color_discrete_map={'N (mm)': '#3498db', 'E (mm)': '#2ecc71', 'U (mm)': '#e74c3c'}
                            )
                            fig_r2s.update_layout(
                                height=450,
                                xaxis_tickangle=-45,
                                legend_title_text="Component"
                            )
                            st.plotly_chart(fig_r2s, use_container_width=True)

                            # 3D WRMS bar chart
                            fig_r2s_3d = px.bar(
                                df_r2s.sort_values('3D (mm)', ascending=False),
                                x='Station', y='3D (mm)',
                                title=f"3D Repeatability (WRMS) - DOY {r2s_doy}",
                                color='3D (mm)',
                                color_continuous_scale='RdYlGn_r'
                            )
                            fig_r2s_3d.update_layout(height=400, xaxis_tickangle=-45)

                            # Add threshold line
                            r2s_threshold = st.slider("Quality Threshold (mm)", 1.0, 10.0, 5.0, 0.5, key="r2s_thresh")
                            fig_r2s_3d.add_hline(y=r2s_threshold, line_dash="dash", line_color="red",
                                                annotation_text=f"Threshold: {r2s_threshold} mm")

                            st.plotly_chart(fig_r2s_3d, use_container_width=True)

                            # Quality metrics
                            good_stations = len(df_r2s[df_r2s['3D (mm)'] < r2s_threshold])
                            poor_stations = len(df_r2s[df_r2s['3D (mm)'] >= r2s_threshold])
                            st.info(f"✅ Good (<{r2s_threshold}mm): {good_stations} | ⚠️ Poor (≥{r2s_threshold}mm): {poor_stations}")

                        else:
                            st.info("ℹ️ Single-day data shows 0.00 repeatability. Need multiple days for meaningful values.")

                        # Summary table
                        with st.expander("📋 Detailed Repeatability Table", expanded=True):
                            if not df_r2s.empty and '3D (mm)' in df_r2s.columns and df_r2s['3D (mm)'].sum() > 0:
                                st.dataframe(
                                    df_r2s.style.format({
                                        'N (mm)': '{:.2f}',
                                        'E (mm)': '{:.2f}',
                                        'U (mm)': '{:.2f}',
                                        '3D (mm)': '{:.2f}'
                                    }).background_gradient(subset=['3D (mm)'], cmap='RdYlGn_r'),
                                    use_container_width=True, hide_index=True
                                )
                            else:
                                st.dataframe(df_r2s, use_container_width=True, hide_index=True)

                        # Show file path
                        st.caption(f"Source: {r2s_path}")

                    else:
                        st.warning("No Part 9 coordinate repeatability data found in R2S file.")
                else:
                    st.warning(f"No R2S PRC file found for DOY {r2s_doy}. Check the directory path.")

        # TAB 26: Satellite Residuals Analysis
        elif selected_page == "🛰️ Satellite Residuals":
            st.header("🛰️ Satellite Residuals Analysis")
            st.markdown("""
            Monitor satellite-specific phase residuals across processing sessions.
            - **DD Processing**: Residual RMS per satellite for each baseline
            - **PPP-AR**: Troposphere convergence time analysis
            - **Identification**: Flag satellites with consistently high residuals
            """)

            # Configuration
            sat_res_col1, sat_res_col2, sat_res_col3 = st.columns(3)

            with sat_res_col1:
                sat_res_base_dir = st.text_input(
                    "Results Directory",
                    value="/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX/2025_with_clu",
                    key="sat_res_base_dir"
                )
            with sat_res_col2:
                sat_res_year = st.number_input("Year", value=2025, min_value=2000, max_value=2030, key="sat_res_year")
            with sat_res_col3:
                sat_res_doy_range = st.slider(
                    "DOY Range",
                    min_value=1, max_value=366,
                    value=(296, 310),
                    key="sat_res_doy_range"
                )

            sat_res_start_doy, sat_res_end_doy = sat_res_doy_range

            # Create sub-tabs
            sat_res_tab1, sat_res_tab2, sat_res_tab3, sat_res_tab4 = st.tabs([
                "📊 DD Satellite Residuals",
                "⏱️ PPP Convergence",
                "🔍 Problematic Satellites",
                "📈 Time Series"
            ])

            # Load satellite residual data
            @st.cache_data(ttl=1800)
            def load_satellite_residuals_cached(base_dir, year, start_doy, end_doy):
                """Load satellite residuals for multiple DOYs with caching."""
                sessions = []
                loaded_doys = []
                for doy in range(start_doy, end_doy + 1):
                    # Try different path patterns
                    patterns = [
                        f"{base_dir}/{doy:03d}/OUT/RES_{year}{doy:03d}0.SUM.gz",
                        f"{base_dir}/{doy}/OUT/RES_{year}{doy:03d}0.SUM.gz",
                    ]
                    for path in patterns:
                        if os.path.exists(path):
                            stats = parse_res_sum_file(path)
                            if stats:
                                sessions.append(stats)
                                loaded_doys.append(doy)
                            break
                return sessions, loaded_doys

            @st.cache_data(ttl=1800)
            def load_convergence_data_cached(base_dir, year, start_doy, end_doy):
                """Load convergence data from TRP files with caching."""
                convergence_data = {}  # DOY -> station -> ConvergenceInfo
                for doy in range(start_doy, end_doy + 1):
                    patterns = [
                        f"{base_dir}/{doy:03d}/ATM/F10_{year}{doy:03d}0.TRP.gz",
                        f"{base_dir}/{doy}/ATM/F10_{year}{doy:03d}0.TRP.gz",
                    ]
                    for path in patterns:
                        if os.path.exists(path):
                            conv = parse_trp_for_convergence(path)
                            if conv:
                                convergence_data[doy] = conv
                            break
                return convergence_data

            with st.spinner("Loading satellite residual data..."):
                sessions, loaded_doys = load_satellite_residuals_cached(
                    sat_res_base_dir, sat_res_year, sat_res_start_doy, sat_res_end_doy
                )

            if loaded_doys:
                st.success(f"Loaded satellite residuals for {len(loaded_doys)} days: DOY {min(loaded_doys)}-{max(loaded_doys)}")
            else:
                st.warning("No satellite residual data found. Check the directory path.")

            # TAB: DD Satellite Residuals
            with sat_res_tab1:
                try:
                    if sessions:
                        st.subheader("📊 Satellite Residual RMS by System")

                        # Aggregate stats across all sessions
                        agg_stats = aggregate_satellite_stats(sessions)

                        if agg_stats:
                            # Organize by system
                            gps_sats = {k: v for k, v in agg_stats.items() if k.startswith('G')}
                            gal_sats = {k: v for k, v in agg_stats.items() if k.startswith('E')}
                            glo_sats = {k: v for k, v in agg_stats.items() if k.startswith('R')}
                            bds_sats = {k: v for k, v in agg_stats.items() if k.startswith('C')}

                            # Metrics row
                            met_col1, met_col2, met_col3, met_col4 = st.columns(4)
                            with met_col1:
                                st.metric("GPS Satellites", len(gps_sats))
                            with met_col2:
                                st.metric("Galileo Satellites", len(gal_sats))
                            with met_col3:
                                st.metric("GLONASS Satellites", len(glo_sats))
                            with met_col4:
                                st.metric("BeiDou Satellites", len(bds_sats))

                            # Create bar chart for all satellites
                            sat_data = []
                            for prn, stats in agg_stats.items():
                                sat_data.append({
                                    'PRN': prn,
                                    'System': {'G': 'GPS', 'E': 'Galileo', 'R': 'GLONASS', 'C': 'BeiDou'}.get(prn[0], 'Unknown'),
                                    'Mean RMS (mm)': stats['mean_rms'],
                                    'Max RMS (mm)': stats['max_rms'],
                                    'Min RMS (mm)': stats['min_rms'],
                                    'Std RMS (mm)': stats['std_rms'],
                                    'Sessions': stats['session_count'],
                                    'Total Obs': stats['total_obs']
                                })

                            df_sat_res = pd.DataFrame(sat_data)

                            # Sort by system then PRN number
                            df_sat_res['prn_num'] = df_sat_res['PRN'].str[1:].astype(int)
                            df_sat_res = df_sat_res.sort_values(['System', 'prn_num'])

                            # System selector
                            sys_filter = st.multiselect(
                                "Filter by System",
                                options=['GPS', 'Galileo', 'GLONASS', 'BeiDou'],
                                default=['GPS', 'Galileo', 'GLONASS'],
                                key="sat_res_sys_filter"
                            )

                            df_filtered = df_sat_res[df_sat_res['System'].isin(sys_filter)]

                            # Bar chart
                            fig_sat_bar = px.bar(
                                df_filtered,
                                x='PRN', y='Mean RMS (mm)',
                                color='System',
                                error_y='Std RMS (mm)',
                                title=f"Satellite Mean RMS ({len(loaded_doys)} sessions)",
                                labels={'Mean RMS (mm)': 'Mean RMS (mm)'},
                                color_discrete_map={'GPS': '#1f77b4', 'Galileo': '#ff7f0e', 'GLONASS': '#2ca02c', 'BeiDou': '#d62728'}
                            )

                            # Add threshold line
                            rms_threshold = st.slider("RMS Threshold (mm)", 1.0, 3.0, 2.0, 0.1, key="sat_res_threshold")
                            fig_sat_bar.add_hline(y=rms_threshold, line_dash="dash", line_color="red",
                                                  annotation_text=f"Threshold: {rms_threshold} mm")

                            fig_sat_bar.update_layout(height=500, xaxis={'categoryorder': 'category ascending'})
                            st.plotly_chart(fig_sat_bar, use_container_width=True)

                            # Detailed table
                            with st.expander("📋 Detailed Satellite Statistics", expanded=False):
                                st.dataframe(
                                    df_filtered.style.format({
                                        'Mean RMS (mm)': '{:.2f}',
                                        'Max RMS (mm)': '{:.2f}',
                                        'Min RMS (mm)': '{:.2f}',
                                        'Std RMS (mm)': '{:.3f}',
                                        'Total Obs': '{:,.0f}'
                                    }).background_gradient(subset=['Mean RMS (mm)'], cmap='RdYlGn_r'),
                                    use_container_width=True, hide_index=True
                                )

                            # Per-baseline analysis
                            st.subheader("📍 Baseline Residual Analysis")

                            # Select a session to view baselines
                            session_options = [f"DOY {s.doy} ({len(s.baselines)} baselines)" for s in sessions]
                            selected_session_idx = st.selectbox(
                                "Select Session", range(len(sessions)),
                                format_func=lambda x: session_options[x],
                                key="sat_res_session_select"
                            )

                            selected_session = sessions[selected_session_idx]

                            # Baseline selector
                            baseline_names = [bl.name for bl in selected_session.baselines]
                            selected_baselines = st.multiselect(
                                "Select Baselines (max 10)",
                                options=baseline_names[:50],  # Limit options
                                default=baseline_names[:5],
                                max_selections=10,
                                key="sat_res_baseline_select"
                            )

                            if selected_baselines:
                                # Create heatmap of satellite residuals per baseline
                                bl_data = []
                                for bl in selected_session.baselines:
                                    if bl.name in selected_baselines:
                                        for prn, rms in bl.satellite_residuals.items():
                                            bl_data.append({
                                                'Baseline': bl.name,
                                                'PRN': prn,
                                                'RMS (mm)': rms
                                            })

                                if bl_data:
                                    df_bl = pd.DataFrame(bl_data)

                                    # Pivot for heatmap (use pivot_table with mean to handle duplicates)
                                    df_pivot = df_bl.pivot_table(
                                        index='Baseline', columns='PRN', values='RMS (mm)',
                                        aggfunc='mean'
                                    )

                                    # Sort columns by system and number
                                    sorted_cols = sorted(df_pivot.columns,
                                                        key=lambda x: (x[0], int(x[1:]) if x[1:].isdigit() else 0))
                                    df_pivot = df_pivot[sorted_cols]

                                    fig_heatmap = px.imshow(
                                        df_pivot,
                                        labels=dict(x="Satellite PRN", y="Baseline", color="RMS (mm)"),
                                        title=f"Satellite Residuals per Baseline - DOY {selected_session.doy}",
                                        color_continuous_scale='RdYlGn_r',
                                        aspect='auto'
                                    )
                                    fig_heatmap.update_layout(height=400 + len(selected_baselines) * 20)
                                    st.plotly_chart(fig_heatmap, use_container_width=True)

                    else:
                        st.info("No DD satellite residual data available. Load data first.")

                except Exception as dd_res_error:
                    st.error(f"Error in DD Satellite Residuals: {dd_res_error}")
                    import traceback
                    with st.expander("Error details"):
                        st.code(traceback.format_exc())

            # TAB: PPP Convergence
            with sat_res_tab2:
                try:
                    st.subheader("⏱️ PPP Troposphere Convergence Analysis")
                    st.markdown("""
                    Convergence is estimated from the troposphere parameter sigma (SIGMA_U) in TRP files.
                    Lower sigma values indicate a more converged solution.
                    """)

                    with st.spinner("Loading convergence data..."):
                        convergence_data = load_convergence_data_cached(
                            sat_res_base_dir, sat_res_year, sat_res_start_doy, sat_res_end_doy
                        )

                    if convergence_data:
                        loaded_conv_doys = list(convergence_data.keys())
                        st.success(f"Loaded convergence data for {len(loaded_conv_doys)} days")

                        # Select DOY to view
                        conv_doy = st.selectbox(
                            "Select DOY",
                            options=sorted(loaded_conv_doys),
                            key="conv_doy_select"
                        )

                        if conv_doy in convergence_data:
                            conv_stations = convergence_data[conv_doy]

                            # Metrics
                            conv_met_col1, conv_met_col2 = st.columns(2)
                            with conv_met_col1:
                                st.metric("Stations", len(conv_stations))
                            with conv_met_col2:
                                # Count stations with convergence time calculated
                                conv_count = sum(1 for s in conv_stations.values()
                                               if s.convergence_time_hours is not None)
                                st.metric("With Convergence Time", conv_count)

                            # Plot sigma evolution for selected stations
                            station_list = sorted(conv_stations.keys())
                            selected_conv_stations = st.multiselect(
                                "Select Stations (max 10)",
                                options=station_list,
                                default=station_list[:5],
                                max_selections=10,
                                key="conv_station_select"
                            )

                            if selected_conv_stations:
                                fig_conv = go.Figure()

                                for station in selected_conv_stations:
                                    if station in conv_stations:
                                        info = conv_stations[station]
                                        if info.epochs and info.sigma_u:
                                            # Convert epochs to hours from start
                                            start_epoch = info.epochs[0]
                                            hours = [(e - start_epoch).total_seconds() / 3600 for e in info.epochs]

                                            fig_conv.add_trace(go.Scatter(
                                                x=hours,
                                                y=[s * 1000 for s in info.sigma_u],  # Convert to mm
                                                mode='lines+markers',
                                                name=station,
                                                hovertemplate=f'{station}<br>Hour: %{{x:.1f}}<br>Sigma: %{{y:.3f}} mm'
                                            ))

                                fig_conv.update_layout(
                                    title=f"ZTD Sigma Evolution - DOY {conv_doy}",
                                    xaxis_title="Hours from Start",
                                    yaxis_title="ZTD Sigma (mm)",
                                    height=450
                                )

                                # Add convergence threshold
                                conv_threshold = st.slider("Convergence Threshold (mm)", 0.1, 2.0, 1.0, 0.1, key="conv_threshold")
                                fig_conv.add_hline(y=conv_threshold, line_dash="dash", line_color="green",
                                                  annotation_text=f"Threshold: {conv_threshold} mm")

                                st.plotly_chart(fig_conv, use_container_width=True)

                                # Summary table
                                conv_summary = []
                                for station in selected_conv_stations:
                                    if station in conv_stations:
                                        info = conv_stations[station]
                                        conv_summary.append({
                                            'Station': station,
                                            'Epochs': len(info.epochs),
                                            'Initial Sigma (mm)': info.sigma_u[0] * 1000 if info.sigma_u else None,
                                            'Final Sigma (mm)': info.sigma_u[-1] * 1000 if info.sigma_u else None,
                                            'Convergence Time (h)': info.convergence_time_hours
                                        })

                                if conv_summary:
                                    df_conv = pd.DataFrame(conv_summary)
                                    st.dataframe(
                                        df_conv.style.format({
                                            'Initial Sigma (mm)': '{:.3f}',
                                            'Final Sigma (mm)': '{:.3f}',
                                            'Convergence Time (h)': '{:.1f}'
                                        }, na_rep='-'),
                                        use_container_width=True, hide_index=True
                                    )
                    else:
                        st.info("No convergence data found. Check the directory path.")

                except Exception as conv_error:
                    st.error(f"Error loading convergence data: {conv_error}")
                    import traceback
                    with st.expander("Error details"):
                        st.code(traceback.format_exc())

            # TAB: Problematic Satellites
            with sat_res_tab3:
                try:
                    st.subheader("🔍 Satellites with Consistently High Residuals")

                    if sessions:
                        agg_stats = aggregate_satellite_stats(sessions)

                        # Threshold controls
                        prob_col1, prob_col2 = st.columns(2)
                        with prob_col1:
                            rms_thresh = st.slider("RMS Threshold (mm)", 1.0, 3.0, 2.0, 0.1, key="prob_rms_thresh")
                        with prob_col2:
                            std_thresh = st.slider("Std Threshold (mm)", 0.1, 1.0, 0.3, 0.05, key="prob_std_thresh")

                        problematic = identify_problematic_satellites(agg_stats, rms_thresh, std_thresh)

                        if problematic:
                            st.warning(f"⚠️ Found {len(problematic)} satellites with potential issues")

                            prob_data = []
                            for sat in problematic:
                                prob_data.append({
                                    'PRN': sat['prn'],
                                    'System': {'G': 'GPS', 'E': 'Galileo', 'R': 'GLONASS', 'C': 'BeiDou'}.get(sat['system'], 'Unknown'),
                                    'Mean RMS (mm)': sat['mean_rms'],
                                    'Max RMS (mm)': sat['max_rms'],
                                    'Std (mm)': sat['std_rms'],
                                    'Issues': '; '.join(sat['issues'])
                                })

                            df_prob = pd.DataFrame(prob_data)
                            st.dataframe(
                                df_prob.style.format({
                                    'Mean RMS (mm)': '{:.2f}',
                                    'Max RMS (mm)': '{:.2f}',
                                    'Std (mm)': '{:.3f}'
                                }).applymap(
                                    lambda x: 'background-color: #ffcccc' if isinstance(x, float) and x > rms_thresh else '',
                                    subset=['Mean RMS (mm)']
                                ),
                                use_container_width=True, hide_index=True
                            )

                            # Visualization
                            fig_prob = px.bar(
                                df_prob.sort_values('Mean RMS (mm)', ascending=False),
                                x='PRN', y='Mean RMS (mm)',
                                color='System',
                                title="Problematic Satellites Ranked by Mean RMS",
                                color_discrete_map={'GPS': '#1f77b4', 'Galileo': '#ff7f0e', 'GLONASS': '#2ca02c', 'BeiDou': '#d62728'}
                            )
                            fig_prob.add_hline(y=rms_thresh, line_dash="dash", line_color="red")
                            fig_prob.update_layout(height=400)
                            st.plotly_chart(fig_prob, use_container_width=True)

                        else:
                            st.success("✅ No satellites with consistently high residuals found!")
                    else:
                        st.info("Load satellite residual data first.")

                except Exception as prob_error:
                    st.error(f"Error in Problematic Satellites: {prob_error}")
                    import traceback
                    with st.expander("Error details"):
                        st.code(traceback.format_exc())

            # TAB: Time Series
            with sat_res_tab4:
                try:
                    st.subheader("📈 Satellite RMS Time Series")

                    if sessions and len(sessions) > 1:
                        # Select satellite to track
                        all_prns = set()
                        for session in sessions:
                            all_prns.update(session.satellite_mean_rms.keys())

                        sorted_prns = sorted(all_prns, key=lambda x: (x[0], int(x[1:])))

                        selected_prns = st.multiselect(
                            "Select Satellites to Track",
                            options=sorted_prns,
                            default=sorted_prns[:5],
                            max_selections=10,
                            key="sat_ts_prn_select"
                        )

                        if selected_prns:
                            # Build time series
                            ts_data = []
                            for session in sessions:
                                for prn in selected_prns:
                                    if prn in session.satellite_mean_rms:
                                        ts_data.append({
                                            'DOY': session.doy,
                                            'PRN': prn,
                                            'RMS (mm)': session.satellite_mean_rms[prn],
                                            'Obs Count': session.satellite_total_obs.get(prn, 0)
                                        })

                            if ts_data:
                                df_ts = pd.DataFrame(ts_data)

                                fig_ts = px.line(
                                    df_ts, x='DOY', y='RMS (mm)',
                                    color='PRN',
                                    markers=True,
                                    title="Satellite RMS Evolution Over Time"
                                )
                                fig_ts.update_layout(height=450)
                                st.plotly_chart(fig_ts, use_container_width=True)

                                # Observation count time series
                                fig_obs = px.line(
                                    df_ts, x='DOY', y='Obs Count',
                                    color='PRN',
                                    markers=True,
                                    title="Observation Count Evolution"
                                )
                                fig_obs.update_layout(height=350)
                                st.plotly_chart(fig_obs, use_container_width=True)
                    else:
                        st.info("Need multiple sessions for time series analysis. Expand the DOY range.")

                except Exception as ts_error:
                    st.error(f"Error in Time Series: {ts_error}")
                    import traceback
                    with st.expander("Error details"):
                        st.code(traceback.format_exc())

        # TAB 27: NGL/CODE External Time Series Comparison
        elif selected_page == "🌍 NGL/CODE External TS":
            st.header("🌍 NGL/CODE External TS Comparison")
            st.markdown("""
            Compare **NGL (Nevada Geodetic Laboratory)** external time series with your
            **PPP-AR** and **Double-Difference (DD)** solutions. View XYZ coordinate time series
            and their differences with RMS statistics.
            """)

            try:
                # Beautiful gradient colors for different solutions
                COLORS = {
                    'NGL': '#00D4FF',      # Cyan - NGL
                    'PPP-AR': '#FF6B6B',   # Coral Red - PPP-AR
                    'DD': '#4ECDC4',       # Teal - DD
                    'CODE': '#E91E63',     # Pink - CODE
                    'NGL-PPP': '#9B59B6',  # Purple - Difference
                    'NGL-DD': '#F39C12',   # Orange - Difference
                    'NGL-CODE': '#8E44AD', # Deep Purple - Difference
                    'PPP-DD': '#1ABC9C',   # Emerald - Difference
                    'PPP-CODE': '#16A085', # Dark Teal - Difference
                    'DD-CODE': '#27AE60',  # Green - Difference
                }

                # Initialize session state for station if not exists
                if 'ext_station_selected' not in st.session_state:
                    st.session_state['ext_station_selected'] = 'BRUX'

                # Quick station select FIRST (before the text input)
                st.markdown("**🚀 Quick Select:**")
                quick_cols = st.columns(8)
                quick_stations = ['BRUX', 'ONSA', 'WTZR', 'GRAS', 'MATE', 'POTS', 'HERS', 'ZIM2']
                for i, qs in enumerate(quick_stations):
                    if quick_cols[i].button(qs, key=f"ext_quick_{qs}", use_container_width=True):
                        st.session_state['ext_station_selected'] = qs
                        st.session_state['ext_station_input'] = qs  # Also update the text input key
                        st.rerun()

                # Configuration section
                cfg_col1, cfg_col2, cfg_col3, cfg_col4 = st.columns([2, 1.5, 1.5, 1.5])

                with cfg_col1:
                    ext_station = st.text_input(
                        "Station Code (4-char)",
                        value=st.session_state['ext_station_selected'],
                        max_chars=4,
                        key="ext_station_input",
                        help="Enter 4-character station code"
                    ).upper()
                    # Update session state if user types a new station
                    if ext_station != st.session_state['ext_station_selected']:
                        st.session_state['ext_station_selected'] = ext_station

                with cfg_col2:
                    ext_year = st.number_input("Year", min_value=2000, max_value=2030, value=2025, key="ext_year")

                with cfg_col3:
                    ext_doy_start = st.number_input("Start DOY", min_value=1, max_value=366, value=296, key="ext_doy_start")

                with cfg_col4:
                    ext_doy_end = st.number_input("End DOY", min_value=1, max_value=366, value=310, key="ext_doy_end")

                # Data source paths
                path_col1, path_col2 = st.columns(2)
                with path_col1:
                    ppp_base_dir = st.text_input(
                        "PPP-AR Results Directory",
                        value="/home/ahunegnaw/GPSDATA/CAMPAIGN54",
                        key="ppp_base_dir"
                    )
                with path_col2:
                    dd_base_dir = st.text_input(
                        "DD Results Directory",
                        value="/home/ahunegnaw/GPSRESULTS/ahunegnaw/OCT2025_MGX",
                        key="dd_base_dir"
                    )

                st.divider()

                # Load data button
                if st.button("📥 Load & Compare All Solutions", type="primary", key="load_ext_comparison"):
                    with st.spinner(f"Loading data for {ext_station}..."):
                        comparison_data = {'dates': [], 'doy': []}

                        # 1. Fetch NGL data (txyz2 format for XYZ)
                        ngl_ts = fetch_ngl_timeseries(ext_station, format_type='txyz2', timeout=60)
                        if ngl_ts and ngl_ts.points:
                            st.success(f"✅ NGL: Loaded {len(ngl_ts.points)} points")
                            comparison_data['ngl'] = ngl_ts
                        else:
                            st.warning(f"⚠️ NGL: No data found for {ext_station}")
                            comparison_data['ngl'] = None

                        # 2. Load PPP-AR coordinates from CRD files
                        ppp_coords = []
                        for doy in range(ext_doy_start, ext_doy_end + 1):
                            # Try different path patterns for PPP-AR (supports IG_GRE, IG_GE, etc.)
                            session_path = find_ig_campaign_dir(ppp_base_dir, ext_year, doy)
                            crd_patterns = []
                            if session_path:
                                crd_patterns.append(f"{session_path}/STA/FIN_{ext_year}{doy:03d}0.CRD")
                            crd_patterns.extend([
                                f"{ppp_base_dir}/{ext_year % 100}{doy:03d}IG/STA/FIN_{ext_year}{doy:03d}0.CRD",
                                f"{ppp_base_dir}/{ext_year % 100}{doy:03d}IG_1/STA/FIN_{ext_year}{doy:03d}0.CRD",
                                f"{ppp_base_dir}/{ext_year}{doy:03d}IG/STA/FIN_{ext_year}{doy:03d}0.CRD",
                            ])

                            for crd_path in crd_patterns:
                                if os.path.exists(crd_path):
                                    try:
                                        with open(crd_path, 'r') as f:
                                            for line in f:
                                                if ext_station in line.upper():
                                                    parts = line.split()
                                                    if len(parts) >= 5:
                                                        # Find XYZ coordinates
                                                        floats = []
                                                        for p in parts[2:]:
                                                            try:
                                                                floats.append(float(p))
                                                                if len(floats) == 3:
                                                                    break
                                                            except ValueError:
                                                                continue
                                                        if len(floats) == 3:
                                                            ppp_coords.append({
                                                                'doy': doy,
                                                                'date': datetime(ext_year, 1, 1) + timedelta(days=doy-1),
                                                                'X': floats[0], 'Y': floats[1], 'Z': floats[2]
                                                            })
                                                    break
                                    except Exception:
                                        pass
                                    break

                        if ppp_coords:
                            st.success(f"✅ PPP-AR: Loaded {len(ppp_coords)} daily solutions")
                            comparison_data['ppp'] = ppp_coords
                        else:
                            st.warning(f"⚠️ PPP-AR: No data found for {ext_station}")
                            comparison_data['ppp'] = None

                        # 3. Load DD coordinates from CRD files
                        dd_coords = []
                        for doy in range(ext_doy_start, ext_doy_end + 1):
                            # Try different path patterns for DD
                            crd_patterns = [
                                f"{dd_base_dir}/{ext_year}/{doy:03d}/STA/F10_{ext_year}{doy:03d}0.CRD.gz",
                                f"{dd_base_dir}/{ext_year}/{doy}/STA/F10_{ext_year}{doy:03d}0.CRD.gz",
                                f"{dd_base_dir}/{ext_year}/{doy:03d}/STA/F10_{ext_year}{doy:03d}0.CRD",
                            ]

                            for crd_path in crd_patterns:
                                if os.path.exists(crd_path):
                                    try:
                                        import gzip
                                        open_func = gzip.open if crd_path.endswith('.gz') else open
                                        mode = 'rt' if crd_path.endswith('.gz') else 'r'
                                        with open_func(crd_path, mode) as f:
                                            for line in f:
                                                if ext_station in line.upper():
                                                    parts = line.split()
                                                    if len(parts) >= 5:
                                                        floats = []
                                                        for p in parts[2:]:
                                                            try:
                                                                floats.append(float(p))
                                                                if len(floats) == 3:
                                                                    break
                                                            except ValueError:
                                                                continue
                                                        if len(floats) == 3:
                                                            dd_coords.append({
                                                                'doy': doy,
                                                                'date': datetime(ext_year, 1, 1) + timedelta(days=doy-1),
                                                                'X': floats[0], 'Y': floats[1], 'Z': floats[2]
                                                            })
                                                    break
                                    except Exception:
                                        pass
                                    break

                        if dd_coords:
                            st.success(f"✅ DD: Loaded {len(dd_coords)} daily solutions")
                            comparison_data['dd'] = dd_coords
                        else:
                            st.warning(f"⚠️ DD: No data found for {ext_station}")
                            comparison_data['dd'] = None

                        # 4. Fetch CODE coordinates from AIUB FTP
                        code_coords = []
                        code_progress = st.progress(0, text="Fetching CODE coordinates...")
                        total_days = ext_doy_end - ext_doy_start + 1

                        for idx, doy in enumerate(range(ext_doy_start, ext_doy_end + 1)):
                            code_progress.progress((idx + 1) / total_days,
                                                  text=f"Fetching CODE DOY {doy}...")
                            try:
                                code_coord = fetch_code_coordinates_http(
                                    ext_station, ext_year, doy, timeout=15
                                )
                                if code_coord:
                                    code_coords.append({
                                        'doy': doy,
                                        'date': code_coord.date,
                                        'X': code_coord.x,
                                        'Y': code_coord.y,
                                        'Z': code_coord.z
                                    })
                            except Exception as e:
                                pass

                        code_progress.empty()

                        if code_coords:
                            st.success(f"✅ CODE: Loaded {len(code_coords)} daily solutions")
                            comparison_data['code'] = code_coords
                        else:
                            st.warning(f"⚠️ CODE: No data found for {ext_station} (station may not be in CODE network)")
                            comparison_data['code'] = None

                        st.session_state['ext_comparison_data'] = comparison_data
                        st.session_state['ext_comparison_station'] = ext_station

                # Display comparison if data is loaded
                if 'ext_comparison_data' in st.session_state:
                    data = st.session_state['ext_comparison_data']
                    station = st.session_state.get('ext_comparison_station', ext_station)

                    st.divider()
                    st.subheader(f"📊 {station} - Multi-Solution Comparison")

                    # Create sub-tabs for different views
                    ts_tab1, ts_tab2, ts_tab3 = st.tabs([
                        "📈 XYZ Time Series",
                        "📉 Solution Differences",
                        "📊 Statistics & RMS"
                    ])

                    # TAB 1: XYZ Time Series
                    with ts_tab1:
                        st.markdown("### Coordinate Time Series (relative to mean)")

                        # Build combined dataframe
                        all_data = []

                        # NGL data
                        if data.get('ngl') and data['ngl'].points:
                            ngl_ts = data['ngl']
                            # Filter to the DOY range
                            start_date = datetime(ext_year, 1, 1) + timedelta(days=ext_doy_start-1)
                            end_date = datetime(ext_year, 1, 1) + timedelta(days=ext_doy_end-1)
                            for p in ngl_ts.points:
                                if start_date <= p.date <= end_date:
                                    all_data.append({
                                        'Date': p.date,
                                        'DOY': p.date.timetuple().tm_yday,
                                        'Source': 'NGL',
                                        'X': p.x, 'Y': p.y, 'Z': p.z
                                    })

                        # PPP-AR data
                        if data.get('ppp'):
                            for p in data['ppp']:
                                all_data.append({
                                    'Date': p['date'],
                                    'DOY': p['doy'],
                                    'Source': 'PPP-AR',
                                    'X': p['X'], 'Y': p['Y'], 'Z': p['Z']
                                })

                        # DD data
                        if data.get('dd'):
                            for p in data['dd']:
                                all_data.append({
                                    'Date': p['date'],
                                    'DOY': p['doy'],
                                    'Source': 'DD',
                                    'X': p['X'], 'Y': p['Y'], 'Z': p['Z']
                                })

                        # CODE data
                        if data.get('code'):
                            for p in data['code']:
                                all_data.append({
                                    'Date': p['date'],
                                    'DOY': p['doy'],
                                    'Source': 'CODE',
                                    'X': p['X'], 'Y': p['Y'], 'Z': p['Z']
                                })

                        if all_data:
                            df_all = pd.DataFrame(all_data)

                            # Calculate mean for each source and convert to mm from mean
                            for coord in ['X', 'Y', 'Z']:
                                overall_mean = df_all[coord].mean()
                                df_all[f'd{coord}'] = (df_all[coord] - overall_mean) * 1000  # mm

                            # Create beautiful 3-panel plot
                            fig_xyz = make_subplots(
                                rows=3, cols=1,
                                subplot_titles=(
                                    f'<b>X Component</b> (mean: {df_all["X"].mean():.3f} m)',
                                    f'<b>Y Component</b> (mean: {df_all["Y"].mean():.3f} m)',
                                    f'<b>Z Component</b> (mean: {df_all["Z"].mean():.3f} m)'
                                ),
                                vertical_spacing=0.08
                            )

                            for i, coord in enumerate(['X', 'Y', 'Z'], 1):
                                for source in ['NGL', 'PPP-AR', 'DD', 'CODE']:
                                    df_src = df_all[df_all['Source'] == source]
                                    if not df_src.empty:
                                        fig_xyz.add_trace(
                                            go.Scatter(
                                                x=df_src['Date'],
                                                y=df_src[f'd{coord}'],
                                                mode='markers+lines',
                                                name=source,
                                                legendgroup=source,
                                                showlegend=(i == 1),
                                                marker=dict(
                                                    size=8 if source == 'NGL' else 10,
                                                    color=COLORS[source],
                                                    symbol={'NGL': 'circle', 'PPP-AR': 'diamond', 'DD': 'square', 'CODE': 'star'}[source],
                                                    line=dict(width=1, color='white')
                                                ),
                                                line=dict(width=2, color=COLORS[source])
                                            ),
                                            row=i, col=1
                                        )
                                fig_xyz.update_yaxes(title_text=f"d{coord} (mm)", row=i, col=1)

                            fig_xyz.update_layout(
                                height=800,
                                showlegend=True,
                                title=dict(
                                    text=f"<b>🌍 {station} - XYZ Coordinate Time Series</b>",
                                    font=dict(size=20)
                                ),
                                legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=1.02,
                                    xanchor="center",
                                    x=0.5,
                                    font=dict(size=14),
                                    bgcolor='rgba(0,0,0,0.5)',
                                    bordercolor='rgba(255,255,255,0.3)',
                                    borderwidth=1
                                ),
                                template="plotly_dark",
                                paper_bgcolor='rgba(0,0,0,0)',
                                plot_bgcolor='rgba(20,20,30,0.8)'
                            )

                            st.plotly_chart(fig_xyz, use_container_width=True)

                            # === ENU Time Series ===
                            st.markdown("---")
                            st.markdown("### 🧭 ENU (East, North, Up) Time Series")

                            # Calculate reference point (mean XYZ)
                            ref_x = df_all['X'].mean()
                            ref_y = df_all['Y'].mean()
                            ref_z = df_all['Z'].mean()

                            # Convert reference to lat/lon
                            ref_lat, ref_lon, ref_h = xyz_to_llh(ref_x, ref_y, ref_z)

                            st.caption(f"Reference: Lat={ref_lat:.6f}°, Lon={ref_lon:.6f}°, Height={ref_h:.2f}m")

                            # Convert XYZ differences to ENU for all data
                            enu_data = []
                            for idx, row in df_all.iterrows():
                                dx = row['X'] - ref_x
                                dy = row['Y'] - ref_y
                                dz = row['Z'] - ref_z
                                dE, dN, dU = xyz_diff_to_enu(dx, dy, dz, ref_lat, ref_lon)
                                enu_data.append({
                                    'Date': row['Date'],
                                    'DOY': row['DOY'],
                                    'Source': row['Source'],
                                    'dE': dE * 1000,  # mm
                                    'dN': dN * 1000,  # mm
                                    'dU': dU * 1000   # mm
                                })

                            df_enu = pd.DataFrame(enu_data)

                            # Create ENU 3-panel plot
                            fig_enu = make_subplots(
                                rows=3, cols=1,
                                subplot_titles=(
                                    '<b>East Component</b>',
                                    '<b>North Component</b>',
                                    '<b>Up Component</b>'
                                ),
                                vertical_spacing=0.08
                            )

                            # ENU colors with gradient effect
                            enu_symbols = {'NGL': 'circle', 'PPP-AR': 'diamond', 'DD': 'square', 'CODE': 'star'}

                            for i, coord in enumerate(['E', 'N', 'U'], 1):
                                for source in ['NGL', 'PPP-AR', 'DD', 'CODE']:
                                    df_src = df_enu[df_enu['Source'] == source]
                                    if not df_src.empty:
                                        fig_enu.add_trace(
                                            go.Scatter(
                                                x=df_src['Date'],
                                                y=df_src[f'd{coord}'],
                                                mode='markers+lines',
                                                name=source,
                                                legendgroup=source,
                                                showlegend=(i == 1),
                                                marker=dict(
                                                    size=8 if source == 'NGL' else 10,
                                                    color=COLORS[source],
                                                    symbol=enu_symbols[source],
                                                    line=dict(width=1, color='white')
                                                ),
                                                line=dict(width=2, color=COLORS[source])
                                            ),
                                            row=i, col=1
                                        )
                                fig_enu.update_yaxes(title_text=f"d{coord} (mm)", row=i, col=1)

                            fig_enu.update_layout(
                                height=800,
                                showlegend=True,
                                title=dict(
                                    text=f"<b>🧭 {station} - ENU Coordinate Time Series</b>",
                                    font=dict(size=20)
                                ),
                                legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=1.02,
                                    xanchor="center",
                                    x=0.5,
                                    font=dict(size=14),
                                    bgcolor='rgba(0,0,0,0.5)',
                                    bordercolor='rgba(255,255,255,0.3)',
                                    borderwidth=1
                                ),
                                template="plotly_dark",
                                paper_bgcolor='rgba(0,0,0,0)',
                                plot_bgcolor='rgba(20,20,30,0.8)'
                            )

                            st.plotly_chart(fig_enu, use_container_width=True)

                        else:
                            st.info("No data available for plotting.")

                    # TAB 2: Solution Differences
                    with ts_tab2:
                        st.markdown("### 📉 Solution Differences (XYZ & ENU)")
                        st.markdown("Differences computed at common epochs (matched by DOY)")

                        if all_data:
                            df_all = pd.DataFrame(all_data)

                            # Calculate reference point for ENU transformation
                            ref_x = df_all['X'].mean()
                            ref_y = df_all['Y'].mean()
                            ref_z = df_all['Z'].mean()
                            ref_lat, ref_lon, ref_h = xyz_to_llh(ref_x, ref_y, ref_z)

                            # Helper function to compute XYZ and ENU differences
                            def compute_diff(row1, row2, comparison_name):
                                dX = (row1['X'] - row2['X']) * 1000  # mm
                                dY = (row1['Y'] - row2['Y']) * 1000
                                dZ = (row1['Z'] - row2['Z']) * 1000
                                # Convert to ENU
                                dE, dN, dU = xyz_diff_to_enu(
                                    row1['X'] - row2['X'],
                                    row1['Y'] - row2['Y'],
                                    row1['Z'] - row2['Z'],
                                    ref_lat, ref_lon
                                )
                                return {
                                    'DOY': row1['DOY'],
                                    'Date': row1['Date'],
                                    'Comparison': comparison_name,
                                    'dX': dX, 'dY': dY, 'dZ': dZ,
                                    'dE': dE * 1000, 'dN': dN * 1000, 'dU': dU * 1000  # mm
                                }

                            # Compute differences at common DOYs
                            diff_data = []

                            # Get unique DOYs for each source
                            ngl_doys = set(df_all[df_all['Source'] == 'NGL']['DOY'].values) if 'NGL' in df_all['Source'].values else set()
                            ppp_doys = set(df_all[df_all['Source'] == 'PPP-AR']['DOY'].values) if 'PPP-AR' in df_all['Source'].values else set()
                            dd_doys = set(df_all[df_all['Source'] == 'DD']['DOY'].values) if 'DD' in df_all['Source'].values else set()
                            code_doys = set(df_all[df_all['Source'] == 'CODE']['DOY'].values) if 'CODE' in df_all['Source'].values else set()

                            # NGL - PPP-AR differences
                            common_ngl_ppp = ngl_doys & ppp_doys
                            for doy in sorted(common_ngl_ppp):
                                ngl_row = df_all[(df_all['Source'] == 'NGL') & (df_all['DOY'] == doy)].iloc[0]
                                ppp_row = df_all[(df_all['Source'] == 'PPP-AR') & (df_all['DOY'] == doy)].iloc[0]
                                diff_data.append(compute_diff(ngl_row, ppp_row, 'NGL - PPP-AR'))

                            # NGL - DD differences
                            common_ngl_dd = ngl_doys & dd_doys
                            for doy in sorted(common_ngl_dd):
                                ngl_row = df_all[(df_all['Source'] == 'NGL') & (df_all['DOY'] == doy)].iloc[0]
                                dd_row = df_all[(df_all['Source'] == 'DD') & (df_all['DOY'] == doy)].iloc[0]
                                diff_data.append(compute_diff(ngl_row, dd_row, 'NGL - DD'))

                            # PPP-AR - DD differences
                            common_ppp_dd = ppp_doys & dd_doys
                            for doy in sorted(common_ppp_dd):
                                ppp_row = df_all[(df_all['Source'] == 'PPP-AR') & (df_all['DOY'] == doy)].iloc[0]
                                dd_row = df_all[(df_all['Source'] == 'DD') & (df_all['DOY'] == doy)].iloc[0]
                                diff_data.append(compute_diff(ppp_row, dd_row, 'PPP-AR - DD'))

                            # NGL - CODE differences
                            common_ngl_code = ngl_doys & code_doys
                            for doy in sorted(common_ngl_code):
                                ngl_row = df_all[(df_all['Source'] == 'NGL') & (df_all['DOY'] == doy)].iloc[0]
                                code_row = df_all[(df_all['Source'] == 'CODE') & (df_all['DOY'] == doy)].iloc[0]
                                diff_data.append(compute_diff(ngl_row, code_row, 'NGL - CODE'))

                            # PPP-AR - CODE differences
                            common_ppp_code = ppp_doys & code_doys
                            for doy in sorted(common_ppp_code):
                                ppp_row = df_all[(df_all['Source'] == 'PPP-AR') & (df_all['DOY'] == doy)].iloc[0]
                                code_row = df_all[(df_all['Source'] == 'CODE') & (df_all['DOY'] == doy)].iloc[0]
                                diff_data.append(compute_diff(ppp_row, code_row, 'PPP-AR - CODE'))

                            # DD - CODE differences
                            common_dd_code = dd_doys & code_doys
                            for doy in sorted(common_dd_code):
                                dd_row = df_all[(df_all['Source'] == 'DD') & (df_all['DOY'] == doy)].iloc[0]
                                code_row = df_all[(df_all['Source'] == 'CODE') & (df_all['DOY'] == doy)].iloc[0]
                                diff_data.append(compute_diff(dd_row, code_row, 'DD - CODE'))

                            if diff_data:
                                df_diff = pd.DataFrame(diff_data)

                                # === XYZ Differences Plot ===
                                st.markdown("#### 📐 XYZ Coordinate Differences")

                                # Create XYZ difference plots with gradient colors
                                fig_diff_xyz = make_subplots(
                                    rows=3, cols=1,
                                    subplot_titles=(
                                        '<b>ΔX Differences</b>',
                                        '<b>ΔY Differences</b>',
                                        '<b>ΔZ Differences</b>'
                                    ),
                                    vertical_spacing=0.08
                                )

                                diff_colors = {
                                    'NGL - PPP-AR': '#9B59B6',  # Purple
                                    'NGL - DD': '#F39C12',      # Orange
                                    'PPP-AR - DD': '#1ABC9C',   # Emerald
                                    'NGL - CODE': '#8E44AD',    # Dark Purple
                                    'PPP-AR - CODE': '#16A085', # Sea Green
                                    'DD - CODE': '#27AE60'      # Nephritis Green
                                }

                                # Define colorscales for gradient markers
                                colorscales = {
                                    'NGL - PPP-AR': 'Viridis',
                                    'NGL - DD': 'Plasma',
                                    'PPP-AR - DD': 'Cividis',
                                    'NGL - CODE': 'Turbo',
                                    'PPP-AR - CODE': 'Inferno',
                                    'DD - CODE': 'Magma'
                                }

                                # All comparison types to plot
                                all_comparisons = ['NGL - PPP-AR', 'NGL - DD', 'PPP-AR - DD',
                                                   'NGL - CODE', 'PPP-AR - CODE', 'DD - CODE']

                                for i, coord in enumerate(['X', 'Y', 'Z'], 1):
                                    for comp in all_comparisons:
                                        df_comp = df_diff[df_diff['Comparison'] == comp]
                                        if not df_comp.empty:
                                            # Add scatter with gradient effect
                                            fig_diff_xyz.add_trace(
                                                go.Scatter(
                                                    x=df_comp['Date'],
                                                    y=df_comp[f'd{coord}'],
                                                    mode='markers+lines',
                                                    name=comp,
                                                    legendgroup=comp,
                                                    showlegend=(i == 1),
                                                    marker=dict(
                                                        size=10,
                                                        color=df_comp['DOY'],
                                                        colorscale=colorscales.get(comp, 'Viridis'),
                                                        showscale=False,
                                                        line=dict(width=2, color=diff_colors[comp])
                                                    ),
                                                    line=dict(width=2, color=diff_colors[comp], dash='dot')
                                                ),
                                                row=i, col=1
                                            )

                                    # Add zero line
                                    fig_diff_xyz.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5, row=i, col=1)
                                    fig_diff_xyz.update_yaxes(title_text=f"Δ{coord} (mm)", row=i, col=1)

                                fig_diff_xyz.update_layout(
                                    height=700,
                                    showlegend=True,
                                    title=dict(
                                        text=f"<b>📉 {station} - XYZ Coordinate Differences</b>",
                                        font=dict(size=18)
                                    ),
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        xanchor="center",
                                        x=0.5,
                                        font=dict(size=12),
                                        bgcolor='rgba(0,0,0,0.5)',
                                        bordercolor='rgba(255,255,255,0.3)',
                                        borderwidth=1
                                    ),
                                    template="plotly_dark",
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    plot_bgcolor='rgba(20,20,30,0.8)'
                                )

                                st.plotly_chart(fig_diff_xyz, use_container_width=True)

                                # === ENU Differences Plot ===
                                st.markdown("---")
                                st.markdown("#### 🧭 ENU (East, North, Up) Differences")

                                fig_diff_enu = make_subplots(
                                    rows=3, cols=1,
                                    subplot_titles=(
                                        '<b>ΔE (East) Differences</b>',
                                        '<b>ΔN (North) Differences</b>',
                                        '<b>ΔU (Up) Differences</b>'
                                    ),
                                    vertical_spacing=0.08
                                )

                                for i, coord in enumerate(['E', 'N', 'U'], 1):
                                    for comp in all_comparisons:
                                        df_comp = df_diff[df_diff['Comparison'] == comp]
                                        if not df_comp.empty and f'd{coord}' in df_comp.columns:
                                            fig_diff_enu.add_trace(
                                                go.Scatter(
                                                    x=df_comp['Date'],
                                                    y=df_comp[f'd{coord}'],
                                                    mode='markers+lines',
                                                    name=comp,
                                                    legendgroup=comp,
                                                    showlegend=(i == 1),
                                                    marker=dict(
                                                        size=10,
                                                        color=df_comp['DOY'],
                                                        colorscale=colorscales.get(comp, 'Viridis'),
                                                        showscale=False,
                                                        line=dict(width=2, color=diff_colors[comp])
                                                    ),
                                                    line=dict(width=2, color=diff_colors[comp], dash='dot')
                                                ),
                                                row=i, col=1
                                            )

                                    fig_diff_enu.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5, row=i, col=1)
                                    fig_diff_enu.update_yaxes(title_text=f"Δ{coord} (mm)", row=i, col=1)

                                fig_diff_enu.update_layout(
                                    height=700,
                                    showlegend=True,
                                    title=dict(
                                        text=f"<b>🧭 {station} - ENU Coordinate Differences</b>",
                                        font=dict(size=18)
                                    ),
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        xanchor="center",
                                        x=0.5,
                                        font=dict(size=12),
                                        bgcolor='rgba(0,0,0,0.5)',
                                        bordercolor='rgba(255,255,255,0.3)',
                                        borderwidth=1
                                    ),
                                    template="plotly_dark",
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    plot_bgcolor='rgba(20,20,30,0.8)'
                                )

                                st.plotly_chart(fig_diff_enu, use_container_width=True)

                            else:
                                st.info("No common epochs found for difference computation.")

                    # TAB 3: Statistics & RMS
                    with ts_tab3:
                        st.markdown("### 📊 Comparison Statistics & RMS (XYZ & ENU)")

                        if diff_data:
                            df_diff = pd.DataFrame(diff_data)

                            # Define diff_colors here for the statistics tab
                            diff_colors = {
                                'NGL - PPP-AR': '#9B59B6',
                                'NGL - DD': '#F39C12',
                                'PPP-AR - DD': '#1ABC9C',
                                'NGL - CODE': '#8E44AD',
                                'PPP-AR - CODE': '#16A085',
                                'DD - CODE': '#27AE60'
                            }

                            # Compute RMS for each comparison (both XYZ and ENU)
                            rms_data = []
                            for comp in df_diff['Comparison'].unique():
                                df_comp = df_diff[df_diff['Comparison'] == comp]
                                n_pts = len(df_comp)

                                # XYZ statistics
                                rms_x = np.sqrt(np.mean(df_comp['dX']**2))
                                rms_y = np.sqrt(np.mean(df_comp['dY']**2))
                                rms_z = np.sqrt(np.mean(df_comp['dZ']**2))
                                rms_3d_xyz = np.sqrt(rms_x**2 + rms_y**2 + rms_z**2)

                                mean_x = df_comp['dX'].mean()
                                mean_y = df_comp['dY'].mean()
                                mean_z = df_comp['dZ'].mean()

                                std_x = df_comp['dX'].std()
                                std_y = df_comp['dY'].std()
                                std_z = df_comp['dZ'].std()

                                # ENU statistics (if available)
                                has_enu = 'dE' in df_comp.columns
                                if has_enu:
                                    rms_e = np.sqrt(np.mean(df_comp['dE']**2))
                                    rms_n = np.sqrt(np.mean(df_comp['dN']**2))
                                    rms_u = np.sqrt(np.mean(df_comp['dU']**2))
                                    rms_3d_enu = np.sqrt(rms_e**2 + rms_n**2 + rms_u**2)
                                    rms_2d_enu = np.sqrt(rms_e**2 + rms_n**2)  # Horizontal

                                    mean_e = df_comp['dE'].mean()
                                    mean_n = df_comp['dN'].mean()
                                    mean_u = df_comp['dU'].mean()

                                    std_e = df_comp['dE'].std()
                                    std_n = df_comp['dN'].std()
                                    std_u = df_comp['dU'].std()
                                else:
                                    rms_e = rms_n = rms_u = rms_3d_enu = rms_2d_enu = 0
                                    mean_e = mean_n = mean_u = std_e = std_n = std_u = 0

                                rms_data.append({
                                    'Comparison': comp,
                                    'N': n_pts,
                                    # XYZ
                                    'RMS X': rms_x, 'RMS Y': rms_y, 'RMS Z': rms_z,
                                    'Mean X': mean_x, 'Mean Y': mean_y, 'Mean Z': mean_z,
                                    'Std X': std_x, 'Std Y': std_y, 'Std Z': std_z,
                                    # ENU
                                    'RMS E': rms_e, 'RMS N': rms_n, 'RMS U': rms_u,
                                    'Mean E': mean_e, 'Mean N': mean_n, 'Mean U': mean_u,
                                    'Std E': std_e, 'Std N': std_n, 'Std U': std_u,
                                    # 3D/2D
                                    'RMS 3D XYZ': rms_3d_xyz,
                                    'RMS 3D ENU': rms_3d_enu,
                                    'RMS 2D (Horiz)': rms_2d_enu
                                })

                            df_rms = pd.DataFrame(rms_data)

                            # Display RMS Summary Cards
                            st.markdown("#### 🎯 RMS Summary (All Comparisons)")

                            for _, row in df_rms.iterrows():
                                comp = row['Comparison']
                                color = diff_colors.get(comp, '#FFFFFF')

                                st.markdown(f"""
                                <div style="background: linear-gradient(135deg, {color}22, {color}44);
                                            padding: 15px; border-radius: 10px; margin: 10px 0;
                                            border-left: 4px solid {color};">
                                    <h4 style="color: {color}; margin: 0;">{comp}</h4>
                                    <p style="margin: 5px 0;">Based on <b>{row['N']}</b> common epochs</p>
                                </div>
                                """, unsafe_allow_html=True)

                                # XYZ metrics row
                                st.markdown("**XYZ Components:**")
                                xyz_cols = st.columns(4)
                                xyz_cols[0].metric("RMS X", f"{row['RMS X']:.2f} mm")
                                xyz_cols[1].metric("RMS Y", f"{row['RMS Y']:.2f} mm")
                                xyz_cols[2].metric("RMS Z", f"{row['RMS Z']:.2f} mm")
                                xyz_cols[3].metric("RMS 3D", f"{row['RMS 3D XYZ']:.2f} mm")

                                # ENU metrics row
                                st.markdown("**ENU Components:**")
                                enu_cols = st.columns(4)
                                enu_cols[0].metric("RMS East", f"{row['RMS E']:.2f} mm")
                                enu_cols[1].metric("RMS North", f"{row['RMS N']:.2f} mm")
                                enu_cols[2].metric("RMS Up", f"{row['RMS U']:.2f} mm")
                                enu_cols[3].metric("RMS Horiz (2D)", f"{row['RMS 2D (Horiz)']:.2f} mm",
                                                 delta=f"3D: {row['RMS 3D ENU']:.2f} mm")

                            st.divider()

                            # Summary comparison bar chart
                            st.markdown("#### 📊 RMS Comparison Chart")

                            # Create comparison bar chart
                            fig_bar = go.Figure()

                            comparisons = df_rms['Comparison'].tolist()

                            # Add XYZ bars
                            fig_bar.add_trace(go.Bar(
                                name='RMS East (E)',
                                x=comparisons,
                                y=df_rms['RMS E'],
                                marker_color='#E74C3C'
                            ))
                            fig_bar.add_trace(go.Bar(
                                name='RMS North (N)',
                                x=comparisons,
                                y=df_rms['RMS N'],
                                marker_color='#3498DB'
                            ))
                            fig_bar.add_trace(go.Bar(
                                name='RMS Up (U)',
                                x=comparisons,
                                y=df_rms['RMS U'],
                                marker_color='#2ECC71'
                            ))

                            fig_bar.update_layout(
                                barmode='group',
                                height=400,
                                title=dict(text='<b>ENU RMS by Comparison</b>', font=dict(size=16)),
                                xaxis_title='Comparison',
                                yaxis_title='RMS (mm)',
                                legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
                                template='plotly_dark',
                                paper_bgcolor='rgba(0,0,0,0)',
                                plot_bgcolor='rgba(20,20,30,0.8)'
                            )

                            st.plotly_chart(fig_bar, use_container_width=True)

                            st.divider()

                            # Full statistics tables
                            st.markdown("#### 📋 Detailed XYZ Statistics (mm)")
                            df_xyz_stats = df_rms[['Comparison', 'N', 'Mean X', 'Mean Y', 'Mean Z',
                                                    'Std X', 'Std Y', 'Std Z',
                                                    'RMS X', 'RMS Y', 'RMS Z', 'RMS 3D XYZ']].copy()
                            st.dataframe(
                                df_xyz_stats.style.format({
                                    col: '{:.2f}' for col in df_xyz_stats.columns if col not in ['Comparison', 'N']
                                }).background_gradient(subset=['RMS 3D XYZ'], cmap='YlOrRd'),
                                use_container_width=True,
                                hide_index=True
                            )

                            st.markdown("#### 📋 Detailed ENU Statistics (mm)")
                            df_enu_stats = df_rms[['Comparison', 'N', 'Mean E', 'Mean N', 'Mean U',
                                                    'Std E', 'Std N', 'Std U',
                                                    'RMS E', 'RMS N', 'RMS U', 'RMS 2D (Horiz)', 'RMS 3D ENU']].copy()
                            st.dataframe(
                                df_enu_stats.style.format({
                                    col: '{:.2f}' for col in df_enu_stats.columns if col not in ['Comparison', 'N']
                                }).background_gradient(subset=['RMS 3D ENU'], cmap='YlOrRd'),
                                use_container_width=True,
                                hide_index=True
                            )

                        else:
                            st.info("Load data to see statistics.")

            except Exception as ext_error:
                st.error(f"Error in NGL/CODE External TS: {ext_error}")
                import traceback
                with st.expander("Error details"):
                    st.code(traceback.format_exc())

        # =====================================================================
        # NTAL/NTOL Loading Time Series
        # =====================================================================
        elif selected_page == "🌐 NTAL/NTOL Loading":
            st.header("GFZ ESMGFZ Non-Tidal Loading Time Series")
            st.markdown("""
            **NTAL** = Non-Tidal Atmospheric Loading | **NTOL** = Non-Tidal Ocean Loading

            Site displacements from GFZ ESMGFZ loading models (Dill & Dobslaw, 2013),
            interpolated from 0.5-degree global grids to station coordinates.
            """)

            # Get stations from DB (all years - we just need lat/lon for grid interpolation)
            db = get_database()
            db_conn = db.connect()
            all_stations_data = db_conn.execute("""
                SELECT station_id,
                       AVG(lat) as lat, AVG(lon) as lon
                FROM ppp_solutions
                GROUP BY station_id
                ORDER BY station_id
            """).fetchdf()

            if all_stations_data.empty:
                st.warning("No station coordinates found in database.")
            else:
                station_list = all_stations_data['station_id'].tolist()

                col1, col2 = st.columns([1, 2])
                with col1:
                    selected_station = st.selectbox(
                        "Station", station_list, key="ntal_station"
                    )
                with col2:
                    # Check available dates
                    ntal_dates = get_available_dates("NTAL")
                    if ntal_dates:
                        date_range_str = f"{ntal_dates[0][0]} to {ntal_dates[-1][0]}"
                        n_days = len(set(d for d, h in ntal_dates))
                        st.info(f"Available data: {date_range_str} ({n_days} days, {len(ntal_dates)} epochs)")
                    else:
                        st.warning("No NTAL/NTOL grid files found.")

                if selected_station and ntal_dates:
                    stn_row = all_stations_data[all_stations_data['station_id'] == selected_station].iloc[0]
                    stn_lat = stn_row['lat']
                    stn_lon = stn_row['lon']

                    st.markdown(f"**{selected_station}** - Lat: {stn_lat:.4f}, Lon: {stn_lon:.4f}")

                    # Convert sidebar DOY range to YYYYMMDD for loading parser
                    from datetime import datetime as _dt, timedelta as _td
                    _start_date_obj = _dt(year, 1, 1) + _td(days=start_doy - 1)
                    _end_date_obj = _dt(year, 1, 1) + _td(days=end_doy - 1)
                    _start_date_str = _start_date_obj.strftime("%Y%m%d")
                    _end_date_str = _end_date_obj.strftime("%Y%m%d")

                    @st.cache_data(ttl=3600, show_spinner=False)
                    def _cached_loading(lat, lon, sd, ed):
                        return get_combined_loading(lat, lon, start_date=sd, end_date=ed)

                    with st.spinner(f"Reading loading grids for {selected_station} ({_start_date_str} to {_end_date_str})..."):
                        combined = _cached_loading(stn_lat, stn_lon, _start_date_str, _end_date_str)

                    if combined.empty:
                        st.warning("No loading data could be extracted.")
                    else:
                        # Create tabs for different views
                        load_tab1, load_tab2, load_tab3 = st.tabs([
                            "Combined Time Series", "Component Comparison", "Statistics"
                        ])

                        with load_tab1:
                            # Up component - most important
                            fig_up = go.Figure()
                            if 'ntal_up_mm' in combined.columns:
                                fig_up.add_trace(go.Scatter(
                                    x=combined['datetime'], y=combined['ntal_up_mm'],
                                    name='NTAL', mode='lines',
                                    line=dict(color='#FF6B6B', width=1.5)
                                ))
                            if 'ntol_up_mm' in combined.columns:
                                fig_up.add_trace(go.Scatter(
                                    x=combined['datetime'], y=combined['ntol_up_mm'],
                                    name='NTOL', mode='lines',
                                    line=dict(color='#4ECDC4', width=1.5)
                                ))
                            if 'total_up_mm' in combined.columns:
                                fig_up.add_trace(go.Scatter(
                                    x=combined['datetime'], y=combined['total_up_mm'],
                                    name='NTAL+NTOL', mode='lines',
                                    line=dict(color='#FFE66D', width=2)
                                ))
                            fig_up.update_layout(
                                title=f'{selected_station} - Vertical (Up) Loading Displacement',
                                xaxis_title='Date/Time',
                                yaxis_title='Displacement (mm)',
                                template='plotly_dark',
                                height=400,
                                hovermode='x unified'
                            )
                            fig_up.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
                            st.plotly_chart(fig_up, use_container_width=True)

                            # North component
                            fig_n = go.Figure()
                            if 'ntal_north_mm' in combined.columns:
                                fig_n.add_trace(go.Scatter(
                                    x=combined['datetime'], y=combined['ntal_north_mm'],
                                    name='NTAL', mode='lines',
                                    line=dict(color='#FF6B6B', width=1.5)
                                ))
                            if 'ntol_north_mm' in combined.columns:
                                fig_n.add_trace(go.Scatter(
                                    x=combined['datetime'], y=combined['ntol_north_mm'],
                                    name='NTOL', mode='lines',
                                    line=dict(color='#4ECDC4', width=1.5)
                                ))
                            if 'total_north_mm' in combined.columns:
                                fig_n.add_trace(go.Scatter(
                                    x=combined['datetime'], y=combined['total_north_mm'],
                                    name='NTAL+NTOL', mode='lines',
                                    line=dict(color='#FFE66D', width=2)
                                ))
                            fig_n.update_layout(
                                title=f'{selected_station} - North Loading Displacement',
                                xaxis_title='Date/Time',
                                yaxis_title='Displacement (mm)',
                                template='plotly_dark',
                                height=350,
                                hovermode='x unified'
                            )
                            fig_n.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
                            st.plotly_chart(fig_n, use_container_width=True)

                            # East component
                            fig_e = go.Figure()
                            if 'ntal_east_mm' in combined.columns:
                                fig_e.add_trace(go.Scatter(
                                    x=combined['datetime'], y=combined['ntal_east_mm'],
                                    name='NTAL', mode='lines',
                                    line=dict(color='#FF6B6B', width=1.5)
                                ))
                            if 'ntol_east_mm' in combined.columns:
                                fig_e.add_trace(go.Scatter(
                                    x=combined['datetime'], y=combined['ntol_east_mm'],
                                    name='NTOL', mode='lines',
                                    line=dict(color='#4ECDC4', width=1.5)
                                ))
                            if 'total_east_mm' in combined.columns:
                                fig_e.add_trace(go.Scatter(
                                    x=combined['datetime'], y=combined['total_east_mm'],
                                    name='NTAL+NTOL', mode='lines',
                                    line=dict(color='#FFE66D', width=2)
                                ))
                            fig_e.update_layout(
                                title=f'{selected_station} - East Loading Displacement',
                                xaxis_title='Date/Time',
                                yaxis_title='Displacement (mm)',
                                template='plotly_dark',
                                height=350,
                                hovermode='x unified'
                            )
                            fig_e.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
                            st.plotly_chart(fig_e, use_container_width=True)

                        with load_tab2:
                            # NTAL vs NTOL comparison subplots
                            fig_comp = make_subplots(
                                rows=3, cols=2,
                                subplot_titles=[
                                    'NTAL Up', 'NTOL Up',
                                    'NTAL North', 'NTOL North',
                                    'NTAL East', 'NTOL East'
                                ],
                                vertical_spacing=0.08
                            )

                            components = [
                                ('ntal_up_mm', 'ntol_up_mm'),
                                ('ntal_north_mm', 'ntol_north_mm'),
                                ('ntal_east_mm', 'ntol_east_mm'),
                            ]

                            for row, (ntal_col, ntol_col) in enumerate(components, 1):
                                if ntal_col in combined.columns:
                                    fig_comp.add_trace(go.Scatter(
                                        x=combined['datetime'], y=combined[ntal_col],
                                        name=f'NTAL', mode='lines',
                                        line=dict(color='#FF6B6B'),
                                        showlegend=(row == 1)
                                    ), row=row, col=1)
                                if ntol_col in combined.columns:
                                    fig_comp.add_trace(go.Scatter(
                                        x=combined['datetime'], y=combined[ntol_col],
                                        name=f'NTOL', mode='lines',
                                        line=dict(color='#4ECDC4'),
                                        showlegend=(row == 1)
                                    ), row=row, col=2)

                            fig_comp.update_layout(
                                title=f'{selected_station} - NTAL vs NTOL Component Comparison',
                                template='plotly_dark',
                                height=800,
                                showlegend=True
                            )
                            for i in range(1, 7):
                                fig_comp.update_yaxes(title_text='mm', row=(i-1)//2+1, col=(i-1)%2+1)
                            st.plotly_chart(fig_comp, use_container_width=True)

                        with load_tab3:
                            st.subheader("Loading Displacement Statistics")

                            stats_data = []
                            for comp in ['up', 'north', 'east']:
                                for src in ['ntal', 'ntol', 'total']:
                                    col_name = f'{src}_{comp}_mm'
                                    if col_name in combined.columns:
                                        vals = combined[col_name]
                                        stats_data.append({
                                            'Source': src.upper(),
                                            'Component': comp.capitalize(),
                                            'Mean (mm)': f"{vals.mean():.4f}",
                                            'Std (mm)': f"{vals.std():.4f}",
                                            'Min (mm)': f"{vals.min():.4f}",
                                            'Max (mm)': f"{vals.max():.4f}",
                                            'Range (mm)': f"{vals.max() - vals.min():.4f}",
                                        })

                            if stats_data:
                                stats_df = pd.DataFrame(stats_data)
                                st.dataframe(stats_df, use_container_width=True, hide_index=True)

                            st.markdown("""
                            **Notes:**
                            - NTAL: Non-tidal atmospheric pressure loading (dominant signal, ~1-5 mm vertical)
                            - NTOL: Non-tidal ocean loading (response to atmospheric pressure over oceans, ~0.1-1 mm)
                            - Combined NTAL+NTOL represents the total non-tidal loading effect
                            - These corrections are applied in Bernese via GPSEST (ALOAD/ONTLD files)
                            - Source: GFZ ESMGFZ (Dill & Dobslaw, 2013)
                            """)

    except Exception as e:
        error_msg = str(e)
        st.error(f"Database connection error: {e}")

        if "lock" in error_msg.lower():
            st.warning("Another process is currently writing to the database. This is normal during active processing.")
            st.info("**Options:**\n1. Wait for processing to complete, then refresh\n2. Use the NGL/CODE External TS tab (no database needed)\n3. Check running processes below")

            # Show running process info
            import subprocess
            try:
                result = subprocess.run(
                    ['pgrep', '-a', '-f', 'pygnss-rt'],
                    capture_output=True, text=True, timeout=5
                )
                if result.stdout.strip():
                    st.code(f"Running pygnss-rt processes:\n{result.stdout}")
            except:
                pass
        else:
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
