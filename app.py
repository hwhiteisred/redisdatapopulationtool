"""
Redis Enterprise Data Population Tool
A Streamlit web application for populating Redis databases with various data types.
"""

import streamlit as st
import time
import logging
from typing import Generator, Tuple
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Configure console logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Import local modules
from lib.config import ConfigManager, ConnectionProfile
from lib.connection import RedisConnectionManager, ModuleStatus
from lib.monitoring import (
    get_server_stats, get_memory_stats, get_slowlog,
    get_client_list, scan_bigkeys, get_key_patterns,
    get_command_stats, format_bytes, BigKeysReport,
    run_bigkeys_cli, run_redis_cli_scan, BigKeysSummary
)

# Import populators
from lib.populators.core import (
    populate_strings, populate_hashes, populate_lists,
    populate_sets, populate_sorted_sets, populate_streams,
    populate_hyperloglog, populate_geospatial, populate_bitmaps
)
from lib.populators.json_search import populate_json, populate_search_index
from lib.populators.timeseries import populate_timeseries
from lib.populators.bloom import (
    populate_bloom_filter, populate_cuckoo_filter,
    populate_countmin_sketch, populate_topk
)
from lib.backup import (
    RedisEnterpriseAPI, ClusterCredentials, DatabaseInfo,
    BackupStatus, format_bytes as backup_format_bytes, format_interval
)


# Page configuration
st.set_page_config(
    page_title="Redis Data Population Tool",
    page_icon="🔴",
    layout="wide",
    initial_sidebar_state="expanded"
)


def init_session_state():
    """Initialize session state variables."""
    if 'config_manager' not in st.session_state:
        st.session_state.config_manager = ConfigManager()
    if 'connection_manager' not in st.session_state:
        st.session_state.connection_manager = RedisConnectionManager()
    if 'connected' not in st.session_state:
        st.session_state.connected = False
    if 'module_status' not in st.session_state:
        st.session_state.module_status = ModuleStatus()


def run_populator(
    populator_gen: Generator[Tuple[int, int], None, None],
    progress_bar,
    status_text,
    operation_name: str
) -> bool:
    """
    Run a populator generator with progress tracking.
    
    Returns True if successful, False if an error occurred.
    """
    try:
        logger.info("Populate: starting %s", operation_name)
        start_time = time.time()
        for current, total in populator_gen:
            progress = current / total if total > 0 else 1.0
            progress_bar.progress(progress)
            status_text.text(f"{operation_name}: {current}/{total}")
        
        elapsed = time.time() - start_time
        logger.info("Populate: %s complete in %.2fs", operation_name, elapsed)
        status_text.text(f"{operation_name}: Complete ({elapsed:.2f}s)")
        return True
    except Exception as e:
        logger.exception("Populate: %s failed - %s", operation_name, e)
        status_text.text(f"{operation_name}: Error - {str(e)}")
        return False


def render_sidebar():
    """Render the sidebar with connection and profile management."""
    st.sidebar.title("🔴 Redis Connection")
    
    config = st.session_state.config_manager
    conn = st.session_state.connection_manager
    
    # Profile selection
    profiles = config.get_profile_names()
    if not profiles:
        st.sidebar.warning("No profiles configured. Add one below.")
        profile_id = None
    else:
        active_id = config.get_active_profile_id()
        profile_ids = list(profiles.keys())
        
        if active_id in profile_ids:
            default_index = profile_ids.index(active_id)
        else:
            default_index = 0
        
        profile_id = st.sidebar.selectbox(
            "Select Profile",
            options=profile_ids,
            format_func=lambda x: profiles.get(x, x),
            index=default_index,
            key="profile_selector"
        )
        
        if profile_id != active_id:
            config.set_active_profile(profile_id)
    
    # Connection status
    if st.session_state.connected:
        st.sidebar.success("✅ Connected")
        
        # Show server info
        server_info = conn.get_server_info()
        if server_info:
            with st.sidebar.expander("Server Info"):
                st.write(f"**Version:** {server_info.get('redis_version', 'Unknown')}")
                st.write(f"**Memory:** {server_info.get('used_memory_human', 'Unknown')}")
                st.write(f"**Keys:** {server_info.get('total_keys', 0)}")
        
        if st.sidebar.button("Disconnect", type="secondary"):
            logger.info("Disconnect: user disconnected from Redis")
            conn.disconnect()
            st.session_state.connected = False
            st.session_state.module_status = ModuleStatus()
            st.rerun()
    else:
        st.sidebar.warning("⚠️ Not connected")
        
        if profile_id and st.sidebar.button("Connect", type="primary"):
            profile = config.get_profile(profile_id)
            if profile:
                logger.info("Connect: attempting %s (%s:%s)", profile.name, profile.host, profile.port)
                success, message = conn.connect(profile)
                if success:
                    logger.info("Connect: success - %s", message)
                    st.session_state.connected = True
                    st.session_state.module_status = conn.get_module_status()
                    st.sidebar.success(message)
                    st.rerun()
                else:
                    logger.warning("Connect: failed - %s", message)
                    st.sidebar.error(message)
    
    st.sidebar.divider()
    
    # Module status
    st.sidebar.subheader("Module Status")
    modules = st.session_state.module_status
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        st.write("🟢 Core Redis" if True else "⚫ Core Redis")
        st.write("🟢 RedisJSON" if modules.json else "⚫ RedisJSON")
        st.write("🟢 RediSearch" if modules.search else "⚫ RediSearch")
    with col2:
        st.write("🟢 TimeSeries" if modules.timeseries else "⚫ TimeSeries")
        st.write("🟢 RedisBloom" if modules.bloom else "⚫ RedisBloom")
    
    if st.session_state.connected:
        if st.sidebar.button("🔄 Refresh Modules"):
            st.session_state.module_status = conn.refresh_modules()
            st.rerun()
    
    st.sidebar.divider()
    
    # Profile management
    with st.sidebar.expander("➕ Add/Edit Profile"):
        if "profile_connection_type" not in st.session_state:
            st.session_state.profile_connection_type = "Docker (local)"
        
        connection_type = st.radio(
            "Connection type",
            ["Docker (local)", "External (database endpoint)"],
            key="profile_connection_type",
            help="Docker: local Redis/RE cluster. External: Redis Enterprise Cloud or any remote endpoint (host:port)."
        )
        
        with st.form("profile_form"):
            new_id = st.text_input("Profile ID", value="new-profile")
            new_name = st.text_input("Display Name", value="New Profile")
            
            if connection_type == "Docker (local)":
                new_host = st.text_input("Host", value="localhost", key="profile_host_docker")
                port_options = [6379, 12000, 12002, 12004]
                port_index = st.selectbox(
                    "Port",
                    options=port_options,
                    format_func=lambda x: str(x),
                    index=0,
                    key="profile_port_docker"
                )
                new_port = int(port_index)
                new_ssl_default = False
            else:
                endpoint_input = st.text_input(
                    "Endpoint (host:port)",
                    value="",
                    placeholder="hostname:port",
                    key="profile_endpoint",
                    help="Your database endpoint in host:port form."
                )
                if endpoint_input.strip():
                    parts = endpoint_input.strip().rsplit(":", 1)
                    if len(parts) == 2 and parts[1].isdigit():
                        new_host = parts[0].strip()
                        new_port = int(parts[1])
                    else:
                        new_host = endpoint_input.strip()
                        new_port = 6379
                else:
                    new_host = ""
                    new_port = 6379
                new_ssl_default = True
            
            new_password = st.text_input("Password", type="password", key="profile_password")
            new_ssl = st.checkbox("Use SSL/TLS", value=new_ssl_default, key="profile_ssl")
            
            if st.form_submit_button("Save Profile"):
                if connection_type == "External (database endpoint)" and not new_host:
                    st.error("Enter a database endpoint (host:port).")
                else:
                    profile = ConnectionProfile(
                        name=new_name,
                        host=new_host or "localhost",
                        port=new_port,
                        password=new_password,
                        ssl=new_ssl
                    )
                    config.add_profile(new_id, profile)
                    logger.info("Profile saved: id=%s name=%s host=%s port=%s", new_id, new_name, new_host or "localhost", new_port)
                    st.success(f"Profile '{new_name}' saved!")
                    st.rerun()
    
    # Delete profile
    if profiles and len(profiles) > 1:
        with st.sidebar.expander("🗑️ Delete Profile"):
            delete_id = st.selectbox(
                "Select profile to delete",
                options=list(profiles.keys()),
                format_func=lambda x: profiles.get(x, x),
                key="delete_profile_selector"
            )
            if st.button("Delete", type="secondary"):
                if config.delete_profile(delete_id):
                    logger.info("Profile deleted: id=%s", delete_id)
                    st.success(f"Profile deleted!")
                    st.rerun()


def render_core_types_tab():
    """Render the Core Redis Types tab."""
    st.header("Core Redis Data Types")
    
    conn = st.session_state.connection_manager
    client = conn.get_client() if st.session_state.connected else None
    
    if not client:
        st.warning("Please connect to a Redis server first.")
        return
    
    # Strings
    with st.expander("📝 Strings", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            str_count = st.number_input("Count", value=100, min_value=1, key="str_count")
        with col2:
            str_prefix = st.text_input("Key Prefix", value="string", key="str_prefix")
        with col3:
            str_size = st.number_input("Value Size (bytes)", value=100, min_value=1, key="str_size")
        
        if st.button("Populate Strings", key="btn_strings"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_strings(client, str_count, str_prefix, str_size),
                progress, status, "Strings"
            )
    
    # Hashes
    with st.expander("🗂️ Hashes"):
        col1, col2, col3 = st.columns(3)
        with col1:
            hash_count = st.number_input("Count", value=100, min_value=1, key="hash_count")
        with col2:
            hash_prefix = st.text_input("Key Prefix", value="hash", key="hash_prefix")
        with col3:
            hash_fields = st.number_input("Fields per Hash", value=5, min_value=1, key="hash_fields")
        
        if st.button("Populate Hashes", key="btn_hashes"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_hashes(client, hash_count, hash_prefix, hash_fields),
                progress, status, "Hashes"
            )
    
    # Lists
    with st.expander("📋 Lists"):
        col1, col2, col3 = st.columns(3)
        with col1:
            list_count = st.number_input("Count", value=50, min_value=1, key="list_count")
        with col2:
            list_prefix = st.text_input("Key Prefix", value="list", key="list_prefix")
        with col3:
            list_items = st.number_input("Items per List", value=100, min_value=1, key="list_items")
        
        if st.button("Populate Lists", key="btn_lists"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_lists(client, list_count, list_prefix, list_items),
                progress, status, "Lists"
            )
    
    # Sets
    with st.expander("🎯 Sets"):
        col1, col2, col3 = st.columns(3)
        with col1:
            set_count = st.number_input("Count", value=100, min_value=1, key="set_count")
        with col2:
            set_prefix = st.text_input("Key Prefix", value="set", key="set_prefix")
        with col3:
            set_members = st.number_input("Members per Set", value=50, min_value=1, key="set_members")
        
        if st.button("Populate Sets", key="btn_sets"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_sets(client, set_count, set_prefix, set_members),
                progress, status, "Sets"
            )
    
    # Sorted Sets
    with st.expander("📊 Sorted Sets"):
        col1, col2, col3 = st.columns(3)
        with col1:
            zset_count = st.number_input("Count", value=100, min_value=1, key="zset_count")
        with col2:
            zset_prefix = st.text_input("Key Prefix", value="zset", key="zset_prefix")
        with col3:
            zset_members = st.number_input("Members per Set", value=50, min_value=1, key="zset_members")
        
        if st.button("Populate Sorted Sets", key="btn_zsets"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_sorted_sets(client, zset_count, zset_prefix, zset_members),
                progress, status, "Sorted Sets"
            )
    
    # Streams
    with st.expander("🌊 Streams"):
        col1, col2, col3 = st.columns(3)
        with col1:
            stream_count = st.number_input("Count", value=20, min_value=1, key="stream_count")
        with col2:
            stream_prefix = st.text_input("Key Prefix", value="stream", key="stream_prefix")
        with col3:
            stream_entries = st.number_input("Entries per Stream", value=100, min_value=1, key="stream_entries")
        
        if st.button("Populate Streams", key="btn_streams"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_streams(client, stream_count, stream_prefix, stream_entries),
                progress, status, "Streams"
            )
    
    # HyperLogLog
    with st.expander("📈 HyperLogLog"):
        col1, col2, col3 = st.columns(3)
        with col1:
            hll_count = st.number_input("Count", value=50, min_value=1, key="hll_count")
        with col2:
            hll_prefix = st.text_input("Key Prefix", value="hll", key="hll_prefix")
        with col3:
            hll_elements = st.number_input("Elements per HLL", value=1000, min_value=1, key="hll_elements")
        
        if st.button("Populate HyperLogLog", key="btn_hll"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_hyperloglog(client, hll_count, hll_prefix, hll_elements),
                progress, status, "HyperLogLog"
            )
    
    # Geospatial
    with st.expander("🌍 Geospatial"):
        col1, col2, col3 = st.columns(3)
        with col1:
            geo_count = st.number_input("Count", value=50, min_value=1, key="geo_count")
        with col2:
            geo_prefix = st.text_input("Key Prefix", value="geo", key="geo_prefix")
        with col3:
            geo_locations = st.number_input("Locations per Key", value=100, min_value=1, key="geo_locations")
        
        if st.button("Populate Geospatial", key="btn_geo"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_geospatial(client, geo_count, geo_prefix, geo_locations),
                progress, status, "Geospatial"
            )
    
    # Bitmaps
    with st.expander("🔢 Bitmaps"):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            bm_count = st.number_input("Count", value=50, min_value=1, key="bm_count")
        with col2:
            bm_prefix = st.text_input("Key Prefix", value="bitmap", key="bm_prefix")
        with col3:
            bm_bits = st.number_input("Bits per Bitmap", value=10000, min_value=1, key="bm_bits")
        with col4:
            bm_density = st.slider("Bit Density", 0.0, 1.0, 0.3, key="bm_density")
        
        if st.button("Populate Bitmaps", key="btn_bitmaps"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_bitmaps(client, bm_count, bm_prefix, bm_bits, bm_density),
                progress, status, "Bitmaps"
            )


def render_json_search_tab():
    """Render the JSON/Search tab."""
    st.header("RedisJSON & RediSearch")
    
    conn = st.session_state.connection_manager
    modules = st.session_state.module_status
    client = conn.get_client() if st.session_state.connected else None
    
    if not client:
        st.warning("Please connect to a Redis server first.")
        return
    
    # RedisJSON
    st.subheader("📄 RedisJSON")
    if not modules.json:
        st.warning("⚠️ RedisJSON module is not loaded on this server.")
    else:
        with st.expander("JSON Documents", expanded=True):
            col1, col2, col3 = st.columns(3)
            with col1:
                json_count = st.number_input("Count", value=100, min_value=1, key="json_count")
            with col2:
                json_prefix = st.text_input("Key Prefix", value="json", key="json_prefix")
            with col3:
                json_type = st.selectbox("Document Type", ["product", "user"], key="json_type")
            
            if st.button("Populate JSON Documents", key="btn_json"):
                progress = st.progress(0)
                status = st.empty()
                run_populator(
                    populate_json(client, json_count, json_prefix, json_type),
                    progress, status, "JSON Documents"
                )
    
    st.divider()
    
    # RediSearch
    st.subheader("🔍 RediSearch")
    if not modules.search:
        st.warning("⚠️ RediSearch module is not loaded on this server.")
    else:
        with st.expander("Search Index with Documents", expanded=True):
            col1, col2, col3 = st.columns(3)
            with col1:
                search_count = st.number_input("Document Count", value=100, min_value=1, key="search_count")
            with col2:
                search_index = st.text_input("Index Name", value="idx:products", key="search_index")
            with col3:
                search_prefix = st.text_input("Key Prefix", value="product", key="search_prefix")
            
            search_type = st.selectbox("Document Type", ["product", "user"], key="search_type")
            
            st.info("This will create a search index and populate it with searchable documents.")
            
            if st.button("Create Index & Populate", key="btn_search"):
                progress = st.progress(0)
                status = st.empty()
                run_populator(
                    populate_search_index(client, search_count, search_index, search_prefix, search_type),
                    progress, status, "Search Index"
                )


def render_timeseries_tab():
    """Render the TimeSeries tab."""
    st.header("RedisTimeSeries")
    
    conn = st.session_state.connection_manager
    modules = st.session_state.module_status
    client = conn.get_client() if st.session_state.connected else None
    
    if not client:
        st.warning("Please connect to a Redis server first.")
        return
    
    if not modules.timeseries:
        st.warning("⚠️ RedisTimeSeries module is not loaded on this server.")
        return
    
    with st.expander("📈 Time Series Data", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            ts_count = st.number_input("Number of Series", value=10, min_value=1, key="ts_count")
            ts_prefix = st.text_input("Key Prefix", value="ts", key="ts_prefix")
            ts_samples = st.number_input("Samples per Series", value=1000, min_value=1, key="ts_samples")
        with col2:
            ts_type = st.selectbox(
                "Metric Type",
                ["sensor", "stock", "server", "iot"],
                key="ts_type"
            )
            ts_retention = st.number_input(
                "Retention (ms, 0=unlimited)",
                value=0,
                min_value=0,
                key="ts_retention"
            )
        
        st.info(f"""
        **Metric Types:**
        - **sensor**: Temperature-like data (1 sample/second)
        - **stock**: Price data with volatility (1 sample/minute)
        - **server**: CPU/Memory-like metrics (1 sample/5 seconds)
        - **iot**: Mixed IoT sensor data (1 sample/10 seconds)
        """)
        
        if st.button("Populate Time Series", key="btn_ts"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_timeseries(
                    client, ts_count, ts_prefix, ts_samples,
                    ts_type, ts_retention
                ),
                progress, status, "Time Series"
            )


def render_bloom_tab():
    """Render the RedisBloom tab."""
    st.header("RedisBloom")
    
    conn = st.session_state.connection_manager
    modules = st.session_state.module_status
    client = conn.get_client() if st.session_state.connected else None
    
    if not client:
        st.warning("Please connect to a Redis server first.")
        return
    
    if not modules.bloom:
        st.warning("⚠️ RedisBloom module is not loaded on this server.")
        return
    
    # Bloom Filter
    with st.expander("🌸 Bloom Filter", expanded=True):
        st.markdown("*Probabilistic membership testing - 'Has this item been seen?'*")
        col1, col2, col3 = st.columns(3)
        with col1:
            bf_count = st.number_input("Number of Filters", value=10, min_value=1, key="bf_count")
            bf_prefix = st.text_input("Key Prefix", value="bf", key="bf_prefix")
        with col2:
            bf_items = st.number_input("Items per Filter", value=10000, min_value=1, key="bf_items")
        with col3:
            bf_error = st.slider("Error Rate", 0.001, 0.1, 0.01, key="bf_error")
        
        if st.button("Populate Bloom Filters", key="btn_bf"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_bloom_filter(client, bf_count, bf_prefix, bf_items, bf_error),
                progress, status, "Bloom Filters"
            )
    
    # Cuckoo Filter
    with st.expander("🐦 Cuckoo Filter"):
        st.markdown("*Like Bloom filter but supports deletion*")
        col1, col2, col3 = st.columns(3)
        with col1:
            cf_count = st.number_input("Number of Filters", value=10, min_value=1, key="cf_count")
            cf_prefix = st.text_input("Key Prefix", value="cf", key="cf_prefix")
        with col2:
            cf_items = st.number_input("Items per Filter", value=10000, min_value=1, key="cf_items")
        with col3:
            cf_bucket = st.number_input("Bucket Size", value=2, min_value=1, max_value=8, key="cf_bucket")
        
        if st.button("Populate Cuckoo Filters", key="btn_cf"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_cuckoo_filter(client, cf_count, cf_prefix, cf_items, cf_bucket),
                progress, status, "Cuckoo Filters"
            )
    
    # Count-Min Sketch
    with st.expander("📊 Count-Min Sketch"):
        st.markdown("*Frequency estimation - 'How many times has this item appeared?'*")
        col1, col2 = st.columns(2)
        with col1:
            cms_count = st.number_input("Number of Sketches", value=10, min_value=1, key="cms_count")
            cms_prefix = st.text_input("Key Prefix", value="cms", key="cms_prefix")
            cms_items = st.number_input("Unique Items", value=1000, min_value=1, key="cms_items")
        with col2:
            cms_increments = st.number_input("Total Increments", value=100000, min_value=1, key="cms_increments")
            cms_width = st.number_input("Width", value=2000, min_value=100, key="cms_width")
            cms_depth = st.number_input("Depth", value=5, min_value=1, max_value=10, key="cms_depth")
        
        if st.button("Populate Count-Min Sketches", key="btn_cms"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_countmin_sketch(
                    client, cms_count, cms_prefix, cms_items,
                    cms_increments, cms_width, cms_depth
                ),
                progress, status, "Count-Min Sketches"
            )
    
    # Top-K
    with st.expander("🏆 Top-K"):
        st.markdown("*Heavy hitters tracking - 'What are the most frequent items?'*")
        col1, col2 = st.columns(2)
        with col1:
            topk_count = st.number_input("Number of Top-K Structures", value=10, min_value=1, key="topk_count")
            topk_prefix = st.text_input("Key Prefix", value="topk", key="topk_prefix")
            topk_k = st.number_input("K (top items to track)", value=100, min_value=1, key="topk_k")
        with col2:
            topk_items = st.number_input("Unique Items in Stream", value=10000, min_value=1, key="topk_items")
            topk_adds = st.number_input("Total Adds", value=100000, min_value=1, key="topk_adds")
        
        if st.button("Populate Top-K Structures", key="btn_topk"):
            progress = st.progress(0)
            status = st.empty()
            run_populator(
                populate_topk(client, topk_count, topk_prefix, topk_k, topk_items, topk_adds),
                progress, status, "Top-K"
            )


def render_monitoring_tab():
    """Render the Monitoring & Analytics tab."""
    st.header("Monitoring & Analytics")
    
    conn = st.session_state.connection_manager
    client = conn.get_client() if st.session_state.connected else None
    
    if not client:
        st.warning("Please connect to a Redis server first.")
        return
    
    # Sub-tabs for different monitoring features
    mon_tab1, mon_tab2, mon_tab3, mon_tab4, mon_tab5 = st.tabs([
        "📊 Overview",
        "🔑 Big Keys",
        "🐢 Slow Log",
        "👥 Clients",
        "⚡ Commands"
    ])
    
    with mon_tab1:
        render_overview_section(client)
    
    with mon_tab2:
        render_bigkeys_section(client)
    
    with mon_tab3:
        render_slowlog_section(client)
    
    with mon_tab4:
        render_clients_section(client)
    
    with mon_tab5:
        render_commands_section(client)


def render_backups_tab():
    """Render the Backups tab for Redis Enterprise backup management."""
    st.header("💾 Redis Enterprise Backup Management")
    
    st.markdown("""
    Manage database backups via the Redis Enterprise REST API.
    
    **Note:** This requires Redis Enterprise cluster admin credentials.
    """)
    
    # Initialize session state for API credentials
    if 're_api_credentials' not in st.session_state:
        st.session_state.re_api_credentials = None
    if 're_api_client' not in st.session_state:
        st.session_state.re_api_client = None
    
    # Connection settings
    with st.expander("🔐 Cluster API Credentials", expanded=st.session_state.re_api_client is None):
        col1, col2 = st.columns(2)
        with col1:
            api_host = st.text_input(
                "Cluster Host",
                value="localhost",
                key="re_api_host",
                help="Hostname of a Redis Enterprise cluster node"
            )
            api_username = st.text_input(
                "Username",
                value="",
                key="re_api_username",
                help="Cluster admin email/username"
            )
        with col2:
            api_port = st.number_input(
                "REST API Port",
                value=9443,
                min_value=1,
                max_value=65535,
                key="re_api_port",
                help="REST API port (default: 9443)"
            )
            api_password = st.text_input(
                "Password",
                type="password",
                key="re_api_password"
            )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔗 Connect to Cluster API", type="primary"):
                credentials = ClusterCredentials(
                    host=api_host,
                    port=api_port,
                    username=api_username,
                    password=api_password
                )
                api_client = RedisEnterpriseAPI(credentials)
                success, message = api_client.test_connection()
                
                if success:
                    st.session_state.re_api_credentials = credentials
                    st.session_state.re_api_client = api_client
                    logger.info("Backup API: connected to %s:%s", api_host, api_port)
                    st.success(message)
                    st.rerun()
                else:
                    logger.warning("Backup API: connection failed - %s", message)
                    st.error(f"Connection failed: {message}")
        
        with col2:
            if st.session_state.re_api_client and st.button("🔌 Disconnect"):
                logger.info("Backup API: disconnected")
                st.session_state.re_api_client = None
                st.session_state.re_api_credentials = None
                st.rerun()
    
    # Show connection status
    if st.session_state.re_api_client:
        st.success("✅ Connected to Redis Enterprise REST API")
        api = st.session_state.re_api_client
        
        # Sub-tabs for backup features
        backup_tab1, backup_tab2, backup_tab3 = st.tabs([
            "📋 Databases",
            "⚙️ Configure Backup",
            "🔄 Backup/Restore"
        ])
        
        with backup_tab1:
            render_databases_list(api)
        
        with backup_tab2:
            render_backup_config(api)
        
        with backup_tab3:
            render_backup_restore(api)
    else:
        st.info("👆 Enter your Redis Enterprise cluster admin credentials above to manage backups.")


def render_databases_list(api: RedisEnterpriseAPI):
    """Render list of databases with their backup status."""
    st.subheader("Database Overview")
    
    if st.button("🔄 Refresh", key="refresh_dbs"):
        st.rerun()
    
    success, result = api.get_databases()
    
    if not success:
        st.error(f"Error fetching databases: {result}")
        return
    
    if not result:
        st.warning("No databases found in cluster.")
        return
    
    # Create a nice table
    import pandas as pd
    
    data = []
    for db in result:
        data.append({
            "UID": db.uid,
            "Name": db.name,
            "Status": "🟢 Active" if db.status == "active" else f"🔴 {db.status}",
            "Memory": backup_format_bytes(db.memory_size),
            "Backup": "✅ Enabled" if db.backup_enabled else "❌ Disabled",
            "Interval": format_interval(db.backup_interval),
            "Last Backup": db.last_backup_time or "Never"
        })
    
    df = pd.DataFrame(data)
    st.dataframe(df, width='stretch', hide_index=True)


def render_backup_config(api: RedisEnterpriseAPI):
    """Render backup configuration interface."""
    st.subheader("Configure Database Backup")
    
    success, databases = api.get_databases()
    if not success:
        st.error(f"Error: {databases}")
        return
    
    if not databases:
        st.warning("No databases found.")
        return
    
    # Database selector
    db_options = {f"{db.name} (UID: {db.uid})": db for db in databases}
    selected_db_name = st.selectbox(
        "Select Database",
        options=list(db_options.keys()),
        key="backup_config_db"
    )
    
    selected_db = db_options[selected_db_name]
    
    # Show current config
    st.markdown("**Current Configuration:**")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Backup Status", "Enabled" if selected_db.backup_enabled else "Disabled")
    with col2:
        st.metric("Interval", format_interval(selected_db.backup_interval))
    with col3:
        st.metric("Location", selected_db.backup_location or "Not set")
    
    st.divider()
    st.markdown("**Update Configuration:**")
    
    col1, col2 = st.columns(2)
    with col1:
        enable_backup = st.checkbox(
            "Enable Periodic Backup",
            value=selected_db.backup_enabled,
            key="backup_enable"
        )
        
        backup_interval = st.selectbox(
            "Backup Interval",
            options=[
                ("Every hour", 3600),
                ("Every 6 hours", 21600),
                ("Every 12 hours", 43200),
                ("Every 24 hours", 86400),
                ("Every 7 days", 604800)
            ],
            format_func=lambda x: x[0],
            key="backup_interval_select"
        )
    
    with col2:
        backup_location = st.text_input(
            "Backup Location (file path)",
            value="/var/opt/redislabs/backup",
            key="backup_location",
            help="Path inside the container for local backups"
        )
    
    if st.button("💾 Update Backup Configuration", type="primary"):
        success, message = api.configure_backup(
            db_uid=selected_db.uid,
            enabled=enable_backup,
            interval=backup_interval[1],
            location=backup_location
        )
        
        if success:
            st.success(message)
            st.rerun()
        else:
            st.error(f"Failed: {message}")


def render_backup_restore(api: RedisEnterpriseAPI):
    """Render backup and restore operations."""
    st.subheader("Backup & Restore Operations")
    
    success, databases = api.get_databases()
    if not success:
        st.error(f"Error: {databases}")
        return
    
    if not databases:
        st.warning("No databases found.")
        return
    
    # Database selector
    db_options = {f"{db.name} (UID: {db.uid})": db for db in databases}
    selected_db_name = st.selectbox(
        "Select Database",
        options=list(db_options.keys()),
        key="backup_restore_db"
    )
    
    selected_db = db_options[selected_db_name]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📤 Export/Backup Database")
        st.markdown("Export database to a backup location.")
        
        export_location = st.text_input(
            "Backup Location",
            value="/var/opt/redislabs/backup",
            key="export_location",
            help="Local path (mount point) or URL (s3://, gs://, ftp://, sftp://)"
        )
        
        if st.button("🚀 Start Backup Now", type="primary", key="trigger_backup"):
            with st.spinner("Initiating backup..."):
                logger.info("Backup: triggering for db uid=%s to %s", selected_db.uid, export_location)
                success, message = api.trigger_backup(selected_db.uid, export_location)
                if success:
                    st.success(message)
                else:
                    st.error(f"Failed: {message}")
    
    with col2:
        st.markdown("### 📥 Import/Restore")
        st.markdown("Restore database from a backup file.")
        
        st.warning("⚠️ This will overwrite current database data!")
        
        import_location = st.text_input(
            "Import Source (full path to .rdb.gz file)",
            value="/var/opt/redislabs/backup/",
            key="import_location",
            help="Full path to backup file including filename (e.g., /var/opt/redislabs/backup/backup.rdb.gz)"
        )
        
        st.caption("💡 Tip: Specify full path to .rdb.gz file, not just the directory")
        
        if st.button("📥 Import/Restore", key="import_db"):
            with st.spinner("Importing..."):
                logger.info("Import: db uid=%s from %s", selected_db.uid, import_location)
                success, message = api.import_database(selected_db.uid, import_location)
                if success:
                    st.success(message)
                else:
                    st.error(f"Failed: {message}")
        
        st.markdown("---")
        st.markdown("### 📊 Backup Status")
        
        if st.button("🔄 Check Status", key="check_backup_status"):
            success, status = api.get_backup_status(selected_db.uid)
            if success:
                st.json({
                    "Database": status.db_name,
                    "Status": status.status,
                    "Progress": status.progress,
                    "Started": status.started_at,
                    "Completed": status.completed_at,
                    "Location": status.file_path,
                    "Error": status.error
                })
            else:
                st.error(f"Error: {status}")


def render_overview_section(client):
    """Render the overview/dashboard section."""
    st.subheader("Server Overview")
    
    if st.button("🔄 Refresh Stats", key="refresh_overview"):
        st.rerun()
    
    stats = get_server_stats(client)
    
    if 'error' in stats:
        st.error(f"Error getting stats: {stats['error']}")
        return
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Keys", f"{stats.get('total_keys', 0):,}")
    with col2:
        st.metric("Memory Used", stats.get('used_memory_human', 'Unknown'))
    with col3:
        st.metric("Connected Clients", stats.get('connected_clients', 0))
    with col4:
        st.metric("Ops/sec", f"{stats.get('instantaneous_ops_per_sec', 0):,}")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Hit Rate", f"{stats.get('hit_rate', 0):.1f}%")
    with col2:
        st.metric("Uptime (days)", stats.get('uptime_days', 0))
    with col3:
        st.metric("Peak Memory", stats.get('used_memory_peak_human', 'Unknown'))
    with col4:
        st.metric("Fragmentation", f"{stats.get('mem_fragmentation_ratio', 0):.2f}")
    
    st.divider()
    
    # Memory chart
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Memory Usage")
        memory_data = {
            'Category': ['Used', 'Peak', 'RSS'],
            'Bytes': [
                stats.get('used_memory', 0),
                stats.get('used_memory_peak', 0),
                stats.get('used_memory_rss', 0)
            ]
        }
        df_memory = pd.DataFrame(memory_data)
        fig_memory = px.bar(
            df_memory, x='Category', y='Bytes',
            title='Memory Comparison',
            color='Category'
        )
        fig_memory.update_layout(showlegend=False)
        st.plotly_chart(fig_memory, width='stretch')
    
    with col2:
        st.subheader("Keyspace Distribution")
        keyspace = stats.get('keyspace', {})
        if keyspace:
            db_names = list(keyspace.keys())
            db_keys = [keyspace[db].get('keys', 0) for db in db_names]
            
            fig_keyspace = px.pie(
                values=db_keys,
                names=db_names,
                title='Keys by Database'
            )
            st.plotly_chart(fig_keyspace, width='stretch')
        else:
            st.info("No keyspace data available")
    
    # Hit/Miss ratio
    st.subheader("Cache Performance")
    hits = stats.get('keyspace_hits', 0)
    misses = stats.get('keyspace_misses', 0)
    
    if hits + misses > 0:
        fig_hits = go.Figure(go.Indicator(
            mode="gauge+number",
            value=stats.get('hit_rate', 0),
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Hit Rate (%)"},
            gauge={
                'axis': {'range': [0, 100]},
                'bar': {'color': "darkgreen"},
                'steps': [
                    {'range': [0, 50], 'color': "lightcoral"},
                    {'range': [50, 80], 'color': "lightyellow"},
                    {'range': [80, 100], 'color': "lightgreen"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))
        fig_hits.update_layout(height=300)
        st.plotly_chart(fig_hits, width='stretch')
    else:
        st.info("No hit/miss data available yet")
    
    # Server info expander
    with st.expander("📋 Full Server Info"):
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Server**")
            st.write(f"- Version: {stats.get('redis_version', 'Unknown')}")
            st.write(f"- Mode: {stats.get('redis_mode', 'Unknown')}")
            st.write(f"- OS: {stats.get('os', 'Unknown')}")
            st.write(f"- Role: {stats.get('role', 'Unknown')}")
        with col2:
            st.write("**Stats**")
            st.write(f"- Total Connections: {stats.get('total_connections_received', 0):,}")
            st.write(f"- Total Commands: {stats.get('total_commands_processed', 0):,}")
            st.write(f"- Expired Keys: {stats.get('expired_keys', 0):,}")
            st.write(f"- Evicted Keys: {stats.get('evicted_keys', 0):,}")


def render_bigkeys_section(client):
    """Render the big keys analysis section."""
    st.subheader("🔑 Key Analysis")
    st.markdown("""
    *Uses `redis-cli` scanning options. See [Redis CLI docs](https://redis.io/docs/latest/develop/tools/cli/#scan-for-big-keys-and-memory-usage)*
    """)
    
    # Get current connection info
    conn = st.session_state.connection_manager
    profile = conn.get_current_profile()
    
    if not profile:
        st.warning("No active connection profile.")
        return
    
    col1, col2, col3 = st.columns(3)
    with col1:
        scan_method = st.radio(
            "Scan Method",
            ["redis-cli (recommended)", "Python scanner"],
            key="bigkeys_method",
            help="redis-cli is faster and scans all keys. Python scanner allows sampling."
        )
    
    # Define scan_type outside conditional so it's always available
    scan_type = "bigkeys"  # Default
    
    with col2:
        if scan_method == "redis-cli (recommended)":
            scan_type = st.selectbox(
                "Scan Type",
                ["bigkeys", "memkeys", "keystats"],
                key="cli_scan_type",
                help="""
                **bigkeys**: Find keys with many elements (complexity)  
                **memkeys**: Find keys consuming most memory  
                **keystats**: Combined bigkeys + memkeys with distribution
                """
            )
    with col3:
        if scan_method == "Python scanner":
            sample_size = st.number_input(
                "Keys to scan (0 = all)",
                min_value=0,
                max_value=100000,
                value=0,
                step=100,
                key="bigkeys_sample"
            )
    
    # Display scan type descriptions
    if scan_method == "redis-cli (recommended)":
        scan_descriptions = {
            "bigkeys": "Finds keys with many elements (list length, set cardinality, etc.)",
            "memkeys": "Finds keys consuming the most memory (bytes)",
            "keystats": "Combined analysis: memory size, element count, and distribution stats"
        }
        st.info(f"**--{scan_type}**: {scan_descriptions.get(scan_type, '')}")
        
        # Clear old results if scan type changed
        if 'last_scan_type' in st.session_state and st.session_state.last_scan_type != scan_type:
            if 'bigkeys_summary' in st.session_state:
                st.session_state.bigkeys_summary = None
    
    if st.button("🔍 Scan Keys", type="primary", key="scan_bigkeys"):
        
        if scan_method == "redis-cli (recommended)":
            logger.info("BigKeys: starting redis-cli --%s on %s:%s", scan_type, profile.host, profile.port)
            with st.spinner(f"Running redis-cli --{scan_type} (scanning all keys)..."):
                success, result = run_redis_cli_scan(
                    host=profile.host,
                    port=profile.port,
                    password=profile.password if profile.password else None,
                    scan_type=scan_type
                )
                
                if success:
                    logger.info("BigKeys: scan complete, %s keys", result.total_keys if hasattr(result, 'total_keys') else '?')
                    st.session_state.bigkeys_summary = result
                    st.session_state.bigkeys_report = None  # Clear old Python report
                    st.session_state.last_scan_type = scan_type
                    st.success(f"Scan complete! Analyzed {result.total_keys:,} keys.")
                else:
                    logger.warning("BigKeys: scan failed - %s", result)
                    st.error(f"Error: {result}")
        else:
            # Python scanner
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                scanner = scan_bigkeys(client, sample_size=sample_size, top_n=20)
                report = None
                
                for result in scanner:
                    if isinstance(result, BigKeysReport):
                        report = result
                    elif isinstance(result, tuple) and len(result) == 3:
                        scanned, total, current_key = result
                        progress = min(scanned / max(total, 1), 1.0)
                        progress_bar.progress(progress)
                        status_text.text(f"Scanned {scanned:,} keys... ({current_key})")
                
                if report is not None:
                    progress_bar.progress(1.0)
                    status_text.text(f"Scan complete! Analyzed {report.total_keys_scanned:,} keys in {report.scan_time_seconds:.2f}s")
                    st.session_state.bigkeys_report = report
                    st.session_state.bigkeys_summary = None  # Clear old CLI report
                else:
                    st.warning("No keys found to scan.")
                
            except Exception as e:
                st.error(f"Error scanning keys: {str(e)}")
    
    # Display CLI summary if available
    if 'bigkeys_summary' in st.session_state and st.session_state.bigkeys_summary:
        summary = st.session_state.bigkeys_summary
        
        st.divider()
        
        # Show scan type used
        last_scan = st.session_state.get('last_scan_type', 'bigkeys')
        st.caption(f"Results from `redis-cli --{last_scan}`")
        
        # Key metrics
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Keys Scanned", f"{summary.total_keys:,}")
        with col2:
            st.metric("Avg Key Length", f"{summary.avg_key_length:.2f} bytes")
        
        # Type distribution chart
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Key Type Distribution")
            if summary.type_stats:
                df_types = pd.DataFrame([
                    {'Type': k, 'Count': v['count'], 'Percentage': v['pct']}
                    for k, v in summary.type_stats.items()
                ])
                fig_types = px.pie(
                    df_types, values='Count', names='Type',
                    title='Keys by Type'
                )
                st.plotly_chart(fig_types, width='stretch')
        
        with col2:
            st.subheader("Data by Type")
            if summary.type_stats:
                df_stats = pd.DataFrame([
                    {'Type': k, 'Total': v['total'], 'Unit': v['unit'], 'Avg': v['avg']}
                    for k, v in summary.type_stats.items()
                ])
                fig_stats = px.bar(
                    df_stats, x='Type', y='Total',
                    title='Total Size/Items by Type',
                    hover_data=['Unit', 'Avg']
                )
                st.plotly_chart(fig_stats, width='stretch')
        
        # Biggest keys by type
        st.subheader("🏆 Biggest Key per Type")
        if summary.biggest_by_type:
            biggest_data = []
            for key_type, info in summary.biggest_by_type.items():
                biggest_data.append({
                    'Type': key_type,
                    'Key': info['key'],
                    'Size': f"{info['size']:,} {info['unit']}"
                })
            st.dataframe(pd.DataFrame(biggest_data), width='stretch')
        
        # Type statistics table
        st.subheader("📊 Type Statistics")
        if summary.type_stats:
            stats_data = []
            for key_type, stats in summary.type_stats.items():
                stats_data.append({
                    'Type': key_type,
                    'Count': f"{stats['count']:,}",
                    'Total': f"{stats['total']:,} {stats['unit']}",
                    '% of Keys': f"{stats['pct']:.2f}%",
                    'Avg Size': f"{stats['avg']:.2f}"
                })
            st.dataframe(pd.DataFrame(stats_data), width='stretch')
        
        # Raw output expander
        with st.expander("📋 Raw redis-cli Output"):
            st.code(summary.raw_output, language="text")
    
    # Display Python report if available (fallback)
    elif 'bigkeys_report' in st.session_state and st.session_state.bigkeys_report:
        report = st.session_state.bigkeys_report
        
        st.divider()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Key Type Distribution")
            if report.type_distribution:
                df_types = pd.DataFrame([
                    {'Type': k, 'Count': v}
                    for k, v in report.type_distribution.items()
                ])
                fig_types = px.pie(
                    df_types, values='Count', names='Type',
                    title='Keys by Type'
                )
                st.plotly_chart(fig_types, width='stretch')
        
        with col2:
            st.subheader("Memory by Type")
            if report.memory_by_type:
                df_memory = pd.DataFrame([
                    {'Type': k, 'Memory': v, 'Memory_Readable': format_bytes(v)}
                    for k, v in report.memory_by_type.items()
                ])
                fig_memory = px.bar(
                    df_memory, x='Type', y='Memory',
                    title='Memory Usage by Type',
                    hover_data=['Memory_Readable']
                )
                st.plotly_chart(fig_memory, width='stretch')
        
        st.subheader("Largest Key per Type")
        if report.largest_by_type:
            largest_data = []
            for key_type, key_info in report.largest_by_type.items():
                largest_data.append({
                    'Type': key_type,
                    'Key': key_info.key,
                    'Size': format_bytes(key_info.size),
                    'Elements': key_info.length,
                    'Encoding': key_info.encoding,
                    'TTL': key_info.ttl if key_info.ttl >= 0 else 'No expiry'
                })
            st.dataframe(pd.DataFrame(largest_data), width='stretch')
        
        st.subheader(f"Top {len(report.top_keys_by_memory)} Keys by Memory")
        if report.top_keys_by_memory:
            top_keys_data = []
            for key_info in report.top_keys_by_memory:
                top_keys_data.append({
                    'Key': key_info.key,
                    'Type': key_info.type,
                    'Size': format_bytes(key_info.size),
                    'Size_Bytes': key_info.size,
                    'Elements': key_info.length,
                    'Encoding': key_info.encoding
                })
            
            df_top = pd.DataFrame(top_keys_data)
            
            fig_top = px.bar(
                df_top.head(10), x='Key', y='Size_Bytes',
                title='Top 10 Keys by Memory',
                hover_data=['Type', 'Size', 'Elements']
            )
            fig_top.update_xaxes(tickangle=45)
            st.plotly_chart(fig_top, width='stretch')
            
            st.dataframe(df_top[['Key', 'Type', 'Size', 'Elements', 'Encoding']], width='stretch')


def render_slowlog_section(client):
    """Render the slow log section."""
    st.subheader("🐢 Slow Log")
    st.markdown("*Shows commands that exceeded the slowlog threshold*")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        count = st.slider("Number of entries", 10, 100, 20, key="slowlog_count")
    with col2:
        if st.button("🔄 Refresh", key="refresh_slowlog"):
            pass  # Just triggers rerun
    
    slowlog = get_slowlog(client, count)
    
    if slowlog is None:
        logger.warning("Slow log: could not retrieve (None)")
        st.error("Error getting slow log: Could not retrieve slow log")
    elif len(slowlog) == 0:
        logger.info("Slow log: no entries")
        st.info("No slow log entries found. This is good!")
    elif len(slowlog) > 0 and isinstance(slowlog[0], dict) and 'error' in slowlog[0]:
        err_msg = slowlog[0].get('error', 'Unknown error')
        logger.warning("Slow log error: %s", err_msg)
        st.error(f"Error getting slow log: {err_msg}")
    else:
        logger.info("Slow log: %d entries", len(slowlog))
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        durations = [e['duration_ms'] for e in slowlog]
        with col1:
            st.metric("Total Entries", len(slowlog))
        with col2:
            st.metric("Max Duration", f"{max(durations):.2f} ms")
        with col3:
            st.metric("Avg Duration", f"{sum(durations)/len(durations):.2f} ms")
        
        # Duration chart
        df_slow = pd.DataFrame(slowlog)
        fig_slow = px.bar(
            df_slow, x='id', y='duration_ms',
            title='Slow Commands by Duration',
            hover_data=['command']
        )
        st.plotly_chart(fig_slow, width='stretch')
        
        # Table
        st.dataframe(
            df_slow[['id', 'duration_ms', 'command', 'client_address']],
            width='stretch'
        )


def render_clients_section(client):
    """Render the connected clients section."""
    st.subheader("👥 Connected Clients")
    
    if st.button("🔄 Refresh Clients", key="refresh_clients"):
        pass  # Triggers rerun
    
    clients = get_client_list(client)
    
    if clients and 'error' not in clients[0]:
        st.metric("Total Connected Clients", len(clients))
        
        # Client list table
        df_clients = pd.DataFrame(clients)
        
        # Summary by flags
        if 'flags' in df_clients.columns:
            st.subheader("Client Types")
            flag_counts = df_clients['flags'].value_counts()
            st.bar_chart(flag_counts)
        
        # Age distribution
        if 'age' in df_clients.columns:
            fig_age = px.histogram(
                df_clients, x='age',
                title='Client Connection Age (seconds)',
                nbins=20
            )
            st.plotly_chart(fig_age, width='stretch')
        
        # Full client list
        st.subheader("Client Details")
        st.dataframe(df_clients, width='stretch')
    else:
        err = clients[0].get('error', 'Unknown error') if clients else 'Could not retrieve client list'
        st.error(f"Error getting client list: {err}")


def render_commands_section(client):
    """Render the command statistics section."""
    st.subheader("⚡ Command Statistics")
    st.markdown("*Shows which commands are being used most frequently*")
    
    if st.button("🔄 Refresh Commands", key="refresh_commands"):
        pass  # Triggers rerun
    
    cmd_stats = get_command_stats(client)
    
    if 'error' in cmd_stats:
        st.error(f"Error getting command stats: {cmd_stats['error']}")
        return
    
    if not cmd_stats:
        st.info("No command statistics available")
        return
    
    # Convert to dataframe
    cmd_data = []
    for cmd, stats in cmd_stats.items():
        cmd_data.append({
            'Command': cmd.upper(),
            'Calls': stats.get('calls', 0),
            'Total Time (μs)': stats.get('usec', 0),
            'Avg Time (μs)': stats.get('usec_per_call', 0)
        })
    
    df_cmds = pd.DataFrame(cmd_data)
    df_cmds = df_cmds.sort_values('Calls', ascending=False)
    
    # Top commands by calls
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top Commands by Calls")
        fig_calls = px.bar(
            df_cmds.head(15), x='Command', y='Calls',
            title='Most Called Commands'
        )
        fig_calls.update_xaxes(tickangle=45)
        st.plotly_chart(fig_calls, width='stretch')
    
    with col2:
        st.subheader("Top Commands by Time")
        df_by_time = df_cmds.sort_values('Total Time (μs)', ascending=False)
        fig_time = px.bar(
            df_by_time.head(15), x='Command', y='Total Time (μs)',
            title='Most Time-Consuming Commands'
        )
        fig_time.update_xaxes(tickangle=45)
        st.plotly_chart(fig_time, width='stretch')
    
    # Slowest commands by average
    st.subheader("Slowest Commands (by average)")
    df_by_avg = df_cmds[df_cmds['Calls'] > 10].sort_values('Avg Time (μs)', ascending=False)
    fig_avg = px.bar(
        df_by_avg.head(15), x='Command', y='Avg Time (μs)',
        title='Slowest Commands (min 10 calls)'
    )
    fig_avg.update_xaxes(tickangle=45)
    st.plotly_chart(fig_avg, width='stretch')
    
    # Full table
    with st.expander("📋 All Command Statistics"):
        st.dataframe(df_cmds, width='stretch')


def render_utilities():
    """Render utility functions."""
    st.divider()
    
    conn = st.session_state.connection_manager
    client = conn.get_client() if st.session_state.connected else None
    
    if client:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🧹 Database Operations")
            st.warning("⚠️ These operations are destructive!")
            
            if st.button("🗑️ Flush Current Database", type="secondary"):
                if st.session_state.get('confirm_flush'):
                    logger.warning("Flush DB: executing flushdb")
                    success, message = conn.flush_db()
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
                    st.session_state.confirm_flush = False
                else:
                    st.session_state.confirm_flush = True
                    st.warning("Click again to confirm flush!")
        
        with col2:
            st.subheader("📊 Quick Stats")
            server_info = conn.get_server_info()
            if server_info:
                st.metric("Total Keys", server_info.get('total_keys', 0))
                st.metric("Memory Used", server_info.get('used_memory_human', 'Unknown'))


def main():
    """Main application entry point."""
    logger.info("App starting")
    init_session_state()
    
    # Render sidebar
    render_sidebar()
    
    # Main content
    st.title("🔴 Redis Data Population Tool")
    st.markdown("Populate your Redis Enterprise database with various data types for testing and development.")
    
    # Tabs for different data type categories
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📦 Core Types",
        "📄 JSON / Search",
        "📈 TimeSeries",
        "🌸 Bloom",
        "📊 Monitoring",
        "💾 Backups"
    ])
    
    with tab1:
        render_core_types_tab()
    
    with tab2:
        render_json_search_tab()
    
    with tab3:
        render_timeseries_tab()
    
    with tab4:
        render_bloom_tab()
    
    with tab5:
        render_monitoring_tab()
    
    with tab6:
        render_backups_tab()
    
    # Utilities section
    render_utilities()


if __name__ == "__main__":
    main()
