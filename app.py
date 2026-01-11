import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timezone

# -----------------------------
# Page Config
# -----------------------------
st.set_page_config(
    page_title="YouTube Canada",
    page_icon="▶️",
    layout="wide",
)

# -----------------------------
# Import from trending script
# -----------------------------
from trending_videos_canada import (
    get_last_scrape_time,
    should_refresh_data,
    fetch_and_save_trending,
    OUTPUT_CSV,
    REFRESH_INTERVAL_MINUTES,
)

# -----------------------------
# Country Code Mapping
# -----------------------------
COUNTRY_CODE_TO_NAME = {
    "CA": "Canada",
    "US": "United States",
    "GB": "United Kingdom",
    "AU": "Australia",
    "DE": "Germany",
    "FR": "France",
    "JP": "Japan",
    "KR": "South Korea",
    "IN": "India",
    "BR": "Brazil",
    "MX": "Mexico",
    "ES": "Spain",
    "IT": "Italy",
    "NL": "Netherlands",
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
    "FI": "Finland",
    "PL": "Poland",
    "RU": "Russia",
    "CN": "China",
    "TW": "Taiwan",
    "HK": "Hong Kong",
    "SG": "Singapore",
    "NZ": "New Zealand",
    "IE": "Ireland",
    "AT": "Austria",
    "CH": "Switzerland",
    "BE": "Belgium",
    "PT": "Portugal",
    "AR": "Argentina",
    "CL": "Chile",
    "CO": "Colombia",
    "PE": "Peru",
    "ZA": "South Africa",
    "AE": "UAE",
    "SA": "Saudi Arabia",
    "IL": "Israel",
    "PH": "Philippines",
    "TH": "Thailand",
    "MY": "Malaysia",
    "ID": "Indonesia",
    "VN": "Vietnam",
    "PK": "Pakistan",
    "BD": "Bangladesh",
    "NG": "Nigeria",
    "EG": "Egypt",
    "TR": "Turkey",
    "UA": "Ukraine",
    "CZ": "Czech Republic",
    "RO": "Romania",
    "HU": "Hungary",
    "GR": "Greece",
}

def map_country_codes(series: pd.Series) -> pd.Series:
    """Map country codes to full country names."""
    return series.replace(COUNTRY_CODE_TO_NAME)


# -----------------------------
# Shared Helpers
# -----------------------------
def format_dataframe_for_display(df: pd.DataFrame) -> tuple[pd.DataFrame, str, float, str, float]:
    """
    Format a dataframe for display with scaled views/likes and proper formatting.
    Returns: (display_df, views_format, max_views, likes_format, max_likes)
    """
    display_df = df.copy()
    
    # Convert published_at to date only
    if "video_published_at" in display_df.columns:
        display_df["video_published_at"] = pd.to_datetime(display_df["video_published_at"]).dt.date
    
    # Add video URL
    if "video_id" in display_df.columns:
        display_df["video_url"] = "https://www.youtube.com/watch?v=" + display_df["video_id"]
        display_df = display_df.drop(columns=["video_id"])
    
    # Convert pipe-separated tags to list for ListColumn display
    if "video_tags" in display_df.columns:
        display_df["video_tags"] = display_df["video_tags"].fillna("").apply(
            lambda x: x.split("|") if x else []
        )
    
    # Set index starting at 1
    display_df.index = range(1, len(display_df) + 1)
    
    # Determine formatting unit for views (M or K) and scale values
    max_views_raw = display_df["video_view_count"].max() if len(display_df) > 0 else 0
    if max_views_raw >= 1_000_000:
        display_df["video_view_count"] = display_df["video_view_count"] / 1_000_000
        views_format = "%.1fM"
    elif max_views_raw >= 1_000:
        display_df["video_view_count"] = display_df["video_view_count"] / 1_000
        views_format = "%.1fK"
    else:
        views_format = "%d"
    max_views = float(display_df["video_view_count"].max()) if len(display_df) > 0 else 1
    
    # Determine formatting unit for likes (M or K) and scale values
    max_likes_raw = display_df["video_like_count"].max() if len(display_df) > 0 else 0
    if max_likes_raw >= 1_000_000:
        display_df["video_like_count"] = display_df["video_like_count"] / 1_000_000
        likes_format = "%.1fM"
    elif max_likes_raw >= 1_000:
        display_df["video_like_count"] = display_df["video_like_count"] / 1_000
        likes_format = "%.1fK"
    else:
        likes_format = "%d"
    max_likes = float(display_df["video_like_count"].max()) if len(display_df) > 0 else 1
    
    return display_df, views_format, max_views, likes_format, max_likes


def get_column_config(views_format: str, max_views: float, likes_format: str, max_likes: float) -> dict:
    """Get the standard column configuration for video dataframes."""
    return {
        "video_title": st.column_config.TextColumn("🎥 Video Title", width="large"),
        "channel_title": st.column_config.TextColumn("📺 Channel", width="medium"),
        "channel_country": st.column_config.TextColumn("🌍 Country", width="small"),
        "video_category": st.column_config.TextColumn("📁 Category", width="small"),
        "video_duration": st.column_config.TextColumn("⏱️ Duration", width="small"),
        "video_tags": st.column_config.ListColumn("🏷️ Tags", width="small"),
        "video_view_count": st.column_config.ProgressColumn(
            "👀 Views",
            max_value=max_views,
            format=views_format,
            min_value=0,
            width="medium",
        ),
        "video_like_count": st.column_config.ProgressColumn(
            "❤️ Likes",
            min_value=0,
            format=likes_format,
            max_value=max_likes,
            width="medium",
        ),
        "video_published_at": st.column_config.DateColumn("📅 Published", width="small"),
        "video_url": st.column_config.LinkColumn("🔗 Link", width="small", display_text="Watch"),
    }


def display_video_data(
    df: pd.DataFrame,
    title_suffix: str = "",
    show_category_chart: bool = True,
    show_country_chart: bool = True,
) -> None:
    """
    Unified function to display video data with dataframe, stats, and breakdown charts.
    Used by all 3 tabs for consistent display.
    
    Args:
        df: DataFrame with video data (already filtered)
        title_suffix: Optional suffix for the caption (e.g., "for 'search query'")
        show_category_chart: Whether to show category breakdown chart
        show_country_chart: Whether to show country breakdown chart
    """
    if df.empty:
        st.info("No videos to display")
        return
    
    # Sort by view count
    df = df.sort_values("video_view_count", ascending=False).reset_index(drop=True)
    
    # Map country codes to full names for display
    df_display = df.copy()
    df_display["channel_country"] = map_country_codes(df_display["channel_country"])
    
    # Prepare display columns
    display_cols = ["video_id", "video_title", "channel_title", "channel_country", "video_category",
                    "video_view_count", "video_like_count", "video_published_at", 
                    "video_duration", "video_tags"]
    display_cols = [c for c in display_cols if c in df_display.columns]
    display_df = df_display[display_cols].copy()
    
    # Format for display
    display_df, views_format, max_views, likes_format, max_likes = format_dataframe_for_display(display_df)
    
    # Display dataframe
    st.dataframe(
        display_df,
        column_config=get_column_config(views_format, max_views, likes_format, max_likes),
    use_container_width=True,
    height=500,
    )
    
    st.caption(f"Showing {len(df)} videos{title_suffix}")
    
    # Data expander with stats and breakdowns
    with st.expander("📊 Data"):
        # Metrics row
        num_cols = 4 if "channel_country" in df.columns else 3
        cols = st.columns(num_cols)
        
        with cols[0]:
            st.metric("📊 Total Videos", len(df))
        with cols[1]:
            st.metric("📺 Unique Channels", df["channel_id"].nunique() if "channel_id" in df.columns else "N/A")
        
        col_idx = 2
        if "channel_country" in df.columns:
            with cols[col_idx]:
                st.metric("🌍 Countries", df["channel_country"].nunique())
            col_idx += 1
        
        with cols[col_idx]:
            total_views = df["video_view_count"].sum()
            if total_views >= 1_000_000:
                views_display = f"{total_views / 1_000_000:.1f}M"
            else:
                views_display = f"{total_views / 1_000:.1f}k"
            st.metric("👀 Total Views", views_display)
        
        # Breakdown charts
        chart_cols = []
        if show_country_chart and "channel_country" in df.columns:
            chart_cols.append("country")
        if show_category_chart and "video_category" in df.columns:
            chart_cols.append("category")
        
        if chart_cols:
            chart_columns = st.columns(len(chart_cols))
            
            for i, chart_type in enumerate(chart_cols):
                with chart_columns[i]:
                    if chart_type == "country":
                        st.subheader("🌍 By Country")
                        country_counts = map_country_codes(df["channel_country"]).value_counts().reset_index()
                        country_counts.columns = ["Country", "Count"]
                        country_counts["Count"] = country_counts["Count"].astype(int)
                        
                        fig = px.pie(country_counts, values="Count", names="Country", hole=0.5)
                        fig.update_traces(textposition="inside", textinfo="percent+label")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    elif chart_type == "category":
                        st.subheader("📁 By Category")
                        category_counts = df["video_category"].value_counts().reset_index()
                        category_counts.columns = ["Category", "Count"]
                        category_counts["Count"] = category_counts["Count"].astype(int)
                        
                        fig = px.pie(category_counts, values="Count", names="Category", hole=0.5)
                        fig.update_traces(textposition="inside", textinfo="percent+label")
                        st.plotly_chart(fig, use_container_width=True)


# -----------------------------
# Load Trending Data
# -----------------------------
@st.cache_data(ttl=60)  # Cache for 60 seconds to allow refresh checks
def load_trending_data():
    """Load trending data from CSV."""
    if os.path.exists(OUTPUT_CSV):
        df = pd.read_csv(OUTPUT_CSV)
        df["video_view_count"] = pd.to_numeric(df["video_view_count"], errors="coerce").fillna(0).astype(int)
        df["video_like_count"] = pd.to_numeric(df["video_like_count"], errors="coerce").fillna(0).astype(int)
        df["channel_country"] = df["channel_country"].fillna("Unknown").replace("", "Unknown")
        return df
    
    # Return empty dataframe if nothing exists
    return pd.DataFrame()


# -----------------------------
# Header & Tabs
# -----------------------------
st.title("▶️ YouTube Canada")

tab_trending, tab_by_category, tab_search = st.tabs(["🔥 Trending", "📁 By Category", "🔍 Search"])


# =============================================================================
# TAB 1: TRENDING VIDEOS
# =============================================================================
with tab_trending:
    st.caption("Explore the most popular videos trending in Canada 🇨🇦")
    
    # Check if we need to refresh data (if over 1 hour old)
    if should_refresh_data(OUTPUT_CSV, REFRESH_INTERVAL_MINUTES):
        with st.spinner("🔄 Refreshing data from YouTube..."):
            try:
                fetch_and_save_trending()
                st.cache_data.clear()  # Clear cache to load fresh data
            except Exception as e:
                st.warning(f"Could not refresh data: {e}")
    
    df = load_trending_data()
    
    if df.empty:
        st.info("📭 No data available yet. The scraper will fetch data automatically.")
        st.stop()
    
    # Filters
    filter_col1, filter_col2 = st.columns(2)
    
    with filter_col1:
        other_countries = sorted([c for c in df["channel_country"].unique().tolist() if c not in ["CA", "Unknown"]])
        countries = ["Canada"] + other_countries + (["Unknown"] if "Unknown" in df["channel_country"].values else [])
        country_display_to_value = {"Canada": "CA"}
        
        selected_countries = st.multiselect(
            "Filter by Channel Country",
            countries,
            default=[],
            placeholder="All Countries",
            key="trending_country_filter",
        )
    
    with filter_col2:
        categories = sorted([c for c in df["video_category"].dropna().unique().tolist() if c])
        
        selected_categories = st.multiselect(
            "Filter by Category",
            categories,
            default=[],
            placeholder="All Categories",
            key="trending_category_filter",
        )
    
    # Filter dataframe
    filtered_df = df.copy()
    
    if selected_countries:
        filter_values = [country_display_to_value.get(c, c) for c in selected_countries]
        filtered_df = filtered_df[filtered_df["channel_country"].isin(filter_values)]
    
    if selected_categories:
        filtered_df = filtered_df[filtered_df["video_category"].isin(selected_categories)]
    
    filtered_df = filtered_df.sort_values("video_view_count", ascending=False).reset_index(drop=True)
    
    # Display table
    display_cols = ["video_id", "video_title", "channel_title", "channel_country", "video_category", 
                    "video_view_count", "video_like_count", "video_published_at", "video_duration", "video_tags"]
    display_filtered = filtered_df[display_cols].copy()
    
    # Display "Canada" instead of "CA"
    display_filtered["channel_country"] = map_country_codes(display_filtered["channel_country"])
    
    display_filtered, views_format, max_views, likes_format, max_likes = format_dataframe_for_display(display_filtered)
    
    st.dataframe(
        display_filtered,
        column_config=get_column_config(views_format, max_views, likes_format, max_likes),
        use_container_width=True,
        height=500,
    )
    
    # Build caption with data freshness info
    last_scrape = get_last_scrape_time(OUTPUT_CSV)
    if last_scrape:
        if last_scrape.tzinfo is None:
            last_scrape = last_scrape.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        time_ago = now - last_scrape
        minutes_ago = int(time_ago.total_seconds() / 60)
        
        if minutes_ago < 1:
            time_str = "just now"
        elif minutes_ago < 60:
            time_str = f"{minutes_ago} min ago"
        else:
            hours_ago = minutes_ago // 60
            time_str = f"{hours_ago}h {minutes_ago % 60}m ago"
        
        st.caption(f"Showing {len(filtered_df)} videos • Data updated {time_str} • Auto-refreshes every {REFRESH_INTERVAL_MINUTES} min")
    else:
        st.caption(f"Showing {len(filtered_df)} videos")
    
    # Summary Stats & Breakdowns Expander
    with st.expander("📊 Data"):
        # Metrics row
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📊 Total Videos", len(filtered_df))
        with col2:
            st.metric("📺 Unique Channels", filtered_df["channel_id"].nunique())
        with col3:
            st.metric("🌍 Countries", filtered_df["channel_country"].nunique())
        with col4:
            total_views = filtered_df["video_view_count"].sum()
            if total_views >= 1_000_000:
                views_display = f"{total_views / 1_000_000:.1f}M"
            else:
                views_display = f"{total_views / 1_000:.1f}k"
            st.metric("👀 Total Views", views_display)

        # Breakdown charts
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.subheader("🌍 By Country")
            country_counts = map_country_codes(filtered_df["channel_country"]).value_counts().reset_index()
            country_counts.columns = ["Country", "Count"]
            country_counts["Count"] = country_counts["Count"].astype(int)
            
            fig_country = px.pie(country_counts, values="Count", names="Country", hole=0.5)
            fig_country.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig_country, use_container_width=True)
        
        with chart_col2:
            st.subheader("📁 By Category")
            category_counts = filtered_df["video_category"].value_counts().reset_index()
            category_counts.columns = ["Category", "Count"]
            category_counts["Count"] = category_counts["Count"].astype(int)
            
            fig_category = px.pie(category_counts, values="Count", names="Category", hole=0.5)
            fig_category.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig_category, use_container_width=True)


# =============================================================================
# TAB 2: TRENDING BY CATEGORY
# =============================================================================

# Category caching helpers
CATEGORY_CACHE_DIR = "category_cache"

def get_category_csv_path(category_id: str) -> str:
    """Get the CSV file path for a category."""
    return os.path.join(CATEGORY_CACHE_DIR, f"category_{category_id}.csv")

def load_category_from_cache(category_id: str) -> tuple[pd.DataFrame, datetime | None]:
    """Load category data from CSV cache. Returns (dataframe, scraped_at)."""
    csv_path = get_category_csv_path(category_id)
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        df["video_view_count"] = pd.to_numeric(df["video_view_count"], errors="coerce").fillna(0).astype(int)
        df["video_like_count"] = pd.to_numeric(df["video_like_count"], errors="coerce").fillna(0).astype(int)
        df["channel_country"] = df["channel_country"].fillna("Unknown").replace("", "Unknown")
        
        # Get scraped_at timestamp
        scraped_at = None
        if "scraped_at" in df.columns and len(df) > 0:
            try:
                scraped_at = pd.to_datetime(df["scraped_at"].iloc[0])
            except:
                pass
        return df, scraped_at
    return pd.DataFrame(), None

def save_category_to_cache(category_id: str, df: pd.DataFrame) -> None:
    """Save category data to CSV cache."""
    os.makedirs(CATEGORY_CACHE_DIR, exist_ok=True)
    csv_path = get_category_csv_path(category_id)
    df["scraped_at"] = datetime.now(timezone.utc).isoformat()
    df.to_csv(csv_path, index=False)

def is_category_cache_stale(scraped_at: datetime | None, max_age_minutes: int = 60) -> bool:
    """Check if category cache is stale (older than max_age_minutes)."""
    if scraped_at is None:
        return True
    if scraped_at.tzinfo is None:
        scraped_at = scraped_at.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    age_minutes = (now - scraped_at).total_seconds() / 60
    return age_minutes > max_age_minutes


with tab_by_category:
    st.caption("Explore trending videos by category in Canada 🇨🇦")
    
    # Import category functions
    from trending_videos_canada import (
        fetch_top_videos_by_category,
    )
    
    # Fetch available categories
    @st.cache_data(ttl=3600)  # Cache categories for 1 hour
    def get_available_categories():
        from trending_videos_canada import get_youtube_client, fetch_video_categories
        try:
            youtube = get_youtube_client()
            return fetch_video_categories(youtube, "CA")
        except Exception as e:
            st.error(f"Could not fetch categories: {e}")
            return {}
    
    categories_map = get_available_categories()
    
    # Categories that support trending charts in Canada
    # (Some categories like Classics, Trailers, Movies, Shows, Education don't have trending charts)
    SUPPORTED_TRENDING_CATEGORY_IDS = {
        "1",   # Film & Animation
        "2",   # Autos & Vehicles
        "10",  # Music
        "15",  # Pets & Animals
        "17",  # Sports
        "20",  # Gaming
        "22",  # People & Blogs
        "23",  # Comedy
        "24",  # Entertainment
        "25",  # News & Politics
        "26",  # Howto & Style
        "28",  # Science & Technology
    }
    
    # Filter to only supported categories
    supported_categories = {
        cid: cname for cid, cname in categories_map.items() 
        if cid in SUPPORTED_TRENDING_CATEGORY_IDS
    }
    
    if supported_categories:
        # Filters side by side
        filter_col1, filter_col2 = st.columns(2)
        
        with filter_col1:
            # Create dropdown with category names
            category_names = sorted(supported_categories.values())
            selected_category_name = st.selectbox(
                "Select a Category",
                category_names,
                index=category_names.index("Music") if "Music" in category_names else 0,
                key="category_selector",
            )
        
        # Get category ID from name
        category_id = None
        for cid, cname in supported_categories.items():
            if cname == selected_category_name:
                category_id = cid
                break
        
        if category_id:
            # Load from CSV cache
            cat_df, scraped_at = load_category_from_cache(category_id)
            
            # Check if we need to refresh (cache missing or stale)
            if cat_df.empty or is_category_cache_stale(scraped_at, REFRESH_INTERVAL_MINUTES):
                with st.spinner(f"Fetching trending {selected_category_name} videos..."):
                    try:
                        from trending_videos_canada import (
                            get_youtube_client,
                            fetch_video_categories,
                            fetch_channels_info,
                            videos_to_dataframe,
                        )
                        
                        youtube = get_youtube_client()
                        videos = fetch_top_videos_by_category(youtube, "CA", category_id, 50)
                        
                        if videos:
                            # Fetch channel info for country data
                            channel_ids = [v["snippet"]["channelId"] for v in videos if "snippet" in v]
                            channel_info = fetch_channels_info(youtube, channel_ids)
                            
                            cat_df = videos_to_dataframe(videos, categories_map, channel_info)
                            cat_df["channel_country"] = cat_df["channel_country"].fillna("Unknown").replace("", "Unknown")
                            
                            # Save to CSV cache
                            save_category_to_cache(category_id, cat_df)
                            scraped_at = datetime.now(timezone.utc)  # Update scraped_at after fresh fetch
                        else:
                            cat_df = pd.DataFrame()
                    except Exception as e:
                        st.error(f"Error fetching videos: {e}")
                        # Keep using stale cache if available
                        if cat_df.empty:
                            cat_df = pd.DataFrame()
            
            if not cat_df.empty:
                # Sort by view count
                cat_df = cat_df.sort_values("video_view_count", ascending=False).reset_index(drop=True)
                
                # Country filter (in column 2)
                cat_df_display = cat_df.copy()
                cat_df_display["channel_country"] = map_country_codes(cat_df_display["channel_country"])
                
                with filter_col2:
                    other_countries = sorted([c for c in cat_df_display["channel_country"].unique().tolist() if c not in ["Canada", "Unknown"]])
                    countries = ["Canada"] + other_countries + (["Unknown"] if "Unknown" in cat_df_display["channel_country"].values else [])
                    
                    selected_countries = st.multiselect(
                        "Filter by Channel Country",
                        options=countries,
                        default=[],
                        placeholder="All Countries",
                        key="category_country_filter",
                    )
                
                # Apply country filter
                if selected_countries:
                    cat_filtered = cat_df_display[cat_df_display["channel_country"].isin(selected_countries)]
                else:
                    cat_filtered = cat_df_display
                
                # Prepare display dataframe - include category column for verification
                display_cols = ["video_id", "video_title", "channel_title", "channel_country", "video_category",
                                "video_view_count", "video_like_count", "video_published_at", 
                                "video_duration", "video_tags"]
                cat_display = cat_filtered[display_cols].copy()
                
                cat_display, views_format, max_views, likes_format, max_likes = format_dataframe_for_display(cat_display)
                
                st.dataframe(
                    cat_display,
                    column_config=get_column_config(views_format, max_views, likes_format, max_likes),
                    use_container_width=True,
                    height=500,
                )
                
                # Build caption with data freshness info
                if scraped_at:
                    if scraped_at.tzinfo is None:
                        scraped_at = scraped_at.replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    time_ago = now - scraped_at
                    minutes_ago = int(time_ago.total_seconds() / 60)
                    
                    if minutes_ago < 1:
                        time_str = "just now"
                    elif minutes_ago < 60:
                        time_str = f"{minutes_ago} min ago"
                    else:
                        hours_ago = minutes_ago // 60
                        time_str = f"{hours_ago}h {minutes_ago % 60}m ago"
                    
                    st.caption(f"Showing {len(cat_filtered)} {selected_category_name} videos • Data updated {time_str} • Auto-refreshes every {REFRESH_INTERVAL_MINUTES} min")
                else:
                    st.caption(f"Showing {len(cat_filtered)} {selected_category_name} videos")
                
                # Summary Stats & Breakdowns
                with st.expander("📊 Data"):
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("📊 Total Videos", len(cat_filtered))
                    with col2:
                        st.metric("📺 Unique Channels", cat_filtered["channel_id"].nunique())
                    with col3:
                        st.metric("🌍 Countries", cat_filtered["channel_country"].nunique())
                    with col4:
                        total_views = cat_filtered["video_view_count"].sum()
                        if total_views >= 1_000_000:
                            views_display = f"{total_views / 1_000_000:.1f}M"
                        else:
                            views_display = f"{total_views / 1_000:.1f}k"
                        st.metric("👀 Total Views", views_display)
                    
                    # Breakdown charts
                    chart_col1, chart_col2 = st.columns(2)
                    
                    with chart_col1:
                        st.subheader("🌍 By Country")
                        country_counts = cat_filtered["channel_country"].value_counts().reset_index()
                        country_counts.columns = ["Country", "Count"]
                        country_counts["Count"] = country_counts["Count"].astype(int)
                        
                        fig_country = px.pie(country_counts, values="Count", names="Country", hole=0.5)
                        fig_country.update_traces(textposition="inside", textinfo="percent+label")
                        st.plotly_chart(fig_country, use_container_width=True)
                    
                    with chart_col2:
                        st.subheader("📁 By Category")
                        category_counts = cat_filtered["video_category"].value_counts().reset_index()
                        category_counts.columns = ["Category", "Count"]
                        category_counts["Count"] = category_counts["Count"].astype(int)
                        
                        fig_category = px.pie(category_counts, values="Count", names="Category", hole=0.5)
                        fig_category.update_traces(textposition="inside", textinfo="percent+label")
                        st.plotly_chart(fig_category, use_container_width=True)
            else:
                st.info(f"No videos found for category: {selected_category_name}")
    else:
        st.warning("Could not load categories. Check your YouTube API key.")


# =============================================================================
# TAB 3: SEARCH VIDEOS
# =============================================================================
with tab_search:
    st.caption("Search for videos on YouTube and see results including country of origin")
    
    # Search input
    search_query = st.text_input(
        "🔍 Search YouTube",
        placeholder="Enter search terms...",
        key="search_query",
    )
    
    if search_query:
        # Use session state to cache search results
        cache_key = f"search_results_{search_query}"
        
        if cache_key not in st.session_state:
            with st.spinner("Searching YouTube..."):
                try:
                    from trending_videos_canada import (
                        get_youtube_client,
                        search_videos,
                        videos_to_dataframe,
                        fetch_video_categories,
                        fetch_channels_info,
                    )
                    
                    youtube = get_youtube_client()
                    categories = fetch_video_categories(youtube, "CA")
                    videos = search_videos(youtube, search_query, "CA", 200)  # Fetch up to 200 results
                    
                    if videos:
                        # Fetch channel info for country data
                        channel_ids = [v["snippet"]["channelId"] for v in videos if "snippet" in v]
                        channel_info = fetch_channels_info(youtube, channel_ids)
                        
                        search_df = videos_to_dataframe(videos, categories, channel_info)
                        search_df["channel_country"] = map_country_codes(search_df["channel_country"].fillna("Unknown").replace("", "Unknown"))
                        st.session_state[cache_key] = search_df
                    else:
                        st.session_state[cache_key] = pd.DataFrame()
                        
                except Exception as e:
                    st.error(f"Error searching: {str(e)}")
                    st.info("Make sure your YOUTUBE_API_KEY is set in the .env file")
                    st.session_state[cache_key] = pd.DataFrame()
        
        search_df = st.session_state.get(cache_key, pd.DataFrame())
        
        if not search_df.empty:
            # Filters
            search_filter_col1, search_filter_col2 = st.columns(2)
            
            with search_filter_col1:
                search_countries = sorted([c for c in search_df["channel_country"].unique().tolist() if c and c != "Unknown"])
                search_countries = search_countries + (["Unknown"] if "Unknown" in search_df["channel_country"].values else [])
                
                search_selected_countries = st.multiselect(
                    "Filter by Channel Country",
                    search_countries,
                    default=[],
                    placeholder="All Countries",
                    key="search_country_filter",
                )
            
            with search_filter_col2:
                search_categories = sorted([c for c in search_df["video_category"].dropna().unique().tolist() if c])
                
                search_selected_categories = st.multiselect(
                    "Filter by Category",
                    search_categories,
                    default=[],
                    placeholder="All Categories",
                    key="search_category_filter",
                )
            
            # Filter dataframe
            search_filtered_df = search_df.copy()
            
            if search_selected_countries:
                search_filtered_df = search_filtered_df[search_filtered_df["channel_country"].isin(search_selected_countries)]
            
            if search_selected_categories:
                search_filtered_df = search_filtered_df[search_filtered_df["video_category"].isin(search_selected_categories)]
            
            # Sort by view count descending
            search_filtered_df = search_filtered_df.sort_values("video_view_count", ascending=False).reset_index(drop=True)
            
            st.caption(f"Showing {len(search_filtered_df)} videos for '{search_query}'")
            
            # Prepare display dataframe
            display_cols = ["video_id", "video_title", "channel_title", "channel_country", "video_category",
                            "video_view_count", "video_like_count", "video_published_at", 
                            "video_duration", "video_tags"]
            search_display = search_filtered_df[display_cols].copy()
            
            search_display, views_format, max_views, likes_format, max_likes = format_dataframe_for_display(search_display)
            
            st.dataframe(
                search_display,
                column_config=get_column_config(views_format, max_views, likes_format, max_likes),
                use_container_width=True,
                height=500,
            )
            
            # Summary Stats & Breakdowns Expander
            with st.expander("📊 Data"):
                # Metrics row
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("📊 Total Videos", len(search_filtered_df))
                with col2:
                    st.metric("📺 Unique Channels", search_filtered_df["channel_id"].nunique())
                with col3:
                    st.metric("🌍 Countries", search_filtered_df["channel_country"].nunique())
                with col4:
                    total_views = search_filtered_df["video_view_count"].sum()
                    if total_views >= 1_000_000:
                        views_display = f"{total_views / 1_000_000:.1f}M"
                    else:
                        views_display = f"{total_views / 1_000:.1f}k"
                    st.metric("👀 Total Views", views_display)
                
                # Breakdown charts
                chart_col1, chart_col2 = st.columns(2)
                
                with chart_col1:
                    st.subheader("🌍 By Country")
                    country_counts = search_filtered_df["channel_country"].value_counts().reset_index()
                    country_counts.columns = ["Country", "Count"]
                    country_counts["Count"] = country_counts["Count"].astype(int)
                    
                    fig_country = px.pie(country_counts, values="Count", names="Country", hole=0.5)
                    fig_country.update_traces(textposition="inside", textinfo="percent+label")
                    st.plotly_chart(fig_country, use_container_width=True)
                
                with chart_col2:
                    st.subheader("📁 By Category")
                    category_counts = search_filtered_df["video_category"].value_counts().reset_index()
                    category_counts.columns = ["Category", "Count"]
                    category_counts["Count"] = category_counts["Count"].astype(int)
                    
                    fig_category = px.pie(category_counts, values="Count", names="Category", hole=0.5)
                    fig_category.update_traces(textposition="inside", textinfo="percent+label")
                    st.plotly_chart(fig_category, use_container_width=True)
        else:
            st.info(f"No videos found for '{search_query}'")
    else:
        st.info("Enter a search query above to find videos")


# -----------------------------
# Footer
# -----------------------------
st.caption("Made in Canada with ❤️ by [Brydon Parker](https://www.linkedin.com/in/brydon-parker/)")
