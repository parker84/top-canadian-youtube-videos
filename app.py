import streamlit as st
import pandas as pd
import plotly.express as px

# -----------------------------
# Page Config
# -----------------------------
st.set_page_config(
    page_title="Top YouTube Videos in Canada",
    page_icon="▶️",
    layout="wide",
)

# -----------------------------
# Load Data
# -----------------------------
@st.cache_data
def load_data():
    df = pd.read_csv("top_ca_videos.csv")
    df["video_view_count"] = pd.to_numeric(df["video_view_count"], errors="coerce").fillna(0).astype(int)
    df["video_like_count"] = pd.to_numeric(df["video_like_count"], errors="coerce").fillna(0).astype(int)
    df["channel_country"] = df["channel_country"].fillna("Unknown").replace("", "Unknown")
    return df

df = load_data()

# -----------------------------
# Header
# -----------------------------
st.title("▶️ Top YouTube Videos in Canada")
st.caption("Explore the most popular videos trending in Canada 🇨🇦, with channel origin insights and engagement metrics.")

# -----------------------------
# Filters
# -----------------------------
filter_col1, filter_col2 = st.columns(2)

with filter_col1:
    # Build country list with Canada first, then the rest alphabetically
    other_countries = sorted([c for c in df["channel_country"].unique().tolist() if c not in ["CA", "Unknown"]])
    countries = ["Canada"] + other_countries + (["Unknown"] if "Unknown" in df["channel_country"].values else [])

    # Map display names to data values
    country_display_to_value = {"Canada": "CA"}

    selected_countries = st.multiselect(
        "Filter by Channel Country",
        countries,
        default=[],
        placeholder="All Countries",
    )

with filter_col2:
    # Build category list alphabetically
    categories = sorted([c for c in df["video_category"].dropna().unique().tolist() if c])

    selected_categories = st.multiselect(
        "Filter by Category",
        categories,
        default=[],
        placeholder="All Categories",
    )

# Filter dataframe based on selections
filtered_df = df.copy()

if selected_countries:
    # Map display names back to data values
    filter_values = [country_display_to_value.get(c, c) for c in selected_countries]
    filtered_df = filtered_df[filtered_df["channel_country"].isin(filter_values)]

if selected_categories:
    filtered_df = filtered_df[filtered_df["video_category"].isin(selected_categories)]

# Sort by view count by default
filtered_df = filtered_df.sort_values("video_view_count", ascending=False).reset_index(drop=True)

# -----------------------------
# Data Table with Progress Bars
# -----------------------------
st.caption(f"Showing {len(filtered_df)} videos, ranked by view count")

# Convert published_at to date only and add rank + URL
display_filtered = filtered_df[["video_id", "video_title", "channel_title", "channel_country", "video_category", "video_view_count", "video_like_count", "video_published_at", "video_duration", "video_tags"]].copy()
display_filtered["video_published_at"] = pd.to_datetime(display_filtered["video_published_at"]).dt.date
display_filtered["video_url"] = "https://www.youtube.com/watch?v=" + display_filtered["video_id"]
display_filtered = display_filtered.drop(columns=["video_id"])

# Convert pipe-separated tags to list for ListColumn display
display_filtered["video_tags"] = display_filtered["video_tags"].fillna("").apply(
    lambda x: x.split("|") if x else []
)

display_filtered.index = range(1, len(display_filtered) + 1)

# Determine formatting unit for views (M or K) and scale values
max_views_raw = display_filtered["video_view_count"].max()
if max_views_raw >= 1_000_000:
    display_filtered["video_view_count"] = display_filtered["video_view_count"] / 1_000_000
    views_format = "%.1fM"
elif max_views_raw >= 1_000:
    display_filtered["video_view_count"] = display_filtered["video_view_count"] / 1_000
    views_format = "%.1fK"
else:
    views_format = "%d"
max_views = float(display_filtered["video_view_count"].max())

# Determine formatting unit for likes (M or K) and scale values
max_likes_raw = display_filtered["video_like_count"].max()
if max_likes_raw >= 1_000_000:
    display_filtered["video_like_count"] = display_filtered["video_like_count"] / 1_000_000
    likes_format = "%.1fM"
elif max_likes_raw >= 1_000:
    display_filtered["video_like_count"] = display_filtered["video_like_count"] / 1_000
    likes_format = "%.1fK"
else:
    likes_format = "%d"
max_likes = float(display_filtered["video_like_count"].max())

st.dataframe(
    display_filtered,
    column_config={
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
    },
    use_container_width=True,
    height=500,
)

# -----------------------------
# Metrics Expander
# -----------------------------
with st.expander("📊 Summary Stats"):
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

# -----------------------------
# Breakdown Charts Expander
# -----------------------------
with st.expander("📊 Breakdowns"):
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🌍 By Country")
        country_counts = filtered_df["channel_country"].value_counts().reset_index()
        country_counts.columns = ["Country", "Count"]
        country_counts["Count"] = country_counts["Count"].astype(int)

        fig_country = px.pie(
            country_counts,
            values="Count",
            names="Country",
            hole=0.5,
        )
        fig_country.update_traces(
            textposition="inside",
            textinfo="percent+label",
        )
        st.plotly_chart(fig_country, use_container_width=True)
    
    with col2:
        st.subheader("📁 By Category")
        category_counts = filtered_df["video_category"].value_counts().reset_index()
        category_counts.columns = ["Category", "Count"]
        category_counts["Count"] = category_counts["Count"].astype(int)

        fig_category = px.pie(
            category_counts,
            values="Count",
            names="Category",
            hole=0.5,
        )
        fig_category.update_traces(
            textposition="inside",
            textinfo="percent+label",
        )
        st.plotly_chart(fig_category, use_container_width=True)

# -----------------------------
# Footer
# -----------------------------
st.caption("Data sourced from YouTube Data API v3 • Built with Streamlit & Plotly")

