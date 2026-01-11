import os
import csv
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

from googleapiclient.discovery import build
from dotenv import load_dotenv
from tqdm import tqdm

# -----------------------------
# Logging Setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# -----------------------------
# Config
# -----------------------------
REGION_CODE = "CA"           # Canada
MAX_VIDEOS = 200             # Target number of videos to fetch
PAGE_SIZE = 50               # YouTube API max per call is 50 for mostPopular
OUTPUT_CSV = "top_ca_videos.csv"
REFRESH_INTERVAL_MINUTES = 60  # How often to refresh data

# -----------------------------
# CSV Helpers
# -----------------------------

def get_last_scrape_time(csv_path: str = OUTPUT_CSV) -> Optional[datetime]:
    """Get the scraped_at timestamp from the CSV file."""
    if not os.path.exists(csv_path):
        return None
    
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                scraped_at = row.get("scraped_at", "")
                if scraped_at:
                    return datetime.fromisoformat(scraped_at)
                break  # Only need first row
    except Exception as e:
        logger.warning(f"Could not read scraped_at from CSV: {e}")
    
    return None


def should_refresh_data(csv_path: str = OUTPUT_CSV, interval_minutes: int = REFRESH_INTERVAL_MINUTES) -> bool:
    """Check if data should be refreshed based on last scrape time."""
    last_scrape = get_last_scrape_time(csv_path)
    
    if last_scrape is None:
        logger.info("📭 No data in CSV, refresh needed")
        return True
    
    # Make last_scrape timezone-aware if it isn't
    if last_scrape.tzinfo is None:
        last_scrape = last_scrape.replace(tzinfo=timezone.utc)
    
    now = datetime.now(timezone.utc)
    time_since_scrape = now - last_scrape
    
    if time_since_scrape > timedelta(minutes=interval_minutes):
        logger.info(f"⏰ Last scrape was {time_since_scrape.total_seconds() / 60:.1f} minutes ago, refresh needed")
        return True
    
    logger.info(f"✅ Last scrape was {time_since_scrape.total_seconds() / 60:.1f} minutes ago, no refresh needed")
    return False


# -----------------------------
# YouTube API Helpers
# -----------------------------

def get_youtube_client():
    """Create a YouTube Data API client using an API key from env."""
    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY not set in environment or .env file.")
    return build("youtube", "v3", developerKey=api_key)


def fetch_video_categories(youtube, region_code: str) -> Dict[str, str]:
    """Fetch video category mappings (categoryId -> category name) for a region."""
    request = youtube.videoCategories().list(
        part="snippet",
        regionCode=region_code,
    )
    response = request.execute()
    
    categories = {}
    for item in response.get("items", []):
        cat_id = item["id"]
        cat_name = item["snippet"]["title"]
        categories[cat_id] = cat_name
    
    return categories


def parse_duration(duration: str) -> str:
    """Convert ISO 8601 duration (e.g., PT4M13S) to human-readable format (e.g., 4:13)."""
    import re
    if not duration:
        return ""
    
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
    if not match:
        return duration
    
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes}:{seconds:02d}"


def fetch_top_videos(youtube, region_code: str, max_results: int, page_size: int = 50) -> List[Dict[str, Any]]:
    """Fetch most popular (trending) videos for a region using videos.list with pagination."""
    all_videos: List[Dict[str, Any]] = []
    next_page_token = None
    
    with tqdm(total=max_results, desc="🎬 Fetching trending videos", unit="video") as pbar:
        while len(all_videos) < max_results:
            request = youtube.videos().list(
                part="id,snippet,statistics,contentDetails",
                chart="mostPopular",
                regionCode=region_code,
                maxResults=min(page_size, max_results - len(all_videos)),
                pageToken=next_page_token,
            )
            response = request.execute()
            
            items = response.get("items", [])
            if not items:
                logger.info(f"No more videos available. Fetched {len(all_videos)} total.")
                break
                
            all_videos.extend(items)
            pbar.update(len(items))
            
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                logger.info(f"Reached end of available videos. Fetched {len(all_videos)} total.")
                break
    
    return all_videos[:max_results]


def fetch_top_videos_by_category(
    youtube,
    region_code: str,
    category_id: str,
    max_results: int = 50,
) -> List[Dict[str, Any]]:
    """
    Fetch most popular (trending) videos for a specific category & region.
    Max 50 per API call without pagination.
    """
    request = youtube.videos().list(
        part="id,snippet,statistics,contentDetails",
        chart="mostPopular",
        regionCode=region_code,
        videoCategoryId=category_id,
        maxResults=min(max_results, 50),
    )
    response = request.execute()
    return response.get("items", [])


def fetch_channels_info(youtube, channel_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch channel metadata for a list of channel IDs, including country if available."""
    result: Dict[str, Dict[str, Any]] = {}

    unique_ids: List[str] = list(dict.fromkeys(channel_ids))
    batches = list(range(0, len(unique_ids), 50))

    for i in tqdm(batches, desc="📡 Fetching channel batches", unit="batch"):
        batch_ids = unique_ids[i:i+50]
        request = youtube.channels().list(
            part="snippet,brandingSettings",
            id=",".join(batch_ids),
            maxResults=50,
        )
        response = request.execute()
        for ch in response.get("items", []):
            cid = ch["id"]
            snippet = ch.get("snippet", {})
            branding = ch.get("brandingSettings", {}).get("channel", {}) or {}
            country = snippet.get("country") or branding.get("country")

            result[cid] = {
                "channel_title": snippet.get("title", ""),
                "channel_country": country or "",
            }

    return result


def search_videos(youtube, query: str, region_code: str = "CA", max_results: int = 200) -> List[Dict[str, Any]]:
    """Search for videos matching a query and return detailed video info with pagination."""
    all_video_ids = []
    next_page_token = None
    
    # Paginate through search results (max 50 per request)
    while len(all_video_ids) < max_results:
        search_request = youtube.search().list(
            part="id",
            q=query,
            type="video",
            regionCode=region_code,
            maxResults=min(50, max_results - len(all_video_ids)),
            order="relevance",
            pageToken=next_page_token,
        )
        search_response = search_request.execute()
        
        video_ids = [item["id"]["videoId"] for item in search_response.get("items", [])]
        all_video_ids.extend(video_ids)
        
        next_page_token = search_response.get("nextPageToken")
        if not next_page_token or not video_ids:
            break
    
    if not all_video_ids:
        return []
    
    # Fetch video details in batches of 50
    all_videos = []
    for i in range(0, len(all_video_ids), 50):
        batch_ids = all_video_ids[i:i+50]
        videos_request = youtube.videos().list(
            part="id,snippet,statistics,contentDetails",
            id=",".join(batch_ids),
        )
        videos_response = videos_request.execute()
        all_videos.extend(videos_response.get("items", []))
    
    return all_videos


def videos_to_dataframe(
    videos: List[Dict[str, Any]], 
    categories: Dict[str, str],
    channel_info: Optional[Dict[str, Dict[str, Any]]] = None,
):
    """Convert a list of video API responses to a pandas DataFrame."""
    import pandas as pd
    
    if channel_info is None:
        channel_info = {}
    
    rows = []
    for v in videos:
        vid = v["id"]
        snip = v.get("snippet", {})
        stats = v.get("statistics", {})
        content = v.get("contentDetails", {})
        ch_id = snip.get("channelId", "")
        
        category_id = snip.get("categoryId", "")
        category_name = categories.get(category_id, "")
        
        duration_raw = content.get("duration", "")
        duration = parse_duration(duration_raw)
        
        tags = snip.get("tags", [])
        tags_str = "|".join(tags) if tags else ""
        
        ch_meta = channel_info.get(ch_id, {})
        
        rows.append({
            "video_id": vid,
            "video_title": snip.get("title", ""),
            "video_published_at": snip.get("publishedAt", ""),
            "video_view_count": int(stats.get("viewCount", 0) or 0),
            "video_like_count": int(stats.get("likeCount", 0) or 0),
            "video_duration": duration,
            "video_category": category_name,
            "video_tags": tags_str,
            "channel_id": ch_id,
            "channel_title": ch_meta.get("channel_title", snip.get("channelTitle", "")),
            "channel_country": ch_meta.get("channel_country", ""),
        })
    
    return pd.DataFrame(rows)


def save_to_csv(
    videos: List[Dict[str, Any]],
    channel_info: Dict[str, Dict[str, Any]],
    categories: Dict[str, str],
    output_path: str,
) -> None:
    """Save video + channel data to CSV with scraped_at timestamp."""
    scraped_at = datetime.now(timezone.utc).isoformat()
    
    fieldnames = [
        "video_id",
        "video_title",
        "video_published_at",
        "video_view_count",
        "video_like_count",
        "video_duration",
        "video_category",
        "video_tags",
        "channel_id",
        "channel_title",
        "channel_country",
        "scraped_at",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for v in tqdm(videos, desc="💾 Writing video rows", unit="video"):
            vid = v["id"]
            snip = v.get("snippet", {})
            stats = v.get("statistics", {})
            content = v.get("contentDetails", {})
            ch_id = snip.get("channelId", "")

            view_count = stats.get("viewCount", "")
            like_count = stats.get("likeCount", "")
            
            category_id = snip.get("categoryId", "")
            category_name = categories.get(category_id, "")
            
            duration_raw = content.get("duration", "")
            duration = parse_duration(duration_raw)
            
            tags = snip.get("tags", [])
            tags_str = "|".join(tags) if tags else ""

            ch_meta = channel_info.get(ch_id, {})
            row = {
                "video_id": vid,
                "video_title": snip.get("title", ""),
                "video_published_at": snip.get("publishedAt", ""),
                "video_view_count": view_count,
                "video_like_count": like_count,
                "video_duration": duration,
                "video_category": category_name,
                "video_tags": tags_str,
                "channel_id": ch_id,
                "channel_title": ch_meta.get("channel_title", snip.get("channelTitle", "")),
                "channel_country": ch_meta.get("channel_country", ""),
                "scraped_at": scraped_at,
            }
            writer.writerow(row)


def fetch_and_save_trending() -> int:
    """Fetch trending videos from YouTube and save to CSV. Returns the number of videos fetched."""
    logger.info("🚀 Starting YouTube Trending Videos Fetcher for Canada")
    
    logger.info("🔑 Initializing YouTube API client...")
    youtube = get_youtube_client()
    logger.info("✅ API client ready!")

    logger.info("📂 Fetching video categories...")
    categories = fetch_video_categories(youtube, REGION_CODE)
    logger.info(f"✅ Found {len(categories)} categories!")

    logger.info(f"🎬 Fetching top {MAX_VIDEOS} trending videos in region {REGION_CODE}...")
    videos = fetch_top_videos(youtube, REGION_CODE, MAX_VIDEOS, PAGE_SIZE)
    logger.info(f"✅ Retrieved {len(videos)} videos!")

    channel_ids = [v["snippet"]["channelId"] for v in videos if "snippet" in v]
    unique_channels = len(set(channel_ids))
    logger.info(f"📺 Fetching metadata for {unique_channels} unique channels...")
    channels = fetch_channels_info(youtube, channel_ids)
    logger.info("✅ Channel info retrieved!")

    logger.info(f"💾 Saving results to {OUTPUT_CSV}...")
    save_to_csv(videos, channels, categories, OUTPUT_CSV)
    
    logger.info("🎉 All done! Your data is ready.")
    logger.info(f"📁 Output file: {OUTPUT_CSV}")
    logger.info(f"📊 Total videos: {len(videos)} | 📺 Total channels: {unique_channels}")
    
    return len(videos)


def main():
    """Run a single fetch and save operation."""
    fetch_and_save_trending()


if __name__ == "__main__":
    main()
