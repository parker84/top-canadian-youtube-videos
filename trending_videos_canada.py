import os
import csv
import logging
from itertools import islice
from typing import List, Dict, Any, Set

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
MAX_VIDEOS = 1000            # Target number of videos to fetch
PAGE_SIZE = 50               # YouTube API max per call is 50 for mostPopular
OUTPUT_CSV = "top_ca_videos.csv"

# -----------------------------
# Helpers
# -----------------------------

def get_youtube_client():
    """Create a YouTube Data API client using an API key from env."""
    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY not set in environment or .env file.")
    return build("youtube", "v3", developerKey=api_key)


def fetch_video_categories(youtube, region_code: str) -> Dict[str, str]:
    """
    Fetch video category mappings (categoryId -> category name) for a region.
    """
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
    """
    Convert ISO 8601 duration (e.g., PT4M13S) to human-readable format (e.g., 4:13).
    """
    import re
    if not duration:
        return ""
    
    # Parse ISO 8601 duration format
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
    """
    Fetch most popular (trending) videos for a region using videos.list with pagination.
    """
    all_videos: List[Dict[str, Any]] = []
    next_page_token = None
    
    # Calculate number of pages needed
    total_pages = (max_results + page_size - 1) // page_size
    
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


def fetch_channels_info(youtube, channel_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch channel metadata for a list of channel IDs, including country if available.
    YouTube lets you request up to 50 channel IDs per call.
    """
    result: Dict[str, Dict[str, Any]] = {}

    # De-duplicate and chunk into batches of 50
    unique_ids: List[str] = list(dict.fromkeys(channel_ids))  # preserve order, remove dupes
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

            # snippet.country is the channel’s associated country (may be None)
            # It is derived from brandingSettings.channel.country under the hood. :contentReference[oaicite:3]{index=3}
            country = snippet.get("country") or branding.get("country")

            result[cid] = {
                "channel_title": snippet.get("title", ""),
                "channel_country": country or "",
            }

    return result


def save_to_csv(
    videos: List[Dict[str, Any]],
    channel_info: Dict[str, Dict[str, Any]],
    categories: Dict[str, str],
    output_path: str,
) -> None:
    """
    Save video + channel data to CSV.
    """
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
            
            # Get category name from ID
            category_id = snip.get("categoryId", "")
            category_name = categories.get(category_id, "")
            
            # Parse duration to human-readable format
            duration_raw = content.get("duration", "")
            duration = parse_duration(duration_raw)
            
            # Get tags as pipe-separated string
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
            }
            writer.writerow(row)


def main():
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


if __name__ == "__main__":
    main()
