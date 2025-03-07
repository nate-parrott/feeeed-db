"""Scrapes Substack category pages for top publications"""
import requests
from bs4 import BeautifulSoup
import logging
from typing import List, Optional
from feed_types import Feed
from concurrent.futures import ThreadPoolExecutor
import re
import json
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

# All Substack top category pages
CATEGORIES = [
    "technology", "business", "us-politics", "finance", "food", "sports",
    "art", "world-politics", "health-politics", "news", "fashionandbeauty",
    "music", "faith", "climate", "science", "literature", "fiction",
    "health", "design", "travel", "parenting", "philosophy", "comics",
    "international", "history", "humor", "education"
]

def get_rss_feed_url(pub_url: str) -> Optional[str]:
    """Get RSS feed URL from a Substack publication homepage"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
        }
        resp = requests.get(pub_url, headers=headers)
        resp.raise_for_status()
        
        # Look for RSS feed link
        soup = BeautifulSoup(resp.text, "html.parser")
        feed_link = soup.find("link", type="application/rss+xml") or \
                   soup.find("link", type="application/atom+xml")
                   
        if feed_link and feed_link.get("href"):
            # Resolve relative URLs
            return urljoin(pub_url, feed_link["href"])
            
        # Fallback: try adding /feed to URL
        feed_url = pub_url.rstrip("/") + "/feed"
        resp = requests.head(feed_url, headers=headers)
        if resp.status_code == 200:
            return feed_url
            
    except Exception as e:
        logger.error(f"Failed to get RSS feed for {pub_url}: {e}")
        
    return None

def process_publication(title: str, pub_url: str, cat: str) -> Optional[Feed]:
    """Process a single publication, returning a Feed object if successful"""
    try:
        # Strip leading numbers (e.g. "1. ", "2. " etc)
        title = re.sub(r'^\d+\.\s*', '', title)
        
        feed_url = get_rss_feed_url(pub_url)
        if not feed_url:
            return None

        return Feed(
            id=f"substack:top:{title.lower().replace(' ', '_')}",
            title=title,
            cleaned_title=None,
            summary=None,
            kind="feed",
            feed_url=feed_url,
            details=f'Original category: Substack top newsletters about {cat}',
            sources=["substack_top"],
            tags=[]
        )
    except Exception as e:
        logger.error(f"Error processing publication {title}: {e}")
        return None

def get_top_substacks(category: str = "news", limit: int = 20) -> List[Feed]:
    """
    Fetch top substack publications from a category page and return their feed info
    
    Args:
        category: Substack category (e.g. "news", "culture", etc)
        limit: Maximum number of feeds to return
        
    Returns:
        List of Feed objects with publication data
    """
    url = f"https://substack.com/top/{category}"
    feeds = []

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
        }
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return []

    # Parse HTML
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Find all publication links using the browser-verified selector
    pub_links = soup.select("a.pencraft.pc-gap-16")[:limit]
    print(f"Found {len(pub_links)} publications in {category}")
    
    # Process publications concurrently
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for link in pub_links:
            title = link.get_text(strip=True)
            pub_url = link.get("href")
            if title and pub_url:
                futures.append(
                    executor.submit(process_publication, title, pub_url, category)
                )
        
        # Collect results
        for future in futures:
            try:
                feed = future.result()
                if feed:
                    feeds.append(feed)
            except Exception as e:
                logger.error(f"Error collecting future result: {e}")
    
    return feeds

def get_all_top_substacks(limit_per_category: int = 20, cat_limit: int = 999) -> List[Feed]:
    """Get top Substacks from all categories"""
    all_feeds = []
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(get_top_substacks, category, limit_per_category)
            for category in CATEGORIES[:min(cat_limit, len(CATEGORIES))]
        ]
        
        for future in futures:
            try:
                feeds = future.result()
                all_feeds.extend(feeds)
            except Exception as e:
                logger.error(f"Error collecting category results: {e}")
    
    return all_feeds

if __name__ == "__main__":
    # Get feeds from all categories
    feeds = get_all_top_substacks(limit_per_category=5, cat_limit=999)
    print(f"\nFound {len(feeds)} total feeds across all categories")
    for feed in feeds[:10]:  # Show first 10 as example
        print(f"Found feed: {feed['title']} ({feed['feed_url']})")
    with open('../raw_data/substack.feeds.jsonl', 'w') as f:
        for feed in feeds:
            f.write(json.dumps(feed) + '\n')
    print('Wrote', len(feeds), 'feeds')
