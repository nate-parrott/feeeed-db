from typing import TypedDict, Optional, List
from datetime import datetime
import feedparser
import statistics
import socket
from feed_types import Feed

class FeedItem(TypedDict):
    title: str
    description: Optional[str]
    url: Optional[str]
    date: Optional[float] # Unix timestamp

class FeedContent(TypedDict):
    title: str
    description: Optional[str]  # Feed-level description/subtitle
    items: List[FeedItem]
    last_post_date: Optional[float]
    median_post_interval: Optional[float]  # in seconds

def get_feed_url(feed: Feed) -> str:
    """Convert any feed type into its RSS/Atom/JSON feed URL"""
    if feed.get('feed_url'):
        return feed['feed_url']
    elif feed.get('channel_id'):
        return f"https://www.youtube.com/feeds/videos.xml?channel_id={feed['channel_id']}"
    elif feed.get('subreddit'):
        return f"https://www.reddit.com/r/{feed['subreddit']}.rss"
    elif feed.get('bluesky_did'):
        # https://bsky.app/profile/did:plc:oky5czdrnfjpqslsw2a5iclo/rss
        return f"https://bsky.app/profile/{feed['bluesky_did']}/rss"
    else:
        raise ValueError("Unknown feed type")

def fetch_feed_content(url: str) -> FeedContent:
    """Fetch and parse any feed type (RSS, Atom, JSON) using feedparser"""
    # Handle JSON Feed separately since feedparser doesn't support it
    if url.endswith('/json'):
        import urllib.request
        import json
        
        headers = {'User-Agent': 'Mozilla/5.0 (feed-db fetcher)'}
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read())
            
            items = []
            dates = []
            
            for item in data.get('items', []):
                date = None
                if 'date_published' in item:
                    date = datetime.strptime(item['date_published'], '%Y-%m-%dT%H:%M:%SZ')
                    dates.append(date)
                elif 'date_modified' in item:
                    date = datetime.strptime(item['date_modified'], '%Y-%m-%dT%H:%M:%SZ')
                    dates.append(date)
                
                items.append({
                    'title': item.get('title', ''),
                    'description': item.get('content_html', ''),
                    'url': item.get('url') or item.get('external_url'),
                    'date': date.timestamp() if date else None
                })
            
            intervals = []
            if len(dates) >= 2:
                sorted_dates = sorted(dates)
                intervals = [(sorted_dates[i+1] - sorted_dates[i]).total_seconds() 
                           for i in range(len(sorted_dates)-1)]
                
            return {
                'title': data.get('title', ''),
                'description': data.get('description'),
                'items': items,
                'last_post_date': max(dates).timestamp() if dates and len(dates) > 0 else None,
                'median_post_interval': statistics.median(intervals) if intervals else None
            }
    
    # For RSS/Atom feeds, use feedparser
    # Set socket timeout for feedparser
    original_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(5)  # 5 second timeout
    try:
        parsed = feedparser.parse(url, agent='Mozilla/5.0 (feed-db fetcher)')
    finally:
        # Restore original timeout
        socket.setdefaulttimeout(original_timeout)
    
    items = []
    dates = []
    
    for entry in parsed.entries:
        # Get the date (feedparser handles all date formats)
        date = None
        if hasattr(entry, 'published_parsed'):
            date = datetime(*entry.published_parsed[:6])
            dates.append(date)
        elif hasattr(entry, 'updated_parsed'):
            date = datetime(*entry.updated_parsed[:6])
            dates.append(date)
            
        # Get description (feedparser normalizes various content fields)
        description = None
        if hasattr(entry, 'description'):
            description = entry.description
        elif hasattr(entry, 'summary'):
            description = entry.summary
        elif hasattr(entry, 'content'):
            description = entry.content[0].value
            
        # Get URL
        url = entry.link if hasattr(entry, 'link') else None
            
        items.append({
            'title': entry.title if hasattr(entry, 'title') else '',
            'description': description,
            'url': url,
            'date': date.timestamp() if date else None
        })
    
    # Calculate intervals
    intervals = []
    if len(dates) >= 2:
        sorted_dates = sorted(dates)
        intervals = [(sorted_dates[i+1] - sorted_dates[i]).total_seconds() 
                    for i in range(len(sorted_dates)-1)]
    
    # Get feed description
    description = None
    if hasattr(parsed.feed, 'description'):
        description = parsed.feed.description
    elif hasattr(parsed.feed, 'subtitle'):
        description = parsed.feed.subtitle
        
    return {
        'title': parsed.feed.title if hasattr(parsed.feed, 'title') else '',
        'description': description,
        'items': items,
        'last_post_date': max(dates).timestamp() if dates and len(dates) > 0 else None,
        'median_post_interval': statistics.median(intervals) if intervals else None
    }

if __name__ == '__main__':
    test_feeds = [
        # YouTube - MKBHD
        {
            'kind': 'youtube',
            'channel_id': 'UCBJycsmduvYEL83R_U4JriQ'
        },
        # Subreddit - Technology
        {
            'kind': 'reddit',
            'subreddit': 'technology'
        },
        # RSS - Hacker News
        {
            'kind': 'feed',
            'feed_url': 'https://news.ycombinator.com/rss'
        },
        # Atom - GitHub Blog
        {
            'kind': 'feed',
            'feed_url': 'https://github.blog/feed/'
        },
        # JSON Feed - Daring Fireball
        {
            'kind': 'feed',
            'feed_url': 'https://daringfireball.net/feeds/json'
        }
    ]
    
    for test_feed in test_feeds:
        print("\n" + "="*50)
        print(f"Testing feed type: {test_feed['kind']}")
        if 'channel_id' in test_feed:
            print(f"Channel: {test_feed['channel_id']}")
        elif 'subreddit' in test_feed:
            print(f"Subreddit: r/{test_feed['subreddit']}")
        elif 'feed_url' in test_feed:
            print(f"URL: {test_feed['feed_url']}")
            
        try:
            # Convert to RSS URL
            url = get_feed_url(test_feed)
            print(f"Feed URL: {url}")
            
            # Fetch content
            content = fetch_feed_content(url)
            print(f"\nFeed Title: {content['title']}")
            print(f"Number of items: {len(content['items'])}")
            print(f"Last post date: {content['last_post_date']}")
            if content['median_post_interval']:
                print(f"Median interval between posts: {content['median_post_interval']/3600:.1f} hours")
            
            if content['items']:
                print("\nMost recent item:")
                print(f"Title: {content['items'][0]['title']}")
                print(f"URL: {content['items'][0]['url']}")
                print(f"Date: {content['items'][0]['date']}")
        except Exception as e:
            print(f"Error fetching feed: {e}")