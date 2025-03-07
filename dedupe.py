from typing import Dict, List, Optional, Set, Any
from feed_types import Feed

def merge(primary: Feed, secondary: Feed) -> Feed:
    """
    Merge two feeds, keeping array values from both and preferring primary for scalar values.
    Primary feed takes precedence for scalar values.
    """
    result = primary.copy()
    
    # Merge array fields
    array_fields = ['sources', 'tags']
    for field in array_fields:
        if field in primary and field in secondary:
            if primary.get(field) is None:
                result[field] = secondary.get(field)
            elif secondary.get(field) is None:
                pass  # Keep primary's value
            else:
                # Combine arrays and remove duplicates
                combined = list(set(primary.get(field, []) + secondary.get(field, [])))
                result[field] = combined
        elif field in secondary:
            result[field] = secondary.get(field)
    
    # For scalar fields, prefer primary but use secondary if primary is None or missing
    scalar_fields = ['summary', 'popularity_score', 'details', 'language', 
                    'cleaned_title', 'cleaned_author', 'thumbnail_url']
    for field in scalar_fields:
        if field not in primary or primary.get(field) is None:
            if field in secondary and secondary.get(field) is not None:
                result[field] = secondary[field]
    
    return result

def simple_dedupe(feeds: Dict[str, Feed]) -> Dict[str, Feed]:
    """
    Deduplicate feeds based on metadata (URL, subreddit, YouTube ID).
    Return a new dictionary with deduplicated feeds.
    """
    result = {}
    url_map = {}  # feed_url -> feed_id
    subreddit_map = {}  # subreddit -> feed_id
    youtube_map = {}  # channel_id -> feed_id
    bluesky_map = {}  # bluesky_did -> feed_id
    
    # First pass: identify duplicates
    for feed_id, feed in feeds.items():
        
        # Check for duplicates based on feed URL
        if feed.get('feed_url'):
            if feed['feed_url'] in url_map:
                # This is a duplicate, merge it
                primary_id = url_map[feed['feed_url']]
                primary_feed = result[primary_id]
                result[primary_id] = merge(primary_feed, feed)
                continue
            else:
                url_map[feed['feed_url']] = feed_id
        
        # Check for duplicates based on subreddit
        elif feed.get('subreddit'):
            if feed['subreddit'] in subreddit_map:
                primary_id = subreddit_map[feed['subreddit']]
                primary_feed = result[primary_id]
                result[primary_id] = merge(primary_feed, feed)
                continue
            else:
                subreddit_map[feed['subreddit']] = feed_id
        
        # Check for duplicates based on YouTube channel ID
        elif feed.get('channel_id'):
            if feed['channel_id'] in youtube_map:
                primary_id = youtube_map[feed['channel_id']]
                primary_feed = result[primary_id]
                result[primary_id] = merge(primary_feed, feed)
                continue
            else:
                youtube_map[feed['channel_id']] = feed_id
        
        # Check for duplicates based on Bluesky ID
        elif feed.get('bluesky_did'):
            if feed['bluesky_did'] in bluesky_map:
                primary_id = bluesky_map[feed['bluesky_did']]
                primary_feed = result[primary_id]
                result[primary_id] = merge(primary_feed, feed)
                continue
            else:
                bluesky_map[feed['bluesky_did']] = feed_id
        
        # No duplicates found, add to result
        result[feed_id] = feed
    
    print(f"Simple dedupe: {len(feeds)} -> {len(result)} feeds")
    return result

def _get_item_display_name(item: Dict[str, Any]) -> str:
    """Extract a display name from a feed item."""
    if item.get('title'):
        return item['title']
    elif item.get('description'):
        return item['description']
    return ""

def content_dedupe(feeds: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Deduplicate feeds based on content.
    Feeds are considered duplicates if they have the same content items.
    
    Takes a dictionary of enriched feeds and returns a deduplicated dictionary.
    """
    result = {}
    # Map of content fingerprint -> feed_id
    content_map: Dict[str, str] = {}
    
    for feed_id, enriched_feed in feeds.items():
        # Create a fingerprint of feed content
        items = enriched_feed['items']
        if not items:
            # No items, can't deduplicate based on content
            result[feed_id] = enriched_feed
            continue
        
        # Get display names for all items
        display_names = [_get_item_display_name(item) for item in items]
        # Create a fingerprint by joining all display names
        # Only use up to 5 items to avoid excessive memory usage
        fingerprint = "||".join(display_names[:5])
        
        # Check if we've seen this content before
        if fingerprint in content_map:
            primary_id = content_map[fingerprint]
            primary_feed = result[primary_id]['feed']
            # Merge the feeds, keeping the first one as primary
            result[primary_id]['feed'] = merge(primary_feed, enriched_feed['feed'])
            continue
        else:
            # New content fingerprint, add to map
            content_map[fingerprint] = feed_id
            result[feed_id] = enriched_feed
    
    print(f"Content dedupe: {len(feeds)} -> {len(result)} feeds")
    return result