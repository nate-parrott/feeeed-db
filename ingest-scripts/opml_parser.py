import xml.etree.ElementTree as ET
from typing import List
from urllib.parse import urlparse
import hashlib
from feed_types import Feed

def parse_opml_to_feeds(opml_content: str, source_name: str) -> List[Feed]:
    """Parse OPML content into Feed objects.
    Args:
        opml_content: Raw OPML XML content
        source_name: Name of source to include in sources list
    Returns:
        List of Feed objects
    """
    root = ET.fromstring(opml_content)
    feeds: List[Feed] = []
    
    # OPML feeds are in outline elements
    for outline in root.findall(".//outline"):
        # Only process feed items (type="rss" or has xmlUrl)
        if outline.get('type') in ['rss', 'atom'] or outline.get('xmlUrl'):
            feed_url = outline.get('xmlUrl')
            if not feed_url:
                continue
                
            title = outline.get('title') or outline.get('text', '')
            
            feed: Feed = {
                'id': f'feed:{feed_url}',
                'title': title,
                'kind': 'feed',
                'sources': {source_name},
                'feed_url': feed_url
            }
            
            if outline.get('description'):
                feed['summary'] = outline.get('description')
            
            feeds.append(feed)
            
    return feeds