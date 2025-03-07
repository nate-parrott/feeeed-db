import json
from typing import TypedDict, Optional, List, Set
from datetime import datetime
import os

class FrequencyData(TypedDict):
    fetched_date: datetime
    posts_per_day: float # May be 0
    last_post: Optional[datetime]

class Feed(TypedDict):
    id: str
    
    summary: Optional[str]
    title: str
    kind: str # feed, reddit, youtube, bluesky
    popularity_score: Optional[float]
    details: Optional[str] # E.g. contextual data from sources
    sources: Optional[List[str]] # 'original_feed_db' or 'crowdsourced_popular' or 'ooh_directory'

    # Derived by labelling.py:
    tags: Optional[List[str]]
    language: Optional[str]  # ISO 639-1 language code like 'en', 'fr'
    cleaned_title: Optional[str]
    cleaned_author: Optional[str]
    keywords: Optional[List[str]]

    # For feed
    feed_url: Optional[str]
    thumbnail_url: Optional[str]

    # For reddit
    subreddit: Optional[str]

    # For youtube
    channel_id: Optional[str]

    # For bluesky
    bluesky_did: Optional[str]

    last_frequency_data: Optional[FrequencyData]    


def assign_proper_id(feed: Feed):
    if 'feed_url' in feed:
        feed['id'] = 'feed:' + feed['feed_url']
    elif 'channel_id' in feed:
        feed['id'] = 'youtube:channel:' + feed['channel_id']
    elif 'subreddit' in feed:
        feed['id'] = 'reddit:' + feed['subreddit']
    elif 'bluesky_did' in feed:
        feed['id'] = 'bluesky:' + feed['bluesky_did']
    else:
        # Throw error
        assert False, "Expected this feed to have SOME type"

