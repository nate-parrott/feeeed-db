"""Pipeline for processing and enriching feed data."""
from typing import TypedDict, Optional, List, Dict, cast, Any
from datetime import datetime
import os
from pathlib import Path
import random
import traceback
import urllib.parse

from all_raw_feeds import get_all_raw_feeds
from feed_types import Feed
from feed_fetching import FeedItem, fetch_feed_content, get_feed_url
from cached_map import cached_map, cached_map_batched
from labelling import batch_label, FeedLabels
from chromadb import Client
from embed import build_embeddings
from dedupe import simple_dedupe, content_dedupe


class EnrichedFeed(TypedDict):
    # Original feed data
    feed: Feed
    
    # Fetched content
    items: List[FeedItem]
    feed_description: Optional[str]  # Feed-level description from feed
    
    # Metadata from fetch  
    fetch_date: datetime
    last_post_age_seconds: Optional[float]  # How old was most recent post at fetch time
    posts_per_day: Optional[float]  # Estimated posting frequency



def run_pipeline(test_mode = False, trace: Optional[str] = None) -> Dict[str, EnrichedFeed]:
    """Run the full pipeline to fetch and enrich all feeds."""
    # Get all raw feeds
    feeds = get_all_raw_feeds() # get_all_raw_feeds(limit_to=['curated'])
    print(f"{len(feeds)} feeds in raw_data")
    
    # Trace raw feeds
    output_trace(feeds, "Initial raw feeds", trace)
    
    # Validate feeds
    feeds = validate(feeds)
    output_trace(feeds, "After validation", trace)
    
    feeds = list({feed['id']: feed for feed in feeds}.values())
    print(f"{len(feeds)} unique feeds")

    if test_mode:
        random.seed(4)
        random.shuffle(feeds)
        feeds = feeds[:3]
        print(f"Testing on {len(feeds)} feeds")
    
    # Create feed_id -> feed mapping
    feed_dict = {feed['id']: feed for feed in feeds}
    output_trace(feed_dict, "Before simple deduplication", trace)
    
    # Apply simple deduplication based on metadata
    print("Applying simple deduplication based on metadata...")
    feed_dict = simple_dedupe(feed_dict)
    output_trace(feed_dict, "After simple deduplication", trace)
        
    # Process feeds with caching
    print("ENRICH FEED BY FETCHING LATEST ITEMS")
    enriched_feeds = cached_map(
        inputs=feed_dict,
        map_fn=enrich_feed,
        cache_file=Path(os.path.dirname(__file__)) / 'caches' / 'feed_fetch_cache_20250219.db',
        version='v1',
        num_threads=8  # Parallel processing
    )
    output_trace(enriched_feeds, "After enrichment", trace)

    # Filter to only include blogs that have posted in the past 2 months
    enriched_feeds = {id: feed for id, feed in enriched_feeds.items() 
                     if feed['last_post_age_seconds'] is not None and feed['last_post_age_seconds'] < 60 * 60 * 24 * 60}  # 60 days
    print(f"{len(enriched_feeds)} feeds posted in the past 60 days")
    output_trace(enriched_feeds, "After recency filtering", trace)
    
    # Apply content-based deduplication to find feeds with identical content
    print("Applying content-based deduplication...")
    enriched_feeds = content_dedupe(enriched_feeds)
    print(f"{len(enriched_feeds)} feeds after content deduplication")
    output_trace(enriched_feeds, "After content deduplication", trace)

    # Generate labels using LLM 
    enriched_feeds = _add_llm_labels(enriched_feeds, trace)

    # Build embeddings
    print("Building embeddings...")
    chroma_client = build_embeddings([f['feed'] for f in enriched_feeds.values()])

    if test_mode:
        _print_test_info(enriched_feeds, chroma_client)

    return enriched_feeds


def _print_test_info(enriched_feeds: Dict[str, EnrichedFeed], chroma_client: Client):
    """Print debug info in test mode."""
    # Print example feed info
    for f in enriched_feeds.values():
        print(f"Feed: {f['feed']['title']}")
        print(f"  Last post age: {f['last_post_age_seconds'] / (60 * 60 * 24):.1f} days")
        print(f"  Posts per day: {f['posts_per_day']:.1f}")
        print(f"  Items: {len(f['items'])} items")
        print(f"  Clean title: {f['feed'].get('cleaned_title', '')}")
        print(f"  Clean author: {f['feed'].get('cleaned_author', '')}")
        print(f"  Description: {f['feed'].get('summary', '')}")
        print(f"  Tags: {', '.join(f['feed'].get('tags', []))}")
        print(f"  Lang: {f['feed'].get('language', '')}")
        print()

    # Show similar/dissimilar feeds for a random feed
    random_feed = random.choice(list(enriched_feeds.values()))['feed']
    print(f"\nFinding similar/dissimilar feeds for: {random_feed['title']}")

    # Query Chroma for embedding distances 
    collection = chroma_client.get_collection("feeds")

    if len(enriched_feeds) > 0:
        emb = collection.get(ids=[random_feed['id']], include=['embeddings'])['embeddings']
        # print(emb)
        # print('got emb, ', type(emb))
        results = collection.query(
            query_embeddings=emb,
            n_results=len(enriched_feeds)
        )

        print("\nMost similar feeds:")
        for i in range(3):
            feed_id = results['ids'][0][i]
            distance = results['distances'][0][i]
            feed = enriched_feeds[feed_id]['feed']
            print(f"  {feed['title']} (distance: {distance:.3f})")

        print("\nLeast similar feeds:")
        for i in range(3):
            idx = -(i + 1)  # Get last 3 items
            feed_id = results['ids'][0][idx]
            distance = results['distances'][0][idx]
            feed = enriched_feeds[feed_id]['feed']
            print(f"  {feed['title']} (distance: {distance:.3f})")


def _add_llm_labels(enriched_feeds: Dict[str, EnrichedFeed], trace: Optional[str] = None) -> Dict[str, EnrichedFeed]:
    """Add language labels generated by LLM to feeds."""
    print("LABELLING FEEDS")
    labels = cast(Dict[str, FeedLabels], cached_map_batched(
        inputs=enriched_feeds,
        map_fn=batch_label,
        batch_size=8,
        cache_file=Path(os.path.dirname(__file__)) / 'caches' / 'feed_labels_cache_20250221_v1.db',
        version='v4',
        num_threads=10
    ))

    results = dict()
    deleted_names = []
    
    # Copy labels into enriched feeds
    for id, label in labels.items():
        enriched_feeds[id]['feed']['cleaned_title'] = label['clean_title']
        enriched_feeds[id]['feed']['cleaned_author'] = label.get('clean_author')
        if not enriched_feeds[id]['feed'].get('summary'):
            enriched_feeds[id]['feed']['summary'] = label['description']
        enriched_feeds[id]['feed']['tags'] = label['top_level_tags'] + \
            label['detailed_tags'] + \
            label['hidden_tags']
        enriched_feeds[id]['feed']['language'] = label['language']
        if not enriched_feeds[id]['feed'].get('keywords'):
            enriched_feeds[id]['feed']['keywords'] = label['keywords']

        allow = not label['nsfw'] and not label['spam_or_junk'] and not '_conspiratorial' in label['hidden_tags'] and not '_sensationalized' in label['hidden_tags']
        if 'curated' in enriched_feeds[id]['feed']['sources']:
            allow = True
        if allow:
            results[id] = enriched_feeds[id]
        else:
            deleted_names.append(enriched_feeds[id]['feed']['title'])
    
    print(f"Filtered {len(deleted_names)}/{len(enriched_feeds)} feeds out due to NSFW / spamminess: {deleted_names}")
    
    # Trace after labeling and filtering
    output_trace(results, "After LLM labeling and NSFW filtering", trace)
    
    return results


def enrich_feed(feed: Feed) -> EnrichedFeed:
    """Fetch and analyze a feed's content."""
    try:
        now = datetime.now().timestamp()

        print("Enriching feed: ", feed['title'])
        
        # Fetch feed content
        url = get_feed_url(feed)
        content = fetch_feed_content(url)
        
        # Calculate post frequency and age
        last_post_age = None
        posts_per_day = None
        
        if content['last_post_date']:
            last_post_age = (now - content['last_post_date'])
            
            if content['median_post_interval']:
                # Convert median interval (seconds) to posts per day
                posts_per_day = 86400 / content['median_post_interval']
        
        return {
            'feed': feed,
            'items': content['items'],
            'feed_description': content.get('description'),
            'fetch_date': now,
            'last_post_age_seconds': last_post_age,
            'posts_per_day': posts_per_day
        }
    except Exception as e:
        print(f"Error enriching feed {feed['title']}: {e}")
        return {
            'feed': feed,
            'items': [],
            'feed_description': None,
            'fetch_date': datetime.now().timestamp(),
            'last_post_age_seconds': None,
            'posts_per_day': None
        }


def retry_pipeline(times=5, test_mode=False, trace: Optional[str] = None) -> Optional[Dict[str, EnrichedFeed]]:
    """
    Attempt to run the pipeline multiple times, continuing despite errors.
    
    Args:
        times: Number of retry attempts (default: 5)
        test_mode: Whether to run in test mode (default: False)
        trace: Optional query to trace specific publications (default: None)
        
    Returns:
        Enriched feeds if any attempt succeeds, None if all attempts fail
    """
    if test_mode:
        result = run_pipeline(test_mode=test_mode, trace=trace)
        return result
    
    for attempt in range(1, times + 1):
        try:
            print(f"Pipeline attempt {attempt}/{times}")
            result = run_pipeline(test_mode=test_mode, trace=trace)
            print(f"Pipeline attempt {attempt} succeeded!")
            return result
        except Exception as e:
            print(f"Pipeline attempt {attempt} failed with error:")
            print(traceback.format_exc())
            if attempt < times:
                print(f"Retrying... ({attempt+1}/{times})")
            else:
                print(f"All {times} attempts failed. Giving up.")
    return None


def validate(feeds: List[Feed]) -> List[Feed]:
    """
    Validates feeds by checking if they have valid URLs based on their type.
    Returns only valid feeds and prints validation statistics.
    """
    valid_feeds = []
    invalid_feeds = []
    
    print(f"Validating {len(feeds)} feeds...")
    
    # Track validation failures by reason
    invalid_reasons = {
        "missing_url": 0,
        "malformed_url": 0,
        "invalid_scheme": 0,
        "contains_spaces": 0,
        "missing_required_field": 0
    }
    
    for feed in feeds:
        feed_type = feed.get('kind', '')
        
        if feed_type == 'feed':
            # Validate regular feed
            if not feed.get('feed_url'):
                invalid_reasons["missing_url"] += 1
                invalid_feeds.append(feed)
                continue
                
            # Check for spaces in URL
            if ' ' in feed['feed_url']:
                invalid_reasons["contains_spaces"] += 1
                invalid_feeds.append(feed)
                continue
                
            # Check URL format
            try:
                parsed_url = urllib.parse.urlparse(feed['feed_url'])
                if not parsed_url.scheme or not parsed_url.netloc:
                    invalid_reasons["malformed_url"] += 1
                    invalid_feeds.append(feed)
                    continue
                
                if parsed_url.scheme not in ['http', 'https']:
                    invalid_reasons["invalid_scheme"] += 1
                    invalid_feeds.append(feed)
                    continue
            except Exception:
                invalid_reasons["malformed_url"] += 1
                invalid_feeds.append(feed)
                continue
                
        elif feed_type == 'youtube':
            # Validate YouTube feed
            if not feed.get('channel_id'):
                invalid_reasons["missing_required_field"] += 1
                invalid_feeds.append(feed)
                continue
                
            # Check for spaces in channel ID
            if ' ' in feed['channel_id']:
                invalid_reasons["contains_spaces"] += 1
                invalid_feeds.append(feed)
                continue
                
        elif feed_type == 'reddit':
            # Validate Reddit feed
            if not feed.get('subreddit'):
                invalid_reasons["missing_required_field"] += 1
                invalid_feeds.append(feed)
                continue
                
            # Check for spaces in subreddit name
            if ' ' in feed['subreddit']:
                invalid_reasons["contains_spaces"] += 1
                invalid_feeds.append(feed)
                continue
                
        elif feed_type == 'bluesky':
            # Validate Bluesky feed
            if not feed.get('bluesky_did'):
                invalid_reasons["missing_required_field"] += 1
                invalid_feeds.append(feed)
                continue
                
            # Check for spaces in Bluesky DID
            if ' ' in feed['bluesky_did']:
                invalid_reasons["contains_spaces"] += 1
                invalid_feeds.append(feed)
                continue
                
        # If we reach here, the feed is valid
        valid_feeds.append(feed)
    
    # Print validation statistics
    print(f"Validation complete: {len(valid_feeds)} valid feeds, {len(invalid_feeds)} invalid feeds")
    for reason, count in invalid_reasons.items():
        if count > 0:
            print(f"  - {reason}: {count} feeds")
            
    return valid_feeds


import json

def write_feeds_to_jsonl(feeds_dict: Dict[str, EnrichedFeed], filename: str) -> None:
    """Write feeds dictionary to a JSONL file."""
    print(f"Writing {len(feeds_dict)} feeds to {filename}")
    with open(filename, 'w') as f:
        for feed_id, enriched_feed in feeds_dict.items():
            # Convert datetime objects to timestamps
            if isinstance(enriched_feed['fetch_date'], datetime):
                enriched_feed['fetch_date'] = enriched_feed['fetch_date'].timestamp()
                
            # Write each feed as a JSON line
            f.write(json.dumps({
                'id': feed_id,
                'feed': enriched_feed['feed']
            }) + '\n')
    print("Written. Now you can run browser.py to view and curate feeds, or make_tree.py to generate the final category tree.")

def output_trace(feeds, stage_name: str, trace_query: Optional[str]) -> None:
    """
    Output trace information for feeds matching the trace query.
    
    Args:
        feeds: List or Dict of feeds to search
        stage_name: Current pipeline stage name
        trace_query: Query string to match in feed titles
    """
    if not trace_query:
        return
        
    # Convert dict to list if needed
    feed_list = []
    if isinstance(feeds, dict):
        if isinstance(next(iter(feeds.values()), None), dict) and next(iter(feeds.values()), {}).get('feed', None):
            # It's a dict of EnrichedFeed
            feed_list = [ef['feed'] for ef in feeds.values()]
        else:
            # It's a dict of Feed
            feed_list = list(feeds.values())
    else:
        feed_list = feeds
    
    # Find matches
    matches = [f for f in feed_list if trace_query.lower() in f['title'].lower()]
    
    print(f"[TRACE] Stage: {stage_name} - Found {len(matches)} matching feeds")
    
    # Print details of matches
    for feed in matches:
        print(f"[TRACE]   {feed.get('kind', '')} - {feed['title']} - {feed.get('feed_url', '')}")
        if feed.get('sources'):
            print(f"[TRACE]   Sources: {', '.join(feed['sources'])}")


if __name__ == '__main__':
    test_mode = False
    trace_query = None  # Set this to a string to trace a specific publication, e.g. "New York Times"
    enriched_feeds = retry_pipeline(test_mode=test_mode, trace=trace_query)
    if enriched_feeds:
        print(f"Processed {len(enriched_feeds)} feeds")
        
        # Write to pipeline.jsonl if not in test mode
        if not test_mode:
            pipeline_jsonl = Path(os.path.dirname(__file__)) / 'generated' / 'pipeline.jsonl'
            write_feeds_to_jsonl(enriched_feeds, str(pipeline_jsonl))
    else:
        print("Pipeline failed after all retry attempts")
