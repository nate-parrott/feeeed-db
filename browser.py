"""
Feed Browser - A Flask application to browse the pipeline output.
Loads feed data from pipeline.jsonl and displays it with category navigation.
Includes an overlay system for editing feeds without modifying the original data.
"""
import json
import os
import threading
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict
from flask import Flask, render_template, request, redirect, url_for, abort, jsonify

app = Flask(__name__, 
            template_folder=os.path.join(os.path.dirname(__file__), 'browser_templates'),
            static_folder=os.path.join(os.path.dirname(__file__), 'browser_static'))

# Global variables to store data
feeds_by_id = {}  # Raw feeds from pipeline.jsonl
processed_feeds_by_id = {}  # Feeds after overlay application
feeds_by_tag = defaultdict(list)
feeds_by_language = defaultdict(list)
categories = {}
overlay_data = {}
overlay_lock = threading.Lock()  # Thread safety for writing to overlay

def load_overlay_data():
    """Load overlay data from overlay.jsonl"""
    global overlay_data
    overlay_path = Path(os.path.dirname(__file__)) / 'raw_data' / 'overlay.jsonl'
    
    if overlay_path.exists():
        overlay_data = {}
        with open(overlay_path, 'r') as f:
            for line in f:
                if line.strip() and not line.strip().startswith("%%"):  # Skip comments
                    try:
                        data = json.loads(line.strip())
                        feed_id = data['feed_id']
                        overlay_data[feed_id] = data
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"Warning: Error parsing overlay data: {e}")
        
        print(f"Loaded {len(overlay_data)} feed overlays")
    else:
        print(f"No overlay file found at {overlay_path}, creating empty overlay")
        overlay_data = {}
        # Create the overlay file
        with open(overlay_path, 'w') as f:
            f.write("%% Feed modification overlay file - DO NOT EDIT MANUALLY %%\n")

def save_overlay_data():
    """Save overlay data to overlay.jsonl"""
    overlay_path = Path(os.path.dirname(__file__)) / 'raw_data' / 'overlay.jsonl'
    
    # Use a separate try/finally block to ensure the lock is always released
    try:
        overlay_lock.acquire()
        with open(overlay_path, 'w') as f:
            f.write("%% Feed modification overlay file - DO NOT EDIT MANUALLY %%\n")
            for feed_id, data in overlay_data.items():
                f.write(json.dumps(data) + '\n')
        
        print(f"Saved {len(overlay_data)} feed overlays")
    finally:
        overlay_lock.release()

def apply_overlay_to_feed(feed, feed_id):
    """Apply overlay edits to a feed"""
    if feed_id in overlay_data:
        overlay = overlay_data[feed_id]
        
        # Apply hidden status (grayed out, not actually hidden)
        hidden_status = overlay.get('hidden', False)
        feed['_hidden'] = hidden_status
        print(f"Applying hidden status to {feed_id}: {hidden_status}")
        
        # Apply high quality status
        high_quality = overlay.get('high_quality', False)
        if 'tags' not in feed:
            feed['tags'] = []
            
        # Check for high quality tag
        has_high_quality_tag = '_high_quality' in feed['tags']
        
        # Add or remove high quality tag as needed
        if high_quality and not has_high_quality_tag:
            feed['tags'].append('_high_quality')
            print(f"Adding high quality tag to {feed_id}")
            
            # Apply score bump for high quality
            if 'computed_score' in feed:
                feed['computed_score'] += 3
        elif not high_quality and has_high_quality_tag:
            feed['tags'].remove('_high_quality')
            print(f"Removing high quality tag from {feed_id}")
            
        # Apply custom tags
        if overlay.get('tags'):
            for tag in overlay.get('tags', []):
                if tag not in feed['tags']:
                    feed['tags'].append(tag)
                    
        # Check for removed tags
        if overlay.get('removed_tags'):
            for tag in overlay.get('removed_tags', []):
                if tag in feed['tags']:
                    feed['tags'].remove(tag)
    
    return feed

def load_data():
    """Load feed data from pipeline.jsonl and categories from categories.json"""
    global feeds_by_id, feeds_by_tag, feeds_by_language, categories
    
    # Load overlay data first
    load_overlay_data()
    
    # Load categories
    categories_path = Path(os.path.dirname(__file__)) / 'categories.json'
    if categories_path.exists():
        with open(categories_path, 'r') as f:
            categories = json.load(f)
    else:
        print(f"Warning: Categories file not found at {categories_path}")
        categories = {"tags": {"top_level": [], "detailed": [], }}
    
    # Load feeds from pipeline.jsonl
    feeds_by_id = {}
    feeds_path = Path(os.path.dirname(__file__)) / 'generated' / 'pipeline.jsonl'
    if feeds_path.exists():
        with open(feeds_path, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    feed_id = data['id']
                    feed = data['feed']
                    feeds_by_id[feed_id] = feed
                    
                    # Organize feeds by tag
                    if 'tags' in feed and feed['tags']:
                        for tag in feed['tags']:
                            feeds_by_tag[tag].append(feed_id)
                    
                    # Organize feeds by language
                    if 'language' in feed and feed['language']:
                        feeds_by_language[feed['language']].append(feed_id)
                except json.JSONDecodeError:
                    print(f"Warning: Invalid JSON in line: {line}")
                except KeyError as e:
                    print(f"Warning: Missing key in feed data: {e}")
        
        print(f"Loaded {len(feeds_by_id)} feeds from {feeds_path}")
    else:
        print(f"Warning: Feeds file not found at {feeds_path}")

@app.route('/')
def home():
    """Home page - shows stats and top-level categories"""
    top_level_tags = categories['tags']['top_level']
    
    # Count non-hidden feeds for each tag
    tag_counts = {}
    for tag in top_level_tags:
        feed_ids = feeds_by_tag[tag]
        # Count how many feeds are not hidden via overlay
        visible_count = 0
        for feed_id in feed_ids:
            if feed_id in overlay_data and overlay_data[feed_id].get('hidden', False):
                continue
            visible_count += 1
        tag_counts[tag] = visible_count
    
    # Get popular languages (those with at least 5 feeds)
    popular_languages = {lang: len(feeds) for lang, feeds in feeds_by_language.items() if len(feeds) >= 5}
    # Sort by number of feeds in descending order
    popular_languages = dict(sorted(popular_languages.items(), key=lambda x: x[1], reverse=True))
    
    # Count total visible feeds
    total_visible_feeds = sum(1 for feed_id in feeds_by_id 
                             if feed_id not in overlay_data or 
                             not overlay_data[feed_id].get('hidden', False))
    
    return render_template('home.html', 
                           categories=categories,
                           tag_counts=tag_counts,
                           total_feeds=total_visible_feeds,
                           popular_languages=popular_languages)

@app.route('/category/<tag>')
def category(tag):
    """Show feeds for a specific category/tag"""
    if tag not in feeds_by_tag:
        abort(404)
        
    feed_ids = feeds_by_tag[tag]
    feeds = []
    
    # Apply overlay to each feed
    for feed_id in feed_ids:
        feed = dict(feeds_by_id[feed_id])  # Create a copy
        feed['id'] = feed_id  # Ensure the ID is in the copy
        feed = apply_overlay_to_feed(feed, feed_id)
        feeds.append(feed)
    
    # Get popular languages for sidebar
    popular_languages = {lang: len(feeds) for lang, feeds in feeds_by_language.items() if len(feeds) >= 5}
    popular_languages = dict(sorted(popular_languages.items(), key=lambda x: x[1], reverse=True))
    
    # Rank feeds based on popularity score and tags
    for feed in feeds:
        score = feed.get('popularity_score', 0) or 0
        
        # Add bonus points for high quality feeds
        if '_high_quality' in feed.get('tags', []):
            score += 3
            
        # Subtract points for low quality feeds
        if '_clickbait' in feed.get('tags', []) or '_spammy' in feed.get('tags', []):
            score -= 5
            
        # Add bonus points for curated sources
        if 'curated' in feed.get('sources', []):
            score += 5
            
        feed['computed_score'] = score
    
    # Sort feeds by computed score in descending order
    feeds.sort(key=lambda x: x.get('computed_score', 0), reverse=True)
    
    return render_template('category.html',
                           categories=categories,
                           tag=tag,
                           feeds=feeds,
                           feed_count=len(feeds),
                           popular_languages=popular_languages)

@app.route('/feed')
def feed():
    """Show details for a specific feed"""
    feed_id = request.args.get('id')
    if not feed_id or feed_id not in feeds_by_id:
        abort(404)
        
    # Get a copy of the feed to modify
    feed = dict(feeds_by_id[feed_id])
    
    # Apply any overlay edits
    feed = apply_overlay_to_feed(feed, feed_id)
    
    # Calculate score with overlay effects
    score = feed.get('popularity_score', 0) or 0
    
    # Add bonus points for high quality feeds
    if '_high_quality' in feed.get('tags', []):
        score += 3
        
    # Subtract points for low quality feeds
    if '_clickbait' in feed.get('tags', []) or '_spammy' in feed.get('tags', []):
        score -= 5
        
    # Add bonus points for curated sources
    if 'curated' in feed.get('sources', []):
        score += 5
        
    feed['computed_score'] = score
    
    # Get popular languages for sidebar
    popular_languages = {lang: len(feeds) for lang, feeds in feeds_by_language.items() if len(feeds) >= 5}
    popular_languages = dict(sorted(popular_languages.items(), key=lambda x: x[1], reverse=True))
    
    # Get all valid tags for adding
    all_valid_tags = categories['tags']['top_level'] + categories['tags']['detailed']
    
    return render_template('feed.html',
                           categories=categories,
                           feed=feed,
                           feed_id=feed_id,
                           all_valid_tags=all_valid_tags,
                           popular_languages=popular_languages,
                           is_hidden=feed.get('_hidden', False),
                           is_high_quality='_high_quality' in feed.get('tags', []))

@app.route('/search')
def search():
    """Search for feeds by title, author, summary, or tags"""
    query = request.args.get('q', '').strip().lower()
    
    if not query:
        return redirect(url_for('home'))
    
    matching_feeds = []
    
    # Search through all feeds
    for feed_id, feed in feeds_by_id.items():
        # Apply overlay to get the most up-to-date version
        feed_copy = dict(feed)
        feed_copy['id'] = feed_id
        feed_with_overlay = apply_overlay_to_feed(feed_copy, feed_id)
        
        # Skip hidden feeds
        if feed_with_overlay.get('_hidden', False):
            continue
            
        # Search in various fields
        title = (feed_with_overlay.get('cleaned_title') or feed_with_overlay.get('title', '')).lower()
        author = (feed_with_overlay.get('cleaned_author') or '').lower()
        summary = (feed_with_overlay.get('summary') or '').lower()
        tags = ' '.join(feed_with_overlay.get('tags', [])).lower()
        
        # If query is found in any field, add to results
        if (query in title or query in author or 
            query in summary or query in tags):
            
            # Calculate score for ranking
            score = feed_with_overlay.get('popularity_score', 0) or 0
            
            # Add bonus points for high quality feeds
            if '_high_quality' in feed_with_overlay.get('tags', []):
                score += 3
                
            # Add bonus for matching in title (most relevant)
            if query in title:
                score += 5
                
            # Add bonus for matching in tags (also very relevant)
            if query in tags:
                score += 3
                
            feed_with_overlay['computed_score'] = score
            matching_feeds.append(feed_with_overlay)
    
    # Sort by computed score in descending order
    matching_feeds.sort(key=lambda x: x.get('computed_score', 0), reverse=True)
    
    # Get popular languages for sidebar
    popular_languages = {lang: len(feeds) for lang, feeds in feeds_by_language.items() if len(feeds) >= 5}
    popular_languages = dict(sorted(popular_languages.items(), key=lambda x: x[1], reverse=True))
    
    return render_template('category.html',
                           categories=categories,
                           tag=f"Search: {query}",
                           feeds=matching_feeds,
                           feed_count=len(matching_feeds),
                           query=query,
                           popular_languages=popular_languages)

@app.route('/language/<lang_code>')
def language(lang_code):
    """Show feeds for a specific language"""
    if lang_code not in feeds_by_language:
        abort(404)
        
    feed_ids = feeds_by_language[lang_code]
    feeds = []
    
    # Apply overlay to each feed
    for feed_id in feed_ids:
        feed = dict(feeds_by_id[feed_id])  # Create a copy
        feed['id'] = feed_id  # Ensure the ID is in the copy
        feed = apply_overlay_to_feed(feed, feed_id)
        feeds.append(feed)
    
    # Rank feeds based on popularity score and tags
    for feed in feeds:
        score = feed.get('popularity_score', 0) or 0
        
        # Add bonus points for high quality feeds
        if '_high_quality' in feed.get('tags', []):
            score += 3
            
        # Subtract points for low quality feeds
        if '_clickbait' in feed.get('tags', []) or '_spammy' in feed.get('tags', []):
            score -= 5
            
        # Add bonus points for curated sources
        if 'curated' in feed.get('sources', []):
            score += 5
            
        feed['computed_score'] = score
    
    # Sort feeds by computed score in descending order
    feeds.sort(key=lambda x: x.get('computed_score', 0), reverse=True)
    
    # Get popular languages for sidebar
    popular_languages = {lang: len(feeds) for lang, feeds in feeds_by_language.items() if len(feeds) >= 5}
    popular_languages = dict(sorted(popular_languages.items(), key=lambda x: x[1], reverse=True))
    
    return render_template('category.html',
                           categories=categories,
                           tag=f"Language: {lang_code}",
                           feeds=feeds,
                           feed_count=len(feeds),
                           popular_languages=popular_languages)

@app.context_processor
def utility_processor():
    """Utility functions for templates"""
    def feed_url(feed):
        """Generate a proper URL for a feed based on its type"""
        if feed.get('kind') == 'feed' and feed.get('feed_url'):
            return feed['feed_url']
        elif feed.get('kind') == 'youtube' and feed.get('channel_id'):
            return f"https://www.youtube.com/channel/{feed['channel_id']}"
        elif feed.get('kind') == 'reddit' and feed.get('subreddit'):
            return f"https://www.reddit.com/r/{feed['subreddit']}"
        elif feed.get('kind') == 'bluesky' and feed.get('bluesky_did'):
            return f"https://bsky.app/profile/{feed['bluesky_did']}"
        return "#"
    
    return dict(feed_url=feed_url)

# API routes for feed editing
@app.route('/feed/actions', methods=['POST'])
def feed_actions():
    """Handle all feed actions from forms"""
    feed_id = request.form.get('feed_id')
    action = request.form.get('action')
    
    if not feed_id or feed_id not in feeds_by_id:
        abort(404)
        
    if not action:
        abort(400)
    
    try:
        # Use a try/finally block to ensure lock is released
        overlay_lock.acquire()
        
        # Create or update the overlay for this feed if it doesn't exist
        if feed_id not in overlay_data:
            overlay_data[feed_id] = {'feed_id': feed_id}
            
        if action == 'toggle_hidden':
            # Toggle hidden status
            current_hidden = overlay_data[feed_id].get('hidden', False)
            overlay_data[feed_id]['hidden'] = not current_hidden
            print(f"Toggled feed {feed_id} hidden status: {current_hidden} -> {not current_hidden}")
            
            # Update in-memory data
            feeds_by_id[feed_id]['_hidden'] = overlay_data[feed_id]['hidden']
            
        elif action == 'toggle_high_quality':
            # Toggle high quality status
            current_quality = overlay_data[feed_id].get('high_quality', False)
            overlay_data[feed_id]['high_quality'] = not current_quality
            high_quality = overlay_data[feed_id]['high_quality']
            print(f"Toggled feed {feed_id} high quality status: {current_quality} -> {high_quality}")
            
            # Update the in-memory feed data
            if 'tags' not in feeds_by_id[feed_id]:
                feeds_by_id[feed_id]['tags'] = []
            
            if high_quality and '_high_quality' not in feeds_by_id[feed_id]['tags']:
                feeds_by_id[feed_id]['tags'].append('_high_quality')
            elif not high_quality and '_high_quality' in feeds_by_id[feed_id]['tags']:
                feeds_by_id[feed_id]['tags'].remove('_high_quality')
                
        elif action == 'add_tag':
            tag = request.form.get('tag')
            if not tag:
                abort(400)
                
            # Validate tag
            all_valid_tags = categories['tags']['top_level'] + categories['tags']['detailed']
            if tag not in all_valid_tags:
                abort(400)
                
            # Update tags in overlay
            if 'tags' not in overlay_data[feed_id]:
                overlay_data[feed_id]['tags'] = []
            
            if tag not in overlay_data[feed_id]['tags']:
                overlay_data[feed_id]['tags'].append(tag)
                print(f"Added tag {tag} to feed {feed_id}")
            
            # Update in-memory data
            if 'tags' not in feeds_by_id[feed_id]:
                feeds_by_id[feed_id]['tags'] = []
            
            if tag not in feeds_by_id[feed_id]['tags']:
                feeds_by_id[feed_id]['tags'].append(tag)
                # Also update the tags index
                feeds_by_tag[tag].append(feed_id)
                
        elif action == 'remove_tag':
            tag = request.form.get('tag')
            if not tag:
                abort(400)
                
            # Update removed tags in overlay
            if 'removed_tags' not in overlay_data[feed_id]:
                overlay_data[feed_id]['removed_tags'] = []
                
            if tag not in overlay_data[feed_id]['removed_tags']:
                overlay_data[feed_id]['removed_tags'].append(tag)
                print(f"Removed tag {tag} from feed {feed_id}")
            
            # Update in-memory data
            if 'tags' in feeds_by_id[feed_id] and tag in feeds_by_id[feed_id]['tags']:
                feeds_by_id[feed_id]['tags'].remove(tag)
                
                # Also update the tags index
                if feed_id in feeds_by_tag[tag]:
                    feeds_by_tag[tag].remove(feed_id)
        else:
            abort(400)
        
        # Save overlay directly without using save_overlay_data() to avoid another lock
        overlay_path = Path(os.path.dirname(__file__)) / 'raw_data' / 'overlay.jsonl'
        with open(overlay_path, 'w') as f:
            f.write("%% Feed modification overlay file - DO NOT EDIT MANUALLY %%\n")
            for overlay_feed_id, data in overlay_data.items():
                f.write(json.dumps(data) + '\n')
        
        print(f"Saved {len(overlay_data)} feed overlays")
        
    finally:
        # Make sure the lock is released even if there's an error
        if overlay_lock.locked():
            overlay_lock.release()
    
    # Redirect back to the feed page
    return redirect(url_for('feed', id=feed_id))

def main():
    """Run the Flask application"""
    load_data()
    app.run(debug=True, port=5000)

if __name__ == '__main__':
    main()