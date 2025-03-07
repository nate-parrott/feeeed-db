# Takes rendered output of pipeline.py (pipeline.jsonl), the 'overlay file' which contains edits made in the UI,
# and `categories.json`, and generates a tree of JSON files that can be consumed by a UI to render a final feed list.
# Rules:
# - Read pipeline and apply overlay (see browser.py)
# - Filter out all categories that do not have 2+ feeds
# - First construct treepages for each category (top level and detail) then append CategoryInfo links from child (detail) categories to their top-level owners (see categories.json)
# - root = top level categories (see categories.json)
# - Detail categories are nested beneath root categories

import json
import os
from pathlib import Path
from typing import TypedDict, Optional, List, Set, Dict
from collections import defaultdict
from feed_types import Feed

class CategoryInfo(TypedDict):
    id: str # name of category
    display_name: str # original name for display
    emoji: Optional[str]
    sfSymbol: Optional[str]
    count: int

# We will create one of these files for each category
class TreePage(TypedDict):
    id: str # name of category
    display_name: str # original name for display
    feeds: List[Feed]
    child_categories: List[CategoryInfo] # names of child categories

# We will create one of these, saved at pipeline/tree/root.json
class TreeRoot(TypedDict):
    root_categories: List[CategoryInfo] # names of root categories

def sanitize_category_name(category_name: str) -> str:
    """Convert a category name to a valid filename and ID."""
    return category_name.lower().replace(' & ', '_').replace(' ', '_')

def create_category_info(category_name: str, feed_count: int, categories_data: Dict) -> CategoryInfo:
    """Create a CategoryInfo object for a category."""
    emoji = None
    sf_symbol = None
    
    if category_name in categories_data.get('tag_to_icon', {}):
        icon_info = categories_data['tag_to_icon'][category_name]
        emoji = icon_info.get('emoji')
        sf_symbol = icon_info.get('sf_symbol')
    
    return {
        'id': sanitize_category_name(category_name),
        'display_name': category_name,
        'emoji': emoji,
        'sfSymbol': sf_symbol,
        'count': feed_count
    }


def main():
    # Load categories
    categories_path = Path(os.path.dirname(__file__)) / 'raw_data' / 'categories.json'
    with open(categories_path, 'r') as f:
        categories = json.load(f)
    print(f"Loaded categories from {categories_path}")
    
    # Load overlay data
    overlay_data = {}
    overlay_path = Path(os.path.dirname(__file__)) / 'raw_data' / 'overlay.jsonl'
    if overlay_path.exists():
        with open(overlay_path, 'r') as f:
            for line in f:
                if line.strip() and not line.strip().startswith("%%"):
                    data = json.loads(line.strip())
                    if 'feed_id' in data:
                        overlay_data[data['feed_id']] = data
        print(f"Loaded {len(overlay_data)} feed overlays")
    
    # Load feeds from pipeline.jsonl and apply overlays
    feeds_by_id = {}
    feeds_by_tag = defaultdict(list)
    feeds_path = Path(os.path.dirname(__file__)) / 'generated' / 'pipeline.jsonl'
    
    with open(feeds_path, 'r') as f:
        for line in f:
            if not line.strip():
                continue
                
            data = json.loads(line.strip())
            feed_id = data['id']
            feed = data['feed']
            
            # Apply overlay if exists
            if feed_id in overlay_data:
                overlay = overlay_data[feed_id]
                
                # Skip hidden feeds
                if overlay.get('hidden', False):
                    continue
                    
                # Apply high quality status
                if overlay.get('high_quality', False) and '_high_quality' not in feed.get('tags', []):
                    if 'tags' not in feed:
                        feed['tags'] = []
                    feed['tags'].append('_high_quality')
                
                # Add custom tags
                if overlay.get('tags'):
                    if 'tags' not in feed:
                        feed['tags'] = []
                    for tag in overlay.get('tags', []):
                        if tag not in feed['tags']:
                            feed['tags'].append(tag)
                
                # Remove tags
                if overlay.get('removed_tags') and 'tags' in feed:
                    for tag in overlay.get('removed_tags', []):
                        if tag in feed['tags']:
                            feed['tags'].remove(tag)
            
            # Store feed
            feeds_by_id[feed_id] = feed
            
            # Organize by tag
            if 'tags' in feed and feed['tags']:
                for tag in feed['tags']:
                    feeds_by_tag[tag].append(feed_id)
    
    print(f"Loaded {len(feeds_by_id)} feeds from {feeds_path}")
    
    # Create tree directory if it doesn't exist
    tree_dir = Path(os.path.dirname(__file__)) / 'generated' / 'tree'
    if not tree_dir.exists():
        tree_dir.mkdir()
    
    # Filter categories with fewer than 2 feeds and exclude hidden tags
    valid_categories = {}
    hidden_tags = set(categories['tags'].get('hidden', []))
    
    for tag, feed_ids in feeds_by_tag.items():
        if tag not in hidden_tags and len(feed_ids) >= 2:
            valid_categories[tag] = feed_ids
    
    # Create TreePages for all valid categories
    tree_pages = {}
    
    for category, feed_ids in valid_categories.items():
        category_feeds = [feeds_by_id[feed_id] for feed_id in feed_ids]
        
        tree_pages[category] = {
            'id': sanitize_category_name(category),
            'display_name': category,
            'feeds': category_feeds,
            'child_categories': []
        }
    
    # Connect child categories to parent categories
    tag_to_parent = categories.get('tag_top_level_membership', {})
    
    for detailed_category in categories['tags']['detailed']:
        if detailed_category not in valid_categories:
            continue
        
        parent_categories = tag_to_parent.get(detailed_category, [])
        
        for parent in parent_categories:
            if parent not in valid_categories:
                continue
            
            # Create CategoryInfo for child category and add to parent
            child_info = create_category_info(
                detailed_category, 
                len(valid_categories[detailed_category]),
                categories
            )
            
            tree_pages[parent]['child_categories'].append(child_info)
    
    # Create TreeRoot with top-level categories
    root_categories = []
    
    for top_level in categories['tags']['top_level']:
        if top_level in valid_categories:
            category_info = create_category_info(
                top_level, 
                len(valid_categories[top_level]),
                categories
            )
            root_categories.append(category_info)
    
    # Create and save root.json
    tree_root = {
        'root_categories': root_categories
    }
    
    with open(tree_dir / 'root.json', 'w') as f:
        json.dump(tree_root, f, indent=2)
    
    print(f"Saved root.json with {len(root_categories)} top-level categories")
    
    # Save TreePage for each category
    for category, tree_page in tree_pages.items():
        filename = f"{sanitize_category_name(category)}.json"
        with open(tree_dir / filename, 'w') as f:
            json.dump(tree_page, f, indent=2)
    
    print(f"Saved {len(tree_pages)} category tree pages")


if __name__ == "__main__":
    main()