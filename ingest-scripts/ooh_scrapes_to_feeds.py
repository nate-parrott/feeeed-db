import json
from pathlib import Path
from typing import List, Set, Dict
from opml_parser import parse_opml_to_feeds
from feed_types import Feed

def load_descriptions(json_path: Path) -> Dict[str, str]:
    """Load descriptions from a .descriptions.json file, keyed by title"""
    if not json_path.exists():
        return {}
    
    try:
        descriptions = json.loads(json_path.read_text(encoding='utf-8'))
        return {item['title']: item['description'] for item in descriptions}
    except Exception as e:
        print(f"Error loading descriptions from {json_path}: {e}")
        return {}

def get_all_opml_files(root_dir: Path) -> List[Path]:
    """Recursively find all .opml files in directory and subdirectories"""
    return list(root_dir.rglob('*.opml'))

def main():
    # Get all OPML files from the ooh_scraper directory
    ooh_dir = Path('../scraped/ooh_opmls')
    opml_files = get_all_opml_files(ooh_dir)
    
    all_feeds: List[Feed] = []
    
    # Process each OPML file
    for opml_path in opml_files:
        if opml_path.name == 'personal.opml':
            print('Skipping personal.opml')
            continue
        print(f"Reading {opml_path}")
        # Get category from parent directory name
        # category = opml_path.parent.name if opml_path.parent.name != 'opmls' else None
        category_path = ' -> '.join(opml_path.relative_to(ooh_dir).parts)
        
        try:
            # Load descriptions if available
            desc_path = opml_path.with_suffix('.descriptions.json')
            descriptions = load_descriptions(desc_path)
            
            # Read and parse OPML content
            opml_content = opml_path.read_text(encoding='utf-8')
            feeds = parse_opml_to_feeds(opml_content, 'ooh_directory')
                        
            # Add tags and try to match descriptions
            for feed in feeds:
                # Try to find matching description
                if feed['title'] in descriptions:
                    feed['summary'] = descriptions[feed['title']]
                feed['details'] = f"Original Category: {category_path}"
                
            
            # print(f"  Found {len(feeds)} feeds")
            all_feeds.extend(feeds)
        except Exception as e:
            print(f"Error processing {opml_path}: {e}")
    
    # Write all feeds to output file
    output_path = Path('../raw_data/ooh.feeds.jsonl')
    
    # Convert to JSONL format
    with output_path.open('w', encoding='utf-8') as f:
        for feed in all_feeds:
            # Convert sets to lists for JSON serialization
            feed_dict = dict(feed)
            if feed_dict.get('tags') is not None:
                feed_dict['tags'] = list(feed_dict['tags'])
            if feed_dict.get('sources') is not None:
                feed_dict['sources'] = list(feed_dict['sources'])
            # print(feed_dict)
            
            f.write(json.dumps(feed_dict) + '\n')
    
    print(f"Processed {len(opml_files)} OPML files")
    print(f"Found {len(all_feeds)} feeds")
    print(f"Output written to {output_path}")

if __name__ == '__main__':
    main()