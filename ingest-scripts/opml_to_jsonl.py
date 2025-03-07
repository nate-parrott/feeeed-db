"""
used for ad-hoc ingest of OPML files
"""

import argparse
import json
from pathlib import Path
from opml_parser import parse_opml_to_feeds

def main():
    parser = argparse.ArgumentParser(description='Convert OPML file to feeds.jsonl')
    parser.add_argument('opml_file', help='Path to OPML file')
    parser.add_argument('--source', default='opml_import', help='Source name for the feeds')
    parser.add_argument('--output', default='pipeline/raw_data/opml.feeds.jsonl', 
                      help='Output .feeds.jsonl path')
    
    args = parser.parse_args()
    
    # Read OPML
    with open(args.opml_file) as f:
        opml_content = f.read()
        
    # Parse to feeds
    feeds = parse_opml_to_feeds(opml_content, args.source)
    
    # Write JSONL
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        for feed in feeds:
            # Convert sets to lists for JSON serialization
            feed_dict = feed.copy()
            feed_dict['sources'] = list(feed_dict['sources'])
            feed_dict['tags'] = list(feed_dict['tags'])
            f.write(json.dumps(feed_dict) + '\n')
            
if __name__ == '__main__':
    main()